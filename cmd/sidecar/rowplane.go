package main

// Row-plane emit path shared by handleAdvanceStream and
// handleAddQueriesStream when a request opts into rowMode on the in-process
// (NAPI) transport. See rowrecord.go for the record format and kind tags.
//
// ORDERING INVARIANT (the reason this file routes EVERYTHING through
// abiDeliver and never mixes in streamW): all of one RPC's output must pass
// through ONE ordered queue. Row records go straight to abiDeliver (the
// addon's TSFN); if any partial ALSO went through streamW it would take the
// slower flushCh→pipe→pump path and could arrive after later row records —
// reordering the stream. So in row mode:
//
//   - row-encodable changes  → abiDeliver kind 2/3 (records)
//   - fallback changes       → abiDeliver kind 1 (msgpack partial frame,
//     same RPCResponse shape the pump delivers — the TS client's frame
//     dispatch cannot tell the difference)
//   - terminal partial       → abiDeliver kind 1
//   - the "done" sentinel    → the ordinary respCh→flushCh→pipe path, which
//     is safe because it is enqueued only after the handler returns, i.e.
//     after every abiDeliver above (TSFN FIFO puts it last).
//
// Fallback contract (ALL-OR-NOTHING per partial — REVIEW-napi-transport
// B2): if ANY change in a partial can't be row-encoded (non-homogeneous
// column set — see encodeRow), the ENTIRE partial ships inside one
// positional msgpack partial and ZERO of its rows go out as records. Mixing
// planes within a partial would reorder its changes — encodable rows left
// immediately as records while fallback rows waited for the trailing frame,
// so [add X (fallback), remove X (record)] arrived at the client as
// remove-then-add: net phantom row → drift. chunkSize=1 partials mostly
// dodge this, and remove-first groups regained the record path via
// groupFor's replacement-def minting (user's-audit fix), but the
// residual-drain path still produces multi-change partials, so the
// interleave stays reachable. Correct over fast; the TS row-mode
// accumulator accepts both planes.

import (
	"fmt"
	"os"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/engine"
)

// rowPlane wraps a rowRecordEncoder with the delivery callback and a mutex
// (hydrate lanes call onResult concurrently; advance's onResult is already
// serialized under the engine's flushMu but the lock is cheap insurance).
type rowPlane struct {
	mu      sync.Mutex
	enc     *rowRecordEncoder
	deliver func(kind int32, payload []byte)
	reqID   interface{}
}

// rowPlaneEngagedOnce emits a single operator-facing line the first time any
// RPC actually opts into the row plane. Answers "is row-by-row delivery ON?"
// from logs alone — without it the plane engages silently (it only logged on
// error/drift) and a misconfigured deployment (e.g. napiRowMode=false, or a
// socket transport silently degrading rowMode) is indistinguishable from a
// working one.
var rowPlaneEngagedOnce sync.Once

// newRowPlane returns nil when row mode cannot be honored (no in-process
// transport, or non-numeric request id) — callers fall back to the
// ordinary streamW path.
func newRowPlane(s *Server, reqID interface{}, want bool) *rowPlane {
	if !want || s.abiDeliver == nil {
		return nil
	}
	rid, ok := numericReqID(reqID)
	if !ok {
		return nil
	}
	rowPlaneEngagedOnce.Do(func() {
		fmt.Fprintln(os.Stderr,
			"[GO-IVM][napi] row plane engaged (per-row Go→JS delivery active)")
	})
	return &rowPlane{enc: newRowRecordEncoder(rid), deliver: s.abiDeliver, reqID: reqID}
}

// emitChanges routes one partial's changes. ALL-OR-NOTHING: row records
// are buffered and delivered only if EVERY change in the partial
// row-encodes; on the first unencodable change it returns the ENTIRE
// partial for frame delivery with zero records delivered (see the file
// header for the reorder this prevents).
//
// Group defs still deliver EAGERLY (before the whole partial's
// encodability is known): defs are metadata-only — the JS registry just
// interns them and no row references a def until a record actually
// delivers — so a def whose partial ends up framed is harmless. Deferring
// defs on an aborted partial would be WORSE: the group stays interned on
// the Go side, so a later partial's record for it would reference a def
// the JS side never received ("row record references unknown group").
func (rp *rowPlane) emitChanges(changes []engine.RowChange) []engine.RowChange {
	if len(changes) == 0 {
		return nil
	}
	// Fast path — the PRODUCTION case (REVIEW-napi-transport perf #1). rowMode
	// forces chunkSize=1, so every partial carries exactly one change and the
	// all-or-nothing buffering below has nothing to protect. deliver copies
	// synchronously (the addon memcpy's the payload before returning; the test
	// sink copies too — verified), so the record may alias the encoder's
	// scratch buffer and go out immediately: no recs slice, no per-row
	// defensive copy (2 allocations/row saved on the hottest path). The def
	// (if any) is delivered — hence copied — before encodeRow overwrites the
	// scratch buffer, and no later encode can clobber the row after its
	// deliver returns.
	if len(changes) == 1 {
		c := &changes[0]
		g, def := rp.enc.groupFor(c)
		if def != nil {
			rp.deliver(abiKindGroupDef, def)
		}
		rec, ok := rp.enc.encodeRow(g, c)
		if !ok {
			return changes // → one frame; no records delivered
		}
		rp.deliver(abiKindRow, rec)
		return nil
	}

	// Multi-change partial (residual-drain, or a non-rowMode chunk size):
	// Phase 1 encodes every row into a COPY (encodeRow's return aliases the
	// encoder's scratch buffer, so buffered records must not share it) so a
	// later unencodable change can abort the WHOLE partial with zero records
	// already delivered (the F2 all-or-nothing rule).
	recs := make([][]byte, 0, len(changes))
	for i := range changes {
		c := &changes[i]
		g, def := rp.enc.groupFor(c)
		if def != nil {
			rp.deliver(abiKindGroupDef, def)
		}
		rec, ok := rp.enc.encodeRow(g, c)
		if !ok {
			return changes // whole partial → one frame; no records delivered
		}
		recs = append(recs, append([]byte(nil), rec...))
	}
	// Phase 2: every change encoded — deliver in order.
	for _, rec := range recs {
		rp.deliver(abiKindRow, rec)
	}
	return nil
}

// deliverFrame msgpack-encodes a partial as a full RPCResponse and delivers
// it as a kind-1 frame — indistinguishable from a pump-delivered frame to
// the TS client. Encode errors are converted to an rpcError frame for the
// same request id (mirrors handleConnection's encodeFrame fallback).
func (rp *rowPlane) deliverFrame(partial interface{}) {
	data, err := mpMarshal(RPCResponse{JSONRPC: "2.0", Result: partial, ID: rp.reqID})
	if err != nil {
		data, _ = mpMarshal(rpcError(rp.reqID, -32603, "encode row-mode partial: "+err.Error()))
	}
	if capped, over := capFrameBytes(rp.reqID, data, maxFrameSize); over {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] row-mode partial frame too large: %d > %d (id=%v) — sending error\n",
			len(data), maxFrameSize, rp.reqID)
		data = capped
	}
	rp.deliver(abiKindFrame, data)
}

// emitAdvancePartial handles one engine partial in row mode: rows →
// records; fallback rows + every FINAL partial → kind-1 frames. Non-final
// partials with no fallback rows produce no frame at all (that's the win).
func (rp *rowPlane) emitAdvancePartial(r engine.AdvanceStreamPartial) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	fallback := rp.emitChanges(r.Changes)
	if len(fallback) == 0 && !r.Final {
		return
	}
	pc := toPositional(fallback)
	rp.deliverFrame(advanceStreamPartial{
		Dict:       pc.Dict,
		Rows:       pc.Rows,
		ChunkIndex: r.ChunkIndex,
		Final:      r.Final,
		Timings:    r.Timings,
		Drift:      r.Drift,
	})
}

// emitAdvanceToHeadPartial is the DRIVE-mode (advanceToHeadStream)
// counterpart of emitAdvancePartial. Identical row routing; the only
// difference is the fallback/terminal frame shape — advanceToHeadStreamPartial
// additionally carries Version + NumChanges on the Final frame (the TS
// accumulator commits the CVR watermark from them). Reset never reaches here:
// the reset path aborts before the engine apply and ships its single Final
// frame via streamW (no records exist, so ordering is trivially preserved).
func (rp *rowPlane) emitAdvanceToHeadPartial(r engine.AdvanceStreamPartial, version string, numChanges int) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	fallback := rp.emitChanges(r.Changes)
	if len(fallback) == 0 && !r.Final {
		return
	}
	pc := toPositional(fallback)
	part := advanceToHeadStreamPartial{
		Dict:       pc.Dict,
		Rows:       pc.Rows,
		ChunkIndex: r.ChunkIndex,
		Final:      r.Final,
		Timings:    r.Timings,
		Drift:      r.Drift,
	}
	if r.Final {
		part.Version = version
		part.NumChanges = numChanges
	}
	rp.deliverFrame(part)
}

// emitHydratePartial is the hydrate counterpart: one engine QueryResult in
// row mode. Every query's Final partial ships as a frame (it carries the
// per-query TimingMs + completion signal).
func (rp *rowPlane) emitHydratePartial(r engine.QueryResult) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	fallback := rp.emitChanges(r.Changes)
	if len(fallback) == 0 && !r.Final {
		return
	}
	pc := toPositional(fallback)
	rp.deliverFrame(addQueriesStreamPartial{
		QueryID:    r.QueryID,
		Dict:       pc.Dict,
		Rows:       pc.Rows,
		ChunkIndex: r.ChunkIndex,
		Final:      r.Final,
		TimingMs:   r.TimingMs,
	})
}
