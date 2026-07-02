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
// Fallback contract: a change that can't be row-encoded (non-homogeneous
// column set, remove-first group taking a later add — see encodeRow) ships
// inside a positional msgpack partial instead. Correct over fast; the TS
// row-mode accumulator accepts both planes.

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
	return &rowPlane{enc: newRowRecordEncoder(rid), deliver: s.abiDeliver, reqID: reqID}
}

// emitChanges routes one partial's changes: records for the encodable rows,
// and returns the changes that must fall back to a msgpack frame (nil when
// everything was row-encoded).
func (rp *rowPlane) emitChanges(changes []engine.RowChange) []engine.RowChange {
	var fallback []engine.RowChange
	for i := range changes {
		c := &changes[i]
		g, def := rp.enc.groupFor(c)
		if def != nil {
			rp.deliver(abiKindGroupDef, def)
		}
		rec, ok := rp.enc.encodeRow(g, c)
		if !ok {
			fallback = append(fallback, *c)
			continue
		}
		rp.deliver(abiKindRow, rec)
	}
	return fallback
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
