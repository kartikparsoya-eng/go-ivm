package main

// Row-plane emit path shared by handleAdvanceToHeadStream and
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
//
// LOCK DISCIPLINE (2026-07-09, the G13 wedge fix — root-caused from the
// wedge watchdog's self-captured stacks): rp.mu may NEVER be held across a
// deliver that waits. The addon's TSFN queue is bounded (default 8192) and
// the pre-v4 deliver used napi_tsfn_blocking — "backpressure like a slow
// socket" — but a Node event loop starved for minutes (43-46s synchronous
// TS materializations, observed) parked the deliver in an UNCANCELLABLE
// cgo call while it held rp.mu: every sibling producer of the RPC piled
// onto the mutex with gate waiters==0 (invisible to the pull idle
// sweeper), wg.Wait (engine.go, phase-2 drain) never returned, the CG
// worker stayed inFlight forever (reap-proof by design, A4), and every
// retry for the cgID starved behind it in ~122s lockstep with the TS 120s
// RPC deadline. A blocked cgo call cannot be interrupted from outside, so
// the ONLY viable cancellation point is a Go-side retry loop around a
// NONBLOCKING enqueue (ABI v4):
//
//   - the fast path attempts the enqueue UNDER rp.mu with the zero-copy
//     scratch-aliased payload (deliver copies synchronously on success —
//     the production steady state pays nothing new);
//   - on queue-full, the payload is copied, rp.mu is RELEASED, and the
//     delivery parks in retryDeliver — nonblocking attempts with escalating
//     sleeps, checking cancellation between attempts (pull-gate cancelled /
//     group teardown / GO_IVM_DELIVER_TIMEOUT) — so a stalled consumer
//     costs a bounded, cancellable wait holding NO lock;
//   - kind-1 frames (owned buffers, no encoder access) deliver entirely
//     outside rp.mu.
//
// Ordering survives the lock release because everything that must stay
// ordered is single-producer: a group's def and all its records belong to
// ONE query's producer goroutine (groups are keyed per (queryID, table) —
// rowrecord.go), and that goroutine delivers sequentially — a payload
// either enqueues on the spot or the producer parks until it does, so its
// next payload cannot overtake. Cross-producer interleaving (different
// queries) was always legal: records are self-describing (reqID + groupID)
// and the TS accumulator demultiplexes. The terminal "done" still follows
// everything because the handler returns only after every producer's emit
// call has returned.
//
// emit* return false when the stream is DEAD — the delivery was refused
// (TSFN closing), cancelled (client gone / group teardown), or timed out
// (GO_IVM_DELIVER_TIMEOUT — the JS loop stayed starved past any plausible
// recovery). Hydrate callers feed that into the engine's existing
// consumer-refusal unwind (onResult false → D4); the advance caller panics
// into handleStreamWithRecover (the engine's sink has no error return —
// same pattern as the economic abort).

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/engine"
)

// Deliver statuses (ABI v4). The addon's deliver callback returns one of
// these; see napi_lib.go's ABI notes and addon.c deliver_from_go.
const (
	deliverOK     int32 = 0 // enqueued; payload was copied synchronously
	deliverFull   int32 = 1 // TSFN queue full; nothing enqueued — retryable
	deliverClosed int32 = 2 // TSFN closing/gone; the transport is dead
)

// deliverTimeoutDefault bounds how long one payload may retry against a
// full TSFN queue before the stream is declared dead. It must sit ABOVE
// both the longest observed recoverable JS-loop stall (43-46s synchronous
// materializations — a 44s hydrate COMPLETED in the incident soak) and the
// TS 120s RPC deadline (past which the client has abandoned the RPC and
// usually already fired the pull-gate cancel, which unparks the retry far
// earlier). Firing therefore means the loop stayed starved beyond any
// plausible recovery — an incident ([GO-IVM][DELIVER-TIMEOUT]), not load.
// Env-tunable via GO_IVM_DELIVER_TIMEOUT_SEC (read lazily — the env sync
// from the embedder happens at goivm_start, after package init).
const deliverTimeoutDefault = 150 * time.Second

var deliverTimeoutOnce sync.Once
var deliverTimeoutVal = deliverTimeoutDefault

func deliverTimeoutDur() time.Duration {
	deliverTimeoutOnce.Do(func() {
		if v := os.Getenv("GO_IVM_DELIVER_TIMEOUT_SEC"); v != "" {
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
				deliverTimeoutVal = time.Duration(n) * time.Second
			}
		}
	})
	return deliverTimeoutVal
}

// deliverLogW is the sink for the [GO-IVM][DELIVER-TIMEOUT] incident marker
// (test-swappable, like wedgeLogW; production is always os.Stderr).
var deliverLogW io.Writer = os.Stderr

// rowPlane wraps a rowRecordEncoder with the delivery callback and a mutex
// (hydrate lanes call onResult concurrently; advance's onResult is already
// serialized under the engine's flushMu but the lock is cheap insurance).
// The mutex guards the ENCODER (groupFor / encodeRow / the scratch buffer);
// it is never held across a waiting delivery — see the file header.
type rowPlane struct {
	mu      sync.Mutex
	enc     *rowRecordEncoder
	deliver func(kind int32, payload []byte) int32
	reqID   interface{}
	// cgID is observability-only (DELIVER-TIMEOUT / dead-stream lines).
	cgID string
	// cancelled reports whether this RPC's consumer is gone — checked
	// between retry attempts while parked on a full TSFN queue. Wired to
	// the group's done channel at construction; the pull path additionally
	// folds in its gate's cancelled flag (setPullGate). nil = only the
	// deadline bounds the park (tests).
	cancelled func() bool
	// timeout bounds one payload's park (deliverTimeoutDur in production;
	// tests shrink it directly).
	timeout time.Duration
}

// rowPlaneEngagedOnce emits a single operator-facing line the first time any
// RPC actually opts into the row plane. Answers "is row-by-row delivery ON?"
// from logs alone — without it the plane engages silently (it only logged on
// error) and a misconfigured deployment (e.g. a non-numeric reqID degrading
// rowMode) is indistinguishable from a working one.
var rowPlaneEngagedOnce sync.Once

// newRowPlane returns nil when row mode cannot be honored (no in-process
// transport, or non-numeric request id) — callers fall back to the
// ordinary streamW path. done is the owning group's teardown channel
// (host-shutdown unpark for a delivery parked on a full queue); nil is
// legal (tests).
func newRowPlane(s *Server, reqID interface{}, want bool, cgID string, done <-chan struct{}) *rowPlane {
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
	rp := &rowPlane{
		enc:     newRowRecordEncoder(rid),
		deliver: s.abiDeliver,
		reqID:   reqID,
		cgID:    cgID,
		timeout: deliverTimeoutDur(),
	}
	if done != nil {
		rp.cancelled = func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}
	}
	return rp
}

// setPullGate folds the pull gate's cancelled flag into the plane's
// cancellation check: the TS client's RPC timeout / .return() crosses the
// boundary as goivm_stream_cancel, which flips the gate — and must also
// unpark a delivery stuck on a full queue (the gate's cond broadcast only
// reaches producers parked in gate.acquire). Called once, before any
// producer starts (no lock needed).
func (rp *rowPlane) setPullGate(gate *streamGate) {
	prev := rp.cancelled
	rp.cancelled = func() bool {
		return gate.isCancelled() || (prev != nil && prev())
	}
}

// retryDeliver parks one OWNED payload against a full TSFN queue:
// nonblocking attempts with escalating sleeps (100µs → 5ms), checking
// cancellation between attempts. Holding rp.mu here is FORBIDDEN — this is
// the wait the lock discipline exists to keep lock-free. Returns false when
// the stream is dead (closed / cancelled / deadline).
func (rp *rowPlane) retryDeliver(kind int32, payload []byte) bool {
	metrics.napiDeliverStalls.Add(1)
	deadline := time.Now().Add(rp.timeout)
	sleep := 100 * time.Microsecond
	for {
		if rp.cancelled != nil && rp.cancelled() {
			return false
		}
		if time.Now().After(deadline) {
			metrics.napiDeliverTimeouts.Add(1)
			fmt.Fprintf(deliverLogW,
				"[GO-IVM][DELIVER-TIMEOUT] cg=%s reqID=%v kind=%d bytes=%d waited=%v — TSFN queue full past the deadline (JS loop starved); failing the stream\n",
				rp.cgID, rp.reqID, kind, len(payload), rp.timeout)
			return false
		}
		time.Sleep(sleep)
		if sleep < 5*time.Millisecond {
			sleep *= 2
		}
		switch rp.deliver(kind, payload) {
		case deliverOK:
			return true
		case deliverClosed:
			return false
		}
	}
}

// sendLocked delivers one payload that may ALIAS the encoder's scratch
// buffer. Called with rp.mu HELD; returns with rp.mu HELD. The nonblocking
// attempt runs under the lock (success copies synchronously — the zero-copy
// fast path). On queue-full it copies the payload, RELEASES rp.mu for the
// duration of the park, and re-acquires before returning — sibling
// producers keep encoding+delivering while this one waits.
func (rp *rowPlane) sendLocked(kind int32, payload []byte) bool {
	switch rp.deliver(kind, payload) {
	case deliverOK:
		return true
	case deliverClosed:
		return false
	}
	owned := append([]byte(nil), payload...)
	rp.mu.Unlock()
	ok := rp.retryDeliver(kind, owned)
	rp.mu.Lock()
	return ok
}

// emitChangesLocked routes one partial's changes. ALL-OR-NOTHING: row
// records are buffered and delivered only if EVERY change in the partial
// row-encodes; on the first unencodable change it returns the ENTIRE
// partial for frame delivery with zero records delivered (see the file
// header for the reorder this prevents). ok=false means the stream is dead
// (delivery refused/cancelled/timed out) — the caller must abort the RPC.
//
// Called with rp.mu HELD; may release it around a parked delivery
// (sendLocked); returns with rp.mu HELD. Encoder state is only ever touched
// under the lock; the group pointer from groupFor stays valid across a
// release (the map holds pointers, and only THIS producer's queryID can
// touch its groups).
//
// Group defs still deliver EAGERLY (before the whole partial's
// encodability is known): defs are metadata-only — the JS registry just
// interns them and no row references a def until a record actually
// delivers — so a def whose partial ends up framed is harmless. Deferring
// defs on an aborted partial would be WORSE: the group stays interned on
// the Go side, so a later partial's record for it would reference a def
// the JS side never received ("row record references unknown group").
func (rp *rowPlane) emitChangesLocked(changes []engine.RowChange) (fallback []engine.RowChange, ok bool) {
	if len(changes) == 0 {
		return nil, true
	}
	// Fast path — the PRODUCTION case (REVIEW-napi-transport perf #1). rowMode
	// forces chunkSize=1, so every partial carries exactly one change and the
	// all-or-nothing buffering below has nothing to protect. deliver copies
	// synchronously ON SUCCESS (the addon memcpy's the payload before
	// returning; the test sink copies too — verified), so the record may
	// alias the encoder's scratch buffer for the in-lock attempt: no recs
	// slice, no per-row defensive copy (2 allocations/row saved on the
	// hottest path). Only a queue-full attempt pays a copy (sendLocked). The
	// def (if any) is delivered — hence copied or owned — before encodeRow
	// overwrites the scratch buffer.
	if len(changes) == 1 {
		c := &changes[0]
		g, def := rp.enc.groupFor(c)
		if def != nil {
			if !rp.sendLocked(abiKindGroupDef, def) {
				return nil, false
			}
		}
		rec, encOK := rp.enc.encodeRow(g, c)
		if !encOK {
			return changes, true // → one frame; no records delivered
		}
		if !rp.sendLocked(abiKindRow, rec) {
			return nil, false
		}
		return nil, true
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
			if !rp.sendLocked(abiKindGroupDef, def) {
				return nil, false
			}
		}
		rec, encOK := rp.enc.encodeRow(g, c)
		if !encOK {
			return changes, true // whole partial → one frame; no records delivered
		}
		recs = append(recs, append([]byte(nil), rec...))
	}
	// Phase 2: every change encoded — deliver in order. The records are
	// owned copies with no encoder access left, so a parked delivery
	// releases the lock for its whole duration.
	for _, rec := range recs {
		switch rp.deliver(abiKindRow, rec) {
		case deliverOK:
			continue
		case deliverClosed:
			return nil, false
		}
		rp.mu.Unlock()
		parked := rp.retryDeliver(abiKindRow, rec)
		rp.mu.Lock()
		if !parked {
			return nil, false
		}
	}
	return nil, true
}

// deliverFrame msgpack-encodes a partial as a full RPCResponse and delivers
// it as a kind-1 frame — indistinguishable from a pump-delivered frame to
// the TS client. Encode errors are converted to an rpcError frame for the
// same request id (mirrors handleConnection's encodeFrame fallback). Runs
// entirely OUTSIDE rp.mu (owned buffer, no encoder access); per-producer
// sequencing keeps it after the caller's records. Returns false when the
// stream is dead.
func (rp *rowPlane) deliverFrame(partial interface{}) bool {
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
	switch rp.deliver(abiKindFrame, data) {
	case deliverOK:
		return true
	case deliverClosed:
		return false
	}
	return rp.retryDeliver(abiKindFrame, data)
}

// emitAdvanceToHeadPartial handles one engine partial in row mode for the
// drive advance (advanceToHeadStream): rows → records; fallback rows + every
// FINAL partial → kind-1 frames. Non-final partials with no fallback rows
// produce no frame at all (that's the win). The Final frame additionally
// carries Version + NumChanges (the TS accumulator commits the CVR watermark
// from them). Reset never reaches here:
// the reset path aborts before the engine apply and ships its single Final
// frame via streamW (no records exist, so ordering is trivially preserved).
// Returns false when the stream is dead — the caller must abort the advance.
func (rp *rowPlane) emitAdvanceToHeadPartial(r engine.AdvanceStreamPartial, version string, numChanges int) bool {
	rp.mu.Lock()
	fallback, ok := rp.emitChangesLocked(r.Changes)
	rp.mu.Unlock()
	if !ok {
		return false
	}
	if len(fallback) == 0 && !r.Final {
		return true
	}
	pc := toPositional(fallback)
	part := advanceToHeadStreamPartial{
		Dict:       pc.Dict,
		Rows:       pc.Rows,
		ChunkIndex: r.ChunkIndex,
		Final:      r.Final,
		Timings:    r.Timings,
	}
	if r.Final {
		part.Version = version
		part.NumChanges = numChanges
	}
	return rp.deliverFrame(part)
}

// emitHydratePartial is the hydrate counterpart: one engine QueryResult in
// row mode. Every query's Final partial ships as a frame (it carries the
// per-query TimingMs + completion signal). Returns false when the stream is
// dead — the caller must refuse further results (onResult false → the
// engine's consumer-refusal unwind).
func (rp *rowPlane) emitHydratePartial(r engine.QueryResult) bool {
	rp.mu.Lock()
	fallback, ok := rp.emitChangesLocked(r.Changes)
	rp.mu.Unlock()
	if !ok {
		return false
	}
	if len(fallback) == 0 && !r.Final {
		return true
	}
	pc := toPositional(fallback)
	return rp.deliverFrame(addQueriesStreamPartial{
		QueryID:    r.QueryID,
		Dict:       pc.Dict,
		Rows:       pc.Rows,
		ChunkIndex: r.ChunkIndex,
		Final:      r.Final,
		TimingMs:   r.TimingMs,
	})
}
