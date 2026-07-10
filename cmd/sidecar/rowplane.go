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
//   - row-encodable changes  → abiDeliver kind 2/3 (records), or kind 5
//     (a batch of staged records — see the STAGING section below)
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
// socket" — but a Node event loop starved for minutes parked the deliver in
// an UNCANCELLABLE cgo call while it held rp.mu: every sibling producer of
// the RPC piled onto the mutex with gate waiters==0 (invisible to the pull
// idle sweeper), wg.Wait (engine.go, phase-2 drain) never returned, the CG
// worker stayed inFlight forever (reap-proof by design, A4), and every
// retry for the cgID starved behind it in ~122s lockstep with the TS 120s
// RPC deadline. A blocked cgo call cannot be interrupted from outside, so
// the ONLY viable cancellation point is a Go-side retry around a
// NONBLOCKING enqueue (ABI v4). Additionally (F1, parallelism audit
// 2026-07-10): every rp.mu section is DEFER-unlocked and every
// unlock-for-park window DEFER-relocks — a panic anywhere in the encoder
// path (groupFor, encodeRow) previously escaped with rp.mu held forever,
// wedging every sibling producer on the MUTEX (not in a park): no
// deliver-timeout applied, no gate cancel reaches a mutex wait — the exact
// convoy shape v4 killed, reintroduced through the panic door.
//
// STAGING + EVENT-DRIVEN WAKEUP (2026-07-10, the v4 latency-tax fix — the
// PASS soak's telemetry showed 19,242 queue-full parks in 20 min with NO
// pathological JS stalls: the two taxes were (1) the poll — v4's escalating
// 100µs→5ms sleep meant every park ate up to 5ms of dead air after the
// queue had already drained, ≈40-95s of injected idle concentrated in busy
// windows; (2) row-granular queue slots — one row per TSFN entry let any
// ordinary 100-300ms JS busy slice look like congestion at 8192 slots).
// The boundary is now demand-shaped:
//
//   - Queue has room → single-record items, exactly the zero-copy fast
//     path (first-row latency untouched; the production steady state pays
//     nothing new).
//   - Deliver returns FULL → the record is STAGED (owned copy, framed) and
//     the producer RETURNS TO THE ENGINE — it keeps producing (SQLite
//     fetch, encode) instead of sleeping. Every subsequent record appends
//     to the stage (nothing may overtake it) and opportunistically
//     re-attempts a whole-stage flush (one cheap C call).
//   - A flush ships the ENTIRE current stage as ONE kind-5 batch item —
//     always atomically under rp.mu with a nonblocking attempt, so no
//     flush ever parks holding a partial batch: parked producers all wake
//     on drain, whoever re-acquires first flushes everything in append
//     order, and per-producer order is preserved by construction.
//   - Parks now happen at exactly TWO sites — the stage hard bound
//     (memory backstop) and frame delivery (frames carry terminal
//     errors/Finals and cannot be staged) — and they wake EVENT-DRIVEN:
//     the addon signals goivm_queue_drained (ABI v5) when its queue drains
//     below the low-water mark, closing the drain channel every parker
//     selects on. The 10ms tick in the park bounds CANCELLATION detection
//     only, never wakeup latency.
//
// Hydration stays pull: every row reaching this plane already holds a gate
// credit (the sidecar acquires BEFORE emit), so staged rows never exceed
// the client-granted window — demand remains the clock. Advance stays
// push: the stage hard bound + park is the backpressure, now event-woken.
//
// WAL-pin note (parallelism audit): an ADVANCE producer parked here holds
// its prev-tx WAL pin for up to the deliver timeout (150s), which exceeds
// the 60s advance-budget promise — budget checks are pre-emit only. The
// park is cancellable (group teardown) and the deadline bounds it; folding
// the park into the budget clock is future work if soaks show it matters.
//
// emit* return false when the stream is DEAD — the delivery was refused
// (TSFN closing), cancelled (client gone / group teardown), or timed out
// (GO_IVM_DELIVER_TIMEOUT — the JS loop stayed starved past any plausible
// recovery). Hydrate callers feed that into the engine's existing
// consumer-refusal unwind (onResult false → D4); the advance caller panics
// into handleStreamWithRecover (the engine's sink has no error return —
// same pattern as the economic abort).

import (
	"encoding/binary"
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

// drainBroadcast fans the addon's "TSFN queue drained below its low-water
// mark" signal (goivm_queue_drained, ABI v5) out to every producer parked
// on a full queue. close-and-remake semantics: waiters grab the CURRENT
// channel, the next broadcast closes it (waking everyone), and a fresh
// channel replaces it for future waiters.
//
// Lost-wakeup rule (every parker follows it): grab waitCh() BEFORE the
// delivery attempt. A drain firing between a failed attempt and the park
// then closes the very channel the parker already holds — the select falls
// through immediately instead of waiting a tick.
type drainBroadcast struct {
	mu sync.Mutex
	ch chan struct{}
}

func newDrainBroadcast() *drainBroadcast {
	return &drainBroadcast{ch: make(chan struct{})}
}

func (d *drainBroadcast) waitCh() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ch
}

func (d *drainBroadcast) broadcast() {
	d.mu.Lock()
	close(d.ch)
	d.ch = make(chan struct{})
	d.mu.Unlock()
}

// tsfnDrain is the process-wide drain signal — one TSFN queue per process,
// one broadcaster. The napilib export goivm_queue_drained (JS-thread direct
// call, same class as goivm_stream_credit) invokes broadcast(); tests do too.
var tsfnDrain = newDrainBroadcast()

// deliverCancelTick bounds how quickly a PARKED producer notices
// cancellation or its deadline — wakeup is normally event-driven via
// tsfnDrain, so this is NOT the typical wakeup latency. (One narrow
// exception, on record: the addon sets its FULL latch after a failed
// enqueue's decrement, so a drain that completes inside that gap can lose
// the episode's signal — the tick then doubles as the recovery re-attempt,
// worst case one tick of extra latency, never a hang.) v4's escalating
// 100µs→5ms sleep-poll made every park eat up to 5ms of dead air after the
// queue had already drained — the "poll tax" the drain broadcast exists to
// kill.
const deliverCancelTick = 10 * time.Millisecond

// Stage hard bounds — the memory backstop for records accumulated while
// the TSFN queue is full (an advance can produce tens of thousands of
// rows; unbounded staging would buffer the whole diff). Crossing either
// bound parks the producer until a flush succeeds. Vars so tests can
// shrink them; production values are deliberately modest — a stage flush
// is one TSFN item, and the addon caps items at 64MB.
var (
	stageMaxRecords = 256
	stageMaxBytes   = 1 << 20 // 1MB
)

// deliverTimeoutDefault bounds how long one payload (or the stage) may wait
// against a full TSFN queue before the stream is declared dead. It must sit
// ABOVE the longest observed recoverable JS-loop stall (43-46s synchronous
// materializations — a 44s hydrate COMPLETED in the incident soak) but BELOW
// the advance budget (advanceBudgetMs, default 60s) so a parked advance
// producer can't hold a WAL pin past the budget. 55s gives a 9s buffer past
// the 46s stall ceiling and 5s under the 60s budget. Env-tunable via
// GO_IVM_DELIVER_TIMEOUT_SEC (read lazily — the env sync from the embedder
// happens at goivm_start, after package init).
const deliverTimeoutDefault = 55 * time.Second

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
// The mutex guards the ENCODER (groupFor / encodeRow / the scratch buffer)
// and the STAGE; it is never held across a waiting delivery — see the file
// header's lock discipline.
type rowPlane struct {
	mu      sync.Mutex
	enc     *rowRecordEncoder
	sig     *RowSigAccumulator
	deliver func(kind int32, payload []byte) int32
	reqID   interface{}
	// cgID is observability-only (DELIVER-TIMEOUT / dead-stream lines).
	cgID string
	// cancelled reports whether this RPC's consumer is gone — checked
	// between park slices while waiting on a full TSFN queue. Wired to
	// the group's done channel at construction; the pull path additionally
	// folds in its gate's cancelled flag (setPullGate). nil = only the
	// deadline bounds the park (tests).
	cancelled func() bool
	// timeout bounds one payload's (or the stage's) wait against a full
	// queue (deliverTimeoutDur in production; tests shrink it directly).
	timeout time.Duration

	// stage holds framed records ([u8 kind][u32le len][bytes], kinds 2/3)
	// that found the queue FULL, awaiting a whole-stage kind-5 batch flush.
	// Guarded by mu. While non-empty, EVERY record appends here (nothing
	// may overtake the stage); flushes always ship the entire current
	// stage atomically under mu — see the file header's STAGING section.
	stage        []byte
	stageRecords int
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
		sig:     NewRowSigAccumulator(),
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

// noteDeliverTimeout emits the incident marker and bumps the counter.
func (rp *rowPlane) noteDeliverTimeout(kind int32, payloadLen int) {
	metrics.napiDeliverTimeouts.Add(1)
	fmt.Fprintf(deliverLogW,
		"[GO-IVM][DELIVER-TIMEOUT] cg=%s reqID=%v kind=%d bytes=%d waited=%v — TSFN queue full past the deadline (JS loop starved); failing the stream\n",
		rp.cgID, rp.reqID, kind, payloadLen, rp.timeout)
}

// parkSlice waits for a drain signal (event — instant) or one cancellation
// tick. ch MUST have been grabbed via tsfnDrain.waitCh() BEFORE the failed
// delivery attempt (the lost-wakeup rule). Holding rp.mu here is FORBIDDEN.
func parkSlice(ch <-chan struct{}, t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(deliverCancelTick)
	select {
	case <-ch:
	case <-t.C:
	}
}

// retryDeliver parks one OWNED payload against a full TSFN queue: an
// event-driven wait on the drain broadcast, checking cancellation each
// tick. Holding rp.mu here is FORBIDDEN — this is the wait the lock
// discipline exists to keep lock-free. Returns false when the stream is
// dead (closed / cancelled / deadline).
func (rp *rowPlane) retryDeliver(kind int32, payload []byte) bool {
	metrics.napiDeliverStalls.Add(1)
	deadline := time.Now().Add(rp.timeout)
	t := time.NewTimer(deliverCancelTick)
	defer t.Stop()
	for {
		ch := tsfnDrain.waitCh() // BEFORE the attempt — lost-wakeup rule
		switch rp.deliver(kind, payload) {
		case deliverOK:
			return true
		case deliverClosed:
			return false
		}
		if rp.cancelled != nil && rp.cancelled() {
			return false
		}
		if time.Now().After(deadline) {
			rp.noteDeliverTimeout(kind, len(payload))
			return false
		}
		parkSlice(ch, t)
	}
}

// stageAppendLocked frames one record onto the stage. Called with rp.mu
// held. The payload may alias the encoder scratch — the append copies.
func (rp *rowPlane) stageAppendLocked(kind int32, payload []byte) {
	metrics.napiStagedRecords.Add(1)
	var hdr [5]byte
	hdr[0] = byte(kind)
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(payload)))
	rp.stage = append(rp.stage, hdr[:]...)
	rp.stage = append(rp.stage, payload...)
	rp.stageRecords++
}

// tryFlushStageLocked makes ONE nonblocking whole-stage flush attempt.
// Returns false only when the transport is dead (CLOSED); FULL leaves the
// stage intact — the producer keeps producing. Called with rp.mu held.
func (rp *rowPlane) tryFlushStageLocked() bool {
	if len(rp.stage) == 0 {
		return true
	}
	switch rp.deliver(abiKindBatch, rp.stage) {
	case deliverOK:
		metrics.napiBatchFlushes.Add(1)
		rp.stage = rp.stage[:0]
		rp.stageRecords = 0
	case deliverClosed:
		return false
	}
	return true
}

// flushStageLocked flushes the ENTIRE current stage, parking (with rp.mu
// RELEASED — the F1 defer discipline makes the window panic-safe) until a
// drain signal frees queue space. Because the attempt always runs under
// rp.mu against the whole stage, no flush ever ships a partial batch:
// siblings appending during the park simply grow the batch the next
// attempt ships, in append order — per-producer order is preserved by
// construction. Called with rp.mu held; returns with rp.mu held. false =
// stream dead.
func (rp *rowPlane) flushStageLocked() bool {
	if len(rp.stage) == 0 {
		return true
	}
	var deadline time.Time
	t := time.NewTimer(deliverCancelTick)
	defer t.Stop()
	for {
		// Re-check after every park (multi-parker wake, review LOW
		// 2026-07-10): flushes are whole-stage atomic under mu, so a sibling
		// parker that re-acquired FIRST may have shipped the entire stage —
		// this parker's work is done. Without the re-check it delivered the
		// now-empty stage as a zero-record kind-5: a wasted TSFN slot during
		// exactly the congestion window slots are scarce, a phantom
		// napiBatchFlushes bump, and (queue still FULL) a producer
		// re-parking to deliver nothing.
		if len(rp.stage) == 0 {
			return true
		}
		ch := tsfnDrain.waitCh() // BEFORE the attempt — lost-wakeup rule
		switch rp.deliver(abiKindBatch, rp.stage) {
		case deliverOK:
			metrics.napiBatchFlushes.Add(1)
			rp.stage = rp.stage[:0]
			rp.stageRecords = 0
			return true
		case deliverClosed:
			return false
		}
		if deadline.IsZero() {
			metrics.napiDeliverStalls.Add(1)
			deadline = time.Now().Add(rp.timeout)
		}
		if rp.cancelled != nil && rp.cancelled() {
			return false
		}
		if time.Now().After(deadline) {
			rp.noteDeliverTimeout(abiKindBatch, len(rp.stage))
			return false
		}
		// Park with rp.mu RELEASED; the defer re-lock keeps the caller's
		// defer-unlock balanced even if the park path ever panics (F1).
		func() {
			rp.mu.Unlock()
			defer rp.mu.Lock()
			parkSlice(ch, t)
		}()
	}
}

// sendOrStageLocked routes one record (def or row): direct zero-copy
// delivery when the stage is empty and the queue has room (the production
// fast path — deliver copies synchronously on OK, so the payload may alias
// the encoder scratch); otherwise the record is STAGED (owned copy) and
// the producer continues — with one opportunistic whole-stage flush
// attempt so the stage drains promptly once the queue recovers. Crossing
// the stage hard bound parks until a flush succeeds (memory backstop).
// Called with rp.mu held; may release it inside flushStageLocked's park.
// false = stream dead.
func (rp *rowPlane) sendOrStageLocked(kind int32, payload []byte) bool {
	if len(rp.stage) == 0 {
		switch rp.deliver(kind, payload) {
		case deliverOK:
			return true
		case deliverClosed:
			return false
		}
		// FULL → open the stage with this record.
	}
	rp.stageAppendLocked(kind, payload)
	if !rp.tryFlushStageLocked() {
		return false
	}
	if rp.stageRecords >= stageMaxRecords || len(rp.stage) >= stageMaxBytes {
		return rp.flushStageLocked()
	}
	return true
}

// emitChangesGuarded is the ONLY entry to emitChangesLocked: it owns the
// panic-safe rp.mu section (F1, parallelism audit 2026-07-10). The v4
// rewrite had replaced the pre-v4 `defer rp.mu.Unlock()` with bare
// Lock/Unlock pairs in the emit* callers — a panic anywhere in the encoder
// path escaped with rp.mu held FOREVER: every sibling producer then
// blocked on the MUTEX (not in a park), where no deliver-timeout applies
// and no gate cancel reaches — the exact convoy shape v4 exists to kill,
// reintroduced via the panic path. Every unlock-for-park window inside
// (flushStageLocked) defer-relocks, so this defer can never double-unlock.
// flushStage ships any staged records ahead of a wait whose UNPARKER
// depends on the staged data being VISIBLE to the client — deliverFrame's
// flush-first rule, exported for the pull loop's credit park (see
// acquirePullCredit). Cancellable + drain-woken like every stage flush;
// returns false when the stream is dead. Cheap no-op when the stage is
// empty.
func (rp *rowPlane) flushStage() bool {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.flushStageLocked()
}

// acquirePullCredit consumes one pull credit for a row-bearing delivery,
// flushing the stage BEFORE any park on client demand. Returns false when
// the stream is dead (gate cancelled, or the flush was cancelled/timed
// out/closed).
//
// The rule (credit-park stalemate, 2026-07-10 — found auditing the v5
// staging design): staged rows are granted-but-undelivered, INVISIBLE to
// the client. Its top-up policy grants only when outstanding
// (granted − consumed) falls to the low-water mark — and it can never
// consume rows sitting in the stage. So a producer that parks in
// gate.acquire with a non-empty stage can deadlock the demand loop:
// queue-full episode stages > lowWater rows → credits exhaust → every
// producer parks as a GATE waiter → the TSFN drain broadcast wakes nobody
// (they are not drain waiters) → the stage never flushes → the client
// never grants. Only the 60s pull idle sweep breaks it — as a SPURIOUS
// stream cancel + client re-hydrate. Small stages dodge it (a sibling's
// next emit opportunistically flushes), which is why light soaks never
// saw it; a single full-queue episode staging > lowWater rows with
// credits running out is deterministic.
//
// tryAcquire first: credit in hand → zero new cost on the hot path. Only
// the about-to-park case pays the flush — which is exactly when shipping
// the stage is REQUIRED for the park to ever end. The flush itself may
// park on the full queue (drain-woken, cancellable): that is the correct
// wait — the client is slow at the TRANSPORT level, not the demand level.
func acquirePullCredit(gate *streamGate, rp *rowPlane) bool {
	if gate.tryAcquire() {
		return true
	}
	if !rp.flushStage() {
		return false
	}
	return gate.acquire()
}

func (rp *rowPlane) emitChangesGuarded(changes []engine.RowChange) (fallback []engine.RowChange, ok bool) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.emitChangesLocked(changes)
}

// emitChangesLocked routes one partial's changes. ALL-OR-NOTHING: row
// records are buffered and delivered only if EVERY change in the partial
// row-encodes; on the first unencodable change it returns the ENTIRE
// partial for frame delivery with zero records delivered (see the file
// header for the reorder this prevents). ok=false means the stream is dead
// (delivery refused/cancelled/timed out) — the caller must abort the RPC.
//
// Called with rp.mu HELD (via emitChangesGuarded); may release it around a
// parked stage flush; returns with rp.mu HELD. Encoder state is only ever
// touched under the lock; the group pointer from groupFor stays valid
// across a release (the map holds pointers, and only THIS producer's
// queryID can touch its groups).
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
	// synchronously ON SUCCESS and staging copies on FULL, so the record may
	// alias the encoder's scratch buffer either way: no recs slice, no
	// per-row defensive copy (2 allocations/row saved on the hottest path).
	// The def (if any) is delivered/staged — hence copied — before encodeRow
	// overwrites the scratch buffer.
	if len(changes) == 1 {
		c := &changes[0]
		g, def := rp.enc.groupFor(c)
		if def != nil {
			if !rp.sendOrStageLocked(abiKindGroupDef, def) {
				return nil, false
			}
		}
		rec, encOK := rp.enc.encodeRow(g, c)
		if !encOK {
			return changes, true // → one frame; no records delivered
		}
		if !rp.sendOrStageLocked(abiKindRow, rec) {
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
			if !rp.sendOrStageLocked(abiKindGroupDef, def) {
				return nil, false
			}
		}
		rec, encOK := rp.enc.encodeRow(g, c)
		if !encOK {
			return changes, true // whole partial → one frame; no records delivered
		}
		recs = append(recs, append([]byte(nil), rec...))
	}
	// Phase 2: every change encoded — deliver in order.
	for _, rec := range recs {
		if !rp.sendOrStageLocked(abiKindRow, rec) {
			return nil, false
		}
	}
	return nil, true
}

// deliverFrame msgpack-encodes a partial as a full RPCResponse and delivers
// it as a kind-1 frame — indistinguishable from a pump-delivered frame to
// the TS client. Encode errors are converted to an rpcError frame for the
// same request id (mirrors handleConnection's encodeFrame fallback).
//
// Frames must not overtake staged records, so the stage is flushed FIRST
// (parking if the queue stays full — a frame, unlike a record, cannot be
// staged: it may carry the terminal error/Final the client is waiting on).
// The frame itself then delivers OUTSIDE rp.mu (owned buffer, no encoder
// access); per-producer sequencing keeps it after the caller's records.
// Returns false when the stream is dead.
func (rp *rowPlane) deliverFrame(partial interface{}) bool {
	flushed := func() bool {
		rp.mu.Lock()
		defer rp.mu.Unlock()
		return rp.flushStageLocked()
	}()
	if !flushed {
		return false
	}
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
	rp.sig.accumulateChanges(r.Changes)
	fallback, ok := rp.emitChangesGuarded(r.Changes)
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
		part.SigDeltas = rp.sig.allDeltasHex()
	}
	return rp.deliverFrame(part)
}

// emitHydratePartial is the hydrate counterpart: one engine QueryResult in
// row mode. Every query's Final partial ships as a frame (it carries the
// per-query TimingMs + completion signal). Returns false when the stream is
// dead — the caller must refuse further results (onResult false → the
// engine's consumer-refusal unwind).
func (rp *rowPlane) emitHydratePartial(r engine.QueryResult) bool {
	rp.sig.accumulateChanges(r.Changes)
	fallback, ok := rp.emitChangesGuarded(r.Changes)
	if !ok {
		return false
	}
	if len(fallback) == 0 && !r.Final {
		return true
	}
	pc := toPositional(fallback)
	part := addQueriesStreamPartial{
		QueryID:    r.QueryID,
		Dict:       pc.Dict,
		Rows:       pc.Rows,
		ChunkIndex: r.ChunkIndex,
		Final:      r.Final,
		TimingMs:   r.TimingMs,
	}
	if r.Final {
		if sig, ok := rp.sig.deltaHex(r.QueryID); ok {
			part.SigDelta = sig
		}
	}
	return rp.deliverFrame(part)
}
