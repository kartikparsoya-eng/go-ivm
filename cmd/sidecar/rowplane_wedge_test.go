package main

// Pins for the ABI v4/v5 delivery contract (rowplane.go):
//
// v4 (cancellable parks — the G13 wedge fix): a wait on a full TSFN queue
// unwinds PROMPTLY on cancellation (group teardown or pull-gate cancel),
// is bounded by the deliver timeout ([GO-IVM][DELIVER-TIMEOUT]), never
// holds rp.mu, and deliverClosed unwinds immediately.
//
// v5 (staging + event-driven wakeup — the v4 latency-tax fix): a record
// that finds the queue FULL is STAGED, not parked — the emit returns and
// the producer keeps producing; staged records ship as ONE ordered kind-5
// batch on recovery; parks survive only at the stage hard bound and frame
// delivery, and they wake on the drain broadcast (goivm_queue_drained)
// instead of a sleep-poll.
//
// F1 (parallelism audit): a panic under rp.mu (encoder/deliver path) must
// NOT leak the mutex — pre-fix, bare Lock/Unlock in the emit* callers let
// a panic escape with rp.mu held forever, wedging every sibling producer
// on the MUTEX where no timeout or cancel could reach (the v4 wedge shape
// reintroduced through the panic door).
//
// Red-proof protocol (run and verified during development):
//   - removing retryDeliver/flushStageLocked's cancellation checks fails
//     the cancel tests (uncancellable park — the pre-v4 shape);
//   - reverting emitChangesGuarded to bare Lock/Unlock fails the F1
//     TryLock test;
//   - reverting the fixed-deadline tripwire fails the engine's F2
//     progress test (engine/pipeline_reader_test.go).

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/engine"
)

// scriptedSink is a deliver fake whose per-call status is driven by the
// test: mode=full/closed/ok, plus a flaky "first attempt of each distinct
// payload is full" mode.
type scriptedSink struct {
	mu       sync.Mutex
	mode     int32 // deliverOK / deliverFull / deliverClosed
	flaky    bool  // first attempt per distinct payload returns Full
	seen     map[string]bool
	calls    atomic.Int64
	kinds    []int32
	payloads [][]byte
}

func newScriptedSink(mode int32) *scriptedSink {
	return &scriptedSink{mode: mode, seen: map[string]bool{}}
}

func (s *scriptedSink) setMode(mode int32) {
	s.mu.Lock()
	s.mode = mode
	s.mu.Unlock()
}

func (s *scriptedSink) sink(kind int32, payload []byte) int32 {
	s.calls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flaky && !s.seen[string(payload)] {
		s.seen[string(payload)] = true
		return deliverFull
	}
	if s.mode != deliverOK {
		return s.mode
	}
	s.kinds = append(s.kinds, kind)
	s.payloads = append(s.payloads, append([]byte(nil), payload...))
	return deliverOK
}

func (s *scriptedSink) kindSeq() []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int32(nil), s.kinds...)
}

func (s *scriptedSink) payloadAt(i int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.payloads[i]
}

func wedgeTestPlane(t *testing.T, s *scriptedSink, timeout time.Duration) *rowPlane {
	t.Helper()
	rp := newRowPlane(&Server{abiDeliver: s.sink}, 7.0, true, "cg-wedge", nil)
	if rp == nil {
		t.Fatal("newRowPlane returned nil")
	}
	rp.timeout = timeout
	return rp
}

func setStageBounds(t *testing.T, recs, byteCap int) {
	t.Helper()
	savedR, savedB := stageMaxRecords, stageMaxBytes
	stageMaxRecords, stageMaxBytes = recs, byteCap
	t.Cleanup(func() { stageMaxRecords, stageMaxBytes = savedR, savedB })
}

// decodeBatch splits a kind-5 payload into its framed sub-records.
func decodeBatch(t *testing.T, payload []byte) (kinds []int32, payloads [][]byte) {
	t.Helper()
	for off := 0; off < len(payload); {
		if off+5 > len(payload) {
			t.Fatalf("batch header truncated at offset %d (len %d)", off, len(payload))
		}
		k := int32(payload[off])
		l := int(binary.LittleEndian.Uint32(payload[off+1 : off+5]))
		off += 5
		if off+l > len(payload) {
			t.Fatalf("batch body truncated at offset %d (want %d bytes, have %d)", off, l, len(payload)-off)
		}
		kinds = append(kinds, k)
		payloads = append(payloads, payload[off:off+l])
		off += l
	}
	return
}

func finalPartial(q, id string) engine.QueryResult {
	return engine.QueryResult{
		QueryID: q, Changes: []engine.RowChange{rcAdd(q, id)}, Final: true,
	}
}

// TestRowPlane_FullQueueUnwindsOnCancel: a FINAL partial on a perpetually
// full queue parks (the frame path must flush the stage, and frames cannot
// be staged) — and must return false PROMPTLY once the plane's cancellation
// flips (the group-teardown unpark). Pre-v4 this park was a blocked cgo
// call nothing could interrupt (the wedge); red-proof: removing the
// cancellation checks makes this park until the 30s timeout → test failure.
func TestRowPlane_FullQueueUnwindsOnCancel(t *testing.T) {
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second) // deadline must NOT be the unpark
	var cancelled atomic.Bool
	rp.cancelled = func() bool { return cancelled.Load() }

	done := make(chan bool, 1)
	go func() {
		done <- rp.emitHydratePartial(finalPartial("q1", "a"))
	}()
	time.Sleep(50 * time.Millisecond) // let it park in the flush/frame wait
	cancelled.Store(true)
	select {
	case ok := <-done:
		if ok {
			t.Fatal("emit reported success on a cancelled, never-delivered stream")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("emit did not unwind after cancellation — the park is not cancellable (the G13 wedge shape)")
	}
}

// TestRowPlane_GateCancelUnparksDeliver: the PRODUCTION unpark path — the
// client's RPC timeout / .return() crosses the boundary as
// goivm_stream_cancel → gate.cancel() → the plane's poll sees it. This is
// load-bearing because a producer parked in the deliver wait is NOT a gate
// waiter (waiters==0), so the gate's cond broadcast alone cannot reach it.
func TestRowPlane_GateCancelUnparksDeliver(t *testing.T) {
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second)
	gate := newStreamGate(nil)
	rp.setPullGate(gate)

	done := make(chan bool, 1)
	go func() {
		done <- rp.emitHydratePartial(finalPartial("q1", "a"))
	}()
	time.Sleep(50 * time.Millisecond)
	gate.cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("emit reported success after gate cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gate cancel did not unpark the parked delivery")
	}
}

// TestRowPlane_FullQueueDeadlineTrips: with no cancellation signal at all
// (advance path with a live group), the deliver timeout is the tripwire:
// bounded unwind + the [GO-IVM][DELIVER-TIMEOUT] incident marker.
func TestRowPlane_FullQueueDeadlineTrips(t *testing.T) {
	out := captureDeliverLog(t)
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 100*time.Millisecond)

	start := time.Now()
	ok := rp.emitHydratePartial(finalPartial("q1", "a"))
	elapsed := time.Since(start)
	if ok {
		t.Fatal("emit reported success past the deliver deadline")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("deadline unwind took %v — not bounded", elapsed)
	}
	if !strings.Contains(out.String(), "[GO-IVM][DELIVER-TIMEOUT] cg=cg-wedge") {
		t.Fatalf("no DELIVER-TIMEOUT incident marker; log:\n%s", out.String())
	}
}

// TestRowPlane_StageBoundParkReleasesMu is the cascade-killer pin, v5
// shape: the only record-path park left is the stage hard bound, and while
// a producer waits there rp.mu must be FREE — in the G13 incident, siblings
// blocked on rp.mu were the amplifier that turned one stalled delivery into
// a whole-RPC wedge (and, being mutex waiters rather than gate waiters,
// they were invisible to every sweeper). Red-proof: holding rp.mu across
// the park fails the TryLock.
func TestRowPlane_StageBoundParkReleasesMu(t *testing.T) {
	setStageBounds(t, 3, 1<<20)
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second)
	var cancelled atomic.Bool
	rp.cancelled = func() bool { return cancelled.Load() }

	// Partial 1: def + row stage (2 records, under the bound) — no park.
	if !rp.emitHydratePartial(engine.QueryResult{
		QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "a")},
	}) {
		t.Fatal("first emit failed — staging should absorb a full queue")
	}
	// Partial 2: third record crosses the bound → the emit parks in
	// flushStageLocked.
	done := make(chan bool, 1)
	go func() {
		done <- rp.emitHydratePartial(engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "b")},
		})
	}()
	time.Sleep(50 * time.Millisecond) // let it park
	select {
	case <-done:
		t.Fatal("stage-bound emit returned while the queue was still full — the memory backstop did not park")
	default:
	}
	if !rp.mu.TryLock() {
		t.Fatal("rp.mu is HELD while a flush is parked on a full queue — the cascade wedge shape")
	}
	rp.mu.Unlock()
	cancelled.Store(true)
	select {
	case ok := <-done:
		if ok {
			t.Fatal("parked flush reported success after cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parked flush did not unwind on cancellation")
	}
}

// TestRowPlane_EncoderPanicReleasesMu is the F1 red-proof (parallelism
// audit 2026-07-10): a panic under rp.mu — modeled by a panicking deliver,
// which runs in the same locked section as the encoder — must propagate to
// the caller WITHOUT leaking the mutex. Pre-fix (bare Lock/Unlock in the
// emit* callers) the mutex stayed held forever: every sibling producer then
// wedged on rp.mu where no deliver-timeout applies and no gate cancel
// reaches — the exact v4 convoy, reintroduced through the panic door.
// Red-proof: reverting emitChangesGuarded to bare Lock/Unlock fails the
// TryLock assertion.
func TestRowPlane_EncoderPanicReleasesMu(t *testing.T) {
	var blow atomic.Bool
	blow.Store(true)
	base := newScriptedSink(deliverOK)
	panicSink := func(kind int32, payload []byte) int32 {
		if blow.Load() {
			panic("encoder-path boom (F1)")
		}
		return base.sink(kind, payload)
	}
	rp := newRowPlane(&Server{abiDeliver: panicSink}, 7.0, true, "cg-f1", nil)
	if rp == nil {
		t.Fatal("newRowPlane returned nil")
	}
	rp.timeout = time.Second

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		rp.emitHydratePartial(engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "a")},
		})
	}()
	if recovered == nil {
		t.Fatal("expected the panic to propagate to the emit caller")
	}
	if !rp.mu.TryLock() {
		t.Fatal("rp.mu LEAKED across the panic — sibling producers would wedge on the mutex forever (F1)")
	}
	rp.mu.Unlock()

	// The plane must remain usable once the panic source heals — the
	// recovered batch error must not have poisoned the lock or the stage.
	blow.Store(false)
	if !rp.emitHydratePartial(finalPartial("q1", "b")) {
		t.Fatal("plane unusable after a recovered panic")
	}
}

// TestRowPlane_StageCoalescesWithoutParking pins the v5 congestion
// behavior: records that find the queue FULL stage and RETURN — the
// producer keeps producing instead of sleeping (the v4 poll tax) — and on
// recovery the whole stage ships as ONE ordered kind-5 batch.
func TestRowPlane_StageCoalescesWithoutParking(t *testing.T) {
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second)

	start := time.Now()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if !rp.emitHydratePartial(engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", id)},
		}) {
			t.Fatalf("emit %s failed — staging must absorb a full queue", id)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("5 emits against a full queue took %v — producers must stage and continue, not park", elapsed)
	}
	if got := s.kindSeq(); len(got) != 0 {
		t.Fatalf("deliveries succeeded on a FULL sink: %v", got)
	}

	// Queue recovers: the next emit's opportunistic flush ships everything
	// staged (def + rows a..e + the new row f) as ONE batch, in order.
	s.setMode(deliverOK)
	if !rp.emitHydratePartial(engine.QueryResult{
		QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "f")},
	}) {
		t.Fatal("post-recovery emit failed")
	}
	got := s.kindSeq()
	if len(got) != 1 || got[0] != abiKindBatch {
		t.Fatalf("post-recovery deliveries = %v, want exactly one kind-%d batch", got, abiKindBatch)
	}
	kinds, _ := decodeBatch(t, s.payloadAt(0))
	want := []int32{abiKindGroupDef, abiKindRow, abiKindRow, abiKindRow, abiKindRow, abiKindRow, abiKindRow}
	if len(kinds) != len(want) {
		t.Fatalf("batch sub-records = %v, want %v (def + 6 rows in order)", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("batch sub-record %d = kind %d, want %d (order broke in the stage)", i, kinds[i], want[i])
		}
	}
}

// TestRowPlane_DrainSignalUnparksPromptly pins the ABI v5 event-driven
// wakeup: a park on a full queue must complete promptly once the drain
// broadcast fires (goivm_queue_drained's Go side) — the wakeup is the
// event, not the cancellation tick.
func TestRowPlane_DrainSignalUnparksPromptly(t *testing.T) {
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second)

	done := make(chan bool, 1)
	go func() {
		done <- rp.emitHydratePartial(finalPartial("q1", "a"))
	}()
	time.Sleep(50 * time.Millisecond) // parked (flush or frame wait)
	select {
	case <-done:
		t.Fatal("emit returned before any drain — not parked?")
	default:
	}
	s.setMode(deliverOK)
	tsfnDrain.broadcast()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("emit failed after drain + recovery")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain broadcast did not unpark the delivery")
	}
	// Sequence: the staged records (one batch) then the Final frame.
	got := s.kindSeq()
	if len(got) != 2 || got[0] != abiKindBatch || got[1] != abiKindFrame {
		t.Fatalf("deliveries = %v, want [batch, frame]", got)
	}
}

// TestRowPlane_FlakyQueuePreservesOrder: every distinct payload's FIRST
// attempt returns queue-full (forcing the stage path for each), and the
// delivered stream must still be def, rows..., frame — with the records
// coalesced into one ordered batch. The ordering half of the staging
// design: nothing may overtake the stage.
func TestRowPlane_FlakyQueuePreservesOrder(t *testing.T) {
	s := newScriptedSink(deliverOK)
	s.flaky = true
	rp := wedgeTestPlane(t, s, 10*time.Second)

	for i, id := range []string{"a", "b", "c"} {
		r := engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", id)},
			ChunkIndex: i, Final: i == 2,
		}
		if !rp.emitHydratePartial(r) {
			t.Fatalf("emit %d failed on a flaky-but-live queue", i)
		}
	}
	got := s.kindSeq()
	if len(got) != 2 || got[0] != abiKindBatch || got[1] != abiKindFrame {
		t.Fatalf("delivered kinds = %v, want [batch, frame]", got)
	}
	kinds, _ := decodeBatch(t, s.payloadAt(0))
	want := []int32{abiKindGroupDef, abiKindRow, abiKindRow, abiKindRow}
	if len(kinds) != len(want) {
		t.Fatalf("batch sub-records = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("batch sub-record %d = kind %d, want %d (order broke across retries)", i, kinds[i], want[i])
		}
	}

	// Multi-change partial through the same flaky queue: the all-or-nothing
	// phase-2 path must also hold order through the stage.
	s2 := newScriptedSink(deliverOK)
	s2.flaky = true
	rp2 := wedgeTestPlane(t, s2, 10*time.Second)
	if !rp2.emitHydratePartial(engine.QueryResult{
		QueryID: "q2",
		Changes: []engine.RowChange{rcAdd("q2", "x"), rcAdd("q2", "y")},
		Final:   true,
	}) {
		t.Fatal("multi-change emit failed on a flaky-but-live queue")
	}
	got2 := s2.kindSeq()
	if len(got2) != 2 || got2[0] != abiKindBatch || got2[1] != abiKindFrame {
		t.Fatalf("multi-change kinds = %v, want [batch, frame]", got2)
	}
	kinds2, _ := decodeBatch(t, s2.payloadAt(0))
	want2 := []int32{abiKindGroupDef, abiKindRow, abiKindRow}
	if len(kinds2) != len(want2) {
		t.Fatalf("multi-change batch = %v, want %v", kinds2, want2)
	}
	for i := range want2 {
		if kinds2[i] != want2[i] {
			t.Fatalf("multi-change sub-record %d = kind %d, want %d", i, kinds2[i], want2[i])
		}
	}
}

// TestRowPlane_ClosedUnwindsImmediately: deliverClosed (TSFN torn down) is
// terminal — no staging, no retries, no sleeps, immediate false.
func TestRowPlane_ClosedUnwindsImmediately(t *testing.T) {
	s := newScriptedSink(deliverClosed)
	rp := wedgeTestPlane(t, s, 30*time.Second)
	start := time.Now()
	if rp.emitHydratePartial(engine.QueryResult{
		QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "a")},
	}) {
		t.Fatal("emit reported success on a closed transport")
	}
	if e := time.Since(start); e > time.Second {
		t.Fatalf("closed unwind took %v — it retried a dead transport", e)
	}
	if n := s.calls.Load(); n != 1 {
		t.Fatalf("sink called %d times, want exactly 1 (no retry on closed)", n)
	}
}

// captureDeliverLog swaps deliverLogW for a capture buffer (same pattern as
// captureWedgeLog).
func captureDeliverLog(t *testing.T) *syncBuf {
	t.Helper()
	b := &syncBuf{}
	saved := deliverLogW
	deliverLogW = b
	t.Cleanup(func() { deliverLogW = saved })
	return b
}

// TestPullCredit_FlushBeforeParkPreventsStalemate is the credit-park
// stalemate red-proof (2026-07-10, staging-design audit). The trap: staged
// rows are granted-but-UNDELIVERED — the client's top-up policy grants only
// when outstanding (granted − consumed) falls to the low-water mark, and it
// can never consume rows sitting in the stage. A producer that parks in
// gate.acquire with a non-empty stage therefore deadlocks the demand loop:
// the TSFN drain broadcast wakes only DRAIN waiters, not gate waiters, so
// the stage never flushes and the client never grants — only the 60s pull
// idle sweep would break it, as a spurious stream failure.
//
// This models the client faithfully (grant +W/2 only when DELIVERED rows
// bring outstanding to the low-water mark) and drives acquirePullCredit
// exactly as the pull loop does. Red-proof: replacing acquirePullCredit's
// body with plain gate.acquire() (the pre-fix shape) makes this test time
// out in the stalemate — verified during development.
func TestPullCredit_FlushBeforeParkPreventsStalemate(t *testing.T) {
	const window = 4 // W; lowWater = 2
	const lowWater = window / 2

	s := newScriptedSink(deliverFull) // queue-full episode from the start
	rp := wedgeTestPlane(t, s, 30*time.Second)
	gate := newStreamGate(nil)
	gate.grant(window) // the opening window rides the request params
	rp.setPullGate(gate)

	// Client model: counts DELIVERED rows (records + batch sub-records) off
	// the sink and grants +lowWater whenever outstanding hits the low-water
	// mark — the go-ivm-client.ts policy in miniature. It can only see what
	// actually crossed the boundary; staged rows are invisible to it.
	granted := int64(window)
	consumedFromSink := func() int64 {
		var n int64
		for _, k := range s.kindSeq() {
			if k == abiKindRow {
				n++
			}
		}
		s.mu.Lock()
		for i, k := range s.kinds {
			if k == abiKindBatch {
				kinds, _ := decodeBatch(t, s.payloads[i])
				for _, sk := range kinds {
					if sk == abiKindRow {
						n++
					}
				}
			}
		}
		s.mu.Unlock()
		return n
	}
	stopClient := make(chan struct{})
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		for {
			select {
			case <-stopClient:
				return
			case <-time.After(2 * time.Millisecond):
				if granted-consumedFromSink() <= lowWater {
					granted += lowWater
					gate.grant(lowWater)
				}
			}
		}
	}()
	defer func() { close(stopClient); <-clientDone }()

	// Producer: W row partials against the FULL queue — every credit
	// consumed, every row STAGED (invisible to the client). Then the queue
	// recovers, and partial W+1 needs a credit the client will only grant
	// after it SEES rows.
	done := make(chan bool, 1)
	go func() {
		for i := 0; i < window; i++ {
			if !acquirePullCredit(gate, rp) {
				done <- false
				return
			}
			if !rp.emitHydratePartial(engine.QueryResult{
				QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", fmt.Sprintf("r%d", i))},
			}) {
				done <- false
				return
			}
		}
		// Queue recovers mid-run — but the drain broadcast reaches only
		// DRAIN waiters; a gate waiter would sleep through it (the trap).
		s.setMode(deliverOK)
		tsfnDrain.broadcast()
		// Credit W+1: exhausted → about to park on client demand with a
		// full stage. The fix flushes here; the pre-fix shape parks
		// forever (client never sees the staged rows, never grants).
		if !acquirePullCredit(gate, rp) {
			done <- false
			return
		}
		done <- rp.emitHydratePartial(engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "final")}, Final: true,
		})
	}()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("pull producer failed — stream died instead of flushing before the credit park")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CREDIT-PARK STALEMATE: producer parked on client demand with staged rows — " +
			"the client cannot consume undelivered rows, so no grant ever arrives " +
			"(pre-fix shape; only the 60s idle sweep would break this, as a spurious stream failure)")
	}
	// Everything must have shipped: W staged rows in a batch + the final
	// row + Final frame, in order.
	if got := consumedFromSink(); got != window+1 {
		t.Fatalf("delivered rows = %d, want %d (staged rows lost?)", got, window+1)
	}
}

// TestRowPlane_MultiParkerWake_NoEmptyBatch pins the multi-parker wake
// (review LOW, 2026-07-10): two producers parked in a stage flush, queue
// recovers, the FIRST to re-acquire mu ships the ENTIRE stage (flushes are
// whole-stage atomic) — the second must recognize its work is already done
// and return, NOT deliver the now-empty stage as a zero-record kind-5
// (a wasted TSFN slot during exactly the congestion window slots are
// scarce, a phantom napiBatchFlushes bump, and — queue still FULL — a
// producer re-parking to deliver nothing). Red-proof: removing
// flushStageLocked's loop-top emptiness re-check makes the loser ship an
// empty batch — verified during development.
func TestRowPlane_MultiParkerWake_NoEmptyBatch(t *testing.T) {
	setStageBounds(t, 2, 1<<20) // def+row crosses the bound → park in flush
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second)

	// Two producers of the same RPC (distinct queryIDs → distinct defs),
	// both against the FULL queue: each stages its records, crosses the
	// bound, and parks in flushStageLocked (mu released while parked, so
	// they interleave).
	var wg sync.WaitGroup
	results := make([]bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			q := fmt.Sprintf("q%d", idx+1)
			results[idx] = rp.emitHydratePartial(engine.QueryResult{
				QueryID: q, Changes: []engine.RowChange{rcAdd(q, "a")},
			})
		}(i)
	}
	time.Sleep(100 * time.Millisecond) // both demonstrably parked (10ms retry slices)

	// Queue recovers; both wake and race for mu. The winner ships the
	// whole stage; the loser's stage view is empty.
	s.setMode(deliverOK)
	tsfnDrain.broadcast()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("producers did not complete after queue recovery")
	}
	for i, ok := range results {
		if !ok {
			t.Fatalf("producer %d reported stream death on a recovered queue", i)
		}
	}

	// THE pin: no zero-record kind-5 anywhere; exactly one real batch.
	var batches, emptyBatches, defs, rows int
	s.mu.Lock()
	for i, k := range s.kinds {
		switch k {
		case abiKindBatch:
			batches++
			kinds, _ := decodeBatch(t, s.payloads[i])
			if len(kinds) == 0 {
				emptyBatches++
			}
			for _, sk := range kinds {
				switch sk {
				case abiKindGroupDef:
					defs++
				case abiKindRow:
					rows++
				}
			}
		case abiKindGroupDef:
			defs++
		case abiKindRow:
			rows++
		}
	}
	s.mu.Unlock()
	if emptyBatches != 0 {
		t.Fatalf("%d empty kind-5 batch(es) delivered — the losing parker shipped the already-flushed stage", emptyBatches)
	}
	if batches != 1 {
		t.Fatalf("batches = %d, want exactly 1 (the winner's whole-stage flush)", batches)
	}
	if defs != 2 || rows != 2 {
		t.Fatalf("delivered defs=%d rows=%d, want 2/2 (records lost or duplicated)", defs, rows)
	}
}
