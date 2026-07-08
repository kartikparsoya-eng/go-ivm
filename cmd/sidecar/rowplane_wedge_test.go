package main

// Pins for the ABI v4 cancellable-delivery contract (rowplane.go) — the fix
// for the G13 CG wedge, root-caused from the wedge watchdog's stack dumps:
// the pre-v4 deliver BLOCKED inside cgo on a full TSFN queue (starved JS
// event loop) while holding rp.mu — uncancellable, invisible to the pull
// idle sweeper (gate waiters==0), wedging wg.Wait → the CG worker →
// every subsequent RPC for the cgID in ~122s lockstep. Post-v4:
//   - a delivery parked on a full queue unwinds PROMPTLY on cancellation
//     (group teardown or pull-gate cancel — the client's timeout crossing
//     the boundary as goivm_stream_cancel);
//   - a park with no cancel signal is bounded by the deliver timeout and
//     emits the [GO-IVM][DELIVER-TIMEOUT] incident marker;
//   - rp.mu is NEVER held across the park (the cascade killer — sibling
//     producers keep flowing);
//   - a flaky (full-then-ok) queue preserves per-producer delivery order;
//   - deliverClosed unwinds immediately with no retry.
//
// Red-proof protocol (mechanism-level, since the pre-fix code cannot
// compile these tests — the ABI signature changed): disabling the
// cancellation check in retryDeliver must fail the cancel tests; holding
// rp.mu across the park in sendLocked must fail the mutex-release test.
// Both were run and verified during development.

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/engine"
)

// scriptedSink is a deliver fake whose per-call status is driven by the
// test: mode=full/closed/ok, plus a "full until released" latch and a
// flaky "first attempt of each payload is full" mode.
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

func wedgeTestPlane(t *testing.T, s *scriptedSink, timeout time.Duration) *rowPlane {
	t.Helper()
	rp := newRowPlane(&Server{abiDeliver: s.sink}, 7.0, true, "cg-wedge", nil)
	if rp == nil {
		t.Fatal("newRowPlane returned nil")
	}
	rp.timeout = timeout
	return rp
}

// TestRowPlane_FullQueueUnwindsOnCancel: a delivery parked on a perpetually
// full queue must return false PROMPTLY once the plane's cancellation flips
// — the group-teardown unpark. Pre-v4 this park was a blocked cgo call that
// nothing could interrupt (the wedge); red-proof: removing retryDeliver's
// cancelled check makes this park until the 30s timeout → test failure.
func TestRowPlane_FullQueueUnwindsOnCancel(t *testing.T) {
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second) // deadline must NOT be the unpark
	var cancelled atomic.Bool
	rp.cancelled = func() bool { return cancelled.Load() }

	done := make(chan bool, 1)
	go func() {
		done <- rp.emitHydratePartial(engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "a")},
		})
	}()
	time.Sleep(50 * time.Millisecond) // let it park in the retry loop
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
// load-bearing because a producer parked in deliver-retry is NOT a gate
// waiter (waiters==0), so the gate's cond broadcast alone cannot reach it.
func TestRowPlane_GateCancelUnparksDeliver(t *testing.T) {
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second)
	gate := newStreamGate(nil)
	rp.setPullGate(gate)

	done := make(chan bool, 1)
	go func() {
		done <- rp.emitHydratePartial(engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "a")},
		})
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
	ok := rp.emitHydratePartial(engine.QueryResult{
		QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "a")},
	})
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

// TestRowPlane_StalledDeliverReleasesMu is the cascade killer pin: while
// one producer is parked in the retry loop, rp.mu must be FREE — in the
// incident, siblings blocked on rp.mu were the amplifier that turned one
// stalled delivery into a whole-RPC wedge (and being mutex waiters, not
// gate waiters, they were invisible to every sweeper). Red-proof: holding
// rp.mu across the park (pre-fix shape) fails the TryLock.
func TestRowPlane_StalledDeliverReleasesMu(t *testing.T) {
	s := newScriptedSink(deliverFull)
	rp := wedgeTestPlane(t, s, 30*time.Second)
	var cancelled atomic.Bool
	rp.cancelled = func() bool { return cancelled.Load() }

	done := make(chan bool, 1)
	go func() {
		done <- rp.emitHydratePartial(engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", "a")},
		})
	}()
	// Wait until the producer is demonstrably parked in retry (>=2 sink
	// calls: the in-lock attempt + at least one retry).
	deadline := time.Now().Add(5 * time.Second)
	for s.calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("producer never reached the retry loop")
		}
		time.Sleep(time.Millisecond)
	}
	if !rp.mu.TryLock() {
		t.Fatal("rp.mu is HELD while a delivery is parked on a full queue — the cascade wedge shape")
	}
	rp.mu.Unlock()
	cancelled.Store(true)
	<-done
}

// TestRowPlane_FlakyQueuePreservesOrder: every payload's FIRST attempt
// returns queue-full (forcing the copy + park + retry path for each), and
// the per-producer delivery order must still be def, rows..., frame — the
// ordering half of the lock-release design (a payload either enqueues or
// its producer parks; nothing overtakes).
func TestRowPlane_FlakyQueuePreservesOrder(t *testing.T) {
	s := newScriptedSink(deliverOK)
	s.flaky = true
	rp := wedgeTestPlane(t, s, 10*time.Second)

	for i, id := range []string{"a", "b", "c"} {
		final := i == 2
		r := engine.QueryResult{
			QueryID: "q1", Changes: []engine.RowChange{rcAdd("q1", id)},
			ChunkIndex: i, Final: final,
		}
		if !rp.emitHydratePartial(r) {
			t.Fatalf("emit %d failed on a flaky-but-live queue", i)
		}
	}
	want := []int32{abiKindGroupDef, abiKindRow, abiKindRow, abiKindRow, abiKindFrame}
	got := s.kindSeq()
	if len(got) != len(want) {
		t.Fatalf("delivered kinds = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivery %d = kind %d, want %d (order broke across retries)\nfull: %v", i, got[i], want[i], got)
		}
	}

	// Multi-change partial through the same flaky queue: all-or-nothing
	// phase 2 must also hold order.
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
	want2 := []int32{abiKindGroupDef, abiKindRow, abiKindRow, abiKindFrame}
	got2 := s2.kindSeq()
	if len(got2) != len(want2) {
		t.Fatalf("multi-change kinds = %v, want %v", got2, want2)
	}
	for i := range want2 {
		if got2[i] != want2[i] {
			t.Fatalf("multi-change delivery %d = kind %d, want %d", i, got2[i], want2[i])
		}
	}
}

// TestRowPlane_ClosedUnwindsImmediately: deliverClosed (TSFN torn down) is
// terminal — no retries, no sleeps, immediate false.
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
