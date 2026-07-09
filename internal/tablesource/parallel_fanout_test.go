package tablesource

// Unit tests for fanOut (parallel_fanout.go) — driven directly with stub
// connections so the concurrency contract is pinned without SQLite in the
// loop (Push-level integration is covered by the engine-level parity test,
// engine/parallel_fanout_parity_test.go, and the full advance suites which
// now run through fanOut on every push):
//
//  1. connections of one group NEVER push concurrently (per-group serial),
//  2. connections of different groups DO push concurrently (rendezvous),
//  3. every wired connection is pushed exactly once — fanOut is void (data
//     rides the engine's terminal sink), so delivery is asserted on the
//     per-connection stubs,
//  4. panic discipline: every group still runs, non-drift beats drift on
//     re-raise, group-order determinism, and the caller's goroutine (not a
//     spawned one) observes the panic,
//  5. knob-off / single-group inputs take the strict serial path.

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// stubOutput records pushes and runs an optional hook while "active".
type stubOutput struct {
	hook   func()
	pushed atomic.Int32
}

func (o *stubOutput) Push(change ivm.Change, _ ivm.InputBase) {
	if o.hook != nil {
		o.hook()
	}
	o.pushed.Add(1)
}

// fanOutConn builds a connection in `group` that runs `hook` during the
// push and counts deliveries.
func fanOutConn(group string, hook func()) *connection {
	return &connection{
		group:  group,
		output: &stubOutput{hook: hook},
	}
}

func pushedCount(c *connection) int32 {
	return c.output.(*stubOutput).pushed.Load()
}

func addChange() ivm.SourceChange {
	return ivm.MakeSourceChangeAdd(ivm.Row{"id": "r1"})
}

func withFanoutKnobs(t *testing.T, parallel bool, workers int) {
	t.Helper()
	prevP, prevW := ParallelAdvance, ParallelAdvanceWorkers
	t.Cleanup(func() { ParallelAdvance, ParallelAdvanceWorkers = prevP, prevW })
	ParallelAdvance, ParallelAdvanceWorkers = parallel, workers
}

func TestAdvanceParallelismFromEnv(t *testing.T) {
	clearEnv := func(t *testing.T) {
		t.Setenv("GO_IVM_ADVANCE_PARALLELISM", "")
		t.Setenv("GO_IVM_PARALLELISM", "")
	}

	t.Run("default", func(t *testing.T) {
		clearEnv(t)
		if got := advanceParallelismFromEnv(); got != 4 {
			t.Fatalf("advanceParallelismFromEnv() = %d, want 4", got)
		}
	})

	t.Run("advance-specific knob", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("GO_IVM_ADVANCE_PARALLELISM", "7")
		if got := advanceParallelismFromEnv(); got != 7 {
			t.Fatalf("advanceParallelismFromEnv() = %d, want 7", got)
		}
	})

	t.Run("legacy fallback", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("GO_IVM_PARALLELISM", "5")
		if got := advanceParallelismFromEnv(); got != 5 {
			t.Fatalf("advanceParallelismFromEnv() = %d, want 5", got)
		}
	})

	t.Run("advance-specific wins over legacy", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("GO_IVM_PARALLELISM", "5")
		t.Setenv("GO_IVM_ADVANCE_PARALLELISM", "7")
		if got := advanceParallelismFromEnv(); got != 7 {
			t.Fatalf("advanceParallelismFromEnv() = %d, want 7", got)
		}
	})
}

// Groups run concurrently (rendezvous proves overlap) and same-group conns
// run serially (per-group active counter never exceeds 1).
func TestFanOutParallelAcrossGroupsSerialWithin(t *testing.T) {
	withFanoutKnobs(t, true, 4)
	s := &Source{}

	// Rendezvous: both groups must be inside a push at the same time or the
	// release never fires — a serial execution FAILS via the 10s timeout
	// instead of passing vacuously.
	var arrivals sync.WaitGroup
	arrivals.Add(2)
	release := make(chan struct{})
	go func() {
		arrivals.Wait()
		close(release)
	}()
	rendezvous := func() {
		arrivals.Done()
		select {
		case <-release:
		case <-time.After(10 * time.Second):
			panic("rendezvous timeout: groups did not run concurrently")
		}
	}

	// Per-group in-flight counters: >1 means same-group conns overlapped.
	var activeA, activeB atomic.Int32
	guard := func(counter *atomic.Int32, name string, alsoRendezvous bool) func() {
		first := true
		var mu sync.Mutex
		return func() {
			if counter.Add(1) > 1 {
				panic("same-group connections pushed concurrently: " + name)
			}
			mu.Lock()
			f := first
			first = false
			mu.Unlock()
			if alsoRendezvous && f {
				rendezvous()
			}
			time.Sleep(2 * time.Millisecond) // widen any overlap window
			counter.Add(-1)
		}
	}

	// Registration order deliberately INTERLEAVES the groups (a1, b1, a2,
	// b2) so the grouping logic (group-major partition) is exercised on a
	// shape the engine never produces (a query's connections are appended
	// contiguously during its build).
	conns := []*connection{
		fanOutConn("qa", guard(&activeA, "qa", true)),
		fanOutConn("qb", guard(&activeB, "qb", true)),
		fanOutConn("qa", guard(&activeA, "qa", false)),
		fanOutConn("qb", guard(&activeB, "qb", false)),
	}

	s.fanOut(addChange(), 1, conns)

	for i, c := range conns {
		if got := pushedCount(c); got != 1 {
			t.Fatalf("conn[%d] pushed %d times, want exactly 1", i, got)
		}
		if c.lastPushedEpoch != 1 {
			t.Fatalf("lastPushedEpoch not bumped on all conns")
		}
	}
}

// KNOB-OFF and single-group inputs must be STRICTLY serial (global overlap
// counter never exceeds 1) and execute in registration order. (With
// distinct groups the serial fallback also runs group-major, but these
// cases use group-contiguous or single-group registration so execution
// order == registration order, matching how engine builds register.)
func TestFanOutSerialPaths(t *testing.T) {
	cases := []struct {
		name     string
		parallel bool
		groups   []string
	}{
		{"knobOff", false, []string{"qa", "qb", "qc"}},
		{"workers1", true, []string{"qa", "qb", "qc"}},
		{"singleGroup", true, []string{"q1", "q1", "q1"}},
		{"untaggedShareSerialGroup", true, []string{"", "", ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workers := 4
			if tc.name == "workers1" {
				workers = 1
			}
			withFanoutKnobs(t, tc.parallel, workers)
			s := &Source{}

			var active atomic.Int32
			var order []string
			var orderMu sync.Mutex
			conns := make([]*connection, len(tc.groups))
			for i, g := range tc.groups {
				id := string(rune('A' + i))
				conns[i] = fanOutConn(g, func() {
					if active.Add(1) > 1 {
						panic("serial path overlapped")
					}
					orderMu.Lock()
					order = append(order, id)
					orderMu.Unlock()
					time.Sleep(time.Millisecond)
					active.Add(-1)
				})
			}
			s.fanOut(addChange(), 7, conns)
			for i, c := range conns {
				if got := pushedCount(c); got != 1 {
					t.Fatalf("conn[%d] pushed %d times, want exactly 1", i, got)
				}
			}
			want := []string{"A", "B", "C"}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("execution order = %v, want %v", order, want)
			}
		})
	}
}

// Panic discipline: with one group panicking an error, another panicking a
// plain value, and a third succeeding — every group still runs to
// completion (no early abort), the re-raise happens on the CALLER's
// goroutine, and the FIRST group in registration order wins (determinism).
func TestFanOutPanicOrderAndCompleteness(t *testing.T) {
	withFanoutKnobs(t, true, 4)
	s := &Source{}

	var ranC atomic.Bool
	drift := ivm.SourceDriftError("t", "Add", nil, -1)
	conns := []*connection{
		fanOutConn("qa", func() { panic(drift) }),
		fanOutConn("qb", func() { panic("programmer bug") }),
		fanOutConn("qc", func() { ranC.Store(true) }),
	}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		s.fanOut(addChange(), 1, conns)
	}()

	if recovered != drift {
		t.Fatalf("recovered = %v, want the FIRST registered group's panic (drift)", recovered)
	}
	if !ranC.Load() {
		t.Fatal("group qc did not run — fanout must complete all groups before re-raising")
	}

	// Two error panics: the FIRST group in registration order is the one
	// surfaced (determinism).
	drift2 := ivm.SourceDriftError("t2", "Remove", nil, -1)
	conns = []*connection{
		fanOutConn("q1", func() { panic(drift) }),
		fanOutConn("q2", func() { panic(drift2) }),
	}
	recovered = nil
	func() {
		defer func() { recovered = recover() }()
		s.fanOut(addChange(), 2, conns)
	}()
	if recovered != drift {
		t.Fatalf("recovered = %v, want the FIRST registered group's panic", recovered)
	}
}

// Output-less connections are skipped in both paths (mirror of the serial
// loop's `conn.output == nil` guard) and produce no group.
func TestFanOutSkipsUnwiredConnections(t *testing.T) {
	withFanoutKnobs(t, true, 4)
	s := &Source{}
	conns := []*connection{
		{group: "qa"}, // no output — never wired
		fanOutConn("qb", nil),
	}
	s.fanOut(addChange(), 3, conns)
	if got := pushedCount(conns[1]); got != 1 {
		t.Fatalf("wired conn pushed %d times, want exactly 1", got)
	}
	if conns[0].lastPushedEpoch != 0 {
		t.Fatal("unwired conn must not get an epoch bump")
	}
	// All-unwired fanout is a no-op (no groups → early return, no panic).
	unwired := &connection{group: "x"}
	s.fanOut(addChange(), 4, []*connection{unwired})
	if unwired.lastPushedEpoch != 0 {
		t.Fatal("all-unwired fanout must not bump any epoch")
	}
}
