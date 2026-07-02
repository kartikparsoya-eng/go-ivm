package tablesource

// Unit tests for fanOut (parallel_fanout.go) — driven directly with stub
// connections so the concurrency contract is pinned without SQLite in the
// loop (Push-level integration is covered by the engine-level parity test,
// engine/parallel_fanout_parity_test.go, and the full advance suites which
// now run through fanOut on every push):
//
//  1. connections of one group NEVER push concurrently (per-group serial),
//  2. connections of different groups DO push concurrently (rendezvous),
//  3. the returned Changes are in connection-REGISTRATION order regardless
//     of goroutine completion order,
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
	hook func()
	out  []ivm.Change
}

func (o *stubOutput) Push(change ivm.Change, _ ivm.InputBase) []ivm.Change {
	if o.hook != nil {
		o.hook()
	}
	return o.out
}

// fanOutConn builds a connection in `group` whose output returns `marker`
// and runs `hook` during the push.
func fanOutConn(group string, marker []ivm.Change, hook func()) *connection {
	return &connection{
		group:  group,
		output: &stubOutput{hook: hook, out: marker},
	}
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

func mk(id string) []ivm.Change {
	return []ivm.Change{{Type: ivm.ChangeTypeAdd, Node: ivm.Node{Row: ivm.Row{"id": id}}}}
}

// Groups run concurrently (rendezvous proves overlap), same-group conns run
// serially (per-group active counter never exceeds 1), and the return is in
// registration order even though completion order is scrambled.
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
	// b2) to pin fanOut's return contract: GROUP-MAJOR order (a1, a2, b1,
	// b2). Engine-built pipelines never interleave (a query's connections
	// are appended contiguously during its build, so group-major ==
	// registration order there); this artificial shape exists only to make
	// the contract observable.
	conns := []*connection{
		fanOutConn("qa", mk("a1"), guard(&activeA, "qa", true)),
		fanOutConn("qb", mk("b1"), guard(&activeB, "qb", true)),
		fanOutConn("qa", mk("a2"), guard(&activeA, "qa", false)),
		fanOutConn("qb", mk("b2"), guard(&activeB, "qb", false)),
	}

	out := s.fanOut(addChange(), 1, conns)

	got := make([]string, len(out))
	for i, c := range out {
		got[i] = c.Node.Row["id"].(string)
	}
	want := []string{"a1", "a2", "b1", "b2"} // group-major, deterministic
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("return order = %v, want %v", got, want)
	}
	for _, c := range conns {
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
				conns[i] = fanOutConn(g, mk(id), func() {
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
			out := s.fanOut(addChange(), 7, conns)
			if len(out) != len(conns) {
				t.Fatalf("out len = %d, want %d", len(out), len(conns))
			}
			want := []string{"A", "B", "C"}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("execution order = %v, want %v", order, want)
			}
		})
	}
}

// Panic discipline: with one group panicking a *ivm.DriftError, another
// panicking a plain value, and a third succeeding — every group still runs
// to completion (no early abort), the re-raise happens on the CALLER's
// goroutine, and the non-drift panic wins regardless of group position.
func TestFanOutPanicPriorityAndCompleteness(t *testing.T) {
	withFanoutKnobs(t, true, 4)
	s := &Source{}

	var ranC atomic.Bool
	drift := &ivm.DriftError{Table: "t", Op: "Add"}
	conns := []*connection{
		fanOutConn("qa", nil, func() { panic(drift) }),
		fanOutConn("qb", nil, func() { panic("programmer bug") }),
		fanOutConn("qc", mk("c"), func() { ranC.Store(true) }),
	}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		s.fanOut(addChange(), 1, conns)
	}()

	if recovered != "programmer bug" {
		t.Fatalf("recovered = %v, want the non-drift panic to win over drift", recovered)
	}
	if !ranC.Load() {
		t.Fatal("group qc did not run — fanout must complete all groups before re-raising")
	}

	// Drift-only: the drift re-raises, and with TWO drifts the FIRST group
	// in registration order is the one surfaced (determinism).
	drift2 := &ivm.DriftError{Table: "t2", Op: "Remove"}
	conns = []*connection{
		fanOutConn("q1", nil, func() { panic(drift) }),
		fanOutConn("q2", nil, func() { panic(drift2) }),
	}
	recovered = nil
	func() {
		defer func() { recovered = recover() }()
		s.fanOut(addChange(), 2, conns)
	}()
	if recovered != drift {
		t.Fatalf("recovered = %v, want the FIRST registered group's drift", recovered)
	}
}

// Output-less connections are skipped in both paths (mirror of the serial
// loop's `conn.output == nil` guard) and produce no group.
func TestFanOutSkipsUnwiredConnections(t *testing.T) {
	withFanoutKnobs(t, true, 4)
	s := &Source{}
	conns := []*connection{
		{group: "qa"}, // no output — never wired
		fanOutConn("qb", mk("b"), nil),
	}
	out := s.fanOut(addChange(), 3, conns)
	if len(out) != 1 || out[0].Node.Row["id"] != "b" {
		t.Fatalf("out = %+v, want just b's change", out)
	}
	if conns[0].lastPushedEpoch != 0 {
		t.Fatal("unwired conn must not get an epoch bump")
	}
	if s.fanOut(addChange(), 4, []*connection{{group: "x"}}) != nil {
		t.Fatal("all-unwired fanout must return nil")
	}
}
