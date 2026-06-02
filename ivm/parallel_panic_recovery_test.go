package ivm_test

// Regression coverage for the goroutine-panic crash fixed in parallel.go.
//
// Background (e9d4946): Take's stale-bound path was converted from a string
// panic to *DriftError so engine.Advance's outer recover could catch it and
// return a recoverable drift result instead of crashing the sidecar. That
// contract was silently broken in the parallel push path: GenPushParallel
// fans out into goroutines, and a panic on a Go goroutine is fatal — the
// runtime aborts before any caller's recover can run. With sustained-load
// Go-primary mode + GO_IVM_PARALLEL_THRESHOLD=2 default, the very workload
// the e9d4946 commit was supposed to make survivable instead became a
// guaranteed sidecar crash for every CG.
//
// These tests verify that:
//   1. *DriftError raised inside a parallel goroutine is recovered locally
//      and re-raised on the caller's goroutine so engine.Advance's outer
//      recover catches it.
//   2. Non-Drift panics (programmer bugs) are ALSO recovered locally and
//      re-raised — they must abort the process, but on the caller's
//      goroutine where the abort is intentional, not the spawned one
//      where it crashes the whole runtime.
//   3. When multiple goroutines panic in the same fan-out, non-Drift
//      takes priority over Drift — a real programmer bug must not be
//      masked by a concurrent drift signal.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// panickingOutput.Push panics with a caller-chosen value. Used to inject
// panics into specific connections' downstream pipelines.
type panickingOutput struct {
	panicWith any
}

func (p *panickingOutput) Push(change ivm.Change, pusher ivm.InputBase) []ivm.Change {
	panic(p.panicWith)
}

// TestParallelPush_DriftErrorRecovered confirms that a *DriftError panic
// inside a fan-out goroutine surfaces as a *DriftError on the caller's
// goroutine. Without recover, this test would terminate the test binary
// instead of being catchable here.
func TestParallelPush_DriftErrorRecovered(t *testing.T) {
	src := newTestSource()
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "a", "age": float64(1)}))

	// Two connections so parallel fan-out triggers (default threshold = 2).
	// First connection: well-behaved (records the change). Second: panics
	// with *DriftError, simulating Take's stale-bound condition.
	c1 := src.Connect(nil, nil, nil)
	c1.SetOutput(&collectOutput{})
	c2 := src.Connect(nil, nil, nil)
	driftPanic := &ivm.DriftError{
		Table:    "test",
		Op:       "Edit",
		PK:       map[string]ivm.Value{"id": "1"},
		HasCount: 1,
	}
	c2.SetOutput(&panickingOutput{panicWith: driftPanic})

	src.SetParallel(true)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic to propagate to caller's goroutine, got none")
		}
		got, ok := r.(*ivm.DriftError)
		if !ok {
			t.Fatalf("expected *DriftError, got %T: %v", r, r)
		}
		if got != driftPanic {
			t.Errorf("expected the exact DriftError we injected, got different instance: %+v", got)
		}
	}()

	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "b", "age": float64(2)}))
	t.Fatal("expected Push to panic, returned normally")
}

// TestParallelPush_NonDriftPanicRecovered confirms that a programmer-bug
// panic (anything other than *DriftError) in a fan-out goroutine also
// surfaces on the caller's goroutine — preserves the "programmer bugs
// abort the process" signal while preventing runtime fatal-panic-on-
// goroutine that would crash before any outer abort logic runs.
func TestParallelPush_NonDriftPanicRecovered(t *testing.T) {
	src := newTestSource()
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "a", "age": float64(1)}))

	c1 := src.Connect(nil, nil, nil)
	c1.SetOutput(&collectOutput{})
	c2 := src.Connect(nil, nil, nil)
	c2.SetOutput(&panickingOutput{panicWith: "programmer bug: index out of range"})

	src.SetParallel(true)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected programmer-bug panic to propagate, got none")
		}
		// Non-Drift panic must arrive on the caller's goroutine as-is so
		// the process-abort behavior is preserved at the right scope.
		if msg, ok := r.(string); !ok || msg != "programmer bug: index out of range" {
			t.Errorf("expected original string panic, got %T: %v", r, r)
		}
	}()

	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "b", "age": float64(2)}))
	t.Fatal("expected Push to panic, returned normally")
}

// TestParallelPush_NonDriftBeatsDrift confirms the priority rule: when
// multiple connections panic simultaneously, a real programmer bug
// (non-Drift) takes precedence over a drift signal. Drift is by design
// recoverable; programmer bugs are not. Surfacing drift in this case
// would silently swallow the more dangerous signal.
func TestParallelPush_NonDriftBeatsDrift(t *testing.T) {
	src := newTestSource()
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "a", "age": float64(1)}))

	// Three connections, two of which panic — one drift, one programmer
	// bug. Order of activeConns iteration is insertion-order; both panics
	// will be captured before re-raise.
	c1 := src.Connect(nil, nil, nil)
	c1.SetOutput(&panickingOutput{panicWith: &ivm.DriftError{
		Table: "test", Op: "Edit",
		PK: map[string]ivm.Value{"id": "1"}, HasCount: 1,
	}})
	c2 := src.Connect(nil, nil, nil)
	c2.SetOutput(&collectOutput{})
	c3 := src.Connect(nil, nil, nil)
	c3.SetOutput(&panickingOutput{panicWith: "programmer bug must not be masked"})

	src.SetParallel(true)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic, got none")
		}
		// Non-Drift must win. If Drift won, the programmer bug would be
		// silently dropped and the engine would self-heal around it.
		if _, isDrift := r.(*ivm.DriftError); isDrift {
			t.Errorf("Drift should NOT win over programmer-bug panic; "+
				"got Drift: %v", r)
		}
		if msg, ok := r.(string); !ok || msg != "programmer bug must not be masked" {
			t.Errorf("expected programmer-bug string panic, got %T: %v", r, r)
		}
	}()

	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "b", "age": float64(2)}))
	t.Fatal("expected Push to panic, returned normally")
}
