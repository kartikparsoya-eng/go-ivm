package ivm_test

// Regression coverage for the goroutine-panic crash fixed in parallel.go.
//
// Background: a panic on a Go goroutine is fatal — the runtime aborts
// before any caller's recover can run. GenPushParallel fans out into
// goroutines, so any operator assert (Take stale-bound, source drift) or
// programmer bug panicking inside a fan-out goroutine would crash the whole
// process (every CG) instead of surfacing through the sidecar handler's
// recover as an RPC error. With sustained-load Go-primary mode +
// GO_IVM_PARALLEL_THRESHOLD=2 default, that would be a guaranteed process
// crash for a single bad row.
//
// These tests verify that:
//  1. A panic raised inside a parallel goroutine is recovered locally
//     and re-raised on the CALLER's goroutine, preserving the panic value.
//  2. When multiple goroutines panic in the same fan-out, the FIRST in
//     connection-registration order wins — deterministic, so the surfaced
//     panic doesn't flap between runs. (All panic classes now share one
//     disposition — RPC error → CG teardown — so ordering is a
//     determinism concern, not a masking concern.)

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// panickingOutput.Push panics with a caller-chosen value. Used to inject
// panics into specific connections' downstream pipelines.
type panickingOutput struct {
	panicWith any
}

func (p *panickingOutput) Push(change ivm.Change, pusher ivm.InputBase) {
	panic(p.panicWith)
}

// TestParallelPush_ErrorPanicRecovered confirms that an error panic (the
// source-drift / operator-assert class) inside a fan-out goroutine surfaces
// on the caller's goroutine with the exact injected value. Without the
// per-goroutine recover, this test would terminate the test binary instead
// of being catchable here.
func TestParallelPush_ErrorPanicRecovered(t *testing.T) {
	src := newTestSource()
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "a", "age": float64(1)}))

	// Two connections so parallel fan-out triggers (default threshold = 2).
	// First connection: well-behaved (records the change). Second: panics
	// with a source-drift error, simulating Take's stale-bound assert.
	src.SetNextConnectGroup("q1")
	c1 := src.Connect(nil, nil, nil)
	c1.SetOutput(&collectOutput{})
	src.SetNextConnectGroup("q2")
	c2 := src.Connect(nil, nil, nil)
	driftPanic := ivm.SourceDriftError("test", "Edit", map[string]ivm.Value{"id": "1"}, 1)
	c2.SetOutput(&panickingOutput{panicWith: driftPanic})

	src.SetParallel(true)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic to propagate to caller's goroutine, got none")
		}
		got, ok := r.(error)
		if !ok {
			t.Fatalf("expected error panic, got %T: %v", r, r)
		}
		if got != driftPanic {
			t.Errorf("expected the exact error we injected, got different instance: %+v", got)
		}
		if !strings.Contains(got.Error(), "source drift: table=test") {
			t.Errorf("panic message = %q, want source-drift text", got.Error())
		}
	}()

	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "b", "age": float64(2)}))
	t.Fatal("expected Push to panic, returned normally")
}

// TestParallelPush_StringPanicRecovered confirms that a programmer-bug
// panic (raw string) in a fan-out goroutine also surfaces on the caller's
// goroutine as-is — preventing the runtime fatal-panic-on-goroutine that
// would crash before any outer recovery logic runs.
func TestParallelPush_StringPanicRecovered(t *testing.T) {
	src := newTestSource()
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "a", "age": float64(1)}))

	src.SetNextConnectGroup("q1")
	c1 := src.Connect(nil, nil, nil)
	c1.SetOutput(&collectOutput{})
	src.SetNextConnectGroup("q2")
	c2 := src.Connect(nil, nil, nil)
	c2.SetOutput(&panickingOutput{panicWith: "programmer bug: index out of range"})

	src.SetParallel(true)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected programmer-bug panic to propagate, got none")
		}
		// The panic must arrive on the caller's goroutine as-is so the
		// sidecar handler's recover renders it verbatim.
		if msg, ok := r.(string); !ok || msg != "programmer bug: index out of range" {
			t.Errorf("expected original string panic, got %T: %v", r, r)
		}
	}()

	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "b", "age": float64(2)}))
	t.Fatal("expected Push to panic, returned normally")
}

// TestParallelPush_FirstPanicInOrderWins pins the determinism rule: when
// multiple connections panic in one fan-out, the panic from the FIRST
// connection in registration order is the one re-raised. (Every panic class
// now funnels to the same disposition — RPC error → CG teardown — so the
// choice is about deterministic logs, not about masking.)
func TestParallelPush_FirstPanicInOrderWins(t *testing.T) {
	src := newTestSource()
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "a", "age": float64(1)}))

	// Three connections, two of which panic. Order of activeConns
	// iteration is insertion-order; both panics are captured before the
	// re-raise scan.
	src.SetNextConnectGroup("q1")
	c1 := src.Connect(nil, nil, nil)
	c1.SetOutput(&panickingOutput{panicWith: ivm.SourceDriftError(
		"test", "Edit", map[string]ivm.Value{"id": "1"}, 1)})
	src.SetNextConnectGroup("q2")
	c2 := src.Connect(nil, nil, nil)
	c2.SetOutput(&collectOutput{})
	src.SetNextConnectGroup("q3")
	c3 := src.Connect(nil, nil, nil)
	c3.SetOutput(&panickingOutput{panicWith: "later connection's panic"})

	src.SetParallel(true)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic, got none")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("expected connection 1's error panic to win (first in order), got %T: %v", r, r)
		}
		if !strings.Contains(err.Error(), "source drift: table=test") {
			t.Errorf("expected connection 1's source-drift error, got: %v", err)
		}
	}()

	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "b", "age": float64(2)}))
	t.Fatal("expected Push to panic, returned normally")
}
