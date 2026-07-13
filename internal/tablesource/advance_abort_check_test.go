package tablesource

import (
	"slices"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// sentinelAbort is the typed panic payload the tests fire from the installed
// checkpoint — stands in for the sidecar's *advanceAbortedError (the
// tablesource layer is agnostic to the payload type; it just runs the
// closure).
type sentinelAbort struct{ msg string }

// TestAdvanceAbortCheckFiresInsideFetch pins the per-fetch abort checkpoint
// (TS parity: #shouldAdvanceYieldMaybeAbortAdvance runs on every row fetched
// during push processing). An installed check must run INSIDE the fetch path
// — this is what bounds a push re-fetch that emits nothing and therefore
// never crosses the per-change / per-partial check sites (the 2026-07-13
// advance wedge class).
func TestAdvanceAbortCheckFiresInsideFetch(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)

	// Counting check: fetches run it.
	calls := 0
	src.SetAdvanceAbortCheck(func() { calls++ })
	got := slices.Collect(in.Fetch(ivm.FetchRequest{}))
	if len(got) != 3 {
		t.Fatalf("fetch returned %d rows, want 3", len(got))
	}
	if calls == 0 {
		t.Fatal("installed abort check was never invoked during fetch")
	}

	// Panicking check: the abort propagates out of the fetch (the sidecar's
	// recover ladder turns the typed payload into rpcCodeAdvanceAborted →
	// TS 'advancement-timeout' reset).
	src.SetAdvanceAbortCheck(func() { panic(&sentinelAbort{msg: "budget exceeded"}) })
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected the abort panic to propagate out of Fetch")
			}
			if _, ok := r.(*sentinelAbort); !ok {
				t.Fatalf("expected *sentinelAbort payload, got %T: %v", r, r)
			}
		}()
		_ = slices.Collect(in.Fetch(ivm.FetchRequest{}))
	}()

	// Cleared: fetch runs normally again (hydrate / next advance must not
	// observe a stale checkpoint).
	src.SetAdvanceAbortCheck(nil)
	got = slices.Collect(in.Fetch(ivm.FetchRequest{}))
	if len(got) != 3 {
		t.Fatalf("fetch after clear returned %d rows, want 3", len(got))
	}
}
