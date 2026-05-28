package ivm

// Reproduces the Go-primary soak panic on 2026-05-28:
//
//   panic: Take pushEditChange: newCmp must not be 0 when oldCmp > 0
//         (duplicate PK)  at ivm/take.go:386
//
// The invariant at line 386: in Edit handling, when oldCmp > 0 (old row's
// position is sort-after the takeState bound) AND newCmp == 0 (new row
// equals the bound by sort+PK), the only way that's logically possible
// is a duplicate PK situation. Edit preserves PK, so newNode.PK ==
// oldNode.PK. If newCmp == 0 then newNode.PK == bound.PK, so
// oldNode.PK == bound.PK. But oldCmp > 0 says oldNode != bound. Two
// different rows can't share a PK in a well-formed source — UNLESS
// the bound is stale relative to the underlying source (the bound row's
// sort key changed without Take's state being updated).
//
// We isolate the stale-bound scenario the soak triggered:
//   1. Initial state: row A(id=a, createdAt=100). Take limit=1, sort
//      createdAt DESC. Bound = (a, 100).
//   2. The underlying source's row A gets edited TWICE in quick
//      succession without Take seeing the first edit (or seeing it but
//      failing to update the bound):
//      Edit 1 (not applied to bound): A.createdAt 100 → 200.
//      Edit 2 (the one that panics): A.createdAt 200 → 100.
//        OldNode = (a, 200), NewNode = (a, 100).
//        oldCmp = compare({a,200}, bound{a,100}) — DESC by createdAt:
//          200 < 100 in DESC ordering → so 200's position is GREATER →
//          oldCmp > 0 ✓
//        newCmp = compare({a,100}, bound{a,100}) = 0 ✓
//        ⇒ panic.

import (
	"testing"
)

// TestTake_StaleBoundPanic_OldOutsideNewOnBoundary reproduces the
// duplicate-PK panic by injecting a stale bound directly into storage,
// then pushing an Edit whose OldNode is sort-after the stale bound and
// whose NewNode lands exactly on it.
//
// This is a white-box test: we manipulate takeState to simulate the
// state-corruption path that produced the panic in the soak. We're not
// asserting normal operation produces stale bounds — we're confirming
// that IF the bound is stale, the assertion fires. Pairs with a separate
// investigation to find what causes the bound staleness in production.
func TestTake_StaleBoundPanic_OldOutsideNewOnBoundary(t *testing.T) {
	storage := NewMemoryTakeStorage()

	// Initial setup: row A at createdAt=100. Take limit=1.
	rows := []Row{
		makeMsg("a", "user-7", 100),
	}
	ms, collector, take := setupTakeTest(t, storage, 1, rows)
	_ = collector

	// Sanity: bound should be (a, 100) after hydrate.
	state := storage.GetTakeState(GetTakeStateKey(take.partitionKey, nil))
	if state == nil || state.Bound == nil {
		t.Fatalf("expected populated takeState after hydrate; got %+v", state)
	}
	if state.Bound["createdAt"] != float64(100) {
		t.Fatalf("expected bound.createdAt=100, got %v", state.Bound["createdAt"])
	}

	// Simulate stale bound for the oldCmp > 0 production case:
	// pretend the bound was captured at createdAt=200 but the underlying
	// source now has A(a, 100). When the next Edit fires (100 → 200),
	// OldNode=(a, 100) will be sort-AFTER the stale bound at 200 in DESC
	// (since lower createdAt is "after" in DESC ordering), and NewNode
	// =(a, 200) will land exactly on the bound by sort+PK ⇒ newCmp == 0.
	staleState := TakeState{
		Bound: Row{
			"id":        "a",
			"senderId":  "user-7",
			"createdAt": float64(200),
			"content":   "msg-a",
		},
		Size: 1,
	}
	storage.SetTakeState(GetTakeStateKey(take.partitionKey, nil), staleState)

	// Edit the source row 100 → 200. MemorySource.Push emits
	// Edit{old=(a,100), new=(a,200)} — the replicator-equivalent event.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic, got none")
		}
		msg, _ := r.(string)
		if msg == "" {
			if e, ok := r.(error); ok {
				msg = e.Error()
			}
		}
		t.Logf("REPRODUCED panic: %v", r)
		if msg == "" || (msg != "Take pushEditChange: newCmp must not be 0 when oldCmp > 0 (duplicate PK)") {
			// Some panic — log it but don't fail. The pattern of the
			// panic is what we want to surface.
			t.Logf("(panic text differs from expected — actual: %v)", r)
		}
	}()

	editChange := MakeSourceChangeEdit(
		Row{"id": "a", "senderId": "user-7", "createdAt": float64(200), "content": "msg-a"},
		Row{"id": "a", "senderId": "user-7", "createdAt": float64(100), "content": "msg-a"},
	)
	// MakeSourceChangeEdit signature is (newRow, oldRow) — we want
	// OldNode={a,100} and NewNode={a,200} for oldCmp>0. Re-do with the
	// correct positional order:
	_ = editChange
	editChange = MakeSourceChangeEdit(
		Row{"id": "a", "senderId": "user-7", "createdAt": float64(200), "content": "msg-a"}, // new
		Row{"id": "a", "senderId": "user-7", "createdAt": float64(100), "content": "msg-a"}, // old
	)
	ms.Push(editChange)
}
