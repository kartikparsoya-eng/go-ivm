package ivm

// Attempts to reproduce the soak's stale-bound panic naturally by firing
// a bulk-mutation pattern: multiple Edits arriving back-to-back where
// each changes the sort key, and the bound row is one of those rows.
//
// Soak context: panic followed `bulk priority→MEDIUM on tickets in chan-2`.
// In PG that's a single tx of N UPDATEs; the replicator emits N row
// Edits. If TS bumps updatedAt on every UPDATE (typical ORM behavior),
// all N rows get the SAME new updatedAt — a tie that the PK tiebreaker
// must resolve correctly. Multiple Edits in the same WAL frame stress
// the snapshot-rotation timing inside TableSource.Push.

import (
	"fmt"
	"testing"
)

// TestTake_BulkUpdate_AllSortKeysBumpedToSame stresses the case the soak
// triggered: a bulk UPDATE that bumps the sort key (createdAt) on many
// rows to the SAME value at once. Tie-break via PK ASC. We fire the
// Edits sequentially through a single MemorySource (not TableSource —
// this isolates Take's state machine from the TableSource overlay/
// snapshot timing).
//
// If this passes, the bug is TableSource-specific. If it panics, the
// bug is in Take itself and shows up regardless of source.
func TestTake_BulkUpdate_AllSortKeysBumpedToSame(t *testing.T) {
	storage := NewMemoryTakeStorage()
	// 20 messages in user-7, createdAt 1000, 1100, ..., 2900.
	var rows []Row
	for i := 0; i < 20; i++ {
		rows = append(rows, makeMsg(fmt.Sprintf("msg-%02d", i), "user-7", float64(1000+i*100)))
	}
	ms, collector, _ := setupTakeTest(t, storage, 10, rows)

	// Window = top 10 by createdAt desc: msg-19, msg-18, ..., msg-10.
	// Bound = msg-10 at createdAt=2000.

	// Now bulk-update: bump createdAt on msg-00..msg-09 (the rows OUTSIDE
	// the window) all to the SAME timestamp = 3000. After this, those
	// rows should all enter the window and displace msg-10..msg-19.
	defer func() {
		r := recover()
		if r == nil {
			t.Logf("no panic — Take handled bulk update cleanly with %d emitted changes", len(collector.Changes))
			return
		}
		if d, ok := r.(*DriftError); ok {
			t.Logf("EXPECTED ROOT CAUSE REPRO — DriftError: %v", d.Error())
			return
		}
		// String panic — re-raise so the test fails with the panic.
		panic(r)
	}()

	for i := 0; i < 10; i++ {
		oldRow := makeMsg(fmt.Sprintf("msg-%02d", i), "user-7", float64(1000+i*100))
		newRow := makeMsg(fmt.Sprintf("msg-%02d", i), "user-7", float64(3000))
		ms.Push(MakeSourceChangeEdit(newRow, oldRow))
	}
}

// TestTake_BulkUpdate_BoundRowAmongUpdated extends the above: the bound
// row's sort key is also bumped, along with several others, to the same
// new value. This forces Take to choose a new bound from rows that share
// the same sort key (tie-break by PK).
func TestTake_BulkUpdate_BoundRowAmongUpdated(t *testing.T) {
	storage := NewMemoryTakeStorage()
	var rows []Row
	for i := 0; i < 20; i++ {
		rows = append(rows, makeMsg(fmt.Sprintf("msg-%02d", i), "user-7", float64(1000+i*100)))
	}
	ms, collector, _ := setupTakeTest(t, storage, 10, rows)

	// Bound = msg-10. Now bump msg-08, msg-09, msg-10, msg-11, msg-12
	// to the SAME timestamp = 3500. msg-10 (the bound) is among them.
	defer func() {
		r := recover()
		if r == nil {
			t.Logf("no panic — Take handled bound-among-bulk cleanly with %d emitted changes", len(collector.Changes))
			return
		}
		if d, ok := r.(*DriftError); ok {
			t.Logf("EXPECTED ROOT CAUSE REPRO — DriftError: %v", d.Error())
			return
		}
		panic(r)
	}()

	for _, i := range []int{8, 9, 10, 11, 12} {
		oldRow := makeMsg(fmt.Sprintf("msg-%02d", i), "user-7", float64(1000+i*100))
		newRow := makeMsg(fmt.Sprintf("msg-%02d", i), "user-7", float64(3500))
		ms.Push(MakeSourceChangeEdit(newRow, oldRow))
	}
}

// TestTake_BulkUpdate_BoundRowEditedTwice closest to the production panic:
// the bound row gets edited, then a SECOND edit fires for the same row
// with OldRow at the row's PRE-FIRST-EDIT state. This simulates the
// scenario where Edit's OldNode doesn't reflect the most recent state
// (e.g., snapshot-rotation timing skew, batched WAL frame with multiple
// versions of the same row).
func TestTake_BulkUpdate_BoundRowEditedTwice(t *testing.T) {
	storage := NewMemoryTakeStorage()
	rows := []Row{makeMsg("a", "user-7", 100)}
	ms, collector, _ := setupTakeTest(t, storage, 1, rows)

	// First edit: a(100) → a(200). Bound updates to (a, 200).
	ms.Push(MakeSourceChangeEdit(
		makeMsg("a", "user-7", 200), // new
		makeMsg("a", "user-7", 100), // old
	))

	defer func() {
		r := recover()
		if r == nil {
			t.Logf("no panic — back-to-back edits handled cleanly with %d emitted changes", len(collector.Changes))
			return
		}
		if d, ok := r.(*DriftError); ok {
			t.Logf("EXPECTED ROOT CAUSE REPRO — DriftError: %v", d.Error())
			return
		}
		panic(r)
	}()

	// Second edit: replicator-equivalent scenario where the engine
	// receives an Edit whose OldRow is the row's PRE-FIRST-EDIT state
	// (a, 100) — NOT the post-first-edit state (a, 200). This would
	// happen if the engine's prev-row tracking drifts from Take's view.
	ms.Push(MakeSourceChangeEdit(
		makeMsg("a", "user-7", 250), // new
		makeMsg("a", "user-7", 100), // STALE old — should be 200
	))
}
