package ivm

// Regression tests for intra-batch lazy-prev resolution (faithfulness #3).
//
// TS's diff iterates lazily DURING the advance: prevValues are re-read from
// the prev snapshot after earlier writeChanges in the same batch mutated it
// (snapshotter.ts:519-544), so a second change touching the same PK carries
// the batch-current prev value. Go's diff collects eagerly from the clean
// prev, and pre-fix only compensated for removed (BUG 1/1b) and re-added
// (BUG 1c) PKs — a PK EDITED earlier in the batch passed its stale
// pre-batch value through:
//   - a second Edit carried OldRow = the pre-batch row (TS: Edit(cur))
//   - a Remove after an Edit carried the pre-batch row (TS: Remove(cur))
//   - the split-edit decision compared split keys against the stale OldRow
// All silent (no panic), observable downstream through filter old-row
// predicates, sort position math, and split-vs-no-split change shapes.

import (
	"testing"
)

func batchLazySource(t *testing.T) (*MemorySource, Row, Row) {
	t.Helper()
	ms := NewMemorySource("t", map[string]string{"id": "string", "val": "string"}, []string{"id"})
	rowA := Row{"id": "x", "val": "A"}
	rowC := Row{"id": "x", "val": "C"}
	// Hydrate in its own batch, exactly like production (ClearBatchState
	// fires at every advance-batch boundary via engine.signalAdvanceEnd).
	ms.Push(SourceChange{Type: ChangeTypeAdd, Row: rowA})
	ms.ClearBatchState()
	return ms, rowA, rowC
}

// Two SET changelog entries for one rowKey produce two Edits both carrying
// the eager-collected (clean-prev) OldRow. TS's second push is Edit(C, C);
// pre-fix Go pushed Edit(C, A).
func TestPushEditAfterEditRewritesOldRow(t *testing.T) {
	ms, rowA, rowC := batchLazySource(t)
	si := ms.Connect(nil, nil, nil)
	rec := &testOutput{}
	si.SetOutput(rec)

	ms.Push(MakeSourceChangeEdit(rowC, rowA))
	ms.Push(MakeSourceChangeEdit(rowC, rowA))

	if len(rec.changes) != 2 {
		t.Fatalf("got %d pushed changes, want 2", len(rec.changes))
	}
	second := rec.changes[1]
	if second.Type != ChangeTypeEdit {
		t.Fatalf("second change type = %d, want Edit", second.Type)
	}
	if got := second.OldNode.Row["val"]; got != "C" {
		t.Fatalf("second Edit OldRow.val = %v, want C (batch-current prev value)", got)
	}
}

// UPDATE then DELETE in one batch: the DELETE entry's eager prev read is the
// pre-batch row. TS re-reads prev.getRow → the edited value and pushes
// Remove(C); pre-fix Go pushed Remove(A).
func TestPushRemoveAfterEditRewritesRow(t *testing.T) {
	ms, rowA, rowC := batchLazySource(t)
	si := ms.Connect(nil, nil, nil)
	rec := &testOutput{}
	si.SetOutput(rec)

	ms.Push(MakeSourceChangeEdit(rowC, rowA))
	ms.Push(MakeSourceChangeRemove(rowA))

	if len(rec.changes) != 2 {
		t.Fatalf("got %d pushed changes, want 2", len(rec.changes))
	}
	second := rec.changes[1]
	if second.Type != ChangeTypeRemove {
		t.Fatalf("second change type = %d, want Remove", second.Type)
	}
	if got := second.Node.Row["val"]; got != "C" {
		t.Fatalf("Remove row.val = %v, want C (batch-current prev value)", got)
	}
}

// Remove → re-Add → Remove(stale) in one batch: last write wins, so the
// final Remove must carry the re-added value. Pre-fix the removedInBatch
// set never un-flagged the PK, but the has() guard let the stale Remove(A)
// through untouched.
func TestPushRemoveAfterReAddRewritesRow(t *testing.T) {
	ms, rowA, _ := batchLazySource(t)
	rowA2 := Row{"id": "x", "val": "A2"}
	si := ms.Connect(nil, nil, nil)
	rec := &testOutput{}
	si.SetOutput(rec)

	ms.Push(MakeSourceChangeRemove(rowA))
	ms.Push(MakeSourceChangeAdd(rowA2))
	ms.Push(MakeSourceChangeRemove(rowA)) // stale eager-collected value

	if len(rec.changes) != 3 {
		t.Fatalf("got %d pushed changes, want 3", len(rec.changes))
	}
	third := rec.changes[2]
	if third.Type != ChangeTypeRemove {
		t.Fatalf("third change type = %d, want Remove", third.Type)
	}
	if got := third.Node.Row["val"]; got != "A2" {
		t.Fatalf("Remove row.val = %v, want A2 (batch-current prev value)", got)
	}
}

// Pin: Remove → re-Add stays an Add (TS: prevValues=[] at iteration time →
// Add). The Add→Edit rewrite must only fire when the batch-current state
// HOLDS a row for the PK.
func TestPushAddAfterRemoveStaysAdd(t *testing.T) {
	ms, rowA, _ := batchLazySource(t)
	rowA2 := Row{"id": "x", "val": "A2"}
	si := ms.Connect(nil, nil, nil)
	rec := &testOutput{}
	si.SetOutput(rec)

	ms.Push(MakeSourceChangeRemove(rowA))
	ms.Push(MakeSourceChangeAdd(rowA2))

	if len(rec.changes) != 2 {
		t.Fatalf("got %d pushed changes, want 2", len(rec.changes))
	}
	if rec.changes[1].Type != ChangeTypeAdd {
		t.Fatalf("second change type = %d, want Add (not rewritten to Edit)", rec.changes[1].Type)
	}
}

// The split-edit decision must run on the RESOLVED change: TS's source-level
// split (memory-source.ts:452-506) receives the Edit with the lazily-read
// OldRow, so the second Edit(C, C) has equal split keys and does NOT split.
// Pre-fix Go compared C vs stale A → split → Remove+Add on the wire (a
// divergent change shape).
func TestPushSplitEditDecisionUsesBatchCurrentOldRow(t *testing.T) {
	ms, rowA, rowC := batchLazySource(t)
	si := ms.Connect(nil, nil, map[string]bool{"val": true})
	rec := &testOutput{}
	si.SetOutput(rec)

	ms.Push(MakeSourceChangeEdit(rowC, rowA)) // splits: val A→C changed
	ms.Push(MakeSourceChangeEdit(rowC, rowA)) // resolved to Edit(C,C): must NOT split

	// First push splits into Remove(A)+Add(C); the second must be a single
	// Edit — 3 changes total, not 4.
	if len(rec.changes) != 3 {
		t.Fatalf("got %d pushed changes, want 3 (Remove, Add, Edit)", len(rec.changes))
	}
	if rec.changes[2].Type != ChangeTypeEdit {
		t.Fatalf("third change type = %d, want Edit (second push must not split)", rec.changes[2].Type)
	}
	if got := rec.changes[2].OldNode.Row["val"]; got != "C" {
		t.Fatalf("third change OldRow.val = %v, want C", got)
	}
}

// Same as TestPushEditAfterEditRewritesOldRow but through the parallel
// fan-out path (genPushAndWriteParallel).
func TestPushParallelEditAfterEditRewritesOldRow(t *testing.T) {
	ms, rowA, rowC := batchLazySource(t)
	ms.SetParallel(true, 1)
	si := ms.Connect(nil, nil, nil)
	rec := &testOutput{}
	si.SetOutput(rec)

	ms.Push(MakeSourceChangeEdit(rowC, rowA))
	ms.Push(MakeSourceChangeEdit(rowC, rowA))

	if len(rec.changes) != 2 {
		t.Fatalf("got %d pushed changes, want 2", len(rec.changes))
	}
	second := rec.changes[1]
	if second.Type != ChangeTypeEdit {
		t.Fatalf("second change type = %d, want Edit", second.Type)
	}
	if got := second.OldNode.Row["val"]; got != "C" {
		t.Fatalf("second Edit OldRow.val = %v, want C (batch-current prev value)", got)
	}
}
