package tablesource

// Regression tests for intra-batch lazy-prev resolution through the
// production TableSource (faithfulness #3) — see ivm/batch_lazy_prev_test.go
// for the full rationale. TS re-reads prevValues from the mutated prev
// snapshot during the advance (snapshotter.ts:519-544); Go's eager diff
// collects from the clean prev, so the source substitutes batch-current
// values (resolveBatchChangeLocked). Pre-fix, a PK EDITED earlier in the
// batch passed its stale pre-batch value through.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func lazyPrevRows() (rowA, rowC ivm.Row) {
	rowA = ivm.Row{"id": float64(1), "name": "alice", "score": float64(90), "active": true}
	rowC = ivm.Row{"id": float64(1), "name": "alicia", "score": float64(90), "active": true}
	return rowA, rowC
}

// TS's second push is Edit(C, C); pre-fix Go pushed Edit(C, A) — the
// existsLocked probe saw the PK present and skipped the rewrite entirely.
func TestPushEditAfterEditRewritesOldRow_TableSource(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	rowA, rowC := lazyPrevRows()

	in := src.Connect(nil, nil, nil, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	src.Push(ivm.MakeSourceChangeEdit(rowC, rowA))
	src.Push(ivm.MakeSourceChangeEdit(rowC, rowA))

	if len(rec.pushed) != 2 {
		t.Fatalf("got %d pushed changes, want 2", len(rec.pushed))
	}
	second := rec.pushed[1]
	if second.Type != ivm.ChangeTypeEdit {
		t.Fatalf("second change type = %d, want Edit", second.Type)
	}
	if got := second.OldNode.Row["name"]; got != "alicia" {
		t.Fatalf("second Edit OldRow.name = %v, want alicia (batch-current prev value)", got)
	}
}

// UPDATE then DELETE in one batch: TS pushes Remove(edited value); pre-fix
// Go pushed Remove(pre-batch value).
func TestPushRemoveAfterEditRewritesRow_TableSource(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	rowA, rowC := lazyPrevRows()

	in := src.Connect(nil, nil, nil, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	src.Push(ivm.MakeSourceChangeEdit(rowC, rowA))
	src.Push(ivm.MakeSourceChangeRemove(rowA))

	if len(rec.pushed) != 2 {
		t.Fatalf("got %d pushed changes, want 2", len(rec.pushed))
	}
	second := rec.pushed[1]
	if second.Type != ivm.ChangeTypeRemove {
		t.Fatalf("second change type = %d, want Remove", second.Type)
	}
	if got := second.Node.Row["name"]; got != "alicia" {
		t.Fatalf("Remove row.name = %v, want alicia (batch-current prev value)", got)
	}
}

func TestPushRemoveAfterPartialEditCarriesMergedRow_TableSource(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	rowA, _ := lazyPrevRows()
	patch := ivm.Row{"id": float64(1), "name": "alicia"}

	in := src.Connect(nil, nil, nil, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	src.Push(ivm.MakeSourceChangeEdit(patch, rowA))
	src.Push(ivm.MakeSourceChangeRemove(rowA))

	if len(rec.pushed) != 2 {
		t.Fatalf("got %d pushed changes, want 2", len(rec.pushed))
	}
	second := rec.pushed[1]
	if second.Type != ivm.ChangeTypeRemove {
		t.Fatalf("second change type = %d, want Remove", second.Type)
	}
	if got := second.Node.Row["name"]; got != "alicia" {
		t.Fatalf("Remove row.name = %v, want alicia (batch-current prev value)", got)
	}
	if got := second.Node.Row["score"]; got != float64(90) {
		t.Fatalf("Remove row.score = %v, want 90 (merged unchanged value)", got)
	}
	if got := second.Node.Row["active"]; got != true {
		t.Fatalf("Remove row.active = %v, want true (merged unchanged value)", got)
	}
}

// The split decision runs in Source.Push BEFORE fanout and must see the
// resolved OldRow: the second Edit(C, C) has equal split keys → single Edit,
// not Remove+Add. Pre-fix Go split on the stale A-vs-C comparison.
func TestPushSplitEditDecisionUsesBatchCurrent_TableSource(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	rowA, rowC := lazyPrevRows()

	in := src.Connect(nil, nil, nil, map[string]bool{"name": true})
	rec := &recordingOutput{}
	in.SetOutput(rec)

	src.Push(ivm.MakeSourceChangeEdit(rowC, rowA)) // splits: name changed
	src.Push(ivm.MakeSourceChangeEdit(rowC, rowA)) // resolved Edit(C,C): must NOT split

	if len(rec.pushed) != 3 {
		t.Fatalf("got %d pushed changes, want 3 (Remove, Add, Edit)", len(rec.pushed))
	}
	if rec.pushed[2].Type != ivm.ChangeTypeEdit {
		t.Fatalf("third change type = %d, want Edit (second push must not split)", rec.pushed[2].Type)
	}
	if got := rec.pushed[2].OldNode.Row["name"]; got != "alicia" {
		t.Fatalf("third change OldRow.name = %v, want alicia", got)
	}
}
