package tablesource

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestApplyOverlayPartialCursorBoundary guards the exclusive-cursor boundary
// over-include in the overlay start-gate (overlayRowAtOrAfterStart). The
// channelConversationsPaginatedV3 backward page sorts by [createdAt, conversationId]
// but the cursor is PARTIAL ({createdAt} only) with Basis "after" (exclusive). An
// in-flight overlay-ADD whose createdAt EQUALS the cursor must be DROPPED.
//
// With the plain ivm.MakeComparator the gate did CompareValues(row.conversationId,
// nil)=+1 → ranked the boundary row strictly after the cursor → wrongly kept it
// (Go-vs-TS over-include). ivm.MakePartialBoundComparator stops at the missing
// conversationId, ranks it EQUAL, and Basis "after" drops it.
func TestApplyOverlayPartialCursorBoundary(t *testing.T) {
	sort := ivm.Ordering{{"createdAt", "asc"}, {"conversationId", "asc"}}
	pk := []string{"conversationId"}

	// Existing fetched window (rows strictly after the cursor).
	base := []ivm.Node{
		{Row: ivm.Row{"createdAt": float64(1779813950230), "conversationId": "c-later"}},
	}

	// Overlay ADD exactly ON the cursor's createdAt (the boundary row).
	boundaryAdd := ivm.SourceChange{
		Type: ivm.ChangeTypeAdd,
		Row:  ivm.Row{"createdAt": float64(1779813937134), "conversationId": "8ec82a40"},
	}

	partialCursor := func(basis string) *ivm.Start {
		return &ivm.Start{Row: ivm.Row{"createdAt": float64(1779813937134)}, Basis: basis}
	}

	has := func(nodes []ivm.Node, id string) bool {
		for _, n := range nodes {
			if n.Row["conversationId"] == id {
				return true
			}
		}
		return false
	}

	t.Run("after (exclusive) drops the boundary row", func(t *testing.T) {
		cmp := ivm.MakePartialBoundComparator(sort, false)
		out := applyOverlay(append([]ivm.Node(nil), base...), boundaryAdd, cmp, nil, partialCursor("after"), pk)
		if has(out, "8ec82a40") {
			t.Fatalf("Basis 'after' must EXCLUDE the boundary overlay row at the cursor's createdAt; got it included: %v", out)
		}
	})

	t.Run("at (inclusive) keeps the boundary row", func(t *testing.T) {
		cmp := ivm.MakePartialBoundComparator(sort, false)
		out := applyOverlay(append([]ivm.Node(nil), base...), boundaryAdd, cmp, nil, partialCursor("at"), pk)
		if !has(out, "8ec82a40") {
			t.Fatalf("Basis 'at' must INCLUDE the boundary overlay row; got it dropped: %v", out)
		}
	})

	t.Run("regression: the plain full comparator wrongly keeps it (proves the bug)", func(t *testing.T) {
		cmp := ivm.MakeComparator(sort, false) // the pre-fix comparator
		out := applyOverlay(append([]ivm.Node(nil), base...), boundaryAdd, cmp, nil, partialCursor("after"), pk)
		if !has(out, "8ec82a40") {
			t.Skip("full comparator no longer over-includes — bug premise changed")
		}
	})
}

// TestConstraintMatchesRowNullEquality guards the null-constraint equality
// semantics of constraintMatchesRow against the TS reference.
//
// TS constraintMatchesRow (constraint.ts:21) uses valuesEqual, which treats
// null as UNEQUAL to itself (data.ts:112-118): a NULL-valued constraint does
// NOT match a row whose column is also NULL. The Go port previously used
// ivm.CompareValues (nil==nil → 0 → "match"), which over-included such overlay
// rows — a silent Go-vs-TS divergence. The fix switches to ivm.ValuesEqual,
// matching TS and the constraintsAreCompatible convention (flipped_join.go:416).
func TestConstraintMatchesRowNullEquality(t *testing.T) {
	t.Run("NULL constraint does NOT match a NULL column (matches TS valuesEqual)", func(t *testing.T) {
		c := ivm.Constraint{"ownerId": nil}
		row := ivm.Row{"ownerId": nil, "id": "a"}
		if constraintMatchesRow(c, row) {
			t.Fatalf("NULL constraint must NOT match a NULL column (TS constraint.ts:21 valuesEqual); got match")
		}
	})

	t.Run("NULL constraint does NOT match an absent column", func(t *testing.T) {
		c := ivm.Constraint{"ownerId": nil}
		row := ivm.Row{"id": "a"} // ownerId absent → nil
		if constraintMatchesRow(c, row) {
			t.Fatalf("NULL constraint must NOT match an absent column; got match")
		}
	})

	t.Run("non-null constraint still matches an equal column (no regression)", func(t *testing.T) {
		c := ivm.Constraint{"ownerId": "u1"}
		row := ivm.Row{"ownerId": "u1", "id": "a"}
		if !constraintMatchesRow(c, row) {
			t.Fatalf("non-null constraint must match an equal column; got no match")
		}
	})

	t.Run("non-null constraint does NOT match a different column", func(t *testing.T) {
		c := ivm.Constraint{"ownerId": "u1"}
		row := ivm.Row{"ownerId": "u2", "id": "a"}
		if constraintMatchesRow(c, row) {
			t.Fatalf("non-null constraint must not match a different column; got match")
		}
	})

	// Root cause: the two primitives disagree precisely on nil/nil — which is
	// why CompareValues was the wrong choice here.
	t.Run("root cause: CompareValues(nil,nil)==0 but ValuesEqual(nil,nil)==false", func(t *testing.T) {
		if ivm.CompareValues(nil, nil) != 0 {
			t.Fatalf("premise changed: CompareValues(nil,nil) no longer 0")
		}
		if ivm.ValuesEqual(nil, nil) {
			t.Fatalf("ValuesEqual(nil,nil) must be false to match TS valuesEqual (data.ts:112-118)")
		}
	})

	// End-to-end through applyOverlay: an in-flight ADD whose constraint column
	// is NULL must be DROPPED (TS computeOverlays filters it out via
	// constraintMatchesRow). Pre-fix this row was wrongly spliced into the fetch.
	t.Run("applyOverlay DROPS a NULL-constrained overlay ADD", func(t *testing.T) {
		sort := ivm.Ordering{{"id", "asc"}}
		pk := []string{"id"}
		cmp := ivm.MakeComparator(sort, false)
		constraint := &ivm.Constraint{"ownerId": nil}
		add := ivm.SourceChange{Type: ivm.ChangeTypeAdd, Row: ivm.Row{"ownerId": nil, "id": "a"}}
		out := applyOverlay(nil, add, cmp, constraint, nil, pk)
		if len(out) != 0 {
			t.Fatalf("NULL-constrained overlay ADD must be dropped to match TS; got %v", out)
		}
	})

	t.Run("applyOverlay KEEPS a matching non-null-constrained overlay ADD", func(t *testing.T) {
		sort := ivm.Ordering{{"id", "asc"}}
		pk := []string{"id"}
		cmp := ivm.MakeComparator(sort, false)
		constraint := &ivm.Constraint{"ownerId": "u1"}
		add := ivm.SourceChange{Type: ivm.ChangeTypeAdd, Row: ivm.Row{"ownerId": "u1", "id": "a"}}
		out := applyOverlay(nil, add, cmp, constraint, nil, pk)
		if len(out) != 1 {
			t.Fatalf("matching non-null-constrained overlay ADD must be kept; got %v", out)
		}
	})
}

// TestEditChangesSplitKeys_NullEquality guards the null-equality semantics of
// editChangesSplitKeys against the TS reference.
//
// TS memory-source.ts:466 detects a split with `!valuesEqual(row[k],
// oldRow[k])`. valuesEqual treats null≠null (data.ts:112-118), so a NULL→NULL
// transition on a split-key column SPLITS the edit (remove+add). The Go port
// previously used CompareValues, where nil/nil==0 → "no change" → no split —
// a silent Go-vs-TS divergence. The fix switches to ValuesEqual, matching TS
// and the ivm genPushAndWriteWithSplitEdit convention (source.go).
func TestEditChangesSplitKeys_NullEquality(t *testing.T) {
	splitKeys := map[string]bool{"partitionKey": true}

	t.Run("NULL→NULL on a split key SPLITS (matches TS valuesEqual)", func(t *testing.T) {
		change := ivm.SourceChange{
			Type:   ivm.ChangeTypeEdit,
			Row:    ivm.Row{"id": "a", "partitionKey": nil},
			OldRow: ivm.Row{"id": "a", "partitionKey": nil},
		}
		if !editChangesSplitKeys(change, splitKeys) {
			t.Fatalf("NULL→NULL on a split key must SPLIT (TS valuesEqual: null≠null); got no split")
		}
	})

	t.Run("value→same value does NOT split", func(t *testing.T) {
		change := ivm.SourceChange{
			Type:   ivm.ChangeTypeEdit,
			Row:    ivm.Row{"id": "a", "partitionKey": "p1"},
			OldRow: ivm.Row{"id": "a", "partitionKey": "p1"},
		}
		if editChangesSplitKeys(change, splitKeys) {
			t.Fatalf("value→same value must NOT split; got split")
		}
	})

	t.Run("value→different value splits", func(t *testing.T) {
		change := ivm.SourceChange{
			Type:   ivm.ChangeTypeEdit,
			Row:    ivm.Row{"id": "a", "partitionKey": "p2"},
			OldRow: ivm.Row{"id": "a", "partitionKey": "p1"},
		}
		if !editChangesSplitKeys(change, splitKeys) {
			t.Fatalf("value→different value must split; got no split")
		}
	})

	t.Run("NULL→non-null splits", func(t *testing.T) {
		change := ivm.SourceChange{
			Type:   ivm.ChangeTypeEdit,
			Row:    ivm.Row{"id": "a", "partitionKey": "p1"},
			OldRow: ivm.Row{"id": "a", "partitionKey": nil},
		}
		if !editChangesSplitKeys(change, splitKeys) {
			t.Fatalf("NULL→non-null must split; got no split")
		}
	})

	t.Run("non-null→NULL splits", func(t *testing.T) {
		change := ivm.SourceChange{
			Type:   ivm.ChangeTypeEdit,
			Row:    ivm.Row{"id": "a", "partitionKey": nil},
			OldRow: ivm.Row{"id": "a", "partitionKey": "p1"},
		}
		if !editChangesSplitKeys(change, splitKeys) {
			t.Fatalf("non-null→NULL must split; got no split")
		}
	})

	t.Run("non-Edit change does NOT split", func(t *testing.T) {
		change := ivm.SourceChange{
			Type: ivm.ChangeTypeAdd,
			Row:  ivm.Row{"id": "a", "partitionKey": "p1"},
		}
		if editChangesSplitKeys(change, splitKeys) {
			t.Fatalf("non-Edit change must NOT split; got split")
		}
	})

	t.Run("split key absent from rows (nil/nil) SPLITS", func(t *testing.T) {
		// Column missing from both Row and OldRow → both nil → valuesEqual
		// false → split, matching TS.
		change := ivm.SourceChange{
			Type:   ivm.ChangeTypeEdit,
			Row:    ivm.Row{"id": "a"},
			OldRow: ivm.Row{"id": "a"},
		}
		if !editChangesSplitKeys(change, splitKeys) {
			t.Fatalf("absent split-key column (nil/nil) must split to match TS; got no split")
		}
	})

	// Root cause: the two primitives disagree precisely on nil/nil.
	t.Run("root cause: CompareValues(nil,nil)==0 but ValuesEqual(nil,nil)==false", func(t *testing.T) {
		if ivm.CompareValues(nil, nil) != 0 {
			t.Fatalf("premise changed: CompareValues(nil,nil) no longer 0")
		}
		if ivm.ValuesEqual(nil, nil) {
			t.Fatalf("ValuesEqual(nil,nil) must be false to match TS valuesEqual")
		}
	})
}
