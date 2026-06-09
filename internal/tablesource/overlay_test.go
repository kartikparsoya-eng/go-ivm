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
