package ivm

import "testing"

// TestFetch_PartialCursor_LiveOverlay_Masking proves the FORWARD end-to-end
// correctness of SourceInput.Fetch when the start cursor is PARTIAL (a subset
// of the sort columns) and a live overlay change sits exactly on the cursor's
// boundary.
//
// Why this is a real risk: computeOverlays (source.go computeOverlays) gates
// overlay rows with the PLAIN MakeComparator — `compare(o.Add, start.Row) < 0`
// drops rows sorting before the start. For a partial cursor {createdAt} over
// sort [createdAt, conversationId], the full comparator does NOT stop at the
// missing conversationId: it evaluates CompareValues(row.conversationId, nil)
// → +1 (nil < non-nil), ranking a boundary row (equal createdAt) as strictly
// AFTER the cursor and thus KEEPING it in the overlay (over-include). That
// mis-gated row is then spliced into the node list by
// generateWithOverlayInner.
//
// The saving grace (the "masking"): applyStart runs AFTER the overlay splice
// and re-filters every node — including the spliced overlay row — through
// CompareWithPartialBound, which STOPS at the missing conversationId, ranks
// the boundary row EQUAL to the cursor, and applies the Basis (after = strictly
// after → drops; at = inclusive → keeps). So end-to-end Fetch is correct for
// forward fetches even though computeOverlays over-includes. This test is the
// regression guard that the masking continues to hold.
//
// (The reverse-inclusive counterpart was NOT masked — the gate dropped the
// boundary overlay before applyStart could keep it — and is fixed + tested by
// the C1 change, which switches computeOverlays to MakePartialBoundComparator.)
func TestFetch_PartialCursor_LiveOverlay_Masking(t *testing.T) {
	sort := Ordering{{"createdAt", "asc"}, {"conversationId", "asc"}}
	cols := map[string]string{"createdAt": "number", "conversationId": "string"}
	pk := []string{"conversationId"}

	cursorTs := float64(1779813937134)

	// Base rows: one AT the cursor's createdAt, one strictly after.
	base := []Row{
		{"createdAt": cursorTs, "conversationId": "c-at"},
		{"createdAt": cursorTs + 1, "conversationId": "c-later"},
	}
	src := NewMemorySource("convs", cols, pk)
	src.BulkInsert(base)
	si := src.Connect(sort, nil, nil)

	// Boundary overlay ADD exactly on the cursor's createdAt. The ONLY way
	// this row can appear in a Fetch is via the overlay (no base row shares
	// its conversationId).
	boundaryAdd := Row{"createdAt": cursorTs, "conversationId": "8ec82a40"}
	// Set a live overlay directly: simulate the window during genPush where
	// the overlay is visible to a downstream Fetch but not yet written.
	src.overlay.Store(&Overlay{Epoch: 1, Change: SourceChange{Type: ChangeTypeAdd, Row: boundaryAdd}})
	si.conn.LastPushedEpoch = 1
	t.Cleanup(func() { src.overlay.Store(nil) })

	has := func(nodes []Node, id string) bool {
		for _, n := range nodes {
			if n.Row["conversationId"] == id {
				return true
			}
		}
		return false
	}

	t.Run("after (exclusive) drops the boundary overlay row", func(t *testing.T) {
		req := FetchRequest{
			Start: &Start{Row: Row{"createdAt": cursorTs}, Basis: "after"},
		}
		nodes := si.Fetch(req)
		if has(nodes, "8ec82a40") {
			t.Fatalf("Basis 'after' must EXCLUDE the boundary overlay row; got it included: %v", ids(nodes))
		}
		if has(nodes, "c-at") {
			t.Fatalf("Basis 'after' must EXCLUDE the base row at the cursor; got it included: %v", ids(nodes))
		}
		if !has(nodes, "c-later") {
			t.Fatalf("Basis 'after' must INCLUDE the row strictly after the cursor; got %v", ids(nodes))
		}
	})

	t.Run("at (inclusive) keeps the boundary overlay row", func(t *testing.T) {
		req := FetchRequest{
			Start: &Start{Row: Row{"createdAt": cursorTs}, Basis: "at"},
		}
		nodes := si.Fetch(req)
		if !has(nodes, "8ec82a40") {
			t.Fatalf("Basis 'at' must INCLUDE the boundary overlay row; got it dropped: %v", ids(nodes))
		}
		if !has(nodes, "c-at") {
			t.Fatalf("Basis 'at' must INCLUDE the base row at the cursor; got it dropped: %v", ids(nodes))
		}
		if !has(nodes, "c-later") {
			t.Fatalf("Basis 'at' must INCLUDE the row strictly after the cursor; got %v", ids(nodes))
		}
	})

	// No-cursor control: the overlay is always spliced in regardless of basis.
	t.Run("no start cursor keeps the boundary overlay row", func(t *testing.T) {
		nodes := si.Fetch(FetchRequest{})
		if !has(nodes, "8ec82a40") {
			t.Fatalf("without a start cursor the boundary overlay row must be present; got %v", ids(nodes))
		}
	})
}

func ids(nodes []Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.Row["conversationId"].(string))
	}
	return out
}

// TestFetch_PartialCursor_LiveOverlay_ReverseInclusive proves the C1 fix:
// computeOverlays must use MakePartialBoundComparator (not MakeComparator) so a
// REVERSE, Basis:"at" (inclusive) fetch over a PARTIAL cursor keeps an
// in-flight boundary overlay row.
//
// Before C1 the gate used the plain MakeComparator with reverse=true: for the
// boundary row (createdAt equal to the cursor, conversationId present on the
// row but absent from the partial cursor) it evaluated
// CompareValues(rowVal, nil) → +1, then reverse flipped it to -1, and the gate
// `compare < 0` DROPPED the overlay — before applyStart ever saw it. The row
// went missing from the fetch (a real data-loss bug, not just an over-include
// masked by applyStart as in the forward case). MakePartialBoundComparator
// stops at the missing conversationId → returns 0 → gate keeps it → applyStart
// reverse "at" (cmpCursorVsRow=0 ≤ 0) keeps it. Fixed.
func TestFetch_PartialCursor_LiveOverlay_ReverseInclusive(t *testing.T) {
	sort := Ordering{{"createdAt", "asc"}, {"conversationId", "asc"}}
	cols := map[string]string{"createdAt": "number", "conversationId": "string"}
	pk := []string{"conversationId"}

	cursorTs := float64(1779813937134)

	// Base rows at-or-before the cursor (a reverse "at" fetch includes these).
	base := []Row{
		{"createdAt": cursorTs - 1, "conversationId": "c-before"},
		{"createdAt": cursorTs, "conversationId": "c-at"},
		{"createdAt": cursorTs + 1, "conversationId": "c-after"}, // excluded by reverse "at"
	}
	src := NewMemorySource("convs", cols, pk)
	src.BulkInsert(base)
	si := src.Connect(sort, nil, nil)

	boundaryAdd := Row{"createdAt": cursorTs, "conversationId": "8ec82a40"}
	src.overlay.Store(&Overlay{Epoch: 1, Change: SourceChange{Type: ChangeTypeAdd, Row: boundaryAdd}})
	si.conn.LastPushedEpoch = 1
	t.Cleanup(func() { src.overlay.Store(nil) })

	has := func(nodes []Node, id string) bool {
		for _, n := range nodes {
			if n.Row["conversationId"] == id {
				return true
			}
		}
		return false
	}

	t.Run("reverse at (inclusive) keeps the boundary overlay row", func(t *testing.T) {
		req := FetchRequest{
			Start:   &Start{Row: Row{"createdAt": cursorTs}, Basis: "at"},
			Reverse: true,
		}
		nodes := si.Fetch(req)
		if !has(nodes, "8ec82a40") {
			t.Fatalf("reverse Basis 'at' must INCLUDE the boundary overlay row (C1 fix); got it dropped: %v", ids(nodes))
		}
		if !has(nodes, "c-at") {
			t.Fatalf("reverse Basis 'at' must INCLUDE the base row at the cursor; got %v", ids(nodes))
		}
		if !has(nodes, "c-before") {
			t.Fatalf("reverse Basis 'at' must INCLUDE the row before the cursor; got %v", ids(nodes))
		}
		if has(nodes, "c-after") {
			t.Fatalf("reverse Basis 'at' must EXCLUDE the row after the cursor; got %v", ids(nodes))
		}
	})

	t.Run("reverse after (exclusive) drops the boundary overlay row", func(t *testing.T) {
		req := FetchRequest{
			Start:   &Start{Row: Row{"createdAt": cursorTs}, Basis: "after"},
			Reverse: true,
		}
		nodes := si.Fetch(req)
		if has(nodes, "8ec82a40") {
			t.Fatalf("reverse Basis 'after' must EXCLUDE the boundary overlay row; got it included: %v", ids(nodes))
		}
		if has(nodes, "c-at") {
			t.Fatalf("reverse Basis 'after' must EXCLUDE the base row at the cursor; got %v", ids(nodes))
		}
		if !has(nodes, "c-before") {
			t.Fatalf("reverse Basis 'after' must INCLUDE the row before the cursor; got %v", ids(nodes))
		}
	})
}
