package tablesource

// Overlay helpers for Phase 2d push fanout.
//
// Faithful copy of the helpers from sqlite/table_source.go (applyOverlay,
// constraintMatchesRow, insertSorted, removeByPK, sourceChangeToChange,
// editChangesSplitKeys). They live here so the new package is
// self-contained; when the legacy in-process sqlite.TableSource is
// retired (Phase 5), the duplicates collapse to this set.
//
// Behavior must stay byte-identical with the sqlite/ originals — those
// are the reference and any divergence shows up as Go-vs-TS drift the
// audit will catch.

import "github.com/kartikparsoya-eng/go-ivm/ivm"

// sourceChangeToChange converts a SourceChange (leaf-level) to a Change
// (the downstream operator's input).
func sourceChangeToChange(sc ivm.SourceChange) *ivm.Change {
	switch sc.Type {
	case ivm.ChangeTypeAdd:
		c := ivm.MakeAddChange(ivm.Node{Row: sc.Row})
		return &c
	case ivm.ChangeTypeRemove:
		c := ivm.MakeRemoveChange(ivm.Node{Row: sc.Row})
		return &c
	case ivm.ChangeTypeEdit:
		c := ivm.MakeEditChange(ivm.Node{Row: sc.Row}, ivm.Node{Row: sc.OldRow})
		return &c
	}
	return nil
}

// editChangesSplitKeys reports whether an Edit changed any column listed
// in splitKeys. When true the connection processes it as remove+add
// (matches the TS partition-key semantics).
//
// Uses ValuesEqual (not CompareValues) to match TS memory-source.ts:466
// (`!valuesEqual(change.row[k], change.oldRow[k])`). valuesEqual treats
// null≠null (data.ts:112-118), so a NULL→NULL transition on a split key
// SPLITS the edit — CompareValues(nil,nil)==0 would wrongly treat it as
// unchanged and skip the split, diverging from TS. Mirrors the ivm path's
// genPushAndWriteWithSplitEdit (source.go), which already uses ValuesEqual.
func editChangesSplitKeys(change ivm.SourceChange, splitKeys map[string]bool) bool {
	if change.Type != ivm.ChangeTypeEdit {
		return false
	}
	for k := range splitKeys {
		if !ivm.ValuesEqual(change.Row[k], change.OldRow[k]) {
			return true
		}
	}
	return false
}

// applyOverlay splices the pending change into the already-fetched node
// list so a Fetch during the same Push sees the change as if it had
// already happened. Sort order is maintained via comparator.
//
// comparator MUST be the fetch's effective comparator — i.e.
// MakeComparator(sort, req.Reverse) — so the overlay row lands at the same
// index TS's generateWithOverlay would place it (table-source.ts:298-312).
//
// start gates overlay-ADD rows the same way TS's overlaysForStartAt +
// generateWithStart do (memory-source.ts:696-707): an add whose row sorts
// (in `comparator` order) before req.start — or equal to it when the basis
// is "after" — is dropped, because the windowed fetch starts at that
// cursor. Without this, Take's start:bound reverse fetch would splice an
// in-flight row that belongs outside the requested window.
//
// multis gates overlay-ADD rows by the fetch's MultiConstraints, mirroring
// TS applyMultiConstraintsToOverlays (memory-source.ts @1.7.0): an in-flight
// add that matches none of a multi's entries is outside the batched-IN
// window and must not be spliced. (TS also nils the REMOVE overlay; here
// remove needs no gate — a non-matching removed row is absent from the
// IN-filtered SQL result, so removeByPK is a no-op, same net effect.)
func applyOverlay(
	nodes []ivm.Node,
	change ivm.SourceChange,
	comparator ivm.Comparator,
	constraint *ivm.Constraint,
	multis []ivm.MultiConstraint,
	start *ivm.Start,
	primaryKey []string,
) []ivm.Node {
	// addAllowed mirrors TS computeOverlays' constraint + multi + start
	// filtering for an add candidate row.
	addAllowed := func(row ivm.Row) bool {
		if constraint != nil && !constraintMatchesRow(*constraint, row) {
			return false
		}
		if !ivm.RowMatchesMultiConstraints(multis, row) {
			return false
		}
		return overlayRowAtOrAfterStart(row, start, comparator)
	}

	switch change.Type {
	case ivm.ChangeTypeAdd:
		if !addAllowed(change.Row) {
			return nodes
		}
		return insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)

	case ivm.ChangeTypeRemove:
		return removeByPK(nodes, change.Row, primaryKey)

	case ivm.ChangeTypeEdit:
		oldOutsideWindow := (constraint != nil && !constraintMatchesRow(*constraint, change.OldRow)) ||
			!ivm.RowMatchesMultiConstraints(multis, change.OldRow)
		if oldOutsideWindow {
			if addAllowed(change.Row) {
				return insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)
			}
			return nodes
		}
		nodes = removeByPK(nodes, change.OldRow, primaryKey)
		if addAllowed(change.Row) {
			nodes = insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)
		}
		return nodes
	}
	return nodes
}

// overlayRowAtOrAfterStart reports whether an overlay-add row should be
// kept given the fetch's start cursor — porting TS overlaysForStartAt
// (row < startAt ⇒ drop) plus generateWithStart's basis handling
// (basis "after" ⇒ also drop the row equal to the cursor). `comparator`
// is the fetch's effective (reverse-aware) comparator.
func overlayRowAtOrAfterStart(row ivm.Row, start *ivm.Start, comparator ivm.Comparator) bool {
	if start == nil {
		return true
	}
	c := comparator(row, start.Row)
	if c < 0 {
		return false
	}
	if c == 0 && start.Basis == "after" {
		return false
	}
	return true
}

// overlaySplicePlan reduces the in-flight overlay change to its streaming
// splice ingredients: at most one row to inject (add) and one row to
// suppress (remove), pre-gated by the fetch's constraint + start cursor.
// It is the per-row merge equivalent of applyOverlay — the branch
// structure mirrors it case for case; keep the two in lockstep.
//
// Contract for the caller (fetchDuringPushStream) — the TS streaming form
// (generateWithOverlayInner, memory-source.ts:856-877):
//   - inject `add` immediately BEFORE the first streamed row R with
//     comparator(add, R) < 0 (TS `cmp < 0`, memory-source.ts:858-862), or
//     after the last row if none — the rightmost position insertSorted
//     picks (streamed rows arrive already sorted by the same comparator,
//     so first-strict-match == rightmost-among-equals index). Equal keys
//     are unreachable anyway: the comparator's sort always includes the
//     PK (total order) and the overlay-add row is never in the streamed
//     set, so cmp == 0 would mean the same row on both sides.
//   - drop the first streamed row whose primary key equals `remove`
//     (removeByPK removes exactly one; PKs are unique).
//
// Equivalence with applyOverlay's remove-then-insert sequencing holds
// regardless of per-row check order: the add lands before the first
// SURVIVING row that sorts > it either way (total order + transitivity),
// which is exactly where insertSorted places it post-removal.
func overlaySplicePlan(
	change ivm.SourceChange,
	comparator ivm.Comparator,
	constraint *ivm.Constraint,
	multis []ivm.MultiConstraint,
	start *ivm.Start,
) (add, remove ivm.Row) {
	addAllowed := func(row ivm.Row) bool {
		if constraint != nil && !constraintMatchesRow(*constraint, row) {
			return false
		}
		if !ivm.RowMatchesMultiConstraints(multis, row) {
			return false
		}
		return overlayRowAtOrAfterStart(row, start, comparator)
	}
	switch change.Type {
	case ivm.ChangeTypeAdd:
		if addAllowed(change.Row) {
			add = change.Row
		}
	case ivm.ChangeTypeRemove:
		remove = change.Row
	case ivm.ChangeTypeEdit:
		oldOutsideWindow := (constraint != nil && !constraintMatchesRow(*constraint, change.OldRow)) ||
			!ivm.RowMatchesMultiConstraints(multis, change.OldRow)
		if oldOutsideWindow {
			// Old row outside the constraint/multi window — nothing of it is
			// in the streamed set; the edit degrades to a pure add (mirrors
			// applyOverlay's Edit early branch).
			if addAllowed(change.Row) {
				add = change.Row
			}
			return
		}
		remove = change.OldRow
		if addAllowed(change.Row) {
			add = change.Row
		}
	}
	return
}

// unorderedOverlayPlan reduces the in-flight overlay change to the
// UNORDERED splice ingredients — porting TS generateWithOverlayUnordered's
// overlay gating (memory-source.ts:885-926): the add side is gated by the
// fetch's constraint and multi-constraints; the remove side needs no gate
// (a removed row outside the constraint/multi window is absent from the
// SQL result anyway, so the PK suppression is a no-op — the same
// convention overlaySplicePlan documents for its remove side).
//
// Contract for the caller (per generateWithOverlayInnerUnordered,
// memory-source.ts:929-951):
//   - yield `add` eagerly BEFORE the first streamed row;
//   - drop the first streamed row whose primary key equals `remove`.
//
// No start gate: BuildSelectQuery panics on start-without-ordering, so an
// unordered fetch can never carry a cursor.
func unorderedOverlayPlan(
	change ivm.SourceChange,
	constraint *ivm.Constraint,
	multis []ivm.MultiConstraint,
) (add, remove ivm.Row) {
	addAllowed := func(row ivm.Row) bool {
		if constraint != nil && !constraintMatchesRow(*constraint, row) {
			return false
		}
		return ivm.RowMatchesMultiConstraints(multis, row)
	}
	switch change.Type {
	case ivm.ChangeTypeAdd:
		if addAllowed(change.Row) {
			add = change.Row
		}
	case ivm.ChangeTypeRemove:
		remove = change.Row
	case ivm.ChangeTypeEdit:
		remove = change.OldRow
		if addAllowed(change.Row) {
			add = change.Row
		}
	}
	return
}

// pkRowsEqual reports whether two rows agree on every primary-key column,
// using the same CompareValues convention as removeByPK.
func pkRowsEqual(a, b ivm.Row, primaryKey []string) bool {
	for _, pk := range primaryKey {
		if ivm.CompareValues(a[pk], b[pk]) != 0 {
			return false
		}
	}
	return true
}

// constraintMatchesRow — TS uses valuesEqual (constraint.ts:21), which treats
// null/null as UNEQUAL (data.ts:112-118). CompareValues treats nil/nil as equal
// (returns 0), which would over-include an overlay row whose constraint column
// is NULL — diverging from TS, which drops it. Mirror the ValuesEqual
// convention already used by constraintsAreCompatible (flipped_join.go:416).
func constraintMatchesRow(constraint ivm.Constraint, row ivm.Row) bool {
	for k, v := range constraint {
		if !ivm.ValuesEqual(row[k], v) {
			return false
		}
	}
	return true
}

// insertSorted places node at the RIGHTMOST position among equal keys —
// the first index i where node sorts STRICTLY before nodes[i] — matching
// TS's streaming overlay inject, which yields the add only before a row
// it sorts strictly before (generateWithOverlayInner `cmp < 0`,
// memory-source.ts:858-862; the trailing yield at :875-877 is the
// after-last-row case). Equal keys are unreachable in practice (the
// comparator's sort always includes the PK and the overlay-add row is
// never in the fetched set), so leftmost-vs-rightmost is observationally
// identical — rightmost is kept purely for line-faithfulness with the TS
// form and for lockstep with fetchDuringPushStream's `< 0` inject.
func insertSorted(nodes []ivm.Node, node ivm.Node, comparator ivm.Comparator) []ivm.Node {
	lo, hi := 0, len(nodes)
	for lo < hi {
		mid := (lo + hi) / 2
		if comparator(node.Row, nodes[mid].Row) >= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	nodes = append(nodes, ivm.Node{})
	copy(nodes[lo+1:], nodes[lo:])
	nodes[lo] = node
	return nodes
}

func removeByPK(nodes []ivm.Node, row ivm.Row, primaryKey []string) []ivm.Node {
	for i, n := range nodes {
		match := true
		for _, pk := range primaryKey {
			if ivm.CompareValues(n.Row[pk], row[pk]) != 0 {
				match = false
				break
			}
		}
		if match {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nodes
}
