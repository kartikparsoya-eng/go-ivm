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
func editChangesSplitKeys(change ivm.SourceChange, splitKeys map[string]bool) bool {
	if change.Type != ivm.ChangeTypeEdit {
		return false
	}
	for k := range splitKeys {
		if ivm.CompareValues(change.Row[k], change.OldRow[k]) != 0 {
			return true
		}
	}
	return false
}

// applyOverlay splices the pending change into the already-fetched node
// list so a Fetch during the same Push sees the change as if it had
// already happened. Sort order is maintained via comparator.
func applyOverlay(
	nodes []ivm.Node,
	change ivm.SourceChange,
	comparator ivm.Comparator,
	constraint *ivm.Constraint,
	primaryKey []string,
) []ivm.Node {
	switch change.Type {
	case ivm.ChangeTypeAdd:
		if constraint != nil && !constraintMatchesRow(*constraint, change.Row) {
			return nodes
		}
		return insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)

	case ivm.ChangeTypeRemove:
		return removeByPK(nodes, change.Row, primaryKey)

	case ivm.ChangeTypeEdit:
		if constraint != nil && !constraintMatchesRow(*constraint, change.OldRow) {
			if constraintMatchesRow(*constraint, change.Row) {
				return insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)
			}
			return nodes
		}
		nodes = removeByPK(nodes, change.OldRow, primaryKey)
		if constraint == nil || constraintMatchesRow(*constraint, change.Row) {
			nodes = insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)
		}
		return nodes
	}
	return nodes
}

func constraintMatchesRow(constraint ivm.Constraint, row ivm.Row) bool {
	for k, v := range constraint {
		if ivm.CompareValues(row[k], v) != 0 {
			return false
		}
	}
	return true
}

func insertSorted(nodes []ivm.Node, node ivm.Node, comparator ivm.Comparator) []ivm.Node {
	lo, hi := 0, len(nodes)
	for lo < hi {
		mid := (lo + hi) / 2
		if comparator(node.Row, nodes[mid].Row) > 0 {
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
