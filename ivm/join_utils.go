package ivm

import (
	"iter"
	"slices"
)

// CompoundKey is a list of column names forming a compound key.
type CompoundKey []string

// RowEqualsForCompoundKey checks if two rows are equal on the given compound key.
func RowEqualsForCompoundKey(a, b Row, key CompoundKey) bool {
	for _, k := range key {
		if CompareValues(a[k], b[k]) != 0 {
			return false
		}
	}
	return true
}

// IsJoinMatch checks if a parent and child row match on their respective keys.
func IsJoinMatch(parent Row, parentKey CompoundKey, child Row, childKey CompoundKey) bool {
	for i := range parentKey {
		if !ValuesEqual(parent[parentKey[i]], child[childKey[i]]) {
			return false
		}
	}
	return true
}

// BuildJoinConstraint builds a constraint from sourceRow using sourceKey mapped to targetKey.
// Returns nil if any source value is null (null FK can't match).
func BuildJoinConstraint(sourceRow Row, sourceKey, targetKey CompoundKey) *Constraint {
	c := make(Constraint)
	for i := range targetKey {
		value := sourceRow[sourceKey[i]]
		if value == nil {
			return nil
		}
		c[targetKey[i]] = value
	}
	return &c
}

func GenerateWithOverlaySeq(nodes iter.Seq[Node], overlay Change, schema *SourceSchema) iter.Seq[Node] {
	return func(yield func(Node) bool) {
		applied := false
		editOldApplied := false
		editNewApplied := false

		for node := range nodes {
			yieldNode := true
			if !applied {
				switch overlay.Type {
				case ChangeTypeAdd:
					if schema.CompareRows(overlay.Node.Row, node.Row) == 0 {
						applied = true
						yieldNode = false
					}
				case ChangeTypeRemove:
					if schema.CompareRows(overlay.Node.Row, node.Row) < 0 {
						applied = true
						if !yield(overlay.Node) {
							return
						}
					}
				case ChangeTypeEdit:
					if !editOldApplied && schema.CompareRows(overlay.OldNode.Row, node.Row) < 0 {
						editOldApplied = true
						if editNewApplied {
							applied = true
						}
						if !yield(*overlay.OldNode) {
							return
						}
					}
					if !editNewApplied && schema.CompareRows(overlay.Node.Row, node.Row) == 0 {
						editNewApplied = true
						if editOldApplied {
							applied = true
						}
						yieldNode = false
					}
				case ChangeTypeChild:
					if schema.CompareRows(overlay.Node.Row, node.Row) == 0 {
						applied = true
						childRelName := overlay.Child.RelationshipName
						childChange := overlay.Child.Change
						childSchema := schema.Relationships[childRelName]
						origStream := node.Relationships[childRelName]
						newRels, newOrder := SetRelationship(node.Relationships, node.RelOrder, childRelName, func() iter.Seq[Node] {
							return GenerateWithOverlaySeq(origStream(), childChange, childSchema)
						})
						if !yield(Node{Row: node.Row, Relationships: newRels, RelOrder: newOrder}) {
							return
						}
						yieldNode = false
					}
				}
			}
			if yieldNode && !yield(node) {
				return
			}
		}

		if !applied {
			if overlay.Type == ChangeTypeRemove {
				applied = true
				if !yield(overlay.Node) {
					return
				}
			} else if overlay.Type == ChangeTypeEdit {
				if !editNewApplied {
					panic("GenerateWithOverlay: edit overlay new node was never applied")
				}
				editOldApplied = true
				applied = true
				if !yield(*overlay.OldNode) {
					return
				}
			}
		}

		if !applied {
			panic("GenerateWithOverlay: overlay was never applied to any fetched node")
		}

		_ = editOldApplied
	}
}

// GenerateWithOverlay applies an overlay change to a stream of nodes (ordered).
func GenerateWithOverlay(nodes []Node, overlay Change, schema *SourceSchema) []Node {
	return slices.Collect(GenerateWithOverlaySeq(slices.Values(nodes), overlay, schema))
}

func GenerateWithOverlayUnorderedSeq(nodes iter.Seq[Node], overlay Change, schema *SourceSchema) iter.Seq[Node] {
	return func(yield func(Node) bool) {
		if overlay.Type == ChangeTypeRemove {
			if !yield(overlay.Node) {
				return
			}
		} else if overlay.Type == ChangeTypeEdit {
			if !yield(*overlay.OldNode) {
				return
			}
		}

		suppressed := false
		for node := range nodes {
			if !suppressed {
				if overlay.Type == ChangeTypeAdd || overlay.Type == ChangeTypeEdit {
					if RowEqualsForCompoundKey(overlay.Node.Row, node.Row, schema.PrimaryKey) {
						suppressed = true
						continue
					}
				}
				if overlay.Type == ChangeTypeChild {
					if RowEqualsForCompoundKey(overlay.Node.Row, node.Row, schema.PrimaryKey) {
						suppressed = true
						childRelName := overlay.Child.RelationshipName
						childChange := overlay.Child.Change
						childSchema := schema.Relationships[childRelName]
						origStream := node.Relationships[childRelName]
						newRels, newOrder := SetRelationship(node.Relationships, node.RelOrder, childRelName, func() iter.Seq[Node] {
							return GenerateWithOverlaySeq(origStream(), childChange, childSchema)
						})
						if !yield(Node{Row: node.Row, Relationships: newRels, RelOrder: newOrder}) {
							return
						}
						continue
					}
				}
			}
			if !yield(node) {
				return
			}
		}

		if !suppressed && overlay.Type != ChangeTypeRemove {
			panic("GenerateWithOverlayUnordered: overlay was never applied to any fetched node")
		}
	}
}

// GenerateWithOverlayUnordered applies an overlay change to an unordered stream.
func GenerateWithOverlayUnordered(nodes []Node, overlay Change, schema *SourceSchema) []Node {
	return slices.Collect(GenerateWithOverlayUnorderedSeq(slices.Values(nodes), overlay, schema))
}
