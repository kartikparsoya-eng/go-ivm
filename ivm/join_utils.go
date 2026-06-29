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

// GenerateWithOverlay applies an overlay change to a stream of nodes (ordered).
func GenerateWithOverlay(nodes []Node, overlay Change, schema *SourceSchema) []Node {
	var result []Node
	applied := false
	editOldApplied := false
	editNewApplied := false

	for _, node := range nodes {
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
					result = append(result, overlay.Node)
				}
			case ChangeTypeEdit:
				if !editOldApplied && schema.CompareRows(overlay.OldNode.Row, node.Row) < 0 {
					editOldApplied = true
					if editNewApplied {
						applied = true
					}
					result = append(result, *overlay.OldNode)
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
					// Apply child overlay to the matching relationship
					childRelName := overlay.Child.RelationshipName
					newRels := make(map[string]func() iter.Seq[Node])
					for k, v := range node.Relationships {
						newRels[k] = v
					}
					childChange := overlay.Child.Change
					childSchema := schema.Relationships[childRelName]
					origStream := node.Relationships[childRelName]
					newRels[childRelName] = func() iter.Seq[Node] {
						return func(yield func(Node) bool) {
							origNodes := slices.Collect(origStream())
							overlaid := GenerateWithOverlay(origNodes, childChange, childSchema)
							for _, n := range overlaid {
								if !yield(n) {
									return
								}
							}
						}
					}
					result = append(result, Node{Row: node.Row, Relationships: newRels})
					yieldNode = false
				}
			}
		}
		if yieldNode {
			result = append(result, node)
		}
	}

	if !applied {
		if overlay.Type == ChangeTypeRemove {
			applied = true
			result = append(result, overlay.Node)
		} else if overlay.Type == ChangeTypeEdit {
			if !editNewApplied {
				panic("GenerateWithOverlay: edit overlay new node was never applied")
			}
			editOldApplied = true
			applied = true
			result = append(result, *overlay.OldNode)
		}
	}

	if !applied {
		panic("GenerateWithOverlay: overlay was never applied to any fetched node")
	}

	_ = editOldApplied // used for documentation; TS asserts both are true at end

	return result
}

// GenerateWithOverlayUnordered applies an overlay change to an unordered stream.
func GenerateWithOverlayUnordered(nodes []Node, overlay Change, schema *SourceSchema) []Node {
	var result []Node

	// Eager inject for remove/edit
	if overlay.Type == ChangeTypeRemove {
		result = append(result, overlay.Node)
	} else if overlay.Type == ChangeTypeEdit {
		result = append(result, *overlay.OldNode)
	}

	suppressed := false
	for _, node := range nodes {
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
					newRels := make(map[string]func() iter.Seq[Node])
					for k, v := range node.Relationships {
						newRels[k] = v
					}
					childChange := overlay.Child.Change
					childSchema := schema.Relationships[childRelName]
					origStream := node.Relationships[childRelName]
					newRels[childRelName] = func() iter.Seq[Node] {
						return func(yield func(Node) bool) {
							origNodes := slices.Collect(origStream())
							overlaid := GenerateWithOverlay(origNodes, childChange, childSchema)
							for _, n := range overlaid {
								if !yield(n) {
									return
								}
							}
						}
					}
					result = append(result, Node{Row: node.Row, Relationships: newRels})
					continue
				}
			}
		}
		result = append(result, node)
	}

	if !suppressed && overlay.Type != ChangeTypeRemove {
		panic("GenerateWithOverlayUnordered: overlay was never applied to any fetched node")
	}

	return result
}
