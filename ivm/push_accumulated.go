package ivm

import (
	"fmt"
	"iter"
)

// Pushes accumulated changes from fan-out/fan-in sub-graphs,
// collapsing duplicates and enforcing invariants.

// MergeRelationshipsFunc transforms two changes into one merged change.
type MergeRelationshipsFunc func(existing, incoming Change) Change

// AddEmptyRelationshipsFunc adds empty relationships for missing schema keys.
type AddEmptyRelationshipsFunc func(change Change) Change

// PushAccumulatedChanges collapses accumulated pushes and pushes a single result.
func PushAccumulatedChanges(
	accumulatedPushes []Change,
	output Output,
	pusher InputBase,
	fanOutChangeType ChangeType,
	mergeRelationships MergeRelationshipsFunc,
	addEmptyRelationships AddEmptyRelationshipsFunc,
) []Change {
	if len(accumulatedPushes) == 0 {
		return nil
	}

	// Collapse down to a single change per type
	candidatesToPush := make(map[ChangeType]Change)
	for _, change := range accumulatedPushes {
		// Source: push-accumulated.ts:104-113 — when the fan-out change was a
		// CHILD, any non-child result type must be unique (at most one add / one
		// remove). A second occurrence signals a fan-in invariant breach.
		if fanOutChangeType == ChangeTypeChild && change.Type != ChangeTypeChild {
			if _, has := candidatesToPush[change.Type]; has {
				panic(fmt.Sprintf("Fan-in:child expected at most one %v when fan-out is of type child", change.Type))
			}
		}
		existing, exists := candidatesToPush[change.Type]
		mergedChange := change
		if exists {
			mergedChange = mergeRelationships(existing, change)
		}
		candidatesToPush[change.Type] = mergedChange
	}

	switch fanOutChangeType {
	case ChangeTypeRemove:
		// Source: push-accumulated.ts:139-142 — a REMOVE fan-out must yield only
		// removes (types.length === 1 && types[0] === REMOVE).
		c, ok := candidatesToPush[ChangeTypeRemove]
		if len(candidatesToPush) != 1 || !ok {
			panic("Fan-in:remove expected all removes")
		}
		return output.Push(addEmptyRelationships(c), pusher)

	case ChangeTypeAdd:
		// Source: push-accumulated.ts:149-152 — an ADD fan-out must yield only
		// adds (types.length === 1 && types[0] === ADD).
		c, ok := candidatesToPush[ChangeTypeAdd]
		if len(candidatesToPush) != 1 || !ok {
			panic("Fan-in:add expected all adds")
		}
		return output.Push(addEmptyRelationships(c), pusher)

	case ChangeTypeEdit:
		// Source: push-accumulated.ts:159-167 — an EDIT fan-out may only yield
		// adds, removes, or edits.
		for ct := range candidatesToPush {
			if ct != ChangeTypeAdd && ct != ChangeTypeRemove && ct != ChangeTypeEdit {
				panic("Fan-in:edit expected all adds, removes, or edits")
			}
		}
		addChange, hasAdd := candidatesToPush[ChangeTypeAdd]
		removeChange, hasRemove := candidatesToPush[ChangeTypeRemove]
		editChange, hasEdit := candidatesToPush[ChangeTypeEdit]

		// Edit supersedes add and remove
		if hasEdit {
			if hasAdd {
				editChange = mergeRelationships(editChange, addChange)
			}
			if hasRemove {
				editChange = mergeRelationships(editChange, removeChange)
			}
			return output.Push(addEmptyRelationships(editChange), pusher)
		}

		// Both add and remove → convert back to edit
		if hasAdd && hasRemove {
			edit := MakeEditChange(addChange.Node, removeChange.Node)
			return output.Push(addEmptyRelationships(edit), pusher)
		}

		// Only one of add/remove
		if hasAdd {
			return output.Push(addEmptyRelationships(addChange), pusher)
		}
		if hasRemove {
			return output.Push(addEmptyRelationships(removeChange), pusher)
		}
		// Porting review MEDIUM-4: TS uses must(addChange ?? removeChange),
		// which throws if neither is present. Go was silently falling through
		// to a zero-value removeChange. Panic to surface the invariant
		// violation rather than emit a malformed empty Change.
		panic("PushAccumulated EDIT: expected hasEdit||hasAdd||hasRemove, got none")

	case ChangeTypeChild:
		// Source: push-accumulated.ts:222-234 — a CHILD fan-out may only yield
		// adds, removes, or children, and at most two distinct types.
		for ct := range candidatesToPush {
			if ct != ChangeTypeAdd && ct != ChangeTypeRemove && ct != ChangeTypeChild {
				panic("Fan-in:child expected all adds, removes, or children")
			}
		}
		if len(candidatesToPush) > 2 {
			panic("Fan-in:child expected at most 2 types on a child change from fan-out")
		}
		// Child takes precedence
		childChange, hasChild := candidatesToPush[ChangeTypeChild]
		if hasChild {
			return output.Push(childChange, pusher)
		}

		addChange, hasAdd := candidatesToPush[ChangeTypeAdd]
		removeChange, hasRemove := candidatesToPush[ChangeTypeRemove]

		if hasAdd && hasRemove {
			panic("Fan-in:child expected either add or remove, not both")
		}

		if hasAdd {
			return output.Push(addEmptyRelationships(addChange), pusher)
		}
		if hasRemove {
			return output.Push(addEmptyRelationships(removeChange), pusher)
		}
		// Same MEDIUM-4 invariant for the CHILD branch.
		panic("PushAccumulated CHILD: expected hasChild||hasAdd||hasRemove, got none")
	}

	panic("unreachable: invalid fanOutChangeType")
}

// MergeRelationships puts relationships from right into left if they don't exist.
func MergeRelationships(left, right Change) Change {
	if left.Type == right.Type {
		switch left.Type {
		case ChangeTypeAdd:
			return MakeAddChange(Node{
				Row:           left.Node.Row,
				Relationships: mergeRelationshipMaps(left.Node.Relationships, right.Node.Relationships),
			})
		case ChangeTypeRemove:
			return MakeRemoveChange(Node{
				Row:           left.Node.Row,
				Relationships: mergeRelationshipMaps(left.Node.Relationships, right.Node.Relationships),
			})
		case ChangeTypeEdit:
			// Source: push-accumulated.ts:290-294.
			if right.Type != ChangeTypeEdit {
				panic("mergeRelationships: when left.type is edit and types match, right.type must be edit")
			}
			return MakeEditChange(
				Node{
					Row:           left.Node.Row,
					Relationships: mergeRelationshipMaps(left.Node.Relationships, right.Node.Relationships),
				},
				Node{
					Row:           left.OldNode.Row,
					Relationships: mergeRelationshipMaps(left.OldNode.Relationships, right.OldNode.Relationships),
				},
			)
		case ChangeTypeChild:
			// Source: push-accumulated.ts:317-321.
			if right.Type != ChangeTypeChild {
				panic("mergeRelationships: when left.type is child and types match, right.type must be child")
			}
			return MakeChildChange(
				Node{
					Row:           left.Node.Row,
					Relationships: mergeRelationshipMaps(left.Node.Relationships, right.Node.Relationships),
				},
				*left.Child,
			)
		}
	}

	// Types differ — left must be edit.
	// Source: push-accumulated.ts:337-341.
	if left.Type != ChangeTypeEdit {
		panic(fmt.Sprintf("mergeRelationships: when types differ, left.type must be edit, got left.type=%v right.type=%v", left.Type, right.Type))
	}
	switch right.Type {
	case ChangeTypeAdd:
		return MakeEditChange(
			Node{
				Row:           left.Node.Row,
				Relationships: mergeRelationshipMaps(left.Node.Relationships, right.Node.Relationships),
			},
			*left.OldNode,
		)
	case ChangeTypeRemove:
		return MakeEditChange(
			left.Node,
			Node{
				Row:           left.OldNode.Row,
				Relationships: mergeRelationshipMaps(left.OldNode.Relationships, right.Node.Relationships),
			},
		)
	}

	panic("unreachable: mergeRelationships")
}

// Relationships is a type alias for the relationship map in Node.
type Relationships = map[string]func() iter.Seq[Node]

// mergeRelationshipMaps merges two relationship maps, left takes precedence.
func mergeRelationshipMaps(left, right Relationships) Relationships {
	if left == nil && right == nil {
		return nil
	}
	merged := make(Relationships)
	for k, v := range right {
		merged[k] = v
	}
	for k, v := range left {
		merged[k] = v
	}
	return merged
}

// MakeAddEmptyRelationships returns a function that fills missing relationships with empty streams.
func MakeAddEmptyRelationships(schema *SourceSchema) AddEmptyRelationshipsFunc {
	return func(change Change) Change {
		if len(schema.Relationships) == 0 {
			return change
		}

		switch change.Type {
		case ChangeTypeAdd:
			rels := copyRelationships(change.Node.Relationships)
			mergeEmpty(rels, schema.Relationships)
			return MakeAddChange(Node{Row: change.Node.Row, Relationships: rels})
		case ChangeTypeRemove:
			rels := copyRelationships(change.Node.Relationships)
			mergeEmpty(rels, schema.Relationships)
			return MakeRemoveChange(Node{Row: change.Node.Row, Relationships: rels})
		case ChangeTypeEdit:
			nodeRels := copyRelationships(change.Node.Relationships)
			oldNodeRels := copyRelationships(change.OldNode.Relationships)
			mergeEmpty(nodeRels, schema.Relationships)
			mergeEmpty(oldNodeRels, schema.Relationships)
			return MakeEditChange(
				Node{Row: change.Node.Row, Relationships: nodeRels},
				Node{Row: change.OldNode.Row, Relationships: oldNodeRels},
			)
		case ChangeTypeChild:
			return change // children only have relationships along the path to the change
		}
		return change
	}
}

func copyRelationships(rels Relationships) Relationships {
	if rels == nil {
		return make(Relationships)
	}
	copied := make(Relationships, len(rels))
	for k, v := range rels {
		copied[k] = v
	}
	return copied
}

// mergeEmpty adds empty streams for relationship names not present in rels.
func mergeEmpty(rels Relationships, schemaRels map[string]*SourceSchema) {
	for relName := range schemaRels {
		if _, ok := rels[relName]; !ok {
			rels[relName] = func() iter.Seq[Node] { return func(yield func(Node) bool) {} }
		}
	}
}
