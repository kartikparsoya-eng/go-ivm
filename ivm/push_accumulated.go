package ivm

import "iter"

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
		existing, exists := candidatesToPush[change.Type]
		mergedChange := change
		if exists {
			mergedChange = mergeRelationships(existing, change)
		}
		candidatesToPush[change.Type] = mergedChange
	}

	switch fanOutChangeType {
	case ChangeTypeRemove:
		c, ok := candidatesToPush[ChangeTypeRemove]
		if !ok {
			panic("pushAccumulatedChanges: REMOVE expected but not found in candidates")
		}
		return output.Push(addEmptyRelationships(c), pusher)

	case ChangeTypeAdd:
		c, ok := candidatesToPush[ChangeTypeAdd]
		if !ok {
			panic("pushAccumulatedChanges: ADD expected but not found in candidates")
		}
		return output.Push(addEmptyRelationships(c), pusher)

	case ChangeTypeEdit:
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
			return MakeChildChange(
				Node{
					Row:           left.Node.Row,
					Relationships: mergeRelationshipMaps(left.Node.Relationships, right.Node.Relationships),
				},
				*left.Child,
			)
		}
	}

	// Types differ — left must be edit
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
