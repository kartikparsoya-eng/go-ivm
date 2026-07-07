package ivm

import (
	"fmt"
	"iter"
	"slices"
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
) {
	if len(accumulatedPushes) == 0 {
		return
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
		output.Push(addEmptyRelationships(c), pusher)
		return

	case ChangeTypeAdd:
		// Source: push-accumulated.ts:149-152 — an ADD fan-out must yield only
		// adds (types.length === 1 && types[0] === ADD).
		c, ok := candidatesToPush[ChangeTypeAdd]
		if len(candidatesToPush) != 1 || !ok {
			panic("Fan-in:add expected all adds")
		}
		output.Push(addEmptyRelationships(c), pusher)
		return

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
			output.Push(addEmptyRelationships(editChange), pusher)
			return
		}

		// Both add and remove → convert back to edit
		if hasAdd && hasRemove {
			edit := MakeEditChange(addChange.Node, removeChange.Node)
			output.Push(addEmptyRelationships(edit), pusher)
			return
		}

		// Only one of add/remove
		if hasAdd {
			output.Push(addEmptyRelationships(addChange), pusher)
			return
		}
		if hasRemove {
			output.Push(addEmptyRelationships(removeChange), pusher)
			return
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
			output.Push(childChange, pusher)
			return
		}

		addChange, hasAdd := candidatesToPush[ChangeTypeAdd]
		removeChange, hasRemove := candidatesToPush[ChangeTypeRemove]

		if hasAdd && hasRemove {
			panic("Fan-in:child expected either add or remove, not both")
		}

		if hasAdd {
			output.Push(addEmptyRelationships(addChange), pusher)
			return
		}
		if hasRemove {
			output.Push(addEmptyRelationships(removeChange), pusher)
			return
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
			rels, order := mergeRelationshipMaps(left.Node, right.Node)
			return MakeAddChange(Node{
				Row:           left.Node.Row,
				Relationships: rels,
				RelOrder:      order,
			})
		case ChangeTypeRemove:
			rels, order := mergeRelationshipMaps(left.Node, right.Node)
			return MakeRemoveChange(Node{
				Row:           left.Node.Row,
				Relationships: rels,
				RelOrder:      order,
			})
		case ChangeTypeEdit:
			// Source: push-accumulated.ts:290-294.
			if right.Type != ChangeTypeEdit {
				panic("mergeRelationships: when left.type is edit and types match, right.type must be edit")
			}
			newRels, newOrder := mergeRelationshipMaps(left.Node, right.Node)
			oldRels, oldOrder := mergeRelationshipMaps(*left.OldNode, *right.OldNode)
			return MakeEditChange(
				Node{
					Row:           left.Node.Row,
					Relationships: newRels,
					RelOrder:      newOrder,
				},
				Node{
					Row:           left.OldNode.Row,
					Relationships: oldRels,
					RelOrder:      oldOrder,
				},
			)
		case ChangeTypeChild:
			// Source: push-accumulated.ts:317-321.
			if right.Type != ChangeTypeChild {
				panic("mergeRelationships: when left.type is child and types match, right.type must be child")
			}
			rels, order := mergeRelationshipMaps(left.Node, right.Node)
			return MakeChildChange(
				Node{
					Row:           left.Node.Row,
					Relationships: rels,
					RelOrder:      order,
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
		rels, order := mergeRelationshipMaps(left.Node, right.Node)
		return MakeEditChange(
			Node{
				Row:           left.Node.Row,
				Relationships: rels,
				RelOrder:      order,
			},
			*left.OldNode,
		)
	case ChangeTypeRemove:
		rels, order := mergeRelationshipMaps(*left.OldNode, right.Node)
		return MakeEditChange(
			left.Node,
			Node{
				Row:           left.OldNode.Row,
				Relationships: rels,
				RelOrder:      order,
			},
		)
	}

	panic("unreachable: mergeRelationships")
}

// Relationships is a type alias for the relationship map in Node.
type Relationships = map[string]func() iter.Seq[Node]

// mergeRelationshipMaps merges two nodes' relationship maps, left values
// taking precedence, and returns the TS `{...right, ...left}` iteration
// order (push-accumulated.ts:274-277): right's names in right's order — a
// name present in both keeps RIGHT's position with LEFT's value — then
// left's novel names appended in left's order. The fan-in accumulates
// branch pushes first-branch-first, and each merge folds the LATER branch
// in as `right`, so merged nodes emit later-branch names first on the wire
// exactly as TS does.
func mergeRelationshipMaps(left, right Node) (Relationships, []string) {
	if left.Relationships == nil && right.Relationships == nil {
		return nil, nil
	}
	merged := make(Relationships, len(left.Relationships)+len(right.Relationships))
	order := make([]string, 0, len(left.Relationships)+len(right.Relationships))
	for _, k := range right.RelOrder {
		merged[k] = right.Relationships[k]
		order = append(order, k)
	}
	for _, k := range left.RelOrder {
		if _, ok := merged[k]; !ok {
			order = append(order, k)
		}
		merged[k] = left.Relationships[k]
	}
	return merged, order
}

// MakeAddEmptyRelationships returns a function that fills missing relationships with empty streams.
func MakeAddEmptyRelationships(schema *SourceSchema) AddEmptyRelationshipsFunc {
	return func(change Change) Change {
		if len(schema.Relationships) == 0 {
			return change
		}

		switch change.Type {
		case ChangeTypeAdd:
			rels, order := copyRelationships(change.Node)
			order = mergeEmpty(rels, order, schema)
			return MakeAddChange(Node{Row: change.Node.Row, Relationships: rels, RelOrder: order})
		case ChangeTypeRemove:
			rels, order := copyRelationships(change.Node)
			order = mergeEmpty(rels, order, schema)
			return MakeRemoveChange(Node{Row: change.Node.Row, Relationships: rels, RelOrder: order})
		case ChangeTypeEdit:
			nodeRels, nodeOrder := copyRelationships(change.Node)
			oldNodeRels, oldNodeOrder := copyRelationships(*change.OldNode)
			nodeOrder = mergeEmpty(nodeRels, nodeOrder, schema)
			oldNodeOrder = mergeEmpty(oldNodeRels, oldNodeOrder, schema)
			return MakeEditChange(
				Node{Row: change.Node.Row, Relationships: nodeRels, RelOrder: nodeOrder},
				Node{Row: change.OldNode.Row, Relationships: oldNodeRels, RelOrder: oldNodeOrder},
			)
		case ChangeTypeChild:
			return change // children only have relationships along the path to the change
		}
		return change
	}
}

// copyRelationships returns copies of the node's relationship map and order
// (TS `{...change.node.relationships}` — same key order).
func copyRelationships(node Node) (Relationships, []string) {
	if node.Relationships == nil {
		return make(Relationships), nil
	}
	copied := make(Relationships, len(node.Relationships))
	for k, v := range node.Relationships {
		copied[k] = v
	}
	return copied, slices.Clone(node.RelOrder)
}

// mergeEmpty adds empty streams for schema relationship names not present in
// rels, appending them to order AFTER the node's existing names — TS
// mergeEmpty walks Object.keys(schema.relationships) (push-accumulated.ts
// :421-430), i.e. the schema's insertion order, which schema
// .RelationshipOrder carries. Returns the extended order.
func mergeEmpty(rels Relationships, order []string, schema *SourceSchema) []string {
	for _, relName := range schema.RelationshipOrder {
		if _, ok := rels[relName]; !ok {
			rels[relName] = func() iter.Seq[Node] { return func(yield func(Node) bool) {} }
			order = append(order, relName)
		}
	}
	return order
}
