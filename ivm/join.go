package ivm

import (
	"iter"
	"slices"
)

// The Join operator joins output from two upstream inputs (parent + child).
// Unlike SQL join, it outputs hierarchical data — parent nodes gain a new
// relationship containing their matching child nodes.

type JoinArgs struct {
	Parent           Input
	Child            Input
	ParentKey        CompoundKey
	ChildKey         CompoundKey
	RelationshipName string
	Hidden           bool
	System           string
}

// Join implements Input. It joins parent and child streams hierarchically.
type Join struct {
	parent           Input
	child            Input
	parentKey        CompoundKey
	childKey         CompoundKey
	relationshipName string
	schema           *SourceSchema

	output Output

	// State for in-progress child changes (used by processParentNode)
	inprogressChildChange         *Change
	inprogressChildChangePosition Row
}

func NewJoin(args JoinArgs) *Join {
	if args.Parent == args.Child {
		panic("Parent and child must be different operators")
	}
	if len(args.ParentKey) != len(args.ChildKey) {
		panic("The parentKey and childKey keys must have same length")
	}

	parentSchema := args.Parent.GetSchema()
	childSchema := args.Child.GetSchema()

	// Build merged schema with new relationship
	rels := make(map[string]*SourceSchema)
	for k, v := range parentSchema.Relationships {
		rels[k] = v
	}
	rels[args.RelationshipName] = &SourceSchema{
		TableName:     childSchema.TableName,
		Columns:       childSchema.Columns,
		PrimaryKey:    childSchema.PrimaryKey,
		Relationships: childSchema.Relationships,
		IsHidden:      args.Hidden,
		System:        args.System,
		CompareRows:   childSchema.CompareRows,
		Sort:          childSchema.Sort,
	}

	schema := &SourceSchema{
		TableName:     parentSchema.TableName,
		Columns:       parentSchema.Columns,
		PrimaryKey:    parentSchema.PrimaryKey,
		Relationships: rels,
		IsHidden:      parentSchema.IsHidden,
		System:        parentSchema.System,
		CompareRows:   parentSchema.CompareRows,
		Sort:          parentSchema.Sort,
	}

	j := &Join{
		parent:           args.Parent,
		child:            args.Child,
		parentKey:        args.ParentKey,
		childKey:         args.ChildKey,
		relationshipName: args.RelationshipName,
		schema:           schema,
		output:           ThrowOutput,
	}

	// Wire parent and child outputs to this join
	args.Parent.SetOutput(joinParentOutput{j: j})
	args.Child.SetOutput(joinChildOutput{j: j})

	return j
}

// joinParentOutput adapts Join to receive pushes from parent.
type joinParentOutput struct{ j *Join }

func (o joinParentOutput) Push(change Change, pusher InputBase) []Change {
	return o.j.pushParent(change)
}

// joinChildOutput adapts Join to receive pushes from child.
type joinChildOutput struct{ j *Join }

func (o joinChildOutput) Push(change Change, pusher InputBase) []Change {
	return o.j.pushChild(change)
}

func (j *Join) Destroy() {
	j.parent.Destroy()
	j.child.Destroy()
}

func (j *Join) SetOutput(output Output) {
	j.output = output
}

func (j *Join) GetSchema() *SourceSchema {
	return j.schema
}

// Fetch lazily streams parent nodes, attaching a child relationship closure
// to each. The parent cursor is held open for the lifetime of the seq; the
// child fetch happens on-demand when the consumer calls the relationship
// closure (processParentNode). C_q = C_parent + C_child.
func (j *Join) Fetch(req FetchRequest) iter.Seq[Node] {
	return func(yield func(Node) bool) {
		for pn := range j.parent.Fetch(req) {
			if !yield(j.processParentNode(pn.Row, pn.Relationships)) {
				return
			}
		}
	}
}

// pushParent — handles changes from the parent input.
func (j *Join) pushParent(change Change) []Change {
	switch change.Type {
	case ChangeTypeAdd:
		return j.output.Push(
			MakeAddChange(j.processParentNode(change.Node.Row, change.Node.Relationships)),
			j,
		)
	case ChangeTypeRemove:
		return j.output.Push(
			MakeRemoveChange(j.processParentNode(change.Node.Row, change.Node.Relationships)),
			j,
		)
	case ChangeTypeChild:
		return j.output.Push(
			MakeChildChange(
				j.processParentNode(change.Node.Row, change.Node.Relationships),
				*change.Child,
			),
			j,
		)
	case ChangeTypeEdit:
		// Assert the edit could not change the relationship. A key-changing edit
		// should have been split into Remove(old)+Add(new) at the source; if it
		// wasn't, panic — TS asserts (throws) here (join.ts) and the view-syncer
		// tears the client group down. See joinKeyChangeError.
		if !RowEqualsForCompoundKey(change.OldNode.Row, change.Node.Row, j.parentKey) {
			panic(joinKeyChangeError(j.parent.GetSchema(), change.OldNode.Row, "Join-parent-key-change"))
		}
		return j.output.Push(
			MakeEditChange(
				j.processParentNode(change.Node.Row, change.Node.Relationships),
				j.processParentNode(change.OldNode.Row, change.OldNode.Relationships),
			),
			j,
		)
	}
	panic("unreachable")
}

// pushChild — handles changes from the child input.
func (j *Join) pushChild(change Change) []Change {
	switch change.Type {
	case ChangeTypeAdd, ChangeTypeRemove:
		return j.pushChildChange(change.Node.Row, change)
	case ChangeTypeChild:
		return j.pushChildChange(change.Node.Row, change)
	case ChangeTypeEdit:
		if !RowEqualsForCompoundKey(change.OldNode.Row, change.Node.Row, j.childKey) {
			panic(joinKeyChangeError(j.child.GetSchema(), change.OldNode.Row, "Join-child-key-change"))
		}
		return j.pushChildChange(change.Node.Row, change)
	}
	panic("unreachable")
}

// joinKeyChangeError builds the plain error for a Join/FlippedJoin invariant
// violation: an Edit reached the join with a CHANGED join key. TS asserts
// (throws) here on the premise that the upstream source split-edits any
// key-crossing edit into Remove(old)+Add(new) before it reaches the join — so
// reaching this point means either the split-edit keys weren't registered or
// the source state diverged. The panic propagates out of the engine → RPC
// error → 'unclassified' → CG teardown, exactly TS's disposition for the
// assert. `op` distinguishes the four call sites (parent vs child, Join vs
// FlippedJoin) for diagnostics.
func joinKeyChangeError(schema *SourceSchema, oldRow Row, op string) error {
	pk := map[string]Value{}
	table := ""
	if schema != nil {
		table = schema.TableName
		for _, c := range schema.PrimaryKey {
			pk[c] = oldRow[c]
		}
	}
	return SourceDriftError(table, op, pk, -1)
}

// pushChildChange — finds matching parents and pushes ChildChanges downstream.
func (j *Join) pushChildChange(childRow Row, change Change) []Change {
	j.inprogressChildChange = &change
	j.inprogressChildChangePosition = nil
	defer func() { j.inprogressChildChange = nil }()

	constraint := BuildJoinConstraint(childRow, j.childKey, j.parentKey)
	if constraint == nil {
		return nil
	}

	var allChanges []Change
	for parentNode := range j.parent.Fetch(FetchRequest{Constraint: constraint}) {
		j.inprogressChildChangePosition = parentNode.Row
		childChange := MakeChildChange(
			j.processParentNode(parentNode.Row, parentNode.Relationships),
			ChildData{
				RelationshipName: j.relationshipName,
				Change:           change,
			},
		)
		allChanges = append(allChanges, j.output.Push(childChange, j)...)
	}
	return allChanges
}

// processParentNode — attaches the child relationship stream to a parent node.
func (j *Join) processParentNode(parentNodeRow Row, parentNodeRelations map[string]func() iter.Seq[Node]) Node {
	childStream := func() iter.Seq[Node] {
		return func(yield func(Node) bool) {
			constraint := BuildJoinConstraint(parentNodeRow, j.parentKey, j.childKey)
			if constraint == nil {
				return
			}

			if j.inprogressChildChange != nil &&
				IsJoinMatch(parentNodeRow, j.parentKey, j.inprogressChildChange.Node.Row, j.childKey) &&
				j.inprogressChildChangePosition != nil &&
				j.schema.CompareRows(parentNodeRow, j.inprogressChildChangePosition) > 0 {

				nodes := slices.Collect(j.child.Fetch(FetchRequest{Constraint: constraint}))
				childSchema := j.child.GetSchema()
				var overlaid []Node
				if childSchema.Sort == nil {
					overlaid = GenerateWithOverlayUnordered(nodes, *j.inprogressChildChange, childSchema)
				} else {
					overlaid = GenerateWithOverlay(nodes, *j.inprogressChildChange, childSchema)
				}
				for _, n := range overlaid {
					if !yield(n) {
						return
					}
				}
				return
			}

			for n := range j.child.Fetch(FetchRequest{Constraint: constraint}) {
				if !yield(n) {
					return
				}
			}
		}
	}

	newRels := make(map[string]func() iter.Seq[Node])
	for k, v := range parentNodeRelations {
		newRels[k] = v
	}
	newRels[j.relationshipName] = childStream

	return Node{
		Row:           parentNodeRow,
		Relationships: newRels,
	}
}
