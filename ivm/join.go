package ivm

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
	// Scalar marks this join as the EXISTS side of a scalar correlated
	// subquery (csq.Scalar=true in the AST). TS pre-resolves these via
	// resolveSimpleScalarSubqueries before pipeline construction, so the
	// child relationship never reaches the TS streamer. We still build
	// the join (the EXISTS check is needed for filtering) but mark the
	// child schema IsScalar so the streamer drops its row emissions,
	// matching TS output.
	Scalar bool
	System string
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
		IsScalar:      args.Scalar,
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

// Fetch — fetches parent nodes and attaches child relationship.
func (j *Join) Fetch(req FetchRequest) []Node {
	parentNodes := j.parent.Fetch(req)
	result := make([]Node, 0, len(parentNodes))
	for _, pn := range parentNodes {
		result = append(result, j.processParentNode(pn.Row, pn.Relationships))
	}
	return result
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
		// Assert the edit could not change the relationship
		if !RowEqualsForCompoundKey(change.OldNode.Row, change.Node.Row, j.parentKey) {
			panic("Parent edit must not change relationship.")
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
			panic("Child edit must not change relationship.")
		}
		return j.pushChildChange(change.Node.Row, change)
	}
	panic("unreachable")
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
	parentNodes := j.parent.Fetch(FetchRequest{Constraint: constraint})
	for _, parentNode := range parentNodes {
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
func (j *Join) processParentNode(parentNodeRow Row, parentNodeRelations map[string]func() []Node) Node {
	childStream := func() []Node {
		constraint := BuildJoinConstraint(parentNodeRow, j.parentKey, j.childKey)
		var nodes []Node
		if constraint != nil {
			nodes = j.child.Fetch(FetchRequest{Constraint: constraint})
		}

		if j.inprogressChildChange != nil &&
			IsJoinMatch(parentNodeRow, j.parentKey, j.inprogressChildChange.Node.Row, j.childKey) &&
			j.inprogressChildChangePosition != nil &&
			j.schema.CompareRows(parentNodeRow, j.inprogressChildChangePosition) > 0 {

			childSchema := j.child.GetSchema()
			if childSchema.Sort == nil {
				return GenerateWithOverlayUnordered(nodes, *j.inprogressChildChange, childSchema)
			}
			return GenerateWithOverlay(nodes, *j.inprogressChildChange, childSchema)
		}
		return nodes
	}

	newRels := make(map[string]func() []Node)
	for k, v := range parentNodeRelations {
		newRels[k] = v
	}
	newRels[j.relationshipName] = childStream

	return Node{
		Row:           parentNodeRow,
		Relationships: newRels,
	}
}
