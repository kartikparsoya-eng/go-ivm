package ivm

import (
	"iter"
	"slices"
	"sort"
)

type FlippedJoinArgs struct {
	Parent           Input
	Child            Input
	ParentKey        CompoundKey
	ChildKey         CompoundKey
	RelationshipName string
	Hidden           bool
	System           string
}

// FlippedJoin implements Input. It fetches child nodes first, then finds
// related parent nodes, outputting parents decorated with matching children.
type FlippedJoin struct {
	parent           Input
	child            Input
	parentKey        CompoundKey
	childKey         CompoundKey
	relationshipName string
	schema           *SourceSchema

	output Output

	// State for in-progress child changes (overlay logic)
	inprogressChildChange         *Change
	inprogressChildChangePosition Row
}

func NewFlippedJoin(args FlippedJoinArgs) *FlippedJoin {
	if args.Parent == args.Child {
		panic("Parent and child must be different operators")
	}
	if len(args.ParentKey) != len(args.ChildKey) {
		panic("The parentKey and childKey keys must have same length")
	}

	parentSchema := args.Parent.GetSchema()
	childSchema := args.Child.GetSchema()

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

	fj := &FlippedJoin{
		parent:           args.Parent,
		child:            args.Child,
		parentKey:        args.ParentKey,
		childKey:         args.ChildKey,
		relationshipName: args.RelationshipName,
		schema: &SourceSchema{
			TableName:     parentSchema.TableName,
			Columns:       parentSchema.Columns,
			PrimaryKey:    parentSchema.PrimaryKey,
			Relationships: rels,
			IsHidden:      parentSchema.IsHidden,
			System:        parentSchema.System,
			CompareRows:   parentSchema.CompareRows,
			Sort:          parentSchema.Sort,
		},
		output: ThrowOutput,
	}

	args.Parent.SetOutput(&flippedJoinParentOutput{fj: fj})
	args.Child.SetOutput(&flippedJoinChildOutput{fj: fj})

	return fj
}

func (fj *FlippedJoin) Destroy() {
	fj.child.Destroy()
	fj.parent.Destroy()
}

func (fj *FlippedJoin) SetOutput(output Output) {
	fj.output = output
}

func (fj *FlippedJoin) GetSchema() *SourceSchema {
	return fj.schema
}

// Fetch fetches child nodes first (eager — small filtered set), then lazily
// streams related parents via iter.Pull, merging by parent order and grouping
// children per parent. Parent cursors are held open concurrently (one per
// child); on early stop, defer calls stop() on all pull iterators. Children
// stay materialized because the remove-overlay logic splices into the slice
// and the overlay logic reads related children by index.
func (fj *FlippedJoin) Fetch(req FetchRequest) iter.Seq[Node] {
	// Translate constraints for the parent on parts of the join key to constraints for the child.
	var childConstraint Constraint
	hasChildConstraint := false
	if req.Constraint != nil {
		childConstraint = make(Constraint)
		for key, value := range *req.Constraint {
			idx := indexOf(fj.parentKey, key)
			if idx != -1 {
				hasChildConstraint = true
				childConstraint[fj.childKey[idx]] = value
			}
		}
	}

	var childReq FetchRequest
	if hasChildConstraint {
		childReq = FetchRequest{Constraint: &childConstraint}
	}
	childNodes := slices.Collect(fj.child.Fetch(childReq))

	// For remove overlay: re-insert the removed node so parents that haven't
	// been pushed yet still see it.
	if fj.inprogressChildChange != nil && fj.inprogressChildChange.Type == ChangeTypeRemove {
		removedNode := fj.inprogressChildChange.Node
		compare := fj.child.GetSchema().CompareRows
		insertPos := sort.Search(len(childNodes), func(i int) bool {
			return compare(removedNode.Row, childNodes[i].Row) <= 0
		})
		// splice in
		childNodes = append(childNodes, Node{})
		copy(childNodes[insertPos+1:], childNodes[insertPos:])
		childNodes[insertPos] = removedNode
	}

	// For each child, create a lazy parent pull iterator
	type parentPull struct {
		next func() (Node, bool)
		stop func()
		head Node
		ok   bool
	}
	pulls := make([]parentPull, len(childNodes))
	for i, childNode := range childNodes {
		constraintFromChild := BuildJoinConstraint(childNode.Row, fj.childKey, fj.parentKey)
		if constraintFromChild == nil || (req.Constraint != nil && !constraintsAreCompatible(*constraintFromChild, *req.Constraint)) {
			continue
		}
		merged := mergeConstraints(req.Constraint, constraintFromChild)
		parentReq := FetchRequest{
			Constraint: merged,
			Start:      req.Start,
			Reverse:    req.Reverse,
		}
		next, stop := iter.Pull(fj.parent.Fetch(parentReq))
		pulls[i].next = next
		pulls[i].stop = stop
		pulls[i].head, pulls[i].ok = next()
	}

	compare := func(a, b Node) int {
		cmp := fj.schema.CompareRows(a.Row, b.Row)
		if req.Reverse {
			cmp = -cmp
		}
		return cmp
	}

	return func(yield func(Node) bool) {
		defer func() {
			for i := range pulls {
				if pulls[i].stop != nil {
					pulls[i].stop()
				}
			}
		}()

		for {
			// Find minimum parent node across all pull iterators
			minIdx := -1
			for i := range pulls {
				if !pulls[i].ok {
					continue
				}
				if minIdx == -1 || compare(pulls[i].head, pulls[minIdx].head) < 0 {
					minIdx = i
				}
			}

			if minIdx == -1 {
				return
			}

			minHead := pulls[minIdx].head

			// Find all iterators with head equal to minHead
			var minChildIndexes []int
			for i := range pulls {
				if !pulls[i].ok {
					continue
				}
				if compare(pulls[i].head, minHead) == 0 {
					minChildIndexes = append(minChildIndexes, i)
				}
			}

			// Collect related children for this parent and advance iterators
			relatedChildNodes := make([]Node, 0, len(minChildIndexes))
			for _, idx := range minChildIndexes {
				relatedChildNodes = append(relatedChildNodes, childNodes[idx])
				pulls[idx].head, pulls[idx].ok = pulls[idx].next()
			}

			// Apply overlay
			overlaidRelatedChildNodes := relatedChildNodes
			if fj.inprogressChildChange != nil && fj.inprogressChildChangePosition != nil &&
				IsJoinMatch(fj.inprogressChildChange.Node.Row, fj.childKey, minHead.Row, fj.parentKey) {

				hasBeenPushed := fj.parent.GetSchema().CompareRows(minHead.Row, fj.inprogressChildChangePosition) <= 0

				if fj.inprogressChildChange.Type == ChangeTypeRemove {
					if hasBeenPushed {
						// Filter out the removed node. TS filters by reference
						// identity (flipped-join.ts:271-272: `n !== change.node`)
						// because the removed node was spliced into childNodes by
						// reference. Go copies nodes through slices, so identity
						// is unavailable — we match by the child schema's full
						// comparator instead. Equivalent ONLY because the child
						// sort is total (Zero always appends the PK to the
						// ordering), so CompareRows==0 ⟺ same row. If a non-total
						// child sort is ever introduced, this could filter a
						// DIFFERENT child that ties with the removed one.
						filtered := make([]Node, 0, len(relatedChildNodes))
						for _, n := range relatedChildNodes {
							if fj.child.GetSchema().CompareRows(n.Row, fj.inprogressChildChange.Node.Row) != 0 {
								filtered = append(filtered, n)
							}
						}
						overlaidRelatedChildNodes = filtered
					}
				} else if !hasBeenPushed {
					overlaidRelatedChildNodes = GenerateWithOverlay(relatedChildNodes, *fj.inprogressChildChange, fj.child.GetSchema())
				}
			}

			// Only yield if there are related children (inner join)
			if len(overlaidRelatedChildNodes) > 0 {
				captured := overlaidRelatedChildNodes
				nodeOut := Node{
					Row: minHead.Row,
					Relationships: mergeRelationshipMaps(minHead.Relationships, Relationships{
						fj.relationshipName: func() iter.Seq[Node] { return slices.Values(captured) },
					}),
				}
				if !yield(nodeOut) {
					return
				}
			}
		}
	}
}

// --- Push from child side ---

type flippedJoinChildOutput struct {
	fj *FlippedJoin
}

func (o *flippedJoinChildOutput) Push(change Change, _ InputBase) []Change {
	return o.fj.pushChild(change)
}

func (fj *FlippedJoin) pushChild(change Change) []Change {
	switch change.Type {
	case ChangeTypeAdd, ChangeTypeRemove:
		return fj.pushChildChange(change, false)
	case ChangeTypeEdit:
		if !RowEqualsForCompoundKey(change.OldNode.Row, change.Node.Row, fj.childKey) {
			panic(joinKeyChangeDrift(fj.child.GetSchema(), change.OldNode.Row, "FlippedJoin-child-key-change"))
		}
		return fj.pushChildChange(change, true)
	case ChangeTypeChild:
		return fj.pushChildChange(change, true)
	}
	return nil
}

// pushChildChange — source: flipped-join.ts line 346-425
func (fj *FlippedJoin) pushChildChange(change Change, exists bool) []Change {
	fj.inprogressChildChange = &change
	fj.inprogressChildChangePosition = nil
	defer func() { fj.inprogressChildChange = nil }()

	constraint := BuildJoinConstraint(change.Node.Row, fj.childKey, fj.parentKey)
	if constraint == nil {
		return nil
	}

	parentNodes := slices.Collect(fj.parent.Fetch(FetchRequest{Constraint: constraint}))
	var allChanges []Change

	for _, parentNode := range parentNodes {
		fj.inprogressChildChange = &change
		fj.inprogressChildChangePosition = parentNode.Row

		childNodeStream := func() iter.Seq[Node] {
			return func(yield func(Node) bool) {
				c := BuildJoinConstraint(parentNode.Row, fj.parentKey, fj.childKey)
				if c == nil {
					return
				}
				for n := range fj.child.Fetch(FetchRequest{Constraint: c}) {
					if !yield(n) {
						return
					}
				}
			}
		}

		if !exists {
			for childNode := range childNodeStream() {
				if fj.child.GetSchema().CompareRows(childNode.Row, change.Node.Row) != 0 {
					exists = true
					break
				}
			}
		}

		if exists {
			outNode := Node{
				Row: parentNode.Row,
				Relationships: mergeRelationshipMaps(parentNode.Relationships, Relationships{
					fj.relationshipName: childNodeStream,
				}),
			}
			outChange := MakeChildChange(outNode, ChildData{
				RelationshipName: fj.relationshipName,
				Change:           change,
			})
			allChanges = append(allChanges, fj.output.Push(outChange, fj)...)
		} else {
			outNode := Node{
				Row: parentNode.Row,
				Relationships: mergeRelationshipMaps(parentNode.Relationships, Relationships{
					fj.relationshipName: func() iter.Seq[Node] { return slices.Values([]Node{change.Node}) },
				}),
			}
			var outChange Change
			if change.Type == ChangeTypeAdd {
				outChange = MakeAddChange(outNode)
			} else {
				outChange = MakeRemoveChange(outNode)
			}
			allChanges = append(allChanges, fj.output.Push(outChange, fj)...)
		}
	}

	return allChanges
}

// --- Push from parent side ---

type flippedJoinParentOutput struct {
	fj *FlippedJoin
}

func (o *flippedJoinParentOutput) Push(change Change, _ InputBase) []Change {
	return o.fj.pushParent(change)
}

// pushParent — source: flipped-join.ts line 427-504
func (fj *FlippedJoin) pushParent(change Change) []Change {
	childNodeStream := func(node Node) func() iter.Seq[Node] {
		return func() iter.Seq[Node] {
			return func(yield func(Node) bool) {
				c := BuildJoinConstraint(node.Row, fj.parentKey, fj.childKey)
				if c == nil {
					return
				}
				for n := range fj.child.Fetch(FetchRequest{Constraint: c}) {
					if !yield(n) {
						return
					}
				}
			}
		}
	}

	flip := func(node Node) Node {
		return Node{
			Row: node.Row,
			Relationships: mergeRelationshipMaps(node.Relationships, Relationships{
				fj.relationshipName: childNodeStream(node),
			}),
		}
	}

	// If no related child, don't push (inner join)
	hasChildren := false
	for range childNodeStream(change.Node)() {
		hasChildren = true
		break
	}
	if !hasChildren {
		return nil
	}

	switch change.Type {
	case ChangeTypeAdd:
		return fj.output.Push(MakeAddChange(flip(change.Node)), fj)
	case ChangeTypeRemove:
		return fj.output.Push(MakeRemoveChange(flip(change.Node)), fj)
	case ChangeTypeChild:
		return fj.output.Push(MakeChildChange(flip(change.Node), *change.Child), fj)
	case ChangeTypeEdit:
		if !RowEqualsForCompoundKey(change.OldNode.Row, change.Node.Row, fj.parentKey) {
			panic(joinKeyChangeDrift(fj.schema, change.OldNode.Row, "FlippedJoin-parent-key-change"))
		}
		return fj.output.Push(MakeEditChange(flip(change.Node), flip(*change.OldNode)), fj)
	}
	return nil
}

// --- Helpers ---

func indexOf(key CompoundKey, s string) int {
	for i, k := range key {
		if k == s {
			return i
		}
	}
	return -1
}

// constraintsAreCompatible — TS uses valuesEqual, which treats null/null as
// unequal. CompareValues treats nil/nil as equal (returns 0) which would
// incorrectly mark two null-FK constraints as compatible.
func constraintsAreCompatible(a, b Constraint) bool {
	for k, v := range a {
		if bv, ok := b[k]; ok {
			if !ValuesEqual(v, bv) {
				return false
			}
		}
	}
	return true
}

func mergeConstraints(existing *Constraint, additional *Constraint) *Constraint {
	if existing == nil && additional == nil {
		return nil
	}
	merged := make(Constraint)
	if existing != nil {
		for k, v := range *existing {
			merged[k] = v
		}
	}
	if additional != nil {
		for k, v := range *additional {
			merged[k] = v
		}
	}
	return &merged
}
