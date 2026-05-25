package ivm

// UnionFanIn merges results from multiple OR-condition branches back together,
// deduplicating rows that appear in multiple branches.

// UnionFanIn implements Operator. It receives pushes from multiple branches
// that share a UnionFanOut, and merges/deduplicates them.
type UnionFanIn struct {
	inputs             []Input
	schema             *SourceSchema
	fanOutPushStarted  bool
	output             Output
	accumulatedPushes  []Change
}

func NewUnionFanIn(fanOut *UnionFanOut, inputs []Input) *UnionFanIn {
	fanOutSchema := fanOut.GetSchema()
	if fanOutSchema.Sort == nil {
		panic("UnionFanIn requires sorted input")
	}

	// Build merged schema
	rels := make(map[string]*SourceSchema)
	for k, v := range fanOutSchema.Relationships {
		rels[k] = v
	}

	relationshipsFromBranches := make(map[string]bool)
	for _, input := range inputs {
		inputSchema := input.GetSchema()
		if fanOutSchema.TableName != inputSchema.TableName {
			panic("Table name mismatch in union fan-in")
		}
		if len(fanOutSchema.PrimaryKey) != len(inputSchema.PrimaryKey) {
			panic("Primary key mismatch in union fan-in")
		}
		if fanOutSchema.System != inputSchema.System {
			panic("System mismatch in union fan-in")
		}

		for relName, relSchema := range inputSchema.Relationships {
			if _, inFanOut := fanOutSchema.Relationships[relName]; inFanOut {
				continue
			}
			if relationshipsFromBranches[relName] {
				panic("Relationship " + relName + " exists in multiple upstream inputs to union fan-in")
			}
			rels[relName] = relSchema
			relationshipsFromBranches[relName] = true
		}
	}

	ufi := &UnionFanIn{
		inputs: inputs,
		schema: &SourceSchema{
			TableName:     fanOutSchema.TableName,
			Columns:       fanOutSchema.Columns,
			PrimaryKey:    fanOutSchema.PrimaryKey,
			Relationships: rels,
			IsHidden:      fanOutSchema.IsHidden,
			System:        fanOutSchema.System,
			CompareRows:   fanOutSchema.CompareRows,
			Sort:          fanOutSchema.Sort,
		},
		output: ThrowOutput,
	}

	fanOut.SetFanIn(ufi)
	for _, input := range inputs {
		input.SetOutput(ufi)
	}

	return ufi
}

func (ufi *UnionFanIn) Destroy() {
	for _, input := range ufi.inputs {
		input.Destroy()
	}
}

// Fetch merges sorted fetches from all inputs, deduplicating by row identity.
func (ufi *UnionFanIn) Fetch(req FetchRequest) []Node {
	fetches := make([][]Node, len(ufi.inputs))
	for i, input := range ufi.inputs {
		fetches[i] = input.Fetch(req)
	}
	return MergeFetches(fetches, ufi.schema.CompareRows)
}

func (ufi *UnionFanIn) GetSchema() *SourceSchema {
	return ufi.schema
}

// Push receives a change from a branch. If fan-out is active, accumulates;
// otherwise processes as an internal change.
func (ufi *UnionFanIn) Push(change Change, pusher InputBase) []Change {
	if !ufi.fanOutPushStarted {
		return ufi.pushInternalChange(change, pusher)
	}
	ufi.accumulatedPushes = append(ufi.accumulatedPushes, change)
	return nil
}

// pushInternalChange handles changes from inside the fan-out/fan-in sub-graph.
func (ufi *UnionFanIn) pushInternalChange(change Change, pusher InputBase) []Change {
	if change.Type == ChangeTypeChild {
		return ufi.output.Push(change, ufi)
	}

	if change.Type != ChangeTypeAdd && change.Type != ChangeTypeRemove {
		panic("UnionFanIn: expected add or remove change type")
	}

	hadMatch := false
	for _, input := range ufi.inputs {
		if input == pusher {
			hadMatch = true
			continue
		}

		constraint := make(Constraint)
		for _, key := range ufi.schema.PrimaryKey {
			constraint[key] = change.Node.Row[key]
		}
		fetchResult := input.Fetch(FetchRequest{Constraint: &constraint})
		if len(fetchResult) > 0 {
			// Another branch has the row
			return nil
		}
	}

	if !hadMatch {
		panic("Pusher was not one of the inputs to union-fan-in!")
	}

	return ufi.output.Push(change, ufi)
}

// FanOutStartedPushing signals that the paired fan-out has started pushing.
func (ufi *UnionFanIn) FanOutStartedPushing() {
	if ufi.fanOutPushStarted {
		panic("UnionFanIn: fanOutStartedPushing called while already pushing")
	}
	ufi.fanOutPushStarted = true
}

// FanOutDonePushing processes accumulated pushes after fan-out completes.
func (ufi *UnionFanIn) FanOutDonePushing(fanOutChangeType ChangeType) []Change {
	if !ufi.fanOutPushStarted {
		panic("UnionFanIn: fanOutDonePushing called without fanOutStartedPushing")
	}
	ufi.fanOutPushStarted = false

	if len(ufi.inputs) == 0 {
		return nil
	}

	accumulated := ufi.accumulatedPushes
	ufi.accumulatedPushes = nil

	if len(accumulated) == 0 {
		return nil
	}

	return PushAccumulatedChanges(
		accumulated,
		ufi.output,
		ufi,
		fanOutChangeType,
		MergeRelationships,
		MakeAddEmptyRelationships(ufi.schema),
	)
}

func (ufi *UnionFanIn) SetOutput(output Output) {
	ufi.output = output
}

// MergeFetches merges multiple sorted node slices, deduplicating equal rows.
func MergeFetches(fetches [][]Node, comparator Comparator) []Node {
	// Initialize positions
	positions := make([]int, len(fetches))
	var result []Node
	var lastNode *Node

	for {
		var minNode *Node
		minIdx := -1

		for i, pos := range positions {
			if pos >= len(fetches[i]) {
				continue
			}
			node := &fetches[i][pos]
			if minNode == nil || comparator(node.Row, minNode.Row) < 0 {
				minNode = node
				minIdx = i
			}
		}

		if minNode == nil {
			break
		}

		positions[minIdx]++

		// Deduplicate
		if lastNode != nil && comparator(lastNode.Row, minNode.Row) == 0 {
			continue
		}

		result = append(result, *minNode)
		lastNode = minNode
	}

	return result
}
