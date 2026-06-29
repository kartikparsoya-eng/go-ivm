package ivm

import (
	"iter"
)

// UnionFanIn merges results from multiple OR-condition branches back together,
// deduplicating rows that appear in multiple branches.

// UnionFanIn implements Operator. It receives pushes from multiple branches
// that share a UnionFanOut, and merges/deduplicates them.
type UnionFanIn struct {
	inputs            []Input
	schema            *SourceSchema
	fanOutPushStarted bool
	output            Output
	accumulatedPushes []Change
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

// Fetch lazily merges sorted streams from all inputs, deduplicating by row
// identity. Uses iter.Pull to convert each branch's iter.Seq to a pull
// iterator, then performs a streaming k-way merge by comparing heads. All k
// branch cursors are held open concurrently (C_q = Σ C_branch_i) — this is
// the most cursor-hungry operator. On early stop (yield→false), defer calls
// stop() on all pull iterators, releasing upstream cursors.
func (ufi *UnionFanIn) Fetch(req FetchRequest) iter.Seq[Node] {
	if len(ufi.inputs) == 0 {
		return emptyNodeSeq
	}

	return func(yield func(Node) bool) {
		type pullIter struct {
			next func() (Node, bool)
			stop func()
			head Node
			ok   bool
		}
		iters := make([]pullIter, len(ufi.inputs))

		for i, input := range ufi.inputs {
			next, stop := iter.Pull(input.Fetch(req))
			iters[i].next = next
			iters[i].stop = stop
			iters[i].head, iters[i].ok = next()
		}

		defer func() {
			for i := range iters {
				iters[i].stop()
			}
		}()

		comparator := ufi.schema.CompareRows
		var lastRow Row

		for {
			minIdx := -1
			for i := range iters {
				if !iters[i].ok {
					continue
				}
				if minIdx == -1 || comparator(iters[i].head.Row, iters[minIdx].head.Row) < 0 {
					minIdx = i
				}
			}

			if minIdx == -1 {
				return
			}

			node := iters[minIdx].head
			iters[minIdx].head, iters[minIdx].ok = iters[minIdx].next()

			if lastRow != nil && comparator(lastRow, node.Row) == 0 {
				continue
			}

			lastRow = node.Row
			if !yield(node) {
				return
			}
		}
	}
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
		found := false
		for range input.Fetch(FetchRequest{Constraint: &constraint}) {
			found = true
			break
		}
		if found {
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
