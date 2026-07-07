package ivm

import (
	"iter"
	"reflect"
	"slices"
)

// UnionFanIn merges results from multiple OR-condition branches back together,
// deduplicating rows that appear in multiple branches.

// sameSliceRef reports whether a and b are the SAME slice value — same
// backing-array pointer and same length. This is the Go analog of TS's
// `===` on the array references (union-fan-in.ts:59-62 primaryKey, :71
// sort): reflect.DeepEqual was too lenient — two content-equal slices
// built by unrelated schemas would pass here where TS throws. Branch
// schemas all carry the fan-out schema's own slice headers (filter
// operators pass the schema pointer through unchanged; FlippedJoin copies
// parentSchema.PrimaryKey/Sort by header, flipped_join.go:83,89 — exactly
// as TS's object spread copies the array references), so identity holds
// precisely when TS's reference equality does. Both-nil compares equal,
// matching TS undefined === undefined.
func sameSliceRef[E any](a, b []E) bool {
	return len(a) == len(b) &&
		reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

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

	// Build merged schema.
	// TS: starts from the fan-out schema's relationships and appends each
	// branch's novel names via Object.entries — branch insertion order
	// (union-fan-in.ts:52-88).
	rels := make(map[string]*SourceSchema)
	for k, v := range fanOutSchema.Relationships {
		rels[k] = v
	}
	relOrder := slices.Clone(fanOutSchema.RelationshipOrder)

	relationshipsFromBranches := make(map[string]bool)
	for _, input := range inputs {
		inputSchema := input.GetSchema()
		if fanOutSchema.TableName != inputSchema.TableName {
			panic("Table name mismatch in union fan-in")
		}
		if !sameSliceRef(fanOutSchema.PrimaryKey, inputSchema.PrimaryKey) {
			panic("Primary key mismatch in union fan-in")
		}
		if fanOutSchema.System != inputSchema.System {
			panic("System mismatch in union fan-in")
		}
		if (fanOutSchema.CompareRows == nil) != (inputSchema.CompareRows == nil) ||
			(fanOutSchema.CompareRows != nil && reflect.ValueOf(fanOutSchema.CompareRows).Pointer() != reflect.ValueOf(inputSchema.CompareRows).Pointer()) {
			panic("compareRows mismatch in union fan-in")
		}
		if !sameSliceRef(fanOutSchema.Sort, inputSchema.Sort) {
			panic("Sort mismatch in union fan-in")
		}

		for _, relName := range inputSchema.RelationshipOrder {
			relSchema := inputSchema.Relationships[relName]
			if _, inFanOut := fanOutSchema.Relationships[relName]; inFanOut {
				continue
			}
			if relationshipsFromBranches[relName] {
				panic("Relationship " + relName + " exists in multiple upstream inputs to union fan-in")
			}
			rels[relName] = relSchema
			relationshipsFromBranches[relName] = true
			relOrder = append(relOrder, relName)
		}
	}

	ufi := &UnionFanIn{
		inputs: inputs,
		schema: &SourceSchema{
			TableName:         fanOutSchema.TableName,
			Columns:           fanOutSchema.Columns,
			PrimaryKey:        fanOutSchema.PrimaryKey,
			Relationships:     rels,
			RelationshipOrder: relOrder,
			IsHidden:          fanOutSchema.IsHidden,
			System:            fanOutSchema.System,
			CompareRows:       fanOutSchema.CompareRows,
			Sort:              fanOutSchema.Sort,
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

		defer func() {
			for i := range iters {
				if iters[i].stop != nil {
					iters[i].stop()
				}
			}
		}()

		for i, input := range ufi.inputs {
			next, stop := iter.Pull(input.Fetch(req))
			iters[i].next = next
			iters[i].stop = stop
			iters[i].head, iters[i].ok = next()
		}

		// #5980 (union-fan-in.ts fetch): honor req.Reverse — inputs yield
		// DESCENDING streams when reverse, so the merge must select by the
		// NEGATED comparator or the k-way merge emits scrambled order (and
		// the adjacency dedup below misses duplicates). Reachable: Take
		// issues reverse fetches on its bound-recompute paths.
		compareRows := ufi.schema.CompareRows
		comparator := compareRows
		if req.Reverse {
			comparator = func(a, b Row) int { return compareRows(b, a) }
		}
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
func (ufi *UnionFanIn) Push(change Change, pusher InputBase) {
	if !ufi.fanOutPushStarted {
		ufi.pushInternalChange(change, pusher)
		return
	}
	ufi.accumulatedPushes = append(ufi.accumulatedPushes, change)
}

// pushInternalChange handles changes from inside the fan-out/fan-in sub-graph.
func (ufi *UnionFanIn) pushInternalChange(change Change, pusher InputBase) {
	if change.Type == ChangeTypeChild {
		ufi.output.Push(change, ufi)
		return
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
			return
		}
	}

	if !hadMatch {
		panic("Pusher was not one of the inputs to union-fan-in!")
	}

	ufi.output.Push(change, ufi)
}

// FanOutStartedPushing signals that the paired fan-out has started pushing.
func (ufi *UnionFanIn) FanOutStartedPushing() {
	if ufi.fanOutPushStarted {
		panic("UnionFanIn: fanOutStartedPushing called while already pushing")
	}
	ufi.fanOutPushStarted = true
}

// FanOutDonePushing processes accumulated pushes after fan-out completes.
func (ufi *UnionFanIn) FanOutDonePushing(fanOutChangeType ChangeType) {
	if !ufi.fanOutPushStarted {
		panic("UnionFanIn: fanOutDonePushing called without fanOutStartedPushing")
	}
	ufi.fanOutPushStarted = false

	if len(ufi.inputs) == 0 {
		return
	}

	accumulated := ufi.accumulatedPushes
	ufi.accumulatedPushes = nil

	if len(accumulated) == 0 {
		return
	}

	PushAccumulatedChanges(
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
