package ivm

import (
	"container/heap"
	"encoding/json"
	"iter"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

type FlippedJoinArgs struct {
	Parent           Input
	Child            Input
	ParentKey        CompoundKey
	ChildKey         CompoundKey
	RelationshipName string
	Hidden           bool
	System           System
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

	// TS: {...parentSchema.relationships, [relationshipName]: {...}} —
	// flipped-join.ts:128-138. A new name is appended last; an existing name
	// keeps its original position (JS spread + computed-key semantics).
	rels := make(map[string]*SourceSchema)
	for k, v := range parentSchema.Relationships {
		rels[k] = v
	}
	relOrder := slices.Clone(parentSchema.RelationshipOrder)
	if _, exists := parentSchema.Relationships[args.RelationshipName]; !exists {
		relOrder = append(relOrder, args.RelationshipName)
	}
	rels[args.RelationshipName] = &SourceSchema{
		TableName:         childSchema.TableName,
		Columns:           childSchema.Columns,
		PrimaryKey:        childSchema.PrimaryKey,
		Relationships:     childSchema.Relationships,
		RelationshipOrder: childSchema.RelationshipOrder,
		IsHidden:          args.Hidden,
		System:            args.System,
		CompareRows:       childSchema.CompareRows,
		Sort:              childSchema.Sort,
	}

	fj := &FlippedJoin{
		parent:           args.Parent,
		child:            args.Child,
		parentKey:        args.ParentKey,
		childKey:         args.ChildKey,
		relationshipName: args.RelationshipName,
		schema: &SourceSchema{
			TableName:         parentSchema.TableName,
			Columns:           parentSchema.Columns,
			PrimaryKey:        parentSchema.PrimaryKey,
			Relationships:     rels,
			RelationshipOrder: relOrder,
			IsHidden:          parentSchema.IsHidden,
			System:            parentSchema.System,
			CompareRows:       parentSchema.CompareRows,
			Sort:              parentSchema.Sort,
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

// multiConstraintChunkSize is the maximum number of entries sent in a single
// batched parent fetch (TS MULTI_CONSTRAINT_CHUNK_SIZE, flipped-join.ts @
// 1.7.0). Larger child-node sets are split into multiple fetches whose
// sorted results are merged.
//
// Why bound this (TS rationale, applies to the SQL leaf identically):
//   - Bounded overfetch on early termination at chunk N.
//   - Parameter limit: well under SQLite's default
//     SQLITE_MAX_VARIABLE_NUMBER (32766); compound keys multiply the
//     parameter count by key length.
//   - Statement-cache hits across calls of the same chunk size.
//
// Atomic because the engine fetches pipelines from multiple goroutines
// (parallel hydrate) while a test may have adjusted it; production never
// writes it after init.
var multiConstraintChunkSize atomic.Int32

func init() { multiConstraintChunkSize.Store(256) }

// SetMultiConstraintChunkSizeForTest overrides the chunk size and returns a
// restore function. Test only (TS setMultiConstraintChunkSizeForTest).
func SetMultiConstraintChunkSizeForTest(size int) func() {
	prev := multiConstraintChunkSize.Swap(int32(size))
	return func() { multiConstraintChunkSize.Store(prev) }
}

// Fetch fetches child nodes first (eager — small filtered set), then fetches
// the matching parents in BATCHED calls using FetchRequest.MultiConstraints
// (TS #fetchBatched, zero 1.7.0 #5928): the deduped child→parent key tuples
// become one multi-row IN clause per chunk, so the source issues one indexed
// SQL query per chunk instead of N per-child cursors. Within a chunk the
// source returns parents in compareRows order; across chunks the sorted
// results are merged, so the overall stream is ordered.
//
// This replaces the previous per-child fetch strategy (fetch parents for
// each child sequentially, collect all pairs, sort, group). The batched form
// is result-equivalent: a parent row's key tuple matches exactly one deduped
// multi entry, so it appears in exactly one chunk exactly once, and children
// map back to it via the same canonical key.
//
// For multi sets larger than the chunk size, Go fetches each chunk fully
// (one bounded-IN query per chunk — the eager leaf pays that scan for ANY
// query shape), buffers it, and heap-merges the buffers — TS's
// one-query-per-chunk cost with buffered rows standing in for TS's open
// streaming cursors. See fetchChunkedSequential for why the previous
// per-head keyset-reopen shape was quadratic and wedge-prone.
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

	return fj.fetchBatched(req, childNodes)
}

// fetchBatched builds the deduped multi-constraint + key→child-indexes map
// and yields each fetched parent with its related children (TS
// #fetchBatched). See Fetch for the chunking strategy.
func (fj *FlippedJoin) fetchBatched(req FetchRequest, childNodes []Node) iter.Seq[Node] {
	parentKey := fj.parentKey
	childKey := fj.childKey

	// Build (deduped) multi-constraint and a key→child-indexes map. Same
	// parent-key value across multiple children groups them together.
	var computedMulti MultiConstraint
	childIndexesByKey := make(map[string][]int)
	for i := range childNodes {
		constraintFromChild := BuildJoinConstraint(childNodes[i].Row, childKey, parentKey)
		if constraintFromChild == nil ||
			(req.Constraint != nil && !constraintsAreCompatible(*constraintFromChild, *req.Constraint)) {
			continue
		}
		key := canonicalKey(*constraintFromChild, parentKey)
		if existing, ok := childIndexesByKey[key]; ok {
			childIndexesByKey[key] = append(existing, i)
		} else {
			childIndexesByKey[key] = []int{i}
			computedMulti = append(computedMulti, *constraintFromChild)
		}
	}

	if len(computedMulti) == 0 {
		return emptyNodeSeq
	}

	// The parent request mirrors TS's `{...req, multiConstraints: [...]}`:
	// Constraint/Start/Reverse ride along unchanged; our computed multi is
	// APPENDED to whatever req.MultiConstraints already contained — chained
	// FlippedJoins each contribute one entry, so the source ANDs them all
	// (e.g. `assigneeID IN (…) AND creatorID IN (…)`).
	parentReq := req
	incoming := req.MultiConstraints

	chunkSize := int(multiConstraintChunkSize.Load())
	var parents iter.Seq[Node]
	if len(computedMulti) <= chunkSize {
		parentReq.MultiConstraints = appendMulti(incoming, computedMulti)
		parents = fj.parent.Fetch(parentReq) // fully lazy single-chunk path
	} else {
		parents = fj.fetchChunkedSequential(parentReq, incoming, computedMulti, chunkSize, req.Reverse)
	}

	return func(yield func(Node) bool) {
		for node := range parents {
			key := canonicalKey(node.Row, parentKey)
			idxs, ok := childIndexesByKey[key]
			if !ok {
				// This row's parent-key doesn't match any of our computed
				// multi-constraint entries. Happens when our parent is an
				// intermediate operator (e.g. a chained FlippedJoin) that
				// passes multiConstraints through unchanged instead of
				// filtering — see FetchRequest.MultiConstraints contract.
				// The lookup miss here performs the required filter, so
				// just skip the row. (TS #fetchBatched does the same.)
				continue
			}
			// Children retain their original input order within the group
			// because indexes were appended in iteration order.
			relatedChildNodes := make([]Node, 0, len(idxs))
			for _, i := range idxs {
				relatedChildNodes = append(relatedChildNodes, childNodes[i])
			}
			if !fj.yieldParentWithOverlay(node, relatedChildNodes, yield) {
				return
			}
		}
	}
}

// fetchChunkedSequential fetches each multi-constraint chunk ONCE (one
// bounded-IN query per chunk), buffers it, and heap-merges the buffered
// chunks into one ordered stream.
//
// COST MODEL (2026-07-13 wedge root cause): the previous shape fetched one
// HEAD row per chunk and re-opened the winning chunk with a keyset cursor
// (Start after the emitted row) for every yielded row. That looked lazy but
// wasn't: the production leaf fetch (tablesource fetchSerial) eagerly
// drains the ENTIRE result of every query, so each per-head reopen paid a
// full remaining-chunk scan to keep one row — O(rows) SQL round-trips and
// O(rows²/chunks) row decodes per fetch. Through a chained FlippedJoin
// (whose outer Fetch collects the inner's whole output to build its
// IN-list) a single child-change push re-fetch ran for 6-21+ minutes with
// no abort checkpoint reachable — the live advance wedge. TS never pays
// this: its #fetchChunked holds one open streaming cursor per chunk and
// merges (one query per chunk, rows stream). Buffering whole chunks is the
// same O(chunks) query count with the rows RETAINED instead of re-fetched —
// strictly cheaper than the old shape on every axis (the old prime already
// drained every chunk fully and threw the rows away). The delta vs TS is
// memory (buffered rows vs open cursors), which the eager leaf makes the
// native Go shape; the keyset resume machinery — and with it the whole
// non-advancing-cursor wedge class — is gone from this path.
func (fj *FlippedJoin) fetchChunkedSequential(
	parentReq FetchRequest,
	incoming []MultiConstraint,
	computedMulti MultiConstraint,
	chunkSize int,
	reverse bool,
) iter.Seq[Node] {
	compareRows := fj.schema.CompareRows
	compare := func(a, b Node) int {
		c := compareRows(a.Row, b.Row)
		if reverse {
			c = -c
		}
		return c
	}
	return func(yield func(Node) bool) {
		var chunks []flippedJoinBufferedChunk
		heads := &flippedJoinChunkHeap{compare: compare}
		heap.Init(heads)

		for i := 0; i < len(computedMulti); i += chunkSize {
			end := min(i+chunkSize, len(computedMulti))
			creq := parentReq
			creq.MultiConstraints = appendMulti(incoming, computedMulti[i:end])
			buf := slices.Collect(fj.parent.Fetch(creq))
			if len(buf) == 0 {
				continue
			}
			chunkIdx := len(chunks)
			chunks = append(chunks, flippedJoinBufferedChunk{buf: buf, next: 1})
			heap.Push(heads, flippedJoinChunkHead{chunk: chunkIdx, node: buf[0]})
		}

		for heads.Len() > 0 {
			best := heap.Pop(heads).(flippedJoinChunkHead)
			if !yield(best.node) {
				return
			}
			c := &chunks[best.chunk]
			if c.next < len(c.buf) {
				heap.Push(heads, flippedJoinChunkHead{chunk: best.chunk, node: c.buf[c.next]})
				c.next++
			} else {
				c.buf = nil // drained — release the rows to GC mid-merge
			}
		}
	}
}

// flippedJoinBufferedChunk is one fetched-and-buffered multi-constraint
// chunk: `buf` holds the chunk's rows in parent order, `next` is the index
// of the first row not yet offered to the merge heap.
type flippedJoinBufferedChunk struct {
	buf  []Node
	next int
}

type flippedJoinChunkHead struct {
	chunk int
	node  Node
}

type flippedJoinChunkHeap struct {
	items   []flippedJoinChunkHead
	compare func(a, b Node) int
}

func (h flippedJoinChunkHeap) Len() int { return len(h.items) }
func (h flippedJoinChunkHeap) Less(i, j int) bool {
	c := h.compare(h.items[i].node, h.items[j].node)
	if c == 0 {
		return h.items[i].chunk < h.items[j].chunk
	}
	return c < 0
}
func (h flippedJoinChunkHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}
func (h *flippedJoinChunkHeap) Push(x interface{}) {
	h.items = append(h.items, x.(flippedJoinChunkHead))
}
func (h *flippedJoinChunkHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

// appendMulti returns incoming + mc as a fresh slice (never aliasing
// incoming's backing array — chained joins may reuse it across chunks).
func appendMulti(incoming []MultiConstraint, mc MultiConstraint) []MultiConstraint {
	out := make([]MultiConstraint, 0, len(incoming)+1)
	out = append(out, incoming...)
	return append(out, mc)
}

// yieldParentWithOverlay applies the in-progress child-change overlay to one
// fetched parent's related children and yields the decorated parent node
// (TS #yieldParentWithOverlay). Returns false when the consumer stopped.
// Logic is unchanged from the pre-batched implementation.
func (fj *FlippedJoin) yieldParentWithOverlay(minHead Node, relatedChildNodes []Node, yield func(Node) bool) bool {
	overlaidRelatedChildNodes := relatedChildNodes
	if fj.inprogressChildChange != nil && fj.inprogressChildChangePosition != nil &&
		IsJoinMatch(fj.inprogressChildChange.Node.Row, fj.childKey, minHead.Row, fj.parentKey) {

		hasBeenPushed := fj.parent.GetSchema().CompareRows(minHead.Row, fj.inprogressChildChangePosition) <= 0

		if fj.inprogressChildChange.Type == ChangeTypeRemove {
			if hasBeenPushed {
				// Filter out the removed node. TS filters by reference
				// identity (flipped-join.ts: `n !== change.node`)
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

	if len(overlaidRelatedChildNodes) > 0 {
		captured := overlaidRelatedChildNodes
		// {...minParentNode.relationships, [relName]: children}
		// (flipped-join.ts:377-380): new value wins; position kept when the
		// name already exists, appended when novel.
		rels, order := SetRelationship(minHead.Relationships, minHead.RelOrder, fj.relationshipName,
			func() iter.Seq[Node] { return slices.Values(captured) })
		nodeOut := Node{
			Row:           minHead.Row,
			Relationships: rels,
			RelOrder:      order,
		}
		if !yield(nodeOut) {
			return false
		}
	}
	return true
}

// canonicalKey builds a canonical string key over `keys` of `record`
// (Constraint and Row share the map[string]Value shape), used by
// fetchBatched both to dedupe multi-constraint entries and to map each
// returned parent row back to the children that referenced its parent-key
// tuple. Port of TS canonicalKey (flipped-join.ts @ 1.7.0).
func canonicalKey[M ~map[string]Value](record M, keys CompoundKey) string {
	if len(keys) == 1 {
		return canonicalValue(record[keys[0]])
	}
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(0)
		}
		b.WriteString(canonicalValue(record[k]))
	}
	return b.String()
}

// canonicalValue tags values by type so e.g. 1 (number) and "1" (string)
// don't conflate (TS canonicalValue: n/s/d/b/t/f/j tags). int64/uint64 take
// the bigint tag — TS sees bigint at runtime under zqlite's safeIntegers.
// The exact numeric string format need not match JS: keys never leave the
// process; injectivity within one fetch (same Go value → same key) is the
// only requirement, and both map-build values (child rows) and lookup
// values (parent rows) pass through the same source normalization.
func canonicalValue(v Value) string {
	switch t := v.(type) {
	case nil:
		return "n"
	case string:
		return "s" + t
	case float64:
		if t == 0 {
			// JS String(-0) === "0": TS conflates ±0 into "d0"
			// (flipped-join.ts:607) — the ONLY double pair JS's
			// shortest-round-trip formatting does not separate. A child
			// keyed -0.0 must find a parent fetched back as +0.0 (SQLite
			// stores integral REALs int-serial, normalizing -0.0 to 0);
			// FormatFloat's "d-0" split the keys and silently dropped the
			// parent row.
			return "d0"
		}
		return "d" + strconv.FormatFloat(t, 'g', -1, 64)
	case int64:
		return "b" + strconv.FormatInt(t, 10)
	case uint64:
		return "b" + strconv.FormatUint(t, 10)
	case bool:
		if t {
			return "t"
		}
		return "f"
	default:
		j, err := json.Marshal(t)
		if err != nil {
			// Join-key values are scalars in practice; an unmarshalable
			// value cannot form a coherent key. Fail this query like TS
			// would on a malformed Value.
			panic(NewDataError("canonicalValue: unsupported join-key value %T", v))
		}
		return "j" + string(j)
	}
}

// --- Push from child side ---

type flippedJoinChildOutput struct {
	fj *FlippedJoin
}

func (o *flippedJoinChildOutput) Push(change Change, _ InputBase) {
	o.fj.pushChild(change)
}

func (fj *FlippedJoin) pushChild(change Change) {
	switch change.Type {
	case ChangeTypeAdd, ChangeTypeRemove:
		fj.pushChildChange(change, false)
	case ChangeTypeEdit:
		if !RowEqualsForCompoundKey(change.OldNode.Row, change.Node.Row, fj.childKey) {
			panic(joinKeyChangeError(fj.child.GetSchema(), change.OldNode.Row, "FlippedJoin-child-key-change"))
		}
		fj.pushChildChange(change, true)
	case ChangeTypeChild:
		fj.pushChildChange(change, true)
	}
}

// pushChildChange — source: flipped-join.ts line 346-425
func (fj *FlippedJoin) pushChildChange(change Change, exists bool) {
	fj.inprogressChildChange = &change
	fj.inprogressChildChangePosition = nil
	defer func() { fj.inprogressChildChange = nil }()

	constraint := BuildJoinConstraint(change.Node.Row, fj.childKey, fj.parentKey)
	if constraint == nil {
		return
	}

	for parentNode := range fj.parent.Fetch(FetchRequest{Constraint: constraint}) {
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
			// {...parentNode.relationships, [relName]: children}
			// (flipped-join.ts:457-459).
			rels, order := SetRelationship(parentNode.Relationships, parentNode.RelOrder, fj.relationshipName, childNodeStream)
			outNode := Node{
				Row:           parentNode.Row,
				Relationships: rels,
				RelOrder:      order,
			}
			outChange := MakeChildChange(outNode, ChildData{
				RelationshipName: fj.relationshipName,
				Change:           change,
			})
			fj.output.Push(outChange, fj)
		} else {
			// {...parentNode.relationships, [relName]: [change.node]}
			// (flipped-join.ts:472-474).
			rels, order := SetRelationship(parentNode.Relationships, parentNode.RelOrder, fj.relationshipName,
				func() iter.Seq[Node] { return slices.Values([]Node{change.Node}) })
			outNode := Node{
				Row:           parentNode.Row,
				Relationships: rels,
				RelOrder:      order,
			}
			var outChange Change
			if change.Type == ChangeTypeAdd {
				outChange = MakeAddChange(outNode)
			} else {
				outChange = MakeRemoveChange(outNode)
			}
			fj.output.Push(outChange, fj)
		}
	}
}

// --- Push from parent side ---

type flippedJoinParentOutput struct {
	fj *FlippedJoin
}

func (o *flippedJoinParentOutput) Push(change Change, _ InputBase) {
	o.fj.pushParent(change)
}

// pushParent — source: flipped-join.ts line 427-504
func (fj *FlippedJoin) pushParent(change Change) {
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
		// {...node.relationships, [relName]: children} (flipped-join.ts:502-504).
		rels, order := SetRelationship(node.Relationships, node.RelOrder, fj.relationshipName, childNodeStream(node))
		return Node{
			Row:           node.Row,
			Relationships: rels,
			RelOrder:      order,
		}
	}

	// If no related child, don't push (inner join)
	hasChildren := false
	for range childNodeStream(change.Node)() {
		hasChildren = true
		break
	}
	if !hasChildren {
		return
	}

	switch change.Type {
	case ChangeTypeAdd:
		fj.output.Push(MakeAddChange(flip(change.Node)), fj)
	case ChangeTypeRemove:
		fj.output.Push(MakeRemoveChange(flip(change.Node)), fj)
	case ChangeTypeChild:
		fj.output.Push(MakeChildChange(flip(change.Node), *change.Child), fj)
	case ChangeTypeEdit:
		if !RowEqualsForCompoundKey(change.OldNode.Row, change.Node.Row, fj.parentKey) {
			panic(joinKeyChangeError(fj.schema, change.OldNode.Row, "FlippedJoin-parent-key-change"))
		}
		fj.output.Push(MakeEditChange(flip(change.Node), flip(*change.OldNode)), fj)
	}
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
