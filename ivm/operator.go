package ivm

import "iter"

// Key translation: Stream<T> (TS generator) → iter.Seq[T] (Go lazy iterator).
// The 'yield' scheduling signal is dropped entirely — Go's runtime scheduler
// is preemptive (since 1.14), so there is no single-threaded event loop to
// yield to. iter.Seq[Node] is the faithful translation of TS's
// Stream<Node | 'yield'> → Stream<Node> (operator.ts:43, stream.ts:8).

// emptyNodeSeq is a non-nil iter.Seq[Node] that yields no values. Fetch
// implementations MUST return this (not nil) when they have no results —
// slices.Collect(nil) and `for v := range nilSeq` panic because they call
// the nil function value.
var emptyNodeSeq iter.Seq[Node] = func(yield func(Node) bool) {}

type Constraint map[string]Value

// MultiConstraint is a single multi-row IN clause: a non-empty list of
// Constraints all sharing the same column shape. Sources treat it as
// `(col_a, col_b, …) IN VALUES (…)`. Mirrors operator.ts MultiConstraint
// (zero 1.7.0, #5928).
//
// Caller invariants (sources rely on these — they are not re-checked):
//   - Unique entries. TableSource gets set semantics for free from SQL
//     `IN`; MemorySource/post-filters would otherwise duplicate rows.
//     FlippedJoin dedupes by canonical parent-key before adding entries.
//   - Key-compatible with FetchRequest.Constraint. Entries contradicting
//     the scalar constraint should be dropped upstream (FlippedJoin
//     filters via constraintsAreCompatible).
type MultiConstraint []Constraint

// RowMatchesMultiConstraints reports whether row satisfies every
// MultiConstraint in multis: within one MultiConstraint the entries are
// OR'd (`IN` semantics — any entry may match); across the list they are
// AND'd. Empty MultiConstraint entries are IGNORED (no filtering), matching
// TS: buildSelectQuery skips `mc.length === 0` and MemorySource#fetch only
// takes the batched path when some mc is non-empty.
func RowMatchesMultiConstraints(multis []MultiConstraint, row Row) bool {
	for _, mc := range multis {
		if len(mc) == 0 {
			continue
		}
		any := false
		for i := range mc {
			if ConstraintMatchesRow(&mc[i], row) {
				any = true
				break
			}
		}
		if !any {
			return false
		}
	}
	return true
}

type Start struct {
	Row   Row
	Basis string // "at" | "after"
}

type FetchRequest struct {
	Constraint *Constraint

	// MultiConstraints is a list of multi-row IN clauses, all ANDed together
	// (and ANDed with Constraint if both are provided). Each entry is a
	// MultiConstraint over its own set of columns.
	//
	// Used by FlippedJoin (zero 1.7.0 #5928) to push child→parent fetches
	// into a single batched statement, and to AND together constraints
	// contributed by chained FlippedJoins (e.g. `assigneeID IN (…) AND
	// creatorID IN (…)`). Operators that forward a FetchRequest downstream
	// must preserve it (TS spreads `...req`; Go struct copies carry it —
	// only field-by-field literal construction can drop it).
	MultiConstraints []MultiConstraint

	Start   *Start
	Reverse bool
}

// SourceSchema mirrors schema.ts
type SourceSchema struct {
	TableName     string
	Columns       map[string]string // column name → type name
	PrimaryKey    []string
	Relationships map[string]*SourceSchema
	// RelationshipOrder lists relationship names in TS insertion order — the
	// order Object.entries(schema.relationships) yields in zql. TS object
	// spread appends new names last and keeps the position of a name that is
	// overwritten (join.ts:86-96, flipped-join.ts:128-138,
	// union-fan-in.ts:73-88). The wire emitter (engine streamNodesInto)
	// iterates this order so sibling relationships hit the wire exactly as
	// TS's Object.entries walk does (pipeline-driver.ts:2861).
	//
	// Invariant: same key set as Relationships (len equal, every name
	// present). Maintained by NewJoin/NewFlippedJoin/NewUnionFanIn; leaf
	// sources have both empty.
	RelationshipOrder []string
	IsHidden          bool
	System            string // "client" | "permissions" | "server"
	CompareRows       Comparator
	Sort              Ordering
}

type InputBase interface {
	GetSchema() *SourceSchema
	Destroy()
}

// Fetch returns iter.Seq[Node] (TS Stream<Node | 'yield'> → Go iter.Seq[Node],
// dropping 'yield'). The seq is lazy: the cursor/reader is held for the
// lifetime of the seq, released on exhaustion or early stop (yield returns
// false). See DESIGN-streaming-hydrate.md §3e.
type Input interface {
	InputBase
	SetOutput(output Output)
	Fetch(req FetchRequest) iter.Seq[Node]
}

// Output mirrors operator.ts:93-108. TS's push returns Stream<'yield'> —
// cooperative-scheduling markers only, never data; pushed data reaches the
// app solely through the terminal sink (Go: engine pipelineOutput →
// Streamer; TS: the view / pipeline-driver walk). Go drops the 'yield'
// signal entirely (preemptive scheduler — see the package note at the top
// of this file), so the faithful Go shape is a plain void call.
type Output interface {
	Push(change Change, pusher InputBase)
}

// Operator is Input + Output combined.
type Operator interface {
	Input
	Output
}

// ThrowOutput panics if pushed to. Used as initial output before wiring.
var ThrowOutput Output = throwOutputImpl{}

type throwOutputImpl struct{}

func (throwOutputImpl) Push(change Change, pusher InputBase) {
	panic("Output not set")
}
