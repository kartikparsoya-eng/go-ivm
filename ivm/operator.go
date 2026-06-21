package ivm

// Key translation: Stream<T> (generator) → []T (slice)
// The 'yield' scheduling signal is dropped entirely.

type Constraint map[string]Value

type Start struct {
	Row   Row
	Basis string // "at" | "after"
}

type FetchRequest struct {
	Constraint *Constraint
	Start      *Start
	Reverse    bool

	// Limit is an OPTIONAL hint: when > 0, a leaf source may stop after
	// producing this many rows (in the request's effective order, AFTER its
	// own filter predicate). 0 = unlimited (the zero value, so every existing
	// FetchRequest is unaffected). It restores the early-termination that TS
	// gets for free from lazy generators — see operator.go's Stream→[]Node note.
	//
	// SAFETY: set by Take.initialFetch to t.limit. Operators handle it:
	//   - Source: truncates output to req.Limit rows (early scan termination).
	//   - Filter (FilterStart): STRIPS Limit before calling upstream (Filter
	//     is non-transparent — it can drop rows, so a source Limit would
	//     under-fetch). Filter breaks its own loop at req.Limit post-filter
	//     rows, so EXISTS only runs for ~limit/filter_rate rows.
	//   - Skip: forwards Limit in the forward case (transparent — only changes
	//     Start, doesn't drop rows). In reverse, withholds it (shouldBePresent
	//     can discard rows → under-fetch risk).
	//   - Join: forwards Limit transparently (doesn't filter parent rows).
	Limit int
}

// LeafSource marks an Input that reads directly from a base source (table or
// memory source) rather than from upstream operators. Previously used by Take
// to gate req.Limit pushdown; now vestigial since Take always sets req.Limit
// and each operator handles it independently. Retained for documentation and
// potential future use (e.g. query planning).
type LeafSource interface {
	// LeafSourceMarker is a no-op marker; its presence is the signal.
	LeafSourceMarker()
}

// SourceSchema mirrors schema.ts
type SourceSchema struct {
	TableName     string
	Columns       map[string]string // column name → type name
	PrimaryKey    []string
	Relationships map[string]*SourceSchema
	IsHidden      bool
	// IsScalar marks this relationship as the child side of a scalar
	// EXISTS condition. TS resolves these via resolveSimpleScalarSubqueries
	// before building the pipeline, so the join doesn't exist on the TS
	// side and its child rows are never streamed. Go gets the unresolved
	// AST, so we build the join — but we mark the relationship IsScalar
	// so the streamer drops the entire subtree, matching TS.
	IsScalar      bool
	System        string // "client" | "permissions" | "server"
	CompareRows   Comparator
	Sort          Ordering
}

type InputBase interface {
	GetSchema() *SourceSchema
	Destroy()
}

// fetch returns []Node (TS Stream<Node | 'yield'> → Go []Node, dropping 'yield')
type Input interface {
	InputBase
	SetOutput(output Output)
	Fetch(req FetchRequest) []Node
}

// push returns []Change (TS Stream<'yield'> → Go []Change as output changes)
type Output interface {
	Push(change Change, pusher InputBase) []Change
}

// Operator is Input + Output combined.
type Operator interface {
	Input
	Output
}

// ThrowOutput panics if pushed to. Used as initial output before wiring.
var ThrowOutput Output = throwOutputImpl{}

type throwOutputImpl struct{}

func (throwOutputImpl) Push(change Change, pusher InputBase) []Change {
	panic("Output not set")
}
