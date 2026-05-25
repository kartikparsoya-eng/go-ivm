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
}

// SourceSchema mirrors schema.ts
type SourceSchema struct {
	TableName     string
	Columns       map[string]string // column name → type name
	PrimaryKey    []string
	Relationships map[string]*SourceSchema
	IsHidden      bool
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
