package ivm

// The Filter operator filters data through a predicate. It is stateless.
// The predicate must be pure.

// Filter implements FilterOperator.
type Filter struct {
	input     FilterInput
	predicate func(row Row) bool
	output    FilterOutput
}

func NewFilter(input FilterInput, predicate func(row Row) bool) *Filter {
	f := &Filter{
		input:     input,
		predicate: predicate,
		output:    ThrowFilterOutput,
	}
	input.SetFilterOutput(f)
	return f
}

func (f *Filter) BeginFilter() {
	f.output.BeginFilter()
}

func (f *Filter) EndFilter() {
	f.output.EndFilter()
}

// Filter — returns whether the node passes this filter AND downstream filters.
// TS: *filter(node: Node): Generator<'yield', boolean>
// Go: returns bool directly (generator protocol → return value)
func (f *Filter) Filter(node Node) bool {
	return f.predicate(node.Row) && f.output.Filter(node)
}

func (f *Filter) SetFilterOutput(output FilterOutput) {
	f.output = output
}

func (f *Filter) Destroy() {
	f.input.Destroy()
}

func (f *Filter) GetSchema() *SourceSchema {
	return f.input.GetSchema()
}

// Push — handles incremental changes through the filter.
func (f *Filter) Push(change Change, pusher InputBase) {
	filterPush(change, f.output, f, f.predicate)
}

// filterPush — 1:1 port of filter-push.ts
func filterPush(change Change, output Output, pusher InputBase, predicate func(Row) bool) {
	if predicate == nil {
		output.Push(change, pusher)
		return
	}

	switch change.Type {
	case ChangeTypeAdd, ChangeTypeRemove:
		if predicate(change.Node.Row) {
			output.Push(change, pusher)
		}
	case ChangeTypeChild:
		if predicate(change.Node.Row) {
			output.Push(change, pusher)
		}
	case ChangeTypeEdit:
		maybeSplitAndPushEditChange(change, predicate, output, pusher)
	default:
		panic("filterPush: unreachable change type")
	}
}

// maybeSplitAndPushEditChange is a thin alias for the exported version in
// source.go. The two had duplicate implementations; consolidated per porting
// review LOW-1.
func maybeSplitAndPushEditChange(change Change, predicate func(Row) bool, output Output, pusher InputBase) {
	MaybeSplitAndPushEditChange(change, predicate, output, pusher)
}
