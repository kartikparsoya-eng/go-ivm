package ivm

import (
	"iter"
)

// The FilterOperator abstraction enables efficient fetch for WHERE clauses
// containing OR conditions. FilterOperators don't have fetch — they have
// filter(node) → bool. They also have push (like normal operators).

type FilterInput interface {
	InputBase
	SetFilterOutput(output FilterOutput)
}

type FilterOutput interface {
	Output
	BeginFilter()
	Filter(node Node) bool
	EndFilter()
}

// FilterOperator is FilterInput + FilterOutput combined.
type FilterOperator interface {
	FilterInput
	FilterOutput
}

// ThrowFilterOutput panics if push or filter is called. Initial value before wiring.
var ThrowFilterOutput FilterOutput = throwFilterOutputImpl{}

type throwFilterOutputImpl struct{}

func (throwFilterOutputImpl) Push(change Change, pusher InputBase) []Change {
	panic("Output not set")
}

func (throwFilterOutputImpl) Filter(node Node) bool {
	panic("Output not set")
}

func (throwFilterOutputImpl) BeginFilter() {}
func (throwFilterOutputImpl) EndFilter()   {}

// FilterStart adapts from normal Operator Output to FilterOperator FilterInput.
type FilterStart struct {
	input  Input
	output FilterOutput
}

func NewFilterStart(input Input) *FilterStart {
	fs := &FilterStart{
		input:  input,
		output: ThrowFilterOutput,
	}
	input.SetOutput(fs)
	return fs
}

func (fs *FilterStart) SetFilterOutput(output FilterOutput) {
	fs.output = output
}

func (fs *FilterStart) Destroy() {
	fs.input.Destroy()
}

func (fs *FilterStart) GetSchema() *SourceSchema {
	return fs.input.GetSchema()
}

// Push — called as Output by the upstream input.
func (fs *FilterStart) Push(change Change, pusher InputBase) []Change {
	return fs.output.Push(change, fs)
}

// Fetch — filters nodes from upstream through the filter chain.
//
// Limit handling: Filter is non-transparent (it can drop rows), so it
// MUST NOT forward req.Limit to upstream — the source would truncate
// before Filter runs, causing an under-fetch. Instead, Filter strips
// Limit from the upstream request and enforces early termination in its
// own loop: once len(result) >= req.Limit, it breaks. This matches TS's
// lazy-generator behavior where Take's break propagates through Filter,
// stopping the EXISTS predicate from running on the entire upstream result.
func (fs *FilterStart) Fetch(req FetchRequest) iter.Seq[Node] {
	upstreamReq := req
	upstreamReq.Limit = 0

	return func(yield func(Node) bool) {
		fs.output.BeginFilter()
		defer fs.output.EndFilter()

		count := 0
		for node := range fs.input.Fetch(upstreamReq) {
			if fs.output.Filter(node) {
				if !yield(node) {
					return
				}
				count++
				if req.Limit > 0 && count >= req.Limit {
					return
				}
			}
		}
	}
}

// FilterEnd adapts from FilterOperator FilterOutput back to normal Input.
type FilterEnd struct {
	start  *FilterStart
	input  FilterInput
	output Output
}

func NewFilterEnd(start *FilterStart, input FilterInput) *FilterEnd {
	fe := &FilterEnd{
		start:  start,
		input:  input,
		output: ThrowOutput,
	}
	input.SetFilterOutput(fe)
	return fe
}

func (fe *FilterEnd) Fetch(req FetchRequest) iter.Seq[Node] {
	return fe.start.Fetch(req)
}

func (fe *FilterEnd) BeginFilter() {}
func (fe *FilterEnd) EndFilter()   {}

func (fe *FilterEnd) Filter(node Node) bool {
	return true
}

func (fe *FilterEnd) SetOutput(output Output) {
	fe.output = output
}

func (fe *FilterEnd) Destroy() {
	fe.input.Destroy()
}

func (fe *FilterEnd) GetSchema() *SourceSchema {
	return fe.input.GetSchema()
}

// Push — called as FilterOutput by the filter chain.
func (fe *FilterEnd) Push(change Change, pusher InputBase) []Change {
	return fe.output.Push(change, fe)
}
