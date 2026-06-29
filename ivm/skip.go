package ivm

import (
	"iter"
)

// Skip sets the start position for the pipeline. No rows before the bound
// will be output. It requires sorted input.

// Bound represents the skip boundary.
type Bound struct {
	Row       Row
	Exclusive bool
}

// Skip implements Operator. It skips rows before the bound.
type Skip struct {
	input      Input
	bound      Bound
	comparator Comparator
	output     Output
}

func NewSkip(input Input, bound Bound) *Skip {
	schema := input.GetSchema()
	if schema.Sort == nil {
		panic("Skip requires sorted input")
	}
	s := &Skip{
		input:      input,
		bound:      bound,
		comparator: schema.CompareRows,
		output:     ThrowOutput,
	}
	input.SetOutput(s)
	return s
}

func (s *Skip) GetSchema() *SourceSchema {
	return s.input.GetSchema()
}

func (s *Skip) SetOutput(output Output) {
	s.output = output
}

func (s *Skip) Destroy() {
	s.input.Destroy()
}

// shouldBePresent — determines if a row is at or after the bound.
// Uses partial-cursor comparison: if the bound row is missing a sort
// column, stop comparing — partial cursors are treated as "any value"
// for the unspecified columns. Without this, the full-row comparator
// returned -1 (nil < non-nil) for the rest of the sort key, treating
// a row at the cursor's createdAt as strictly after the cursor — so
// Exclusive=true wasn't excluding the cursor row. Drift seen in
// channelConversationsPaginatedV3 (start={createdAt}, sort by
// [createdAt, conversationId], inclusive=false): Go included the row
// at exactly the start's createdAt.
func (s *Skip) shouldBePresent(row Row) bool {
	cmp := CompareWithPartialBound(s.bound.Row, row, s.input.GetSchema().Sort)
	return cmp < 0 || (cmp == 0 && !s.bound.Exclusive)
}

// CompareWithPartialBound returns -1/0/+1 for bound vs row, stopping
// at the first sort column the bound row doesn't specify (treated as
// "equal so far"). Caller treats the resulting 0 the same way the full
// comparator would treat an exact match.
//
// TS gets this behavior for free in SQL via three-valued logic
// (`column > NULL` is NULL → row excluded), and via the same property
// in the BTree path (RowBound + makeBoundComparator with min/maxValue
// sentinels). Go's in-memory comparator iterates all sort columns and
// treats nil < non-nil at -1, which (a) wrongly includes the cursor
// row when the cursor is partial + basis="after" / exclusive=true,
// and (b) is symmetric to the wrong direction for "at". Stopping at
// the first missing-in-bound column matches TS's SQL boundary.
func CompareWithPartialBound(bound Row, row Row, sort Ordering) int {
	for _, ord := range sort {
		field := ord[0]
		if _, ok := bound[field]; !ok {
			return 0
		}
		comp := CompareValues(bound[field], row[field])
		if comp != 0 {
			if ord[1] == "desc" {
				comp = -comp
			}
			return comp
		}
	}
	return 0
}

// Fetch — fetches nodes respecting the skip bound.
func (s *Skip) Fetch(req FetchRequest) iter.Seq[Node] {
	start, empty := s.getStart(req)
	if empty {
		return emptyNodeSeq
	}

	newReq := FetchRequest{
		Constraint: req.Constraint,
		Start:      start,
		Reverse:    req.Reverse,
	}
	// Forward Limit in the forward case only. In reverse, Skip's
	// shouldBePresent post-filter can discard rows, so forwarding
	// Limit to Source risks under-fetch (Source returns N rows, Skip
	// discards some, Take sets takeState with fewer than N — wrong bound).
	if !req.Reverse {
		newReq.Limit = req.Limit
	}

	if !req.Reverse {
		return s.input.Fetch(newReq)
	}

	return func(yield func(Node) bool) {
		for node := range s.input.Fetch(newReq) {
			if !s.shouldBePresent(node.Row) {
				return
			}
			if !yield(node) {
				return
			}
		}
	}
}

// Push — handles incremental changes respecting the bound.
func (s *Skip) Push(change Change, pusher InputBase) []Change {
	shouldBePresent := func(row Row) bool { return s.shouldBePresent(row) }

	if change.Type == ChangeTypeEdit {
		return maybeSplitAndPushEditChange(change, shouldBePresent, s.output, s)
	}

	// ADD, REMOVE, CHILD
	if shouldBePresent(change.Node.Row) {
		return s.output.Push(change, s)
	}
	return nil
}

// getStart — computes the effective start for a fetch request.
// Returns (*Start, empty bool). If empty=true, caller should return nil (no results).
// If start is nil and empty is false, it means "no override" (use default fetch behavior).
func (s *Skip) getStart(req FetchRequest) (*Start, bool) {
	boundStart := &Start{
		Row:   s.bound.Row,
		Basis: basisFromExclusive(s.bound.Exclusive),
	}

	if req.Start == nil {
		if req.Reverse {
			// No start override for reverse — caller fetches from end
			return nil, false
		}
		return boundStart, false
	}

	cmp := s.comparator(s.bound.Row, req.Start.Row)

	if !req.Reverse {
		if cmp > 0 {
			return boundStart, false
		}
		if cmp == 0 {
			if s.bound.Exclusive || req.Start.Basis == "after" {
				return &Start{Row: s.bound.Row, Basis: "after"}, false
			}
			return boundStart, false
		}
		return req.Start, false
	}

	// reverse
	if cmp > 0 {
		return nil, true // 'empty'
	}
	if cmp == 0 {
		if !s.bound.Exclusive && req.Start.Basis == "at" {
			return boundStart, false
		}
		return nil, true // 'empty'
	}
	return req.Start, false
}

func basisFromExclusive(exclusive bool) string {
	if exclusive {
		return "after"
	}
	return "at"
}
