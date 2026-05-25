package ivm

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
func (s *Skip) shouldBePresent(row Row) bool {
	cmp := s.comparator(s.bound.Row, row)
	return cmp < 0 || (cmp == 0 && !s.bound.Exclusive)
}

// Fetch — fetches nodes respecting the skip bound.
func (s *Skip) Fetch(req FetchRequest) []Node {
	start, empty := s.getStart(req)
	if empty {
		return nil
	}

	newReq := FetchRequest{
		Constraint: req.Constraint,
		Start:      start, // may be nil (undefined) for reverse — means no start override
		Reverse:    req.Reverse,
	}
	nodes := s.input.Fetch(newReq)

	if !req.Reverse {
		return nodes
	}

	// For reverse, filter out nodes before bound
	var result []Node
	for _, node := range nodes {
		if !s.shouldBePresent(node.Row) {
			break
		}
		result = append(result, node)
	}
	return result
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
