package ivm

// Forks a stream into multiple streams.
// Paired with a FanIn operator which merges the forks back together.

// FanOut implements FilterOperator
type FanOut struct {
	input        FilterInput
	outputs      []FilterOutput
	fanIn        *FanIn
	destroyCount int
}

func NewFanOut(input FilterInput) *FanOut {
	fo := &FanOut{
		input: input,
	}
	input.SetFilterOutput(fo)
	return fo
}

func (fo *FanOut) SetFanIn(fanIn *FanIn) {
	fo.fanIn = fanIn
}

func (fo *FanOut) SetFilterOutput(output FilterOutput) {
	fo.outputs = append(fo.outputs, output)
}

func (fo *FanOut) Destroy() {
	if fo.destroyCount < len(fo.outputs) {
		fo.destroyCount++
		if fo.destroyCount == len(fo.outputs) {
			fo.input.Destroy()
		}
	} else {
		panic("FanOut already destroyed once for each output")
	}
}

func (fo *FanOut) GetSchema() *SourceSchema {
	return fo.input.GetSchema()
}

func (fo *FanOut) BeginFilter() {
	for _, output := range fo.outputs {
		output.BeginFilter()
	}
}

func (fo *FanOut) EndFilter() {
	for _, output := range fo.outputs {
		output.EndFilter()
	}
}

// Filter — short-circuits on first true result.
func (fo *FanOut) Filter(node Node) bool {
	for _, output := range fo.outputs {
		if output.Filter(node) {
			return true
		}
	}
	return false
}

// Push — pushes change to all outputs, then signals fan-in.
func (fo *FanOut) Push(change Change, pusher InputBase) []Change {
	var results []Change
	for _, out := range fo.outputs {
		results = append(results, out.Push(change, fo)...)
	}
	if fo.fanIn == nil {
		panic("fan-out must have a corresponding fan-in set!")
	}
	results = append(results, fo.fanIn.FanOutDonePushingToAllBranches(change.Type)...)
	return results
}
