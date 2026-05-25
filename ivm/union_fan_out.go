package ivm

// UnionFanOut broadcasts pushes to multiple outputs (OR condition branches),
// coordinating with UnionFanIn to accumulate and deduplicate results.

// UnionFanOut implements Operator. It broadcasts changes to all outputs
// and coordinates with a paired UnionFanIn for accumulation.
type UnionFanOut struct {
	destroyCount int
	unionFanIn   *UnionFanIn
	input        Input
	outputs      []Output
}

func NewUnionFanOut(input Input) *UnionFanOut {
	ufo := &UnionFanOut{
		input: input,
	}
	input.SetOutput(ufo)
	return ufo
}

func (ufo *UnionFanOut) SetFanIn(fanIn *UnionFanIn) {
	if ufo.unionFanIn != nil {
		panic("FanIn already set for this FanOut")
	}
	ufo.unionFanIn = fanIn
}

// Push broadcasts change to all outputs, then signals fan-in completion.
func (ufo *UnionFanOut) Push(change Change, _ InputBase) []Change {
	if ufo.unionFanIn == nil {
		panic("UnionFanIn not set")
	}
	ufo.unionFanIn.FanOutStartedPushing()
	for _, output := range ufo.outputs {
		output.Push(change, ufo)
	}
	return ufo.unionFanIn.FanOutDonePushing(change.Type)
}

func (ufo *UnionFanOut) SetOutput(output Output) {
	ufo.outputs = append(ufo.outputs, output)
}

func (ufo *UnionFanOut) GetSchema() *SourceSchema {
	return ufo.input.GetSchema()
}

func (ufo *UnionFanOut) Fetch(req FetchRequest) []Node {
	return ufo.input.Fetch(req)
}

func (ufo *UnionFanOut) Destroy() {
	if ufo.destroyCount < len(ufo.outputs) {
		ufo.destroyCount++
		if ufo.destroyCount == len(ufo.outputs) {
			ufo.input.Destroy()
		}
	} else {
		panic("FanOut already destroyed once for each output")
	}
}
