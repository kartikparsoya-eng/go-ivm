package ivm

// Merges multiple streams into one, eliminating duplicates.
// Must be paired with a FanOut operator upstream.

// FanIn implements FilterOperator
type FanIn struct {
	inputs            []FilterInput
	schema            *SourceSchema
	output            FilterOutput
	accumulatedPushes []Change
}

func NewFanIn(fanOut *FanOut, inputs []FilterInput) *FanIn {
	fi := &FanIn{
		inputs: inputs,
		schema: fanOut.GetSchema(),
		output: ThrowFilterOutput,
	}
	for _, input := range inputs {
		input.SetFilterOutput(fi)
		if fi.schema != input.GetSchema() {
			panic("Schema mismatch in fan-in")
		}
	}
	return fi
}

func (fi *FanIn) SetFilterOutput(output FilterOutput) {
	fi.output = output
}

func (fi *FanIn) Destroy() {
	for _, input := range fi.inputs {
		input.Destroy()
	}
}

func (fi *FanIn) GetSchema() *SourceSchema {
	return fi.schema
}

func (fi *FanIn) BeginFilter() {
	fi.output.BeginFilter()
}

func (fi *FanIn) EndFilter() {
	fi.output.EndFilter()
}

func (fi *FanIn) Filter(node Node) bool {
	return fi.output.Filter(node)
}

// Push — accumulates changes; does not forward immediately.
func (fi *FanIn) Push(change Change, pusher InputBase) []Change {
	fi.accumulatedPushes = append(fi.accumulatedPushes, change)
	return nil
}

// FanOutDonePushingToAllBranches is called by FanOut after pushing to all branches.
func (fi *FanIn) FanOutDonePushingToAllBranches(fanOutChangeType ChangeType) []Change {
	if len(fi.inputs) == 0 {
		if len(fi.accumulatedPushes) != 0 {
			panic("If there are no inputs then fan-in should not receive any pushes.")
		}
		return nil
	}

	result := PushAccumulatedChanges(
		fi.accumulatedPushes,
		fi.output,
		fi,
		fanOutChangeType,
		identity,
		identityChange,
	)
	fi.accumulatedPushes = fi.accumulatedPushes[:0]
	return result
}

// identity merge — fan-in uses identity (no relationship merging needed at this level).
// Returns existing (first arg) to match TS behavior: identity(x) => x, called as merge(existing, incoming).
func identity(existing, incoming Change) Change {
	return existing
}

func identityChange(change Change) Change {
	return change
}
