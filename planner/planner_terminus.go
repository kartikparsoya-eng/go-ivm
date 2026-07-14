package planner

// PlannerTerminus is the root of the planner graph. It is the final output
// node where constraint propagation starts and cost estimation begins.
//
// The terminus wraps an input node (any non-terminus PlannerNode) and
// provides entry points that initialize the recursive propagation and
// estimation traversals with default values (empty branch pattern,
// nil constraint, selectivity = 1).
type PlannerTerminus struct {
	input PlannerNode
}

// NewPlannerTerminus creates a terminus wrapping the given input node.
func NewPlannerTerminus(input PlannerNode) *PlannerTerminus {
	return &PlannerTerminus{input: input}
}

func (t *PlannerTerminus) Kind() string { return "terminus" }

func (t *PlannerTerminus) ClosestJoinOrSource() JoinOrConnection {
	return t.input.ClosestJoinOrSource()
}

// PropagateConstraints is the standard interface method. On the terminus it
// is a no-op because the terminus is the end of the chain — nothing to
// propagate to. Use StartPropagateConstraints to initiate propagation.
func (t *PlannerTerminus) PropagateConstraints(branchPattern []int, constraint PlannerConstraint, from PlannerNode, debugger PlanDebugger) {
	// No-op: terminus is the end of the chain.
}

// EstimateCost is the standard interface method. On the terminus it panics
// because cost estimation should be initiated via StartEstimateCost.
func (t *PlannerTerminus) EstimateCost(downstreamChildSelectivity float64, branchPattern []int, debugger PlanDebugger) CostEstimate {
	panic("EstimateCost should not be called directly on PlannerTerminus; use StartEstimateCost")
}

// PropagateUnlimitFromFlippedJoin is a no-op for terminus.
func (t *PlannerTerminus) PropagateUnlimitFromFlippedJoin() {
	// No-op: terminus doesn't need to unlimit anything.
}

// StartPropagateConstraints initiates constraint propagation from the
// terminus through the graph. Sends an empty branch pattern and nil
// constraint to the input node.
func (t *PlannerTerminus) StartPropagateConstraints(debugger PlanDebugger) {
	t.input.PropagateConstraints(nil, nil, t, debugger)
}

// StartEstimateCost initiates cost estimation from the terminus. Returns
// the cost estimate for the entire plan. Uses selectivity = 1 (no downstream
// filtering) and an empty branch pattern.
func (t *PlannerTerminus) StartEstimateCost(debugger PlanDebugger) CostEstimate {
	return t.input.EstimateCost(1, nil, debugger)
}
