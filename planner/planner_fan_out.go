package planner

// FanOutType indicates whether a FanOut is a normal FanOut (FO) or a
// Union FanOut (UFO).
type FanOutType string

const (
	FanOutTypeFO  FanOutType = "FO"
	FanOutTypeUFO FanOutType = "UFO"
)

// PlannerFanOut splits an input stream into multiple OR branches.
//
// A FanOut can be in two states:
//   - "FO": Normal fan-out — all sub-joins share a single branch pattern.
//   - "UFO": Union fan-out — each sub-join gets a unique branch pattern.
//
// FO converts to UFO when a join inside a FO-FI pair gets flipped, because
// the flipped join's branch needs independent constraint tracking.
type PlannerFanOut struct {
	input   PlannerNode
	outputs []PlannerNode
	fanType FanOutType
}

// NewPlannerFanOut creates a new FanOut node wrapping the given input.
func NewPlannerFanOut(input PlannerNode) *PlannerFanOut {
	return &PlannerFanOut{
		input:   input,
		fanType: FanOutTypeFO,
	}
}

func (f *PlannerFanOut) Kind() string { return "fan-out" }

// Type returns the current fan-out type (FO or UFO).
func (f *PlannerFanOut) Type() FanOutType { return f.fanType }

// AddOutput adds an output branch to this fan-out.
func (f *PlannerFanOut) AddOutput(node PlannerNode) {
	f.outputs = append(f.outputs, node)
}

// Outputs returns the list of output branches.
func (f *PlannerFanOut) Outputs() []PlannerNode { return f.outputs }

func (f *PlannerFanOut) ClosestJoinOrSource() JoinOrConnection {
	return f.input.ClosestJoinOrSource()
}

// PropagateConstraints forwards constraints to the input node. The FanOut
// itself is transparent to constraint propagation — the branch pattern is
// managed by the FanIn on the other side.
func (f *PlannerFanOut) PropagateConstraints(branchPattern []int, constraint PlannerConstraint, from PlannerNode, debugger PlanDebugger) {
	if debugger != nil {
		fromKind := "unknown"
		if from != nil {
			fromKind = from.Kind()
		}
		debugger.Log(NodeConstraintEvent{
			Type:          "node-constraint",
			NodeType:      "fan-out",
			Node:          "FO",
			BranchPattern: branchPattern,
			Constraint:    constraint,
			From:          fromKind,
		})
	}

	f.input.PropagateConstraints(branchPattern, constraint, f, debugger)
}

// EstimateCost delegates cost estimation to the input node. The FanOut
// itself is transparent to cost estimation.
func (f *PlannerFanOut) EstimateCost(downstreamChildSelectivity float64, branchPattern []int, debugger PlanDebugger) CostEstimate {
	ret := f.input.EstimateCost(downstreamChildSelectivity, branchPattern, debugger)

	if debugger != nil {
		debugger.Log(NodeCostEvent{
			Type:                       "node-cost",
			NodeType:                   "fan-out",
			Node:                       "FO",
			BranchPattern:              branchPattern,
			DownstreamChildSelectivity: downstreamChildSelectivity,
			CostEstimate:               OmitFanout(ret),
		})
	}

	return ret
}

// ConvertToUFO changes the fan-out type from FO to UFO.
func (f *PlannerFanOut) ConvertToUFO() {
	f.fanType = FanOutTypeUFO
}

// Reset restores the fan-out type to FO for another planning pass.
func (f *PlannerFanOut) Reset() {
	f.fanType = FanOutTypeFO
}

// PropagateUnlimitFromFlippedJoin propagates unlimiting to the input node.
func (f *PlannerFanOut) PropagateUnlimitFromFlippedJoin() {
	f.input.PropagateUnlimitFromFlippedJoin()
}
