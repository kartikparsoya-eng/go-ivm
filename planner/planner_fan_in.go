package planner

// FanInType indicates whether a FanIn is a normal FanIn (FI) or a
// Union FanIn (UFI).
type FanInType string

const (
	FanInTypeFI  FanInType = "FI"
	FanInTypeUFI FanInType = "UFI"
)

// PlannerFanIn merges multiple OR branches into a single output stream.
//
// A FanIn can be in two states:
//   - "FI": Normal fan-in — all inputs share the same branch pattern (with 0
//     prepended). Only a single fetch to FanOut regardless of branch count.
//   - "UFI": Union fan-in — each input gets a unique branch pattern. A fetch
//     per internal branch, causing exponential cost increase when chained.
//
// FI converts to UFI when a join inside a FO-FI pair gets flipped.
//
// Selectivity for OR branches assumes independent events:
// P(A OR B) = 1 - (1 - P(A)) * (1 - P(B))
type PlannerFanIn struct {
	inputs  []PlannerNode
	output  PlannerNode
	fanType FanInType
}

// NewPlannerFanIn creates a new FanIn node merging the given input branches.
func NewPlannerFanIn(inputs []PlannerNode) *PlannerFanIn {
	return &PlannerFanIn{
		inputs:  inputs,
		fanType: FanInTypeFI,
	}
}

func (f *PlannerFanIn) Kind() string { return "fan-in" }

// Type returns the current fan-in type (FI or UFI).
func (f *PlannerFanIn) Type() FanInType { return f.fanType }

// SetOutput wires the output node for this fan-in.
func (f *PlannerFanIn) SetOutput(node PlannerNode) {
	f.output = node
}

// Output returns the output node, panicking if not set.
func (f *PlannerFanIn) Output() PlannerNode {
	if f.output == nil {
		panic("Output not set")
	}
	return f.output
}

func (f *PlannerFanIn) ClosestJoinOrSource() JoinOrConnection {
	return JoinOrConnJoin
}

// Reset restores the fan-in type to FI for another planning pass.
func (f *PlannerFanIn) Reset() {
	f.fanType = FanInTypeFI
}

// ConvertToUFI changes the fan-in type from FI to UFI.
func (f *PlannerFanIn) ConvertToUFI() {
	f.fanType = FanInTypeUFI
}

// PropagateUnlimitFromFlippedJoin propagates unlimiting to all inputs.
func (f *PlannerFanIn) PropagateUnlimitFromFlippedJoin() {
	for _, input := range f.inputs {
		input.PropagateUnlimitFromFlippedJoin()
	}
}

// EstimateCost computes the cost estimate for this fan-in by summing costs
// across all input branches. For FI, all inputs share the same branch pattern
// (with 0 prepended) and the max cost is taken. For UFI, each input gets a
// unique branch pattern and costs are summed.
//
// Selectivity uses OR probability: 1 - prod(1 - s_i) assuming independence.
func (f *PlannerFanIn) EstimateCost(
	downstreamChildSelectivity float64,
	branchPattern []int,
	debugger PlanDebugger,
) CostEstimate {
	total := CostEstimate{
		ReturnedRows: 0,
		Cost:         0,
		ScanEst:      0,
		StartupCost:  0,
		Selectivity:  0,
		Limit:        nil,
		Fanout:       nil,
	}

	noMatchProb := 1.0

	if f.fanType == FanInTypeFI {
		// Normal FanIn: all inputs get the same branch pattern with 0 prepended.
		updatedPattern := prependBranchPattern(0, branchPattern)
		var maxRows, maxRunningCost, maxStartupCost, maxScanEst float64

		for _, input := range f.inputs {
			cost := input.EstimateCost(downstreamChildSelectivity, updatedPattern, debugger)
			total.Fanout = cost.Fanout
			maxRows = max(maxRows, cost.ReturnedRows)
			maxRunningCost = max(maxRunningCost, cost.Cost)
			maxStartupCost = max(maxStartupCost, cost.StartupCost)
			maxScanEst = max(maxScanEst, cost.ScanEst)
			noMatchProb *= 1 - cost.Selectivity
			total.Limit = cost.Limit
		}

		total.ReturnedRows = maxRows
		total.Cost = maxRunningCost
		total.Selectivity = 1 - noMatchProb
		total.StartupCost = maxStartupCost
		total.ScanEst = maxScanEst
	} else {
		// Union FanIn (UFI): each input gets a unique branch pattern.
		for i, input := range f.inputs {
			updatedPattern := prependBranchPattern(i, branchPattern)
			cost := input.EstimateCost(downstreamChildSelectivity, updatedPattern, debugger)
			total.Fanout = cost.Fanout
			total.ReturnedRows += cost.ReturnedRows
			total.Cost += cost.Cost
			total.ScanEst += cost.ScanEst
			total.StartupCost += cost.StartupCost
			noMatchProb *= 1 - cost.Selectivity
			total.Limit = cost.Limit
		}
		total.Selectivity = 1 - noMatchProb
	}

	if debugger != nil {
		debugger.Log(NodeCostEvent{
			Type:                       "node-cost",
			NodeType:                   "fan-in",
			Node:                       string(f.fanType),
			BranchPattern:              branchPattern,
			DownstreamChildSelectivity: downstreamChildSelectivity,
			CostEstimate:               OmitFanout(total),
		})
	}

	return total
}

// PropagateConstraints dispatches constraints to all input branches.
// For FI, all inputs get the same branch pattern (with 0 prepended).
// For UFI, each input gets a unique branch pattern.
func (f *PlannerFanIn) PropagateConstraints(branchPattern []int, constraint PlannerConstraint, from PlannerNode, debugger PlanDebugger) {
	if debugger != nil {
		fromKind := "unknown"
		if from != nil {
			fromKind = from.Kind()
		}
		debugger.Log(NodeConstraintEvent{
			Type:          "node-constraint",
			NodeType:      "fan-in",
			Node:          string(f.fanType),
			BranchPattern: branchPattern,
			Constraint:    constraint,
			From:          fromKind,
		})
	}

	if f.fanType == FanInTypeFI {
		updatedPattern := prependBranchPattern(0, branchPattern)
		for _, input := range f.inputs {
			input.PropagateConstraints(updatedPattern, constraint, f, debugger)
		}
		return
	}

	for i, input := range f.inputs {
		updatedPattern := prependBranchPattern(i, branchPattern)
		input.PropagateConstraints(updatedPattern, constraint, f, debugger)
	}
}

// prependBranchPattern creates a new branch pattern with the given index
// prepended to the existing pattern.
func prependBranchPattern(idx int, branchPattern []int) []int {
	result := make([]int, 0, len(branchPattern)+1)
	result = append(result, idx)
	result = append(result, branchPattern...)
	return result
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
