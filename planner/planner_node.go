package planner

// JoinType indicates whether a join is semi (parent drives) or flipped
// (child drives).
type JoinType string

const (
	JoinTypeSemi    JoinType = "semi"
	JoinTypeFlipped JoinType = "flipped"
)

// JoinOrConnection identifies whether the closest ancestor node (traversing
// toward the root) is a join or a connection. Used by FanOut to delegate
// closestJoinOrSource to its input.
type JoinOrConnection string

const (
	JoinOrConnJoin       JoinOrConnection = "join"
	JoinOrConnConnection JoinOrConnection = "connection"
)

// CostEstimate holds the cost model output for a single planner node during
// the bottom-up cost estimation traversal.
//
// The planner walks the graph from the terminus down, calling EstimateCost
// on each node. Each node calls EstimateCost on its children, accumulates
// their costs, and returns a CostEstimate that the parent uses.
type CostEstimate struct {
	// StartupCost is the one-time cost of setting up the scan (e.g. B-tree
	// sort). Paid once regardless of row count.
	StartupCost float64

	// ScanEst is the estimated number of rows scanned from this node's
	// source, taking into account limits and downstream selectivity.
	ScanEst float64

	// Cost is the cumulative running cost of the pipeline up to and including
	// this node. For a semi-join, this is parent.cost + parent.scanEst *
	// (child.startupCost + child.cost + child.scanEst). For a flipped join,
	// the child drives so the cost is computed differently (see PlannerJoin).
	Cost float64

	// ReturnedRows is the estimated number of rows output by this node.
	ReturnedRows float64

	// Selectivity is the fraction of input rows that pass through this node.
	// For a connection this is the fraction passing filters (1.0 = no
	// filtering). For joins it is the fraction of parent rows that match.
	// For fan-in it is the OR probability: 1 - prod(1 - s_i).
	Selectivity float64

	// Limit is the LIMIT on this node's output, or nil if unlimited.
	Limit *float64

	// Fanout is a function that estimates join fanout for a set of columns.
	// Used by parent joins to scale child selectivity.
	Fanout FanoutCostModel
}

// CostEstimateNoFanout is CostEstimate without the Fanout function field,
// for serialization and debugging.
type CostEstimateNoFanout struct {
	StartupCost  float64
	ScanEst      float64
	Cost         float64
	ReturnedRows float64
	Selectivity  float64
	Limit        *float64
}

// OmitFanout returns a CostEstimate without the Fanout function, suitable
// for serialization and debug logging.
func OmitFanout(cost CostEstimate) CostEstimateNoFanout {
	return CostEstimateNoFanout{
		StartupCost:  cost.StartupCost,
		ScanEst:      cost.ScanEst,
		Cost:         cost.Cost,
		ReturnedRows: cost.ReturnedRows,
		Selectivity:  cost.Selectivity,
		Limit:        cost.Limit,
	}
}

// PlannerNode is the interface implemented by all node types in the planner
// graph: PlannerJoin, PlannerConnection, PlannerFanOut, PlannerFanIn, and
// PlannerTerminus.
//
// All nodes follow the dual-state pattern:
//  1. Immutable structure (set at construction, never changes)
//  2. Mutable planning state (join types, constraints, limits — mutated
//     during plan search and reset between attempts)
type PlannerNode interface {
	// Kind returns the node type identifier: "join", "connection",
	// "fan-out", "fan-in", or "terminus".
	Kind() string

	// ClosestJoinOrSource returns "join" or "connection" depending on
	// whether the nearest non-fan node toward the root is a join or a
	// connection. FanOut delegates to its input.
	ClosestJoinOrSource() JoinOrConnection

	// PropagateConstraints pushes constraint information through the graph
	// during planning. branchPattern identifies the OR-branch path, and
	// constraint is the set of constrained columns (or nil). from is the
	// node that sent the constraint (for debugging).
	PropagateConstraints(branchPattern []int, constraint PlannerConstraint, from PlannerNode, debugger PlanDebugger)

	// EstimateCost computes the cost estimate for this node given the
	// downstream child selectivity (how selective the pipeline is below
	// this node) and the branch pattern (OR-branch path identifier).
	EstimateCost(downstreamChildSelectivity float64, branchPattern []int, debugger PlanDebugger) CostEstimate

	// PropagateUnlimitFromFlippedJoin is called when a parent join is
	// flipped, making this node part of the outer loop that should produce
	// all rows rather than stopping at a limit.
	PropagateUnlimitFromFlippedJoin()
}
