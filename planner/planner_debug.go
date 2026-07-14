package planner

// PlanDebugEvent is the interface for all debug events emitted during query
// planning. Implementations are simple structs with a Type field.
type PlanDebugEvent interface {
	EventType() string
}

// PlanDebugger receives structured debug events during planning. Implementations
// can accumulate events for inspection (AccumulatorDebugger) or discard them
// (NoopDebugger).
type PlanDebugger interface {
	Log(event PlanDebugEvent)
}

// NoopDebugger discards all events. Use this (or nil) when debugging is not
// needed.
type NoopDebugger struct{}

func (NoopDebugger) Log(event PlanDebugEvent) {}

// AccumulatorDebugger stores all events in a slice for later inspection.
type AccumulatorDebugger struct {
	Events []PlanDebugEvent
}

func (d *AccumulatorDebugger) Log(event PlanDebugEvent) {
	d.Events = append(d.Events, event)
}

// --- Event types ---

// AttemptStartEvent is emitted at the start of each planning attempt.
type AttemptStartEvent struct {
	Type          string
	AttemptNumber int
	TotalAttempts int
}

func (e AttemptStartEvent) EventType() string { return e.Type }

// PlanCompleteEvent is emitted when a complete plan is found for an attempt.
type PlanCompleteEvent struct {
	Type          string
	AttemptNumber int
	TotalCost     float64
	FlipPattern   int
}

func (e PlanCompleteEvent) EventType() string { return e.Type }

// BestPlanSelectedEvent is emitted when the best plan across all attempts
// is selected.
type BestPlanSelectedEvent struct {
	Type              string
	BestAttemptNumber int
	TotalCost         float64
	FlipPattern       int
}

func (e BestPlanSelectedEvent) EventType() string { return e.Type }

// NodeCostEvent is emitted by nodes during EstimateCost traversal.
type NodeCostEvent struct {
	Type                       string
	NodeType                   string
	Node                       string
	BranchPattern              []int
	DownstreamChildSelectivity float64
	CostEstimate               CostEstimateNoFanout
	JoinType                   JoinType
}

func (e NodeCostEvent) EventType() string { return e.Type }

// NodeConstraintEvent is emitted by nodes during PropagateConstraints.
type NodeConstraintEvent struct {
	Type          string
	NodeType      string
	Node          string
	BranchPattern []int
	Constraint    PlannerConstraint
	From          string
}

func (e NodeConstraintEvent) EventType() string { return e.Type }
