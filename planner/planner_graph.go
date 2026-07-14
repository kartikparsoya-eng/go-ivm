package planner

import (
	"fmt"
	"math"
)

// maxFlippableJoins is the maximum number of flippable joins to attempt
// exhaustive enumeration. With n flippable joins, 2^n plans are explored.
// 9 joins = 512 plans; 10 = 1024 (~100-200ms); 12 = 4096 (~400ms-1s).
const maxFlippableJoins = 9

// PlanState captures the mutable planning state of a PlannerGraph for
// backtracking during exhaustive enumeration. It stores a snapshot of
// join types, fan types, connection limits, and constraint maps.
type PlanState struct {
	ConnectionLimits      []*float64
	JoinTypes             []JoinType
	FanOutTypes           []FanOutType
	FanInTypes            []FanInType
	ConnectionConstraints []map[string]PlannerConstraint
}

// fofiInfo caches the relationship between a FanOut and its corresponding
// FanIn, plus all joins between them. Computed once during planning to avoid
// redundant BFS traversals in each enumeration iteration.
type fofiInfo struct {
	fi           *PlannerFanIn
	joinsBetween []*PlannerJoin
}

// PlannerGraph holds the complete planner graph for a single query level:
// sources, connections, joins, fan-outs, fan-ins, and the terminus.
//
// The graph is built once by buildPlanGraph and then repeatedly mutated
// during plan search: join types are flipped, fan types are converted, and
// constraints are propagated. The graph supports snapshot/restore for
// backtracking during exhaustive enumeration.
type PlannerGraph struct {
	sources     map[string]*PlannerSource
	terminus    *PlannerTerminus
	Joins       []*PlannerJoin
	FanOuts     []*PlannerFanOut
	FanIns      []*PlannerFanIn
	Connections []*PlannerConnection
}

// NewPlannerGraph creates a new empty planner graph.
func NewPlannerGraph() *PlannerGraph {
	return &PlannerGraph{
		sources: make(map[string]*PlannerSource),
	}
}

// ResetPlanningState resets all mutable planning state to initial values
// for another planning pass. Graph structure is unchanged.
func (g *PlannerGraph) ResetPlanningState() {
	for _, j := range g.Joins {
		j.Reset()
	}
	for _, fo := range g.FanOuts {
		fo.Reset()
	}
	for _, fi := range g.FanIns {
		fi.Reset()
	}
	for _, c := range g.Connections {
		c.Reset()
	}
}

// AddSource creates and registers a new source (table) in the graph.
// Panics if a source with the same name already exists.
func (g *PlannerGraph) AddSource(name string, model ConnectionCostModel) *PlannerSource {
	if _, exists := g.sources[name]; exists {
		panic(fmt.Sprintf("Source %s already exists in the graph", name))
	}
	source := NewPlannerSource(name, model)
	g.sources[name] = source
	return source
}

// GetSource returns the source for the given table name.
// Panics if not found.
func (g *PlannerGraph) GetSource(name string) *PlannerSource {
	source, ok := g.sources[name]
	if !ok {
		panic(fmt.Sprintf("Source %s not found in the graph", name))
	}
	return source
}

// HasSource returns whether a source with the given name exists.
func (g *PlannerGraph) HasSource(name string) bool {
	_, exists := g.sources[name]
	return exists
}

// SetTerminus sets the terminus (final output) node of the graph.
func (g *PlannerGraph) SetTerminus(terminus *PlannerTerminus) {
	g.terminus = terminus
}

// PropagateConstraints initiates constraint propagation from the terminus.
func (g *PlannerGraph) PropagateConstraints(debugger PlanDebugger) {
	if g.terminus == nil {
		panic("Cannot propagate constraints without a terminus node")
	}
	g.terminus.StartPropagateConstraints(debugger)
}

// GetTotalCost calculates the total cost of the current plan, including
// both startup cost and running cost.
func (g *PlannerGraph) GetTotalCost(debugger PlanDebugger) float64 {
	if g.terminus == nil {
		panic("Cannot get total cost without a terminus node")
	}
	estimate := g.terminus.StartEstimateCost(debugger)
	return estimate.Cost + estimate.StartupCost
}

// CapturePlanningSnapshot captures a lightweight snapshot of the current
// planning state for backtracking.
func (g *PlannerGraph) CapturePlanningSnapshot() *PlanState {
	state := &PlanState{
		ConnectionLimits:      make([]*float64, len(g.Connections)),
		JoinTypes:             make([]JoinType, len(g.Joins)),
		FanOutTypes:           make([]FanOutType, len(g.FanOuts)),
		FanInTypes:            make([]FanInType, len(g.FanIns)),
		ConnectionConstraints: make([]map[string]PlannerConstraint, len(g.Connections)),
	}

	for i, c := range g.Connections {
		state.ConnectionLimits[i] = c.Limit
		state.ConnectionConstraints[i] = c.CaptureConstraints()
	}
	for i, j := range g.Joins {
		state.JoinTypes[i] = j.Type()
	}
	for i, fo := range g.FanOuts {
		state.FanOutTypes[i] = fo.Type()
	}
	for i, fi := range g.FanIns {
		state.FanInTypes[i] = fi.Type()
	}

	return state
}

// RestorePlanningSnapshot restores planning state from a previously captured
// snapshot.
func (g *PlannerGraph) RestorePlanningSnapshot(state *PlanState) {
	if len(g.Connections) != len(state.ConnectionLimits) ||
		len(g.Joins) != len(state.JoinTypes) ||
		len(g.FanOuts) != len(state.FanOutTypes) ||
		len(g.FanIns) != len(state.FanInTypes) ||
		len(g.Connections) != len(state.ConnectionConstraints) {
		panic("Plan state mismatch: graph structure does not match snapshot")
	}

	for i, c := range g.Connections {
		c.Limit = state.ConnectionLimits[i]
		c.RestoreConstraints(state.ConnectionConstraints[i])
	}

	for i, j := range g.Joins {
		j.Reset()
		if state.JoinTypes[i] == JoinTypeFlipped && j.Type() != JoinTypeFlipped {
			j.Flip()
		}
	}

	for i, fo := range g.FanOuts {
		if state.FanOutTypes[i] == FanOutTypeUFO && fo.Type() == FanOutTypeFO {
			fo.ConvertToUFO()
		}
	}

	for i, fi := range g.FanIns {
		if state.FanInTypes[i] == FanInTypeUFI && fi.Type() == FanInTypeFI {
			fi.ConvertToUFI()
		}
	}
}

// Plan is the main planning algorithm using exhaustive join flip enumeration.
//
// It enumerates all possible flip patterns for flippable joins (2^n for n
// flippable joins). Each pattern represents a different query execution plan.
// The cost of each plan is evaluated and the lowest-cost plan is selected.
//
// If there are more than maxFlippableJoins flippable joins, optimization is
// skipped (all semi-joins).
func (g *PlannerGraph) Plan(debugger PlanDebugger, warn func(format string, args ...any)) {
	var flippableJoins []*PlannerJoin
	for _, j := range g.Joins {
		if j.IsFlippable() {
			flippableJoins = append(flippableJoins, j)
		}
	}

	if len(flippableJoins) > maxFlippableJoins {
		if warn != nil {
			warn(
				"Query has %d EXISTS checks which would require %d plan evaluations. Skipping optimization.",
				len(flippableJoins), int(math.Pow(2, float64(len(flippableJoins)))),
			)
		}
		return
	}

	fofiCache := buildFOFICache(g)

	numPatterns := 0
	if len(flippableJoins) > 0 {
		numPatterns = 1 << len(flippableJoins)
	}

	var bestCost = math.Inf(1)
	var bestPlan *PlanState
	bestAttemptNumber := -1

	for pattern := 0; pattern < numPatterns; pattern++ {
		g.ResetPlanningState()

		if debugger != nil {
			debugger.Log(AttemptStartEvent{
				Type:          "attempt-start",
				AttemptNumber: pattern,
				TotalAttempts: numPatterns,
			})
		}

		// Apply flip pattern (bitmask: bit i set means flip join i)
		for i, join := range flippableJoins {
			if pattern&(1<<i) != 0 {
				join.Flip()
			}
		}

		// Derive FO/UFO and FI/UFI states from join flip states
		checkAndConvertFOFI(fofiCache)

		// Propagate unlimiting for flipped joins
		propagateUnlimitForFlippedJoins(g)

		// Propagate constraints through the graph
		g.PropagateConstraints(debugger)

		totalCost := g.GetTotalCost(debugger)

		if debugger != nil {
			debugger.Log(PlanCompleteEvent{
				Type:          "plan-complete",
				AttemptNumber: pattern,
				TotalCost:     totalCost,
				FlipPattern:   pattern,
			})
		}

		if totalCost < bestCost {
			bestCost = totalCost
			bestPlan = g.CapturePlanningSnapshot()
			bestAttemptNumber = pattern
		}
	}

	if bestPlan != nil {
		g.RestorePlanningSnapshot(bestPlan)
		g.PropagateConstraints(debugger)

		if debugger != nil {
			debugger.Log(BestPlanSelectedEvent{
				Type:              "best-plan-selected",
				BestAttemptNumber: bestAttemptNumber,
				TotalCost:         bestCost,
				FlipPattern:       bestAttemptNumber,
			})
		}
	}
}

// buildFOFICache builds a cache of FO→FI relationships and joins between them.
func buildFOFICache(g *PlannerGraph) map[*PlannerFanOut]*fofiInfo {
	cache := make(map[*PlannerFanOut]*fofiInfo)
	for _, fo := range g.FanOuts {
		cache[fo] = findFIAndJoins(fo)
	}
	return cache
}

// checkAndConvertFOFI checks if any joins downstream of a FanOut (before
// reaching FanIn) are flipped. If so, converts the FO to UFO and the FI to UFI.
func checkAndConvertFOFI(fofiCache map[*PlannerFanOut]*fofiInfo) {
	for fo, info := range fofiCache {
		hasFlippedJoin := false
		for _, j := range info.joinsBetween {
			if j.Type() == JoinTypeFlipped {
				hasFlippedJoin = true
				break
			}
		}
		if info.fi != nil && hasFlippedJoin {
			fo.ConvertToUFO()
			info.fi.ConvertToUFI()
		}
	}
}

// findFIAndJoins traverses from a FanOut through its outputs to find the
// corresponding FanIn and collect all joins along the way (BFS).
func findFIAndJoins(fo *PlannerFanOut) *fofiInfo {
	info := &fofiInfo{}
	queue := make([]PlannerNode, 0, len(fo.outputs))
	queue = append(queue, fo.outputs...)
	visited := make(map[PlannerNode]bool)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if visited[node] {
			continue
		}
		visited[node] = true

		switch n := node.(type) {
		case *PlannerJoin:
			info.joinsBetween = append(info.joinsBetween, n)
			queue = append(queue, n.Output())
		case *PlannerFanOut:
			queue = append(queue, n.Outputs()...)
		case *PlannerFanIn:
			info.fi = n
		}
	}

	return info
}

// propagateUnlimitForFlippedJoins propagates unlimiting to all flipped joins.
func propagateUnlimitForFlippedJoins(g *PlannerGraph) {
	for _, join := range g.Joins {
		if join.Type() == JoinTypeFlipped {
			join.PropagateUnlimit()
		}
	}
}
