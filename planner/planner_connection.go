package planner

import (
	"fmt"
	"math"
	"strings"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// PlannerConnection represents a connection to a source (table scan).
//
// It follows the dual-state pattern:
//  1. Immutable structure: sort order, filters, cost model (set at construction)
//  2. Mutable state: limit, constraints, cached costs (mutated during planning)
//
// When a connection is pinned as the outer loop, it reveals constraints for
// connected joins. These constraints propagate through the graph, allowing
// other connections to update their cost estimates.
type PlannerConnection struct {
	// Immutable structure
	sort            ivm.Ordering
	filters         *builder.Condition
	model           ConnectionCostModel
	Table           string
	Name            string
	baseConstraints PlannerConstraint
	baseLimit       *float64
	Selectivity     float64
	isRoot          bool
	output          PlannerNode

	// Mutable planning state
	Limit       *float64
	constraints map[string]PlannerConstraint
	cachedCosts map[string]CostEstimate
}

// NewPlannerConnection creates a new connection for a table scan.
func NewPlannerConnection(
	table string,
	model ConnectionCostModel,
	sort ivm.Ordering,
	filters *builder.Condition,
	isRoot bool,
	baseConstraints PlannerConstraint,
	limit *float64,
	name string,
) *PlannerConnection {
	c := &PlannerConnection{
		sort:            sort,
		filters:         filters,
		model:           model,
		Table:           table,
		Name:            name,
		baseConstraints: baseConstraints,
		baseLimit:       limit,
		Limit:           limit,
		isRoot:          isRoot,
		constraints:     make(map[string]PlannerConstraint),
		cachedCosts:     make(map[string]CostEstimate),
	}

	// Compute selectivity for EXISTS child connections (limit === 1).
	// Selectivity = fraction of rows that pass filters.
	if limit != nil && filters != nil {
		costWithFilters := model(table, sort, filters, nil)
		costWithoutFilters := model(table, sort, nil, nil)
		if costWithoutFilters.Rows > 0 {
			c.Selectivity = costWithFilters.Rows / costWithoutFilters.Rows
		} else {
			c.Selectivity = 1.0
		}
	} else {
		c.Selectivity = 1.0
	}

	return c
}

func (c *PlannerConnection) Kind() string { return "connection" }

// SetOutput wires the output node for this connection.
func (c *PlannerConnection) SetOutput(node PlannerNode) {
	c.output = node
}

// Output returns the output node, panicking if not set.
func (c *PlannerConnection) Output() PlannerNode {
	if c.output == nil {
		panic("Output not set")
	}
	return c.output
}

func (c *PlannerConnection) ClosestJoinOrSource() JoinOrConnection {
	return JoinOrConnConnection
}

// PropagateConstraints stores the constraint for the given branch pattern path.
// Constraints are uniquely identified by their path through the graph (branch
// pattern). Clears the per-constraint cost cache since constraints changed.
func (c *PlannerConnection) PropagateConstraints(branchPattern []int, constraint PlannerConstraint, from PlannerNode, debugger PlanDebugger) {
	key := branchPatternKey(branchPattern)
	c.constraints[key] = constraint
	c.cachedCosts = make(map[string]CostEstimate)

	if debugger != nil {
		fromKind := "unknown"
		if from != nil {
			fromKind = from.Kind()
		}
		debugger.Log(NodeConstraintEvent{
			Type:          "node-constraint",
			NodeType:      "connection",
			Node:          c.Name,
			BranchPattern: branchPattern,
			Constraint:    constraint,
			From:          fromKind,
		})
	}
}

// EstimateCost computes the cost estimate for this connection, taking into
// account any constraints propagated from parent joins. Results are cached
// per branch pattern key.
func (c *PlannerConnection) EstimateCost(
	downstreamChildSelectivity float64,
	branchPattern []int,
	debugger PlanDebugger,
) CostEstimate {
	key := branchPatternKey(branchPattern)

	if cached, ok := c.cachedCosts[key]; ok {
		return cached
	}

	constraint := c.constraints[key]
	mergedConstraint := MergeConstraints(c.baseConstraints, constraint)

	result := c.model(c.Table, c.sort, c.filters, mergedConstraint)

	var scanEst float64
	if c.Limit == nil {
		scanEst = result.Rows
	} else {
		scanEst = math.Min(result.Rows, *c.Limit/downstreamChildSelectivity)
	}

	cost := CostEstimate{
		StartupCost:  result.StartupCost,
		ScanEst:      scanEst,
		Cost:         0,
		ReturnedRows: result.Rows,
		Selectivity:  c.Selectivity,
		Limit:        c.Limit,
		Fanout:       result.Fanout,
	}
	c.cachedCosts[key] = cost

	if debugger != nil {
		debugger.Log(NodeCostEvent{
			Type:                       "node-cost",
			NodeType:                   "connection",
			Node:                       c.Name,
			BranchPattern:              branchPattern,
			DownstreamChildSelectivity: downstreamChildSelectivity,
			CostEstimate:               OmitFanout(cost),
		})
	}

	return cost
}

// Unlimit removes the limit from this connection. Called when a parent join
// is flipped, making this connection part of an outer loop that should
// produce all rows rather than stopping at the limit. Root connections
// cannot be unlimited.
func (c *PlannerConnection) Unlimit() {
	if c.isRoot {
		return
	}
	if c.Limit != nil {
		c.Limit = nil
	}
}

// PropagateUnlimitFromFlippedJoin removes the limit when a parent join is
// flipped. For connections, this is equivalent to Unlimit().
func (c *PlannerConnection) PropagateUnlimitFromFlippedJoin() {
	c.Unlimit()
}

// Reset clears mutable planning state (constraints, limits, cost caches)
// back to initial values for another planning pass.
func (c *PlannerConnection) Reset() {
	c.constraints = make(map[string]PlannerConstraint)
	c.Limit = c.baseLimit
	c.cachedCosts = make(map[string]CostEstimate)
}

// CaptureConstraints returns a copy of the current constraint map for
// snapshotting during backtracking.
func (c *PlannerConnection) CaptureConstraints() map[string]PlannerConstraint {
	result := make(map[string]PlannerConstraint, len(c.constraints))
	for k, v := range c.constraints {
		result[k] = v
	}
	return result
}

// RestoreConstraints restores constraint state from a snapshot. Clears the
// per-constraint cost cache since constraints changed.
func (c *PlannerConnection) RestoreConstraints(constraints map[string]PlannerConstraint) {
	c.constraints = make(map[string]PlannerConstraint, len(constraints))
	for k, v := range constraints {
		c.constraints[k] = v
	}
	c.cachedCosts = make(map[string]CostEstimate)
}

// GetConstraintsForDebug returns the current constraints as a map for
// debugging purposes.
func (c *PlannerConnection) GetConstraintsForDebug() map[string]PlannerConstraint {
	result := make(map[string]PlannerConstraint, len(c.constraints))
	for k, v := range c.constraints {
		result[k] = v
	}
	return result
}

// GetFiltersForDebug returns the filters for debugging.
func (c *PlannerConnection) GetFiltersForDebug() *builder.Condition {
	return c.filters
}

// GetSortForDebug returns the sort ordering for debugging.
func (c *PlannerConnection) GetSortForDebug() ivm.Ordering {
	return c.sort
}

// GetConstraintCostsForDebug returns cached per-constraint costs for debugging.
func (c *PlannerConnection) GetConstraintCostsForDebug() map[string]CostEstimateNoFanout {
	result := make(map[string]CostEstimateNoFanout, len(c.cachedCosts))
	for k, v := range c.cachedCosts {
		result[k] = OmitFanout(v)
	}
	return result
}

// branchPatternKey converts a branch pattern slice to a string key for map
// lookups. An empty slice produces "" (matching the TS path.join(',') behavior
// on an empty array).
func branchPatternKey(branchPattern []int) string {
	if len(branchPattern) == 0 {
		return ""
	}
	parts := make([]string, len(branchPattern))
	for i, v := range branchPattern {
		parts[i] = fmt.Sprintf("%d", v)
	}
	return strings.Join(parts, ",")
}
