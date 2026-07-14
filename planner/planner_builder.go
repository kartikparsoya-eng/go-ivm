package planner

import (
	"fmt"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Plans holds the planner graph for a single query level along with
// sub-plans for related subqueries.
type Plans struct {
	Plan     *PlannerGraph
	SubPlans map[string]*Plans
}

// PlanQuery is the entry point for query planning. It builds a planner graph
// from the AST, runs the exhaustive plan enumeration, and returns a modified
// AST with Flip set to true on chosen CSQ conditions.
//
// The planner does NOT build the pipeline — it only modifies the AST.
// The builder's BuildPipeline then uses the modified AST.
func PlanQuery(ast builder.AST, model ConnectionCostModel, debugger PlanDebugger, warn func(format string, args ...any)) builder.AST {
	plans := buildPlanGraph(ast, model, true, nil)
	planRecursively(plans, debugger, warn)
	return applyPlansToAST(ast, plans)
}

// wireOutput connects the output of one node to the input of another.
// Different node types have different wiring methods.
func wireOutput(from, to PlannerNode) {
	switch n := from.(type) {
	case *PlannerConnection:
		n.SetOutput(to)
	case *PlannerJoin:
		n.SetOutput(to)
	case *PlannerFanIn:
		n.SetOutput(to)
	case *PlannerFanOut:
		n.AddOutput(to)
	case *PlannerTerminus:
		panic("Terminus nodes cannot have outputs")
	}
}

// buildPlanGraph constructs a planner graph from an AST. Recursively builds
// sub-plans for related subqueries.
func buildPlanGraph(ast builder.AST, model ConnectionCostModel, isRoot bool, baseConstraints PlannerConstraint) *Plans {
	graph := NewPlannerGraph()
	nextPlanID := 0

	source := graph.AddSource(ast.Table, model)

	var limitPtr *float64
	if ast.Limit != nil {
		l := float64(*ast.Limit)
		limitPtr = &l
	}

	connection := source.Connect(
		ast.OrderBy,
		ast.Where,
		isRoot,
		baseConstraints,
		limitPtr,
	)
	graph.Connections = append(graph.Connections, connection)

	var end PlannerNode = connection
	if ast.Where != nil {
		end = processCondition(ast.Where, end, graph, model, ast.Table, func() int {
			id := nextPlanID
			nextPlanID++
			return id
		})
	}

	terminus := NewPlannerTerminus(end)
	wireOutput(end, terminus)
	graph.SetTerminus(terminus)

	subPlans := make(map[string]*Plans)
	for _, csq := range ast.Related {
		alias := csq.Subquery.Alias
		if alias == "" {
			panic("Related subquery must have alias")
		}
		childConstraints := extractConstraint(csq.Correlation.ChildField)
		subPlans[alias] = buildPlanGraph(csq.Subquery, model, true, childConstraints)
	}

	return &Plans{Plan: graph, SubPlans: subPlans}
}

// processCondition walks a Condition tree and builds the corresponding planner
// nodes (joins, fan-outs, fan-ins).
func processCondition(
	cond *builder.Condition,
	input PlannerNode,
	graph *PlannerGraph,
	model ConnectionCostModel,
	parentTable string,
	getPlanID func() int,
) PlannerNode {
	switch cond.Type {
	case "simple":
		return input
	case "and":
		return processAnd(cond, input, graph, model, parentTable, getPlanID)
	case "or":
		return processOr(cond, input, graph, model, parentTable, getPlanID)
	case "correlatedSubquery":
		return processCorrelatedSubquery(cond, input, graph, model, parentTable, getPlanID)
	}
	return input
}

func processAnd(
	cond *builder.Condition,
	input PlannerNode,
	graph *PlannerGraph,
	model ConnectionCostModel,
	parentTable string,
	getPlanID func() int,
) PlannerNode {
	end := input
	for i := range cond.Conditions {
		end = processCondition(&cond.Conditions[i], end, graph, model, parentTable, getPlanID)
	}
	return end
}

func processOr(
	cond *builder.Condition,
	input PlannerNode,
	graph *PlannerGraph,
	model ConnectionCostModel,
	parentTable string,
	getPlanID func() int,
) PlannerNode {
	var subqueryConditions []builder.Condition
	for i := range cond.Conditions {
		if cond.Conditions[i].Type == "correlatedSubquery" || hasCorrelatedSubquery(&cond.Conditions[i]) {
			subqueryConditions = append(subqueryConditions, cond.Conditions[i])
		}
	}

	if len(subqueryConditions) == 0 {
		return input
	}

	fanOut := NewPlannerFanOut(input)
	graph.FanOuts = append(graph.FanOuts, fanOut)
	wireOutput(input, fanOut)

	var branches []PlannerNode
	for i := range subqueryConditions {
		branch := processCondition(&subqueryConditions[i], fanOut, graph, model, parentTable, getPlanID)
		branches = append(branches, branch)
		fanOut.AddOutput(branch)
	}

	fanIn := NewPlannerFanIn(branches)
	graph.FanIns = append(graph.FanIns, fanIn)
	for _, branch := range branches {
		wireOutput(branch, fanIn)
	}

	return fanIn
}

func processCorrelatedSubquery(
	cond *builder.Condition,
	input PlannerNode,
	graph *PlannerGraph,
	model ConnectionCostModel,
	parentTable string,
	getPlanID func() int,
) PlannerNode {
	if cond.Related == nil {
		return input
	}
	related := cond.Related
	childTable := related.Subquery.Table

	var childSource *PlannerSource
	if graph.HasSource(childTable) {
		childSource = graph.GetSource(childTable)
	} else {
		childSource = graph.AddSource(childTable, model)
	}

	// EXISTS gets limit 1; NOT EXISTS gets no limit (undefined)
	var childLimit *float64
	if cond.Op == "EXISTS" {
		l := 1.0
		childLimit = &l
	}

	childConnection := childSource.Connect(
		related.Subquery.OrderBy,
		related.Subquery.Where,
		false,
		nil,
		childLimit,
	)
	graph.Connections = append(graph.Connections, childConnection)

	var childEnd PlannerNode = childConnection
	if related.Subquery.Where != nil {
		childEnd = processCondition(related.Subquery.Where, childEnd, graph, model, childTable, getPlanID)
	}

	parentConstraint := extractConstraint(related.Correlation.ParentField)
	childConstraint := extractConstraint(related.Correlation.ChildField)

	planID := getPlanID()

	// Determine flippability and initial type based on flip flag and operator
	isNotExists := cond.Op == "NOT EXISTS"
	manualFlip := cond.Flip

	var flippable bool
	var initialType JoinType

	if isNotExists {
		flippable = false
		initialType = JoinTypeSemi
	} else if manualFlip {
		flippable = false
		initialType = JoinTypeFlipped
	} else {
		flippable = true
		initialType = JoinTypeSemi
	}

	join := NewPlannerJoin(
		input,
		childEnd,
		parentConstraint,
		childConstraint,
		related.Correlation.ParentField,
		related.Correlation.ChildField,
		flippable,
		planID,
		initialType,
	)
	graph.Joins = append(graph.Joins, join)

	wireOutput(input, join)
	wireOutput(childEnd, join)

	return join
}

// hasCorrelatedSubquery reports whether the condition tree contains any
// correlatedSubquery node at any depth.
func hasCorrelatedSubquery(cond *builder.Condition) bool {
	if cond == nil {
		return false
	}
	switch cond.Type {
	case "correlatedSubquery":
		return true
	case "and", "or":
		for i := range cond.Conditions {
			if hasCorrelatedSubquery(&cond.Conditions[i]) {
				return true
			}
		}
	}
	return false
}

// extractConstraint creates a PlannerConstraint from a list of field names.
// The constraint represents which columns will be constrained at runtime.
func extractConstraint(fields []string) PlannerConstraint {
	result := make(PlannerConstraint, len(fields))
	for _, field := range fields {
		result[field] = struct{}{}
	}
	return result
}

// planRecursively runs the planner on all sub-plans first (depth-first),
// then on the current plan.
func planRecursively(plans *Plans, debugger PlanDebugger, warn func(format string, args ...any)) {
	for _, subPlan := range plans.SubPlans {
		planRecursively(subPlan, debugger, warn)
	}
	plans.Plan.Plan(debugger, warn)
}

// applyPlansToAST walks the AST and sets Flip=true on CSQ conditions whose
// corresponding join was flipped by the planner.
//
// Since Go's Condition struct doesn't support attaching metadata (like the
// TS planIdSymbol), we reconstruct the plan-ID-to-condition mapping by walking
// the WHERE clause in the same order as buildPlanGraph, incrementing a counter
// for each correlatedSubquery condition encountered.
func applyPlansToAST(ast builder.AST, plans *Plans) builder.AST {
	flippedIDs := make(map[int]bool)
	for _, join := range plans.Plan.Joins {
		if join.Type() == JoinTypeFlipped {
			flippedIDs[join.PlanID] = true
		}
	}

	result := ast

	if result.Where != nil {
		counter := 0
		result.Where = applyToCondition(result.Where, flippedIDs, &counter)
	}

	if len(result.Related) > 0 {
		newRelated := make([]builder.CorrelatedSubquery, len(result.Related))
		for i, csq := range result.Related {
			alias := csq.Subquery.Alias
			if alias == "" {
				panic("Related subquery must have alias")
			}
			subPlan, ok := plans.SubPlans[alias]
			if ok {
				newRelated[i] = csq
				newRelated[i].Subquery = applyPlansToAST(csq.Subquery, subPlan)
			} else {
				newRelated[i] = csq
			}
		}
		result.Related = newRelated
	}

	return result
}

// applyToCondition recursively walks the condition tree and sets Flip=true
// on CSQ conditions whose plan ID is in flippedIDs. The counter is incremented
// for each CSQ encountered, matching the order of plan ID assignment in
// buildPlanGraph.
func applyToCondition(cond *builder.Condition, flippedIDs map[int]bool, counter *int) *builder.Condition {
	switch cond.Type {
	case "simple":
		return cond

	case "correlatedSubquery":
		planID := *counter
		*counter++
		shouldFlip := flippedIDs[planID]

		newCond := *cond
		newCond.Flip = shouldFlip

		if cond.Related != nil {
			newRelated := *cond.Related
			newSubquery := cond.Related.Subquery
			if newSubquery.Where != nil {
				newSubquery.Where = applyToCondition(newSubquery.Where, flippedIDs, counter)
			}
			newRelated.Subquery = newSubquery
			newCond.Related = &newRelated
		}

		return &newCond

	case "and", "or":
		newCond := *cond
		newConditions := make([]builder.Condition, len(cond.Conditions))
		for i := range cond.Conditions {
			result := applyToCondition(&cond.Conditions[i], flippedIDs, counter)
			newConditions[i] = *result
		}
		newCond.Conditions = newConditions
		return &newCond
	}

	return cond
}

// Ensure ivm import is used (for ivm.Ordering used in PlannerSource.Connect).
var _ = ivm.Ordering{}

// Ensure fmt import is used.
var _ = fmt.Sprintf
