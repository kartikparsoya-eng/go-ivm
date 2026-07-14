package planner

import (
	"fmt"
	"math"
	"slices"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// UnflippableJoinError is returned when attempting to flip a join that cannot
// be flipped (e.g. a NOT EXISTS join).
type UnflippableJoinError struct {
	msg string
}

func (e *UnflippableJoinError) Error() string {
	return e.msg
}

// translateConstraintsForFlippedJoin translates constraints from parent key
// space to child key space using index-based key mapping. This matches the
// runtime behavior of FlippedJoin.Fetch() which translates parent constraints
// to child constraints using the positional correspondence between
// parentConstraint keys and childConstraint keys.
//
// Example:
//
//	parentKeys = ["issueID", "projectID"]
//	childKeys  = ["id", "projectID"]
//	incoming   = {"issueID": {}}
//	result     = {"id": {}}  // issueID at index 0 maps to id at index 0
func translateConstraintsForFlippedJoin(
	incomingConstraint PlannerConstraint,
	parentKeys []string,
	childKeys []string,
) PlannerConstraint {
	if incomingConstraint == nil {
		return nil
	}

	translated := make(PlannerConstraint)
	for key := range incomingConstraint {
		idx := slices.Index(parentKeys, key)
		if idx != -1 && idx < len(childKeys) {
			translated[childKeys[idx]] = struct{}{}
		}
	}

	if len(translated) == 0 {
		return nil
	}
	return translated
}

// PlannerJoin represents a join between two data streams (parent and child).
//
// A join can be in two states:
//   - "semi": Parent is outer loop, child is inner (semi-join for EXISTS)
//   - "flipped": Child is outer loop, parent is inner
//
// Flipping is the key optimization: choosing which table scans first.
// NOT EXISTS joins cannot be flipped.
//
// Constraint propagation rules:
//   - Semi-join: Sends childConstraint to child, forwards received constraints
//     to parent.
//   - Flipped join: Translates incoming constraints from parent space to child
//     space, merges parentConstraint with received constraints and sends to
//     parent.
type PlannerJoin struct {
	// Immutable structure
	parent           PlannerNode
	child            PlannerNode
	parentConstraint PlannerConstraint
	childConstraint  PlannerConstraint
	parentKeys       []string // ordered keys for constraint translation
	childKeys        []string // ordered keys for constraint translation
	flippable        bool
	PlanID           int
	initialType      JoinType
	output           PlannerNode

	// Mutable planning state
	joinType JoinType
}

// NewPlannerJoin creates a new join node.
func NewPlannerJoin(
	parent PlannerNode,
	child PlannerNode,
	parentConstraint PlannerConstraint,
	childConstraint PlannerConstraint,
	parentKeys []string,
	childKeys []string,
	flippable bool,
	planID int,
	initialType JoinType,
) *PlannerJoin {
	return &PlannerJoin{
		parent:           parent,
		child:            child,
		parentConstraint: parentConstraint,
		childConstraint:  childConstraint,
		parentKeys:       parentKeys,
		childKeys:        childKeys,
		flippable:        flippable,
		PlanID:           planID,
		initialType:      initialType,
		joinType:         initialType,
	}
}

func (j *PlannerJoin) Kind() string { return "join" }

// SetOutput wires the output node for this join.
func (j *PlannerJoin) SetOutput(node PlannerNode) {
	j.output = node
}

// Output returns the output node, panicking if not set.
func (j *PlannerJoin) Output() PlannerNode {
	if j.output == nil {
		panic("Output not set")
	}
	return j.output
}

func (j *PlannerJoin) ClosestJoinOrSource() JoinOrConnection {
	return JoinOrConnJoin
}

// FlipIfNeeded flips the join if the given input is the child side.
// If the input is the parent side, the join stays semi.
func (j *PlannerJoin) FlipIfNeeded(input PlannerNode) {
	if input == j.child {
		j.Flip()
	}
	// If input == j.parent, no flip needed (stay semi)
}

// Flip changes the join type from semi to flipped. Panics if the join is
// already flipped or not flippable.
func (j *PlannerJoin) Flip() {
	if j.joinType != JoinTypeSemi {
		panic("Can only flip a semi-join")
	}
	if !j.flippable {
		panic(&UnflippableJoinError{msg: "Cannot flip a non-flippable join (e.g., NOT EXISTS)"})
	}
	j.joinType = JoinTypeFlipped
}

// Type returns the current join type.
func (j *PlannerJoin) Type() JoinType { return j.joinType }

// IsFlippable returns whether this join can be flipped.
func (j *PlannerJoin) IsFlippable() bool { return j.flippable }

// PropagateUnlimit propagates unlimiting when this join is flipped. The child
// becomes the outer loop and should no longer be limited by EXISTS semantics.
func (j *PlannerJoin) PropagateUnlimit() {
	if j.joinType != JoinTypeFlipped {
		panic("Can only unlimit a flipped join")
	}
	j.child.PropagateUnlimitFromFlippedJoin()
}

// PropagateUnlimitFromFlippedJoin is called when a parent join is flipped and
// this join is part of its child subgraph. Continues propagation to parent
// (the outer loop).
func (j *PlannerJoin) PropagateUnlimitFromFlippedJoin() {
	j.parent.PropagateUnlimitFromFlippedJoin()
}

// PropagateConstraints pushes constraints through the join based on the
// current join type.
func (j *PlannerJoin) PropagateConstraints(branchPattern []int, constraint PlannerConstraint, from PlannerNode, debugger PlanDebugger) {
	if debugger != nil {
		fromKind := "unknown"
		if from != nil {
			fromKind = getNodeName(from)
		}
		debugger.Log(NodeConstraintEvent{
			Type:          "node-constraint",
			NodeType:      "join",
			Node:          j.GetName(),
			BranchPattern: branchPattern,
			Constraint:    constraint,
			From:          fromKind,
		})
	}

	switch j.joinType {
	case JoinTypeSemi:
		// A semi-join always has constraints for its child, defined by the
		// correlation between parent and child.
		j.child.PropagateConstraints(branchPattern, j.childConstraint, j, debugger)
		// A semi-join forwards constraints to its parent.
		j.parent.PropagateConstraints(branchPattern, constraint, j, debugger)

	case JoinTypeFlipped:
		// A flipped join translates constraints from parent space to child
		// space, matching FlippedJoin.fetch() runtime behavior.
		translated := translateConstraintsForFlippedJoin(
			constraint,
			j.parentKeys,
			j.childKeys,
		)
		j.child.PropagateConstraints(branchPattern, translated, j, debugger)
		// A flipped join merges received constraints with its own parent
		// constraint and sends the result to its parent.
		merged := MergeConstraints(constraint, j.parentConstraint)
		j.parent.PropagateConstraints(branchPattern, merged, j, debugger)
	}
}

// Reset restores the join type to its initial value for another planning pass.
func (j *PlannerJoin) Reset() {
	j.joinType = j.initialType
}

// EstimateCost computes the cost estimate for this join.
//
// downstreamChildSelectivity accumulates up a parent chain and represents how
// selective the pipeline is below this node. It is used to estimate how many
// parent rows will be pulled when trying to satisfy downstream constraints
// and a limit.
//
// branchPattern uniquely identifies OR branches in the graph, allowing
// per-path constraint correlation.
func (j *PlannerJoin) EstimateCost(
	downstreamChildSelectivity float64,
	branchPattern []int,
	debugger PlanDebugger,
) CostEstimate {
	// Child chains represent independent sub-graphs, so downstream child
	// selectivity does not accumulate down child chains.
	child := j.child.EstimateCost(1, branchPattern, debugger)

	// Factor in how many child rows match a parent row (fanout).
	childKeyCols := make([]string, 0, len(j.childConstraint))
	for k := range j.childConstraint {
		childKeyCols = append(childKeyCols, k)
	}
	fanoutFactor := child.Fanout(childKeyCols)

	// Scaled child selectivity: P(at least one child matches) =
	// 1 - (1 - childSelectivity)^fanout
	scaledChildSelectivity := 1 - math.Pow(1-child.Selectivity, fanoutFactor.Fanout)

	// Parent selectivity flows up the graph from child to parent.
	var parentSelectivity float64
	if j.joinType == JoinTypeFlipped {
		parentSelectivity = 1 * downstreamChildSelectivity
	} else {
		parentSelectivity = scaledChildSelectivity * downstreamChildSelectivity
	}
	parent := j.parent.EstimateCost(parentSelectivity, branchPattern, debugger)

	var costEstimate CostEstimate

	if j.joinType == JoinTypeSemi {
		var scanEst float64
		if parent.Limit == nil {
			scanEst = parent.ReturnedRows
		} else {
			if downstreamChildSelectivity == 0 {
				scanEst = 0
			} else {
				scanEst = math.Min(parent.ReturnedRows, *parent.Limit/downstreamChildSelectivity)
			}
		}

		costEstimate = CostEstimate{
			StartupCost:  parent.StartupCost,
			ScanEst:      scanEst,
			Cost:         parent.Cost + parent.ScanEst*(child.StartupCost+child.Cost+child.ScanEst),
			ReturnedRows: parent.ReturnedRows * child.Selectivity,
			Selectivity:  child.Selectivity * parent.Selectivity,
			Limit:        parent.Limit,
			Fanout:       parent.Fanout,
		}
	} else {
		// Flipped join
		var scanEst float64
		if parent.Limit == nil {
			scanEst = parent.ReturnedRows * child.ReturnedRows
		} else {
			if downstreamChildSelectivity == 0 {
				scanEst = 0
			} else {
				scanEst = math.Min(parent.ReturnedRows*child.ReturnedRows, *parent.Limit/downstreamChildSelectivity)
			}
		}

		// FlippedJoin batches child→parent lookups into chunks of
		// GetMultiConstraintChunkSize(), issuing one IN-list query per chunk.
		// So parent.StartupCost is paid once per chunk, not once per child
		// row. The per-seek work (parent.cost + parent.scanEst) still scales
		// with child row count.
		chunkSize := float64(ivm.GetMultiConstraintChunkSize())
		costEstimate = CostEstimate{
			StartupCost: child.StartupCost,
			ScanEst:     scanEst,
			Cost: child.Cost +
				math.Ceil(child.ScanEst/chunkSize)*parent.StartupCost +
				child.ScanEst*(parent.Cost+parent.ScanEst),
			ReturnedRows: parent.ReturnedRows * child.ReturnedRows,
			Selectivity:  parent.Selectivity * child.Selectivity,
			Limit:        parent.Limit,
			Fanout:       parent.Fanout,
		}
	}

	if debugger != nil {
		debugger.Log(NodeCostEvent{
			Type:                       "node-cost",
			NodeType:                   "join",
			Node:                       j.GetName(),
			BranchPattern:              branchPattern,
			DownstreamChildSelectivity: downstreamChildSelectivity,
			CostEstimate:               OmitFanout(costEstimate),
			JoinType:                   j.joinType,
		})
	}

	return costEstimate
}

// GetName returns a human-readable name for this join: "parentName ⋈ childName".
func (j *PlannerJoin) GetName() string {
	return fmt.Sprintf("%s ⋈ %s", getNodeName(j.parent), getNodeName(j.child))
}

// GetDebugInfo returns debug information about this join's state.
func (j *PlannerJoin) GetDebugInfo() struct {
	Name   string
	Type   JoinType
	PlanID int
} {
	return struct {
		Name   string
		Type   JoinType
		PlanID int
	}{
		Name:   j.GetName(),
		Type:   j.joinType,
		PlanID: j.PlanID,
	}
}

// getNodeName returns a human-readable name for any planner node.
func getNodeName(node PlannerNode) string {
	switch n := node.(type) {
	case *PlannerConnection:
		return n.Name
	case *PlannerJoin:
		return n.GetName()
	case *PlannerFanOut:
		return "FO"
	case *PlannerFanIn:
		return "FI"
	case *PlannerTerminus:
		return "terminus"
	default:
		return "unknown"
	}
}
