package planner

import (
	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// PlannerSource is a factory that creates PlannerConnection instances for
// a given table. Each source is registered in the PlannerGraph and looked
// up by table name when building the plan graph.
type PlannerSource struct {
	Name  string
	model ConnectionCostModel
}

// NewPlannerSource creates a source for the given table name and cost model.
func NewPlannerSource(name string, model ConnectionCostModel) *PlannerSource {
	return &PlannerSource{Name: name, model: model}
}

// Connect creates a new PlannerConnection (table scan node) for this source.
//
// sort is the ordering for the scan. filters are the WHERE conditions applied
// at the source level. isRoot indicates whether this is the root table of the
// query (root connections cannot be unlimited). baseConstraints are
// constraints inherited from a parent correlation. limit is the query LIMIT.
func (s *PlannerSource) Connect(
	sort ivm.Ordering,
	filters *builder.Condition,
	isRoot bool,
	baseConstraints PlannerConstraint,
	limit *float64,
) *PlannerConnection {
	return NewPlannerConnection(
		s.Name,
		s.model,
		sort,
		filters,
		isRoot,
		baseConstraints,
		limit,
		s.Name,
	)
}
