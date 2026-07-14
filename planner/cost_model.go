package planner

import (
	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// PlannerConstraint represents the set of columns that will be constrained
// at runtime by a join. The actual values are not known at plan time — only
// the column names matter for cost estimation. Mirrors the TS
// PlannerConstraint = Record<string, undefined>.
type PlannerConstraint map[string]struct{}

// FanoutResult holds a fanout estimate and its confidence.
//
// Fanout is the average number of child rows per distinct parent key value.
// Confidence ranges from 0 (no statistics) to 1 (stat4 histogram).
type FanoutResult struct {
	Fanout     float64
	Confidence float64
}

// FanoutCostModel estimates join fanout for a set of columns.
// The planner calls this during join cost estimation to determine how many
// child rows match each parent key.
type FanoutCostModel func(columns []string) FanoutResult

// ConnectionCostResult holds the cost estimate for a table connection.
//
// StartupCost is the one-time cost of setting up the scan (e.g. B-tree sort).
// Rows is the estimated number of rows output by the scan.
// Fanout is a function that can be called to get join fanout for this table.
type ConnectionCostResult struct {
	StartupCost float64
	Rows        float64
	Fanout      FanoutCostModel
}

// ConnectionCostModel estimates query cost for a table connection.
// The planner calls this with different sort orders, filters, and constraints
// to evaluate different join orderings.
type ConnectionCostModel func(
	table string,
	sort ivm.Ordering,
	filters *builder.Condition,
	constraint PlannerConstraint,
) ConnectionCostResult

// TableSpec describes a table's schema for cost estimation.
type TableSpec struct {
	TableName   string
	PrimaryKey  []string
	UniqueKeys  [][]string
	ColumnTypes map[string]string
}
