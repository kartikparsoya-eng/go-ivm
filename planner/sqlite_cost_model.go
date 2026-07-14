package planner

import (
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// btreeCost estimates the cost of sorting rows using a B-tree.
// B-tree construction is O(n log n); divided by 10 because SQLite's
// internal sort is ~10x faster than sorting in application code.
func btreeCost(rows float64) float64 {
	if rows <= 1 {
		return 0
	}
	return (rows * math.Log2(rows)) / 10
}

// planRow represents a single row from EXPLAIN QUERY PLAN output.
type planRow struct {
	id     int
	parent int
	detail string
}

// sqliteCostModel is the SQLite-based cost model for query planning.
// It uses EXPLAIN QUERY PLAN and sqlite_stat1 to estimate query costs
// based on the SQLite query planner's analysis.
type sqliteCostModel struct {
	db         *sql.DB
	tableSpecs map[string]TableSpec
	fanout     *SQLiteStatFanout
	cache      sync.Map // map[string]ConnectionCostResult
	rowsCache  sync.Map // map[string]float64 (table → total rows)
}

// CreateSQLiteCostModel creates a cost model backed by SQLite statistics.
//
// The cost model builds SELECT queries for each table, runs EXPLAIN QUERY
// PLAN to get SQLite's planner analysis, and uses sqlite_stat1 for row
// count estimation. Results are cached per (table, sort, filters, constraint)
// combination.
//
// Since mattn/go-sqlite3 does not expose sqlite3_stmt_scanstatus_v2, this
// implementation uses EXPLAIN QUERY PLAN for plan structure (index selection,
// sort detection) and sqlite_stat1 for row count estimation as a fallback.
func CreateSQLiteCostModel(db *sql.DB, tableSpecs map[string]TableSpec) ConnectionCostModel {
	cm := &sqliteCostModel{
		db:         db,
		tableSpecs: tableSpecs,
		fanout:     NewSQLiteStatFanout(db, 3),
	}
	return cm.estimate
}

func (cm *sqliteCostModel) estimate(
	table string,
	sortOrder ivm.Ordering,
	filters *builder.Condition,
	constraint PlannerConstraint,
) ConnectionCostResult {
	cacheKey := cm.buildCacheKey(table, sortOrder, filters, constraint)
	if cached, ok := cm.cache.Load(cacheKey); ok {
		return cached.(ConnectionCostResult)
	}

	result := cm.computeCost(table, sortOrder, filters, constraint)
	cm.cache.Store(cacheKey, result)
	return result
}

func (cm *sqliteCostModel) computeCost(
	table string,
	sortOrder ivm.Ordering,
	filters *builder.Condition,
	constraint PlannerConstraint,
) ConnectionCostResult {
	spec, ok := cm.tableSpecs[table]
	if !ok {
		return ConnectionCostResult{
			Rows: 100,
			Fanout: func([]string) FanoutResult {
				return FanoutResult{Fanout: 3, Confidence: 0}
			},
		}
	}

	noSubqueryFilters := removeCorrelatedSubqueries(filters)

	columns := make(map[string]sqlite.ColumnSchema, len(spec.ColumnTypes))
	for name, typ := range spec.ColumnTypes {
		columns[name] = sqlite.ColumnSchema{Type: typ}
	}

	var ivmConstraint *ivm.Constraint
	if len(constraint) > 0 {
		c := ivm.Constraint{}
		for col := range constraint {
			c[col] = nil
		}
		ivmConstraint = &c
	}

	sqliteFilters := builderConditionToSQLite(noSubqueryFilters)

	query := sqlite.BuildSelectQuery(
		table,
		columns,
		ivmConstraint,
		sqliteFilters,
		sortOrder,
		false,
		nil,
		nil,
	)

	rows, hasSort := cm.estimateRowsFromPlan(query.SQL, query.Params, table, constraint)

	startupCost := 0.0
	if hasSort {
		startupCost = btreeCost(rows)
	}

	fanoutFn := func(columns []string) FanoutResult {
		return cm.fanout.GetFanout(table, columns)
	}

	return ConnectionCostResult{
		StartupCost: startupCost,
		Rows:        rows,
		Fanout:      fanoutFn,
	}
}

// estimateRowsFromPlan runs EXPLAIN QUERY PLAN and parses the output to
// estimate row counts and detect whether sorting is needed.
//
// Since PlannerConstraint values are unknown at plan time, nil params are
// replaced with a dummy value (1) so SQLite's planner sees a concrete value
// and can use index statistics for selectivity estimation.
func (cm *sqliteCostModel) estimateRowsFromPlan(
	sqlText string,
	params []interface{},
	tableName string,
	constraint PlannerConstraint,
) (rows float64, hasSort bool) {
	explainSQL := "EXPLAIN QUERY PLAN " + sqlText

	boundParams := make([]interface{}, len(params))
	for i, p := range params {
		if p == nil {
			boundParams[i] = 1
		} else {
			boundParams[i] = p
		}
	}

	qRows, err := cm.db.Query(explainSQL, boundParams...)
	if err != nil {
		return cm.fallbackRowCount(tableName, constraint), cm.detectSortFromOrdering(tableName, constraint)
	}
	defer qRows.Close()

	var planRows []planRow
	for qRows.Next() {
		var pr planRow
		var notused int
		if err := qRows.Scan(&pr.id, &pr.parent, &notused, &pr.detail); err != nil {
			return cm.fallbackRowCount(tableName, constraint), cm.detectSortFromOrdering(tableName, constraint)
		}
		planRows = append(planRows, pr)
	}
	if qRows.Err() != nil {
		return cm.fallbackRowCount(tableName, constraint), cm.detectSortFromOrdering(tableName, constraint)
	}
	if len(planRows) == 0 {
		return cm.fallbackRowCount(tableName, constraint), cm.detectSortFromOrdering(tableName, constraint)
	}

	var topLevel []planRow
	for _, pr := range planRows {
		if pr.parent == 0 {
			topLevel = append(topLevel, pr)
		}
	}
	if len(topLevel) == 0 {
		return cm.fallbackRowCount(tableName, constraint), false
	}

	first := true
	for _, op := range topLevel {
		if first {
			rows = cm.estimateRowsFromDetail(op.detail, tableName, constraint)
			first = false
		} else if strings.Contains(op.detail, "ORDER BY") {
			hasSort = true
		}
	}

	if rows <= 0 {
		rows = cm.fallbackRowCount(tableName, constraint)
	}

	return rows, hasSort
}

// estimateRowsFromDetail parses a single EXPLAIN QUERY PLAN detail line and
// estimates the number of rows the operation will output.
func (cm *sqliteCostModel) estimateRowsFromDetail(
	detail, tableName string,
	constraint PlannerConstraint,
) float64 {
	detail = strings.TrimSpace(detail)

	if strings.HasPrefix(detail, "SCAN") {
		return cm.getTotalRows(tableName)
	}

	if strings.HasPrefix(detail, "SEARCH") {
		if rowEst, ok := extractRowEstimate(detail); ok {
			return rowEst
		}

		idxName := extractIndexName(detail)
		if idxName != "" {
			if stat, ok := cm.getStat1(tableName, idxName); ok {
				parts := strings.Fields(stat)
				if len(parts) > 0 {
					totalRows, _ := strconv.ParseFloat(parts[0], 64)
					depth := extractIndexDepth(detail)
					if depth > 0 && depth < len(parts) {
						if r, err := strconv.ParseFloat(parts[depth], 64); err == nil && r > 0 {
							return r
						}
					}
					if totalRows > 0 {
						return totalRows
					}
				}
			}
		}
		return cm.getTotalRows(tableName)
	}

	return cm.getTotalRows(tableName)
}

// fallbackRowCount estimates rows without EXPLAIN QUERY PLAN, using stat1
// fanout for constrained columns or total table row count.
func (cm *sqliteCostModel) fallbackRowCount(tableName string, constraint PlannerConstraint) float64 {
	totalRows := cm.getTotalRows(tableName)
	if len(constraint) > 0 {
		cols := make([]string, 0, len(constraint))
		for col := range constraint {
			cols = append(cols, col)
		}
		fanout := cm.fanout.GetFanout(tableName, cols)
		if fanout.Fanout > 0 {
			return totalRows / fanout.Fanout
		}
	}
	return totalRows
}

// detectSortFromOrdering checks whether sorting is needed by comparing
// sort columns against available indexes. Used as a fallback when
// EXPLAIN QUERY PLAN is unavailable.
func (cm *sqliteCostModel) detectSortFromOrdering(tableName string, constraint PlannerConstraint) bool {
	return true
}

// getTotalRows returns the total number of rows in a table, using
// sqlite_stat1 (first) or COUNT(*) (fallback), with caching.
func (cm *sqliteCostModel) getTotalRows(tableName string) float64 {
	if cached, ok := cm.rowsCache.Load(tableName); ok {
		return cached.(float64)
	}

	var stat string
	err := cm.db.QueryRow(
		`SELECT stat FROM sqlite_stat1 WHERE tbl = ? LIMIT 1`,
		tableName,
	).Scan(&stat)
	if err == nil {
		parts := strings.Fields(stat)
		if len(parts) > 0 {
			if rows, err := strconv.ParseFloat(parts[0], 64); err == nil && rows > 0 {
				cm.rowsCache.Store(tableName, rows)
				return rows
			}
		}
	}

	var count int64
	err = cm.db.QueryRow(
		fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(tableName)),
	).Scan(&count)
	if err == nil {
		rows := float64(count)
		cm.rowsCache.Store(tableName, rows)
		return rows
	}

	return 1000
}

// getStat1 queries sqlite_stat1 for a specific table/index pair.
func (cm *sqliteCostModel) getStat1(tableName, indexName string) (string, bool) {
	var stat string
	err := cm.db.QueryRow(
		`SELECT stat FROM sqlite_stat1 WHERE tbl = ? AND idx = ?`,
		tableName, indexName,
	).Scan(&stat)
	if err != nil {
		return "", false
	}
	return stat, true
}

// ClearCache clears all cost model caches.
func (cm *sqliteCostModel) ClearCache() {
	cm.cache.Range(func(key, value any) bool {
		cm.cache.Delete(key)
		return true
	})
	cm.rowsCache.Range(func(key, value any) bool {
		cm.rowsCache.Delete(key)
		return true
	})
	cm.fanout.ClearCache()
}

// --- Filter conversion helpers ---

// removeCorrelatedSubqueries strips correlatedSubquery conditions from the
// filter tree. The cost model cannot estimate subquery cost via EXPLAIN QUERY
// PLAN, so we estimate conservatively without them.
func removeCorrelatedSubqueries(cond *builder.Condition) *builder.Condition {
	if cond == nil {
		return nil
	}
	switch cond.Type {
	case "correlatedSubquery":
		return nil
	case "simple":
		return cond
	case "and", "or":
		var filtered []builder.Condition
		for i := range cond.Conditions {
			sub := removeCorrelatedSubqueries(&cond.Conditions[i])
			if sub != nil {
				filtered = append(filtered, *sub)
			}
		}
		if len(filtered) == 0 {
			return nil
		}
		if len(filtered) == 1 {
			return &filtered[0]
		}
		return &builder.Condition{Type: cond.Type, Conditions: filtered}
	}
	return nil
}

// builderConditionToSQLite converts a builder.Condition tree to sqlite.Condition,
// dropping correlatedSubquery nodes (they can't be represented in the
// sqlite.Condition type used by BuildSelectQuery).
func builderConditionToSQLite(cond *builder.Condition) *sqlite.Condition {
	if cond == nil {
		return nil
	}
	switch cond.Type {
	case "correlatedSubquery":
		return nil
	case "simple":
		sc := &sqlite.Condition{
			Type: "simple",
			Op:   cond.Op,
		}
		if cond.Left != nil {
			sc.Left = sqlite.ValuePos{
				Type:  cond.Left.Type,
				Name:  cond.Left.Name,
				Value: cond.Left.Value,
			}
		}
		if cond.Right != nil {
			sc.Right = sqlite.ValuePos{
				Type:  cond.Right.Type,
				Name:  cond.Right.Name,
				Value: cond.Right.Value,
			}
		}
		return sc
	case "and", "or":
		var filtered []*sqlite.Condition
		for i := range cond.Conditions {
			sub := builderConditionToSQLite(&cond.Conditions[i])
			if sub != nil {
				filtered = append(filtered, sub)
			}
		}
		if len(filtered) == 0 {
			return nil
		}
		if len(filtered) == 1 {
			return filtered[0]
		}
		return &sqlite.Condition{
			Type:       cond.Type,
			Conditions: filtered,
		}
	}
	return nil
}

// --- Plan detail parsing helpers ---

// extractIndexName extracts the index name from a SEARCH detail line.
// e.g., "SEARCH users USING INDEX idx_email (email=?)" → "idx_email"
func extractIndexName(detail string) string {
	idx := strings.Index(detail, "INDEX ")
	if idx == -1 {
		return ""
	}
	rest := detail[idx+6:]
	end := strings.IndexAny(rest, " (")
	if end == -1 {
		return rest
	}
	return rest[:end]
}

// extractIndexDepth counts the number of constrained columns in a SEARCH
// detail line by counting "=?)" occurrences in the parentheses.
// e.g., "SEARCH t USING INDEX idx (a=? AND b=?)" → 2
func extractIndexDepth(detail string) int {
	start := strings.Index(detail, "(")
	end := strings.Index(detail, ")")
	if start == -1 || end == -1 || end <= start {
		return 1
	}
	content := detail[start+1 : end]
	depth := strings.Count(content, "=?")
	if depth == 0 {
		depth = 1
	}
	return depth
}

// extractRowEstimate attempts to extract a row count estimate from the
// "(~N rows)" annotation that some SQLite versions include in EXPLAIN
// QUERY PLAN output.
func extractRowEstimate(detail string) (float64, bool) {
	idx := strings.Index(detail, "(~")
	if idx == -1 {
		return 0, false
	}
	rest := detail[idx+2:]
	end := strings.IndexAny(rest, " )")
	if end == -1 {
		return 0, false
	}
	var rows float64
	if _, err := fmt.Sscanf(rest[:end], "%f", &rows); err == nil && rows > 0 {
		return rows, true
	}
	return 0, false
}

// --- Cache key ---

func (cm *sqliteCostModel) buildCacheKey(
	table string,
	sortOrder ivm.Ordering,
	filters *builder.Condition,
	constraint PlannerConstraint,
) string {
	var sb strings.Builder
	sb.WriteString(table)
	sb.WriteByte('|')

	for _, o := range sortOrder {
		sb.WriteString(o[0])
		sb.WriteByte(':')
		sb.WriteString(o[1])
		sb.WriteByte(',')
	}
	sb.WriteByte('|')

	sb.WriteString(conditionKey(filters))
	sb.WriteByte('|')

	cols := make([]string, 0, len(constraint))
	for col := range constraint {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	sb.WriteString(strings.Join(cols, ","))

	return sb.String()
}

func conditionKey(cond *builder.Condition) string {
	if cond == nil {
		return "nil"
	}
	switch cond.Type {
	case "simple":
		leftName, rightName := "", ""
		if cond.Left != nil {
			leftName = cond.Left.Name
		}
		if cond.Right != nil {
			rightName = cond.Right.Name
		}
		return fmt.Sprintf("simple(%s,%s,%s)", cond.Op, leftName, rightName)
	case "and", "or":
		parts := make([]string, len(cond.Conditions))
		for i := range cond.Conditions {
			parts[i] = conditionKey(&cond.Conditions[i])
		}
		return fmt.Sprintf("%s(%s)", cond.Type, strings.Join(parts, ","))
	case "correlatedSubquery":
		return "csq"
	}
	return "unknown"
}

// quoteIdent wraps an identifier in double quotes for SQL.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
