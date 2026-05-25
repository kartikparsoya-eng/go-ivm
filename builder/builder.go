package builder

// Builds an IVM operator tree from a query AST. The shape is always:
//   Source → Skip? → Join(exists)* → Filter? → Take? → Join(related)*
// Called by the sidecar for every addQuery / addQueries / addQueriesStream RPC.

import (
	"fmt"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

const existsLimit = 3

// Source is the interface for a table data source.
// In production this is *sqlite.TableSource.
type Source interface {
	Connect(opts ConnectOptions) ivm.Input
	PrimaryKey() []string
	// NormalizeRow coerces row values to the column types this source
	// expects. Used by the builder to normalize literal cursor rows
	// (ast.Start.Row) so they compare correctly against normalized source
	// rows during Skip — REVIEW-final HIGH-1 (Skip cursor not normalized).
	NormalizeRow(row ivm.Row)
}

// ConnectOptions are passed to Source.Connect when building a pipeline.
type ConnectOptions struct {
	Sort            ivm.Ordering
	Filter          *Condition
	SplitEditKeys   map[string]bool
	FilterPredicate Predicate
}

// Delegate provides dependencies for pipeline construction.
type Delegate interface {
	// GetSource returns the Source for a table name.
	GetSource(tableName string) Source

	// CreateStorage creates a Storage for an operator (used by Take).
	CreateStorage(name string) ivm.TakeStorage
}

// Pipeline is the result of building a query — the terminal Input of the operator chain.
type Pipeline struct {
	Input ivm.Input
	Name  string
	Edges [][2]ivm.InputBase // (from, to) edges for graph tracking
}

// BuildPipeline constructs an operator tree from an AST.
func BuildPipeline(ast AST, delegate Delegate) *Pipeline {
	p := &Pipeline{Name: ast.Table}
	input := buildPipelineInternal(ast, delegate, p, nil)
	p.Input = input
	return p
}

// buildPipelineInternal recursively constructs operator chain.
func buildPipelineInternal(ast AST, delegate Delegate, p *Pipeline, partitionKey []string) ivm.Input {
	name := ast.Table
	if ast.Alias != "" {
		name = ast.Alias
	}

	// Complete ordering: append PK columns as tiebreakers (matches TS completeOrdering).
	ast = completeOrdering(ast, delegate)

	// Uniquify CSQ condition aliases to avoid conflicts with Related aliases
	ast = uniquifyCSQConditionAliases(ast)

	// Collect splitEditKeys from correlations (both where-CSQs and related)
	splitEditKeys := collectSplitEditKeys(ast)

	// Step 1: Connect to source
	source := delegate.GetSource(ast.Table)
	if source == nil {
		panic(fmt.Sprintf("no source for table %q", ast.Table))
	}

	// Build filter condition (strip correlatedSubquery conditions for source-level filter)
	simpleFilter := stripCSQConditions(ast.Where)

	sourceInput := source.Connect(ConnectOptions{
		Sort:            ast.OrderBy,
		Filter:          simpleFilter,
		SplitEditKeys:   splitEditKeys,
		FilterPredicate: BuildPredicate(simpleFilter),
	})

	var end ivm.Input = sourceInput

	// Step 2: Skip (cursor-based pagination)
	if ast.Start != nil {
		// Normalize the cursor row against the source's column schema so
		// CompareValues sees matching types between the cursor and stored
		// rows (REVIEW-final HIGH-1 / porting HIGH-1). Without this, an
		// msgpack-decoded int cursor compared against a float64 source row
		// would mismatch.
		startRow := make(ivm.Row, len(ast.Start.Row))
		for k, v := range ast.Start.Row {
			startRow[k] = v
		}
		source.NormalizeRow(startRow)
		skip := ivm.NewSkip(end, ivm.Bound{
			Row:       startRow,
			Exclusive: ast.Start.Exclusive,
		})
		p.Edges = append(p.Edges, [2]ivm.InputBase{end, skip})
		end = skip
	}

	// Step 3: Build joins for EXISTS conditions in WHERE
	existsJoins := gatherCSQConditions(ast.Where)
	for _, csqCond := range existsJoins {
		if csqCond.Flip {
			continue // Flipped joins handled in applyWhere
		}
		csq := csqCond.Related
		if csq == nil {
			continue
		}

		// Limit subquery for exists check
		existLim := existsLimit
		childAST := csq.Subquery
		childAST.Limit = &existLim

		childAlias := childAST.Alias
		if childAlias == "" {
			childAlias = childAST.Table
		}

		// Recursively build child pipeline
		childInput := buildPipelineInternal(childAST, delegate, p, csq.Correlation.ChildField)

		// Create Join
		join := ivm.NewJoin(ivm.JoinArgs{
			Parent:           end,
			Child:            childInput,
			ParentKey:        csq.Correlation.ParentField,
			ChildKey:         csq.Correlation.ChildField,
			RelationshipName: childAlias,
			Hidden:           csq.Hidden,
			System:           csq.System,
		})
		p.Edges = append(p.Edges, [2]ivm.InputBase{end, join})
		p.Edges = append(p.Edges, [2]ivm.InputBase{childInput, join})
		end = join
	}

	// Step 4: Apply WHERE filter + Exists operators
	// The filter chain requires FilterStart → filters → FilterEnd to bridge Input→FilterInput→Input
	// Note: Only build the filter operator chain when there are EXISTS joins (CSQ conditions).
	// When there are no CSQs, the source fully applies simple filters via FilterPredicate,
	// and building a redundant Filter operator causes Take to malfunction.
	// This matches TS behavior: fullyAppliedFilters=true skips applyWhere.
	if len(existsJoins) > 0 {
		filterStart := ivm.NewFilterStart(end)
		p.Edges = append(p.Edges, [2]ivm.InputBase{end, filterStart})

		var filterEnd ivm.FilterInput = filterStart

		// Apply simple filter predicate
		if hasSimpleConditions(ast.Where) {
			pred := BuildPredicate(stripCSQConditions(ast.Where))
			filter := ivm.NewFilter(filterEnd, pred)
			p.Edges = append(p.Edges, [2]ivm.InputBase{filterEnd, filter})
			filterEnd = filter
		}

		// Apply Exists operators for non-flipped CSQs
		for _, csqCond := range existsJoins {
			if csqCond.Flip || csqCond.Related == nil {
				continue
			}
			relName := csqCond.Related.Subquery.Alias
			if relName == "" {
				relName = csqCond.Related.Subquery.Table
			}
			existsType := ivm.ExistsTypeExists
			if csqCond.Op == "NOT EXISTS" {
				existsType = ivm.ExistsTypeNotExists
			}
			exists := ivm.NewExists(
				filterEnd,
				relName,
				csqCond.Related.Correlation.ParentField,
				existsType,
			)
			p.Edges = append(p.Edges, [2]ivm.InputBase{filterEnd, exists})
			filterEnd = exists
		}

		// Close the filter chain
		endOp := ivm.NewFilterEnd(filterStart, filterEnd)
		p.Edges = append(p.Edges, [2]ivm.InputBase{filterEnd, endOp})
		end = endOp
	}

	// Step 5: Take (LIMIT)
	if ast.Limit != nil {
		_ = fmt.Sprintf("%s:take", name)
		storage := delegate.CreateStorage(fmt.Sprintf("%s:take", name))
		take := ivm.NewTake(end, storage, *ast.Limit, partitionKey)
		p.Edges = append(p.Edges, [2]ivm.InputBase{end, take})
		end = take
	}

	// Step 6: Build joins for related subqueries
	if len(ast.Related) > 0 {
		// Deduplicate by alias (last wins, like TS)
		type entry struct {
			alias string
			idx   int
		}
		var deduped []entry
		seen := map[string]int{}
		for i, csq := range ast.Related {
			alias := csq.Subquery.Alias
			if alias == "" {
				alias = csq.Subquery.Table
			}
			if prev, ok := seen[alias]; ok {
				// Replace previous
				deduped[prev].idx = i
			} else {
				seen[alias] = len(deduped)
				deduped = append(deduped, entry{alias, i})
			}
		}

		for _, e := range deduped {
			csq := ast.Related[e.idx]
			childInput := buildPipelineInternal(csq.Subquery, delegate, p, csq.Correlation.ChildField)

			join := ivm.NewJoin(ivm.JoinArgs{
				Parent:           end,
				Child:            childInput,
				ParentKey:        csq.Correlation.ParentField,
				ChildKey:         csq.Correlation.ChildField,
				RelationshipName: e.alias,
				Hidden:           csq.Hidden,
				System:           csq.System,
			})
			p.Edges = append(p.Edges, [2]ivm.InputBase{end, join})
			p.Edges = append(p.Edges, [2]ivm.InputBase{childInput, join})
			end = join
		}
	}

	return end
}

// --- Helpers ---

// collectSplitEditKeys gathers all correlation parent/child fields from the AST.
func collectSplitEditKeys(ast AST) map[string]bool {
	keys := map[string]bool{}
	if ast.Where != nil {
		for _, csq := range gatherCSQConditions(ast.Where) {
			if csq.Related != nil {
				for _, f := range csq.Related.Correlation.ParentField {
					keys[f] = true
				}
			}
		}
	}
	for _, csq := range ast.Related {
		for _, f := range csq.Correlation.ParentField {
			keys[f] = true
		}
	}
	if len(keys) == 0 {
		return nil
	}
	return keys
}

// gatherCSQConditions recursively collects all correlatedSubquery conditions.
func gatherCSQConditions(cond *Condition) []Condition {
	if cond == nil {
		return nil
	}
	var result []Condition
	switch cond.Type {
	case "correlatedSubquery":
		result = append(result, *cond)
	case "and", "or":
		for i := range cond.Conditions {
			result = append(result, gatherCSQConditions(&cond.Conditions[i])...)
		}
	}
	return result
}

// stripCSQConditions removes correlatedSubquery nodes from the condition tree.
func stripCSQConditions(cond *Condition) *Condition {
	if cond == nil {
		return nil
	}
	switch cond.Type {
	case "correlatedSubquery":
		return nil
	case "simple":
		return cond
	case "and", "or":
		var filtered []Condition
		for i := range cond.Conditions {
			stripped := stripCSQConditions(&cond.Conditions[i])
			if stripped != nil {
				filtered = append(filtered, *stripped)
			}
		}
		if len(filtered) == 0 {
			// Empty AND = TRUE (no-op), can be stripped.
			// Empty OR = FALSE (deny all), must be preserved.
			if cond.Type == "or" {
				return &Condition{
					Type:       "or",
					Conditions: []Condition{},
				}
			}
			return nil
		}
		if len(filtered) == 1 {
			return &filtered[0]
		}
		return &Condition{
			Type:       cond.Type,
			Conditions: filtered,
		}
	}
	return cond
}

// uniquifyCSQConditionAliases renames CSQ condition aliases to avoid collisions.
func uniquifyCSQConditionAliases(ast AST) AST {
	if ast.Where == nil {
		return ast
	}
	if ast.Where.Type != "and" && ast.Where.Type != "or" {
		return ast
	}
	count := 0
	var uniquify func(cond Condition) Condition
	uniquify = func(cond Condition) Condition {
		switch cond.Type {
		case "simple":
			return cond
		case "correlatedSubquery":
			if cond.Related != nil {
				newRelated := *cond.Related
				newRelated.Subquery.Alias = fmt.Sprintf("%s_%d", newRelated.Subquery.Alias, count)
				count++
				cond.Related = &newRelated
			}
			return cond
		case "and", "or":
			newConds := make([]Condition, len(cond.Conditions))
			for i, c := range cond.Conditions {
				newConds[i] = uniquify(c)
			}
			cond.Conditions = newConds
			return cond
		}
		return cond
	}
	newWhere := uniquify(*ast.Where)
	ast.Where = &newWhere
	return ast
}

// hasSimpleConditions returns true if there are any meaningful conditions in the tree.
// An empty OR (conditions=[]) is meaningful — it evaluates to FALSE (rejects all rows).
// An empty AND (conditions=[]) evaluates to TRUE (no-op), so it's not meaningful.
func hasSimpleConditions(cond *Condition) bool {
	if cond == nil {
		return false
	}
	switch cond.Type {
	case "simple":
		return true
	case "or":
		// Empty OR = FALSE, which is a meaningful filter that rejects all rows
		if len(cond.Conditions) == 0 {
			return true
		}
		for i := range cond.Conditions {
			if hasSimpleConditions(&cond.Conditions[i]) {
				return true
			}
		}
	case "and":
		for i := range cond.Conditions {
			if hasSimpleConditions(&cond.Conditions[i]) {
				return true
			}
		}
	}
	return false
}

// completeOrdering appends primary key columns to orderBy for deterministic tie-breaking.
// Port of mono/packages/zql/src/query/complete-ordering.ts
func completeOrdering(ast AST, delegate Delegate) AST {
	source := delegate.GetSource(ast.Table)
	if source == nil {
		return ast
	}
	pk := source.PrimaryKey()
	ast.OrderBy = addPrimaryKeys(pk, ast.OrderBy)

	// Recurse into related subqueries
	if ast.Related != nil {
		newRelated := make([]CorrelatedSubquery, len(ast.Related))
		copy(newRelated, ast.Related)
		for i := range newRelated {
			newRelated[i].Subquery = completeOrdering(newRelated[i].Subquery, delegate)
		}
		ast.Related = newRelated
	}

	// Recurse into CSQ conditions in where
	if ast.Where != nil {
		ast.Where = completeOrderingInCondition(ast.Where, delegate)
	}

	return ast
}

func completeOrderingInCondition(cond *Condition, delegate Delegate) *Condition {
	if cond == nil {
		return nil
	}
	switch cond.Type {
	case "simple":
		return cond
	case "correlatedSubquery":
		if cond.Related == nil {
			return cond
		}
		newCond := *cond
		newRelated := *cond.Related
		newRelated.Subquery = completeOrdering(newRelated.Subquery, delegate)
		newCond.Related = &newRelated
		return &newCond
	case "and", "or":
		newCond := *cond
		newConditions := make([]Condition, len(cond.Conditions))
		for i := range cond.Conditions {
			result := completeOrderingInCondition(&cond.Conditions[i], delegate)
			newConditions[i] = *result
		}
		newCond.Conditions = newConditions
		return &newCond
	}
	return cond
}

func addPrimaryKeys(pk []string, orderBy ivm.Ordering) ivm.Ordering {
	if orderBy == nil {
		orderBy = ivm.Ordering{}
	}
	// Find PK columns not already in orderBy
	existing := make(map[string]bool, len(orderBy))
	for _, entry := range orderBy {
		existing[entry[0]] = true
	}
	var toAdd []string
	for _, col := range pk {
		if !existing[col] {
			toAdd = append(toAdd, col)
		}
	}
	if len(toAdd) == 0 {
		return orderBy
	}
	result := make(ivm.Ordering, len(orderBy), len(orderBy)+len(toAdd))
	copy(result, orderBy)
	for _, col := range toAdd {
		result = append(result, [2]string{col, "asc"})
	}
	return result
}
