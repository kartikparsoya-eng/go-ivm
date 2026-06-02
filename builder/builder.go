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

	// Collect splitEditKeys from correlations (both where-CSQs and related).
	// Also include this pipeline's partitionKey — when this is built as a
	// child of a parent CSQ/Related, partitionKey contains the
	// correlation's ChildField. Edits to those columns are partition-key
	// changes from the upstream Take's perspective; Take asserts that
	// partition keys don't change (take.go:307, mirroring TS take.ts:439).
	// To honor that invariant, the source must split such edits into
	// Remove(old) + Add(new) before they ever reach Take.
	//
	// Discovered via the sandbox soak (2026-05-25): the new H.1/H.2/H.3
	// mutations (move conv/msg/ticket between parents) edited FK columns
	// that act as partition keys for nested-related templates, panicking
	// the sidecar. Sidecar restart recovered each time, but the dropped
	// changes are correctness-adjacent.
	splitEditKeys := collectSplitEditKeys(ast)
	for _, k := range partitionKey {
		if splitEditKeys == nil {
			splitEditKeys = map[string]bool{}
		}
		splitEditKeys[k] = true
	}

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

		// Any CSQ reaching this loop is one the scalar resolver could NOT
		// rewrite (a non-simple subquery — childField is not all unique-key
		// columns). TS leaves these in place and emits the subquery rows as
		// a normal relationship; do the same here. Propagating Scalar:true
		// to the Join would mark the child schema IsScalar so the streamer
		// drops emission entirely — that suppressed conversation rows that
		// TS produces (H18-cont follow-up: scalar-EXISTS over-emit from
		// 20-user soak template #5).
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

	// Step 4: Apply WHERE filter, Exists operators, and FlippedJoin for any
	// flipped EXISTS conditions. Only invoked when there's at least one CSQ
	// (existsJoins covers both flipped and non-flipped). When there are no
	// CSQs the source fully applies simple filters via FilterPredicate, and
	// building a redundant Filter operator causes Take to malfunction. This
	// matches TS behavior: fullyAppliedFilters=true skips applyWhere.
	if len(existsJoins) > 0 && ast.Where != nil {
		end = applyWhere(end, ast.Where, delegate, p)
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

// stripCSQConditions returns the part of the WHERE tree that can be applied
// at the source level. CSQ branches require the join+Exists machinery built
// later in the pipeline, so they must be deferred to the post-join filter
// chain — but how they're deferred depends on the parent boolean operator:
//
//   - Under AND: dropping a CSQ branch is safe. The remaining simple
//     branches are still necessary conditions; rows that fail them can
//     never satisfy the full AND.
//
//   - Under OR: dropping a CSQ branch is UNSAFE. A row may satisfy the OR
//     solely via the CSQ branch with all simple branches false. Stripping
//     the CSQ would then incorrectly reject that row at the source. So an
//     OR that contains any CSQ (at any depth) is deferred entirely — the
//     post-join filter is responsible for evaluating it.
//
// (Source-level filter being a strict superset of the real predicate is
// the contract: it can drop only rows the full WHERE would also drop.)
func stripCSQConditions(cond *Condition) *Condition {
	if cond == nil {
		return nil
	}
	switch cond.Type {
	case "correlatedSubquery":
		return nil
	case "simple":
		return cond
	case "and":
		var filtered []Condition
		for i := range cond.Conditions {
			stripped := stripCSQConditions(&cond.Conditions[i])
			if stripped != nil {
				filtered = append(filtered, *stripped)
			}
		}
		if len(filtered) == 0 {
			return nil
		}
		if len(filtered) == 1 {
			return &filtered[0]
		}
		return &Condition{
			Type:       "and",
			Conditions: filtered,
		}
	case "or":
		for i := range cond.Conditions {
			if containsCSQ(&cond.Conditions[i]) {
				return nil
			}
		}
		return cond
	}
	return cond
}

// containsCSQ reports whether the condition tree contains any
// correlatedSubquery node at any depth.
func containsCSQ(cond *Condition) bool {
	if cond == nil {
		return false
	}
	switch cond.Type {
	case "correlatedSubquery":
		return true
	case "simple":
		return false
	case "and", "or":
		for i := range cond.Conditions {
			if containsCSQ(&cond.Conditions[i]) {
				return true
			}
		}
	}
	return false
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

// applyFilter walks the WHERE tree and emits a chain of filter operators.
// Port of TS applyFilter at mono/packages/zql/src/builder/builder.ts:484.
// CSQ leaves become Exists operators against the relationship rows produced
// upstream by the EXISTS-join construction; OR branches containing CSQs
// produce a FanOut/FanIn so each branch is evaluated independently.
func applyFilter(input ivm.FilterInput, cond *Condition, p *Pipeline) ivm.FilterInput {
	switch cond.Type {
	case "and":
		return applyAnd(input, cond, p)
	case "or":
		return applyOr(input, cond, p)
	case "correlatedSubquery":
		return applyCorrelatedSubqueryCondition(input, cond, p)
	case "simple":
		return applySimpleFilter(input, cond, p)
	}
	panic(fmt.Sprintf("applyFilter: unknown condition type %q", cond.Type))
}

func applyAnd(input ivm.FilterInput, cond *Condition, p *Pipeline) ivm.FilterInput {
	for i := range cond.Conditions {
		input = applyFilter(input, &cond.Conditions[i], p)
	}
	return input
}

// applyOr partitions the disjunction into subquery-containing branches and
// non-subquery branches. When no branch contains a CSQ a single OR-predicate
// Filter suffices. Otherwise we fork the input via FanOut and let each
// subquery branch flow through its own Exists chain; non-subquery branches
// are merged into one OR-predicate Filter on the fan-out. FanIn unions the
// passing rows (dedup by node identity) before sending downstream.
//
// Port of TS applyOr at mono/packages/zql/src/builder/builder.ts:514.
func applyOr(input ivm.FilterInput, cond *Condition, p *Pipeline) ivm.FilterInput {
	subqueryBranches, simpleBranches := groupSubqueryConditions(cond)

	if len(subqueryBranches) == 0 {
		orCond := &Condition{Type: "or", Conditions: simpleBranches}
		filter := ivm.NewFilter(input, BuildPredicate(orCond))
		p.Edges = append(p.Edges, [2]ivm.InputBase{input, filter})
		return filter
	}

	fanOut := ivm.NewFanOut(input)
	p.Edges = append(p.Edges, [2]ivm.InputBase{input, fanOut})

	branches := make([]ivm.FilterInput, 0, len(subqueryBranches)+1)
	for i := range subqueryBranches {
		branches = append(branches, applyFilter(fanOut, &subqueryBranches[i], p))
	}
	if len(simpleBranches) > 0 {
		orCond := &Condition{Type: "or", Conditions: simpleBranches}
		filter := ivm.NewFilter(fanOut, BuildPredicate(orCond))
		p.Edges = append(p.Edges, [2]ivm.InputBase{fanOut, filter})
		branches = append(branches, filter)
	}

	fanIn := ivm.NewFanIn(fanOut, branches)
	for _, b := range branches {
		p.Edges = append(p.Edges, [2]ivm.InputBase{b, fanIn})
	}
	fanOut.SetFanIn(fanIn)
	return fanIn
}

func applySimpleFilter(input ivm.FilterInput, cond *Condition, p *Pipeline) ivm.FilterInput {
	filter := ivm.NewFilter(input, BuildPredicate(cond))
	p.Edges = append(p.Edges, [2]ivm.InputBase{input, filter})
	return filter
}

// applyCorrelatedSubqueryCondition emits an Exists operator against the
// relationship rows that the EXISTS-join (built earlier in
// buildPipelineInternal) attached to each parent node.
//
// Flipped CSQs must be routed via applyFilterWithFlips, which constructs a
// FlippedJoin (child-side-first inner join) + UnionFanOut/UnionFanIn merge
// for any enclosing OR. Reaching this path with cond.Flip set is a routing
// bug.
func applyCorrelatedSubqueryCondition(input ivm.FilterInput, cond *Condition, p *Pipeline) ivm.FilterInput {
	if cond.Flip {
		panic("applyCorrelatedSubqueryCondition: flipped CSQ should be routed via applyFilterWithFlips")
	}
	if cond.Related == nil {
		return input
	}
	relName := cond.Related.Subquery.Alias
	if relName == "" {
		relName = cond.Related.Subquery.Table
	}
	existsType := ivm.ExistsTypeExists
	if cond.Op == "NOT EXISTS" {
		existsType = ivm.ExistsTypeNotExists
	}
	exists := ivm.NewExists(input, relName, cond.Related.Correlation.ParentField, existsType)
	p.Edges = append(p.Edges, [2]ivm.InputBase{input, exists})
	return exists
}

// applyWhere is the Input→Input bridge that routes a WHERE clause through
// either the standard filter chain or, when any flipped CSQ is present,
// applyFilterWithFlips. Port of TS applyWhere
// (mono/packages/zql/src/builder/builder.ts:371).
func applyWhere(input ivm.Input, cond *Condition, delegate Delegate, p *Pipeline) ivm.Input {
	if !conditionIncludesFlippedSubquery(cond) {
		return wrapInFilter(input, cond, p)
	}
	return applyFilterWithFlips(input, cond, delegate, p)
}

// wrapInFilter applies a (non-flipped) condition through the
// FilterStart → applyFilter → FilterEnd bridge.
func wrapInFilter(input ivm.Input, cond *Condition, p *Pipeline) ivm.Input {
	filterStart := ivm.NewFilterStart(input)
	p.Edges = append(p.Edges, [2]ivm.InputBase{input, filterStart})
	filterEnd := applyFilter(filterStart, cond, p)
	endOp := ivm.NewFilterEnd(filterStart, filterEnd)
	p.Edges = append(p.Edges, [2]ivm.InputBase{filterEnd, endOp})
	return endOp
}

// applyFilterWithFlips handles WHERE clauses containing flipped CSQs.
// AND branches partition into non-flipped (normal filter chain) and flipped
// (recursive FlippedJoin). OR branches partition the same way but converge
// through UnionFanOut/UnionFanIn so per-branch outputs are merged with
// first-occurrence-wins dedup — matching TS's branch-order semantics so a
// branch with no nested relationship beats one with the same row plus an
// added inner relationship (the H18 case).
//
// Port of TS applyFilterWithFlips
// (mono/packages/zql/src/builder/builder.ts:386).
func applyFilterWithFlips(input ivm.Input, cond *Condition, delegate Delegate, p *Pipeline) ivm.Input {
	switch cond.Type {
	case "simple":
		panic("applyFilterWithFlips: simple conditions cannot have flips")
	case "and":
		var withFlipped, withoutFlipped []Condition
		for _, c := range cond.Conditions {
			if conditionIncludesFlippedSubquery(&c) {
				withFlipped = append(withFlipped, c)
			} else {
				withoutFlipped = append(withoutFlipped, c)
			}
		}
		end := input
		if len(withoutFlipped) > 0 {
			end = wrapInFilter(end, &Condition{Type: "and", Conditions: withoutFlipped}, p)
		}
		if len(withFlipped) == 0 {
			panic("applyFilterWithFlips(and): no flipped branches but routed here")
		}
		for i := range withFlipped {
			end = applyFilterWithFlips(end, &withFlipped[i], delegate, p)
		}
		return end
	case "or":
		var withFlipped, withoutFlipped []Condition
		for _, c := range cond.Conditions {
			if conditionIncludesFlippedSubquery(&c) {
				withFlipped = append(withFlipped, c)
			} else {
				withoutFlipped = append(withoutFlipped, c)
			}
		}
		if len(withFlipped) == 0 {
			panic("applyFilterWithFlips(or): no flipped branches but routed here")
		}
		ufo := ivm.NewUnionFanOut(input)
		p.Edges = append(p.Edges, [2]ivm.InputBase{input, ufo})

		var branches []ivm.Input
		if len(withoutFlipped) > 0 {
			branches = append(branches, wrapInFilter(ufo, &Condition{Type: "or", Conditions: withoutFlipped}, p))
		}
		for i := range withFlipped {
			branches = append(branches, applyFilterWithFlips(ufo, &withFlipped[i], delegate, p))
		}
		ufi := ivm.NewUnionFanIn(ufo, branches)
		for _, b := range branches {
			p.Edges = append(p.Edges, [2]ivm.InputBase{b, ufi})
		}
		return ufi
	case "correlatedSubquery":
		if cond.Related == nil {
			return input
		}
		csq := cond.Related
		childAlias := csq.Subquery.Alias
		if childAlias == "" {
			childAlias = csq.Subquery.Table
		}
		childInput := buildPipelineInternal(csq.Subquery, delegate, p, csq.Correlation.ChildField)
		flippedJoin := ivm.NewFlippedJoin(ivm.FlippedJoinArgs{
			Parent:           input,
			Child:            childInput,
			ParentKey:        csq.Correlation.ParentField,
			ChildKey:         csq.Correlation.ChildField,
			RelationshipName: childAlias,
			Hidden:           csq.Hidden,
			System:           csq.System,
		})
		p.Edges = append(p.Edges, [2]ivm.InputBase{input, flippedJoin})
		p.Edges = append(p.Edges, [2]ivm.InputBase{childInput, flippedJoin})
		return flippedJoin
	}
	panic(fmt.Sprintf("applyFilterWithFlips: unknown condition type %q", cond.Type))
}

// conditionIncludesFlippedSubquery reports whether the condition tree
// contains any flipped correlatedSubquery at any depth. Port of TS
// conditionIncludesFlippedSubqueryAtAnyLevel.
func conditionIncludesFlippedSubquery(cond *Condition) bool {
	if cond == nil {
		return false
	}
	switch cond.Type {
	case "correlatedSubquery":
		return cond.Flip
	case "and", "or":
		for i := range cond.Conditions {
			if conditionIncludesFlippedSubquery(&cond.Conditions[i]) {
				return true
			}
		}
	}
	return false
}

// groupSubqueryConditions partitions an OR's children into branches that
// contain a correlatedSubquery anywhere in their subtree and those that
// don't. Mirrors TS groupSubqueryConditions / isNotAndDoesNotContainSubquery
// (mono/packages/zql/src/builder/builder.ts:559).
func groupSubqueryConditions(cond *Condition) (subquery, simple []Condition) {
	for _, c := range cond.Conditions {
		if isNotAndDoesNotContainSubquery(&c) {
			simple = append(simple, c)
		} else {
			subquery = append(subquery, c)
		}
	}
	return
}

func isNotAndDoesNotContainSubquery(cond *Condition) bool {
	switch cond.Type {
	case "correlatedSubquery":
		return false
	case "simple":
		return true
	case "and", "or":
		for i := range cond.Conditions {
			if !isNotAndDoesNotContainSubquery(&cond.Conditions[i]) {
				return false
			}
		}
		return true
	}
	return true
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
