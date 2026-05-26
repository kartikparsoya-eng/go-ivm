package builder

// Port of mono/packages/zqlite/src/resolve-scalar-subqueries.ts.
//
// Some user queries are wrapped by Zero's permission system with a "scalar
// EXISTS" — `whereExists(rel, q, {scalar: true})` — that returns at most one
// row by virtue of an equality constraint on a unique key of the subquery
// table. TS pre-resolves these at hydrate time: the EXISTS is replaced by
// a literal `parentField = value` (or ALWAYS_FALSE if no match), and the
// matched row of the subquery table is emitted as a companion row so the
// client can re-evaluate the EXISTS itself. Without this rewrite, every
// scalar EXISTS would trigger the IVM join machinery for what is logically
// a single-row lookup, ballooning hydrate cost.
//
// This file ports the AST walker. The executor (which actually runs the
// subquery against a source) and the companion-row plumbing live in
// engine.AddQuery — they need access to the Source / Pipeline machinery
// that's not in scope here.

// CompanionSubquery records a resolved scalar subquery whose matched row
// must be emitted alongside the main query's hydration (so the client's
// own EXISTS rewrite has the row it needs).
type CompanionSubquery struct {
	// AST is the (possibly partially-resolved) subquery AST whose unique-
	// key filter selected the matched row.
	AST AST
	// ChildField is the column on the subquery row whose value replaced
	// the EXISTS in the parent's WHERE.
	ChildField string
	// ResolvedValue is the value the executor pulled from ChildField.
	// Nil with Matched=false means "no row found" (→ ALWAYS_FALSE).
	// Nil with Matched=true means "row found but field was NULL" (same
	// effect: parentField = NULL is false in SQL, so → ALWAYS_FALSE).
	ResolvedValue interface{}
	Matched       bool
}

// ResolveResult is the output of ResolveSimpleScalarSubqueries.
type ResolveResult struct {
	AST        AST
	Companions []CompanionSubquery
}

// ScalarExecutor runs `subqueryAST` against the live data and returns the
// value of `childField` on the (at most one) matched row.
//   - matched=false: no row matched
//   - matched=true, value=nil: row matched but field was NULL
//   - matched=true, value=X: row matched, field had value X
//
// The executor is also responsible for any companion-pipeline bookkeeping
// it needs (registering a live input on the subquery table, etc.) — the
// resolver only cares about the value.
type ScalarExecutor func(subqueryAST AST, childField string) (value interface{}, matched bool)

// ResolveSimpleScalarSubqueries walks the AST, finds correlatedSubquery
// conditions with Scalar=true whose subquery is "simple" (all columns of
// at least one unique key are equality-constrained by literals), runs the
// executor to fetch the matched row's value, and replaces the EXISTS
// condition with a literal equality (or ALWAYS_FALSE). Non-simple scalar
// subqueries are left untouched so the existing EXISTS rewrite in
// buildPipelineInternal still handles them.
func ResolveSimpleScalarSubqueries(
	ast AST,
	tableUniqueKeys map[string][][]string,
	execute ScalarExecutor,
) ResolveResult {
	var companions []CompanionSubquery
	resolved := resolveASTRecursive(ast, tableUniqueKeys, execute, &companions)
	return ResolveResult{AST: resolved, Companions: companions}
}

func resolveASTRecursive(
	ast AST,
	tableUniqueKeys map[string][][]string,
	execute ScalarExecutor,
	companions *[]CompanionSubquery,
) AST {
	var newWhere *Condition
	if ast.Where != nil {
		w := resolveCondition(*ast.Where, tableUniqueKeys, execute, companions)
		newWhere = &w
	}

	var newRelated []CorrelatedSubquery
	if len(ast.Related) > 0 {
		newRelated = make([]CorrelatedSubquery, len(ast.Related))
		for i, r := range ast.Related {
			r.Subquery = resolveASTRecursive(r.Subquery, tableUniqueKeys, execute, companions)
			newRelated[i] = r
		}
	} else {
		newRelated = ast.Related
	}

	ast.Where = newWhere
	ast.Related = newRelated
	return ast
}

func resolveCondition(
	condition Condition,
	tableUniqueKeys map[string][][]string,
	execute ScalarExecutor,
	companions *[]CompanionSubquery,
) Condition {
	switch condition.Type {
	case "correlatedSubquery":
		if condition.Scalar {
			return resolveScalarSubquery(condition, tableUniqueKeys, execute, companions)
		}
		// Non-scalar correlated subquery: recurse into its subquery so any
		// nested scalar EXISTS still gets resolved.
		if condition.Related != nil {
			rel := *condition.Related
			rel.Subquery = resolveASTRecursive(rel.Subquery, tableUniqueKeys, execute, companions)
			condition.Related = &rel
		}
		return condition
	case "and", "or":
		newConds := make([]Condition, len(condition.Conditions))
		for i, c := range condition.Conditions {
			newConds[i] = resolveCondition(c, tableUniqueKeys, execute, companions)
		}
		condition.Conditions = newConds
		return condition
	default:
		return condition
	}
}

func resolveScalarSubquery(
	condition Condition,
	tableUniqueKeys map[string][][]string,
	execute ScalarExecutor,
	companions *[]CompanionSubquery,
) Condition {
	if condition.Related == nil ||
		len(condition.Related.Correlation.ParentField) == 0 ||
		len(condition.Related.Correlation.ChildField) == 0 {
		return condition
	}
	parentField := condition.Related.Correlation.ParentField[0]
	childField := condition.Related.Correlation.ChildField[0]

	// Recursively resolve any scalar subqueries nested in the subquery's own
	// WHERE (and related) before evaluating this one — mirrors TS.
	subquery := resolveASTRecursive(
		condition.Related.Subquery,
		tableUniqueKeys,
		execute,
		companions,
	)

	if !IsSimpleSubquery(subquery, tableUniqueKeys) {
		// Carry the (possibly partially-resolved) subquery back through.
		rel := *condition.Related
		rel.Subquery = subquery
		condition.Related = &rel
		return condition
	}

	value, matched := execute(subquery, childField)

	*companions = append(*companions, CompanionSubquery{
		AST:           subquery,
		ChildField:    childField,
		ResolvedValue: value,
		Matched:       matched,
	})

	if !matched || value == nil {
		// No rows OR row matched but field was NULL — both x = NULL and
		// x != NULL are false in SQL, so collapse to ALWAYS_FALSE.
		return alwaysFalseCondition()
	}

	op := "="
	if condition.Op == "NOT EXISTS" {
		op = "IS NOT"
	}
	return Condition{
		Type:  "simple",
		Op:    op,
		Left:  &ValuePos{Type: "column", Name: parentField},
		Right: &ValuePos{Type: "literal", Value: value},
	}
}

// alwaysFalseCondition returns a constant-false simple condition: `1 = 0`.
// Matches the TS ALWAYS_FALSE shape so downstream filter compilation
// produces the same predicate.
func alwaysFalseCondition() Condition {
	return Condition{
		Type:  "simple",
		Op:    "=",
		Left:  &ValuePos{Type: "literal", Value: float64(1)},
		Right: &ValuePos{Type: "literal", Value: float64(0)},
	}
}

// IsSimpleSubquery reports whether `subquery` is guaranteed to return at
// most one deterministic row: its WHERE equality-constrains (via literals
// AND-chained at top level) every column of at least one unique key.
func IsSimpleSubquery(subquery AST, tableUniqueKeys map[string][][]string) bool {
	uniqueKeys, ok := tableUniqueKeys[subquery.Table]
	if !ok || len(uniqueKeys) == 0 {
		return false
	}
	if subquery.Where == nil {
		return false
	}
	constraints := ExtractLiteralEqualityConstraints(*subquery.Where)
	if len(constraints) == 0 {
		return false
	}
	for _, key := range uniqueKeys {
		if len(key) == 0 {
			continue
		}
		allCovered := true
		for _, col := range key {
			if _, ok := constraints[col]; !ok {
				allCovered = false
				break
			}
		}
		if allCovered {
			return true
		}
	}
	return false
}

// ExtractLiteralEqualityConstraints collects column=literal constraints
// from `condition`, descending into AND nodes only. OR and correlated
// subqueries don't contribute — a row may satisfy them via a branch that
// doesn't actually pin the column.
func ExtractLiteralEqualityConstraints(condition Condition) map[string]interface{} {
	constraints := map[string]interface{}{}
	collectConstraints(condition, constraints)
	return constraints
}

func collectConstraints(condition Condition, out map[string]interface{}) {
	switch condition.Type {
	case "simple":
		if condition.Op == "=" &&
			condition.Left != nil && condition.Left.Type == "column" &&
			condition.Right != nil && condition.Right.Type == "literal" {
			out[condition.Left.Name] = condition.Right.Value
		}
	case "and":
		for _, c := range condition.Conditions {
			collectConstraints(c, out)
		}
		// OR / correlatedSubquery / default: contribute nothing.
	}
}
