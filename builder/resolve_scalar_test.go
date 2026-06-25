package builder

import (
	"reflect"
	"testing"
)

// Mirrors mono/packages/zqlite/src/resolve-scalar-subqueries.test.ts so any
// future TS-side change can be ported by translating the corresponding test
// here.

// --- ExtractLiteralEqualityConstraints ---

func TestExtractLiteralEqualityConstraints_SimpleEquality(t *testing.T) {
	cond := Condition{
		Type:  "simple",
		Op:    "=",
		Left:  &ValuePos{Type: "column", Name: "id"},
		Right: &ValuePos{Type: "literal", Value: "42"},
	}
	got := ExtractLiteralEqualityConstraints(cond)
	want := map[string]interface{}{"id": "42"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestExtractLiteralEqualityConstraints_IgnoresNonEquality(t *testing.T) {
	cond := Condition{
		Type:  "simple",
		Op:    ">",
		Left:  &ValuePos{Type: "column", Name: "id"},
		Right: &ValuePos{Type: "literal", Value: 10},
	}
	got := ExtractLiteralEqualityConstraints(cond)
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestExtractLiteralEqualityConstraints_AndCollects(t *testing.T) {
	cond := Condition{
		Type: "and",
		Conditions: []Condition{
			{
				Type:  "simple",
				Op:    "=",
				Left:  &ValuePos{Type: "column", Name: "a"},
				Right: &ValuePos{Type: "literal", Value: 1},
			},
			{
				Type:  "simple",
				Op:    "=",
				Left:  &ValuePos{Type: "column", Name: "b"},
				Right: &ValuePos{Type: "literal", Value: 2},
			},
		},
	}
	got := ExtractLiteralEqualityConstraints(cond)
	want := map[string]interface{}{"a": 1, "b": 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestExtractLiteralEqualityConstraints_OrDoesNotDescend(t *testing.T) {
	cond := Condition{
		Type: "or",
		Conditions: []Condition{
			{
				Type:  "simple",
				Op:    "=",
				Left:  &ValuePos{Type: "column", Name: "a"},
				Right: &ValuePos{Type: "literal", Value: 1},
			},
		},
	}
	got := ExtractLiteralEqualityConstraints(cond)
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestExtractLiteralEqualityConstraints_IgnoresColumnColumn(t *testing.T) {
	cond := Condition{
		Type:  "simple",
		Op:    "=",
		Left:  &ValuePos{Type: "column", Name: "a"},
		Right: &ValuePos{Type: "column", Name: "b"},
	}
	got := ExtractLiteralEqualityConstraints(cond)
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

// --- IsSimpleSubquery ---

func TestIsSimpleSubquery_UniqueKeyFullyConstrained(t *testing.T) {
	tableUniqueKeys := map[string][][]string{"users": {{"id"}}}
	sub := AST{
		Table: "users",
		Where: &Condition{
			Type:  "simple",
			Op:    "=",
			Left:  &ValuePos{Type: "column", Name: "id"},
			Right: &ValuePos{Type: "literal", Value: "0001"},
		},
	}
	if !IsSimpleSubquery(sub, tableUniqueKeys) {
		t.Fatal("expected simple")
	}
}

func TestIsSimpleSubquery_CompositeUniqueKey(t *testing.T) {
	tableUniqueKeys := map[string][][]string{
		"issueLabel": {{"issueId", "labelId"}},
	}
	sub := AST{
		Table: "issueLabel",
		Where: &Condition{
			Type: "and",
			Conditions: []Condition{
				{
					Type:  "simple",
					Op:    "=",
					Left:  &ValuePos{Type: "column", Name: "issueId"},
					Right: &ValuePos{Type: "literal", Value: "1"},
				},
				{
					Type:  "simple",
					Op:    "=",
					Left:  &ValuePos{Type: "column", Name: "labelId"},
					Right: &ValuePos{Type: "literal", Value: "2"},
				},
			},
		},
	}
	if !IsSimpleSubquery(sub, tableUniqueKeys) {
		t.Fatal("expected simple")
	}
}

func TestIsSimpleSubquery_PartiallyConstrained(t *testing.T) {
	tableUniqueKeys := map[string][][]string{
		"issueLabel": {{"issueId", "labelId"}},
	}
	sub := AST{
		Table: "issueLabel",
		Where: &Condition{
			Type:  "simple",
			Op:    "=",
			Left:  &ValuePos{Type: "column", Name: "issueId"},
			Right: &ValuePos{Type: "literal", Value: "1"},
		},
	}
	if IsSimpleSubquery(sub, tableUniqueKeys) {
		t.Fatal("expected NOT simple")
	}
}

func TestIsSimpleSubquery_NoWhere(t *testing.T) {
	tableUniqueKeys := map[string][][]string{"users": {{"id"}}}
	sub := AST{Table: "users"}
	if IsSimpleSubquery(sub, tableUniqueKeys) {
		t.Fatal("expected NOT simple")
	}
}

func TestIsSimpleSubquery_TableNotInSpecs(t *testing.T) {
	tableUniqueKeys := map[string][][]string{}
	sub := AST{
		Table: "unknown",
		Where: &Condition{
			Type:  "simple",
			Op:    "=",
			Left:  &ValuePos{Type: "column", Name: "id"},
			Right: &ValuePos{Type: "literal", Value: "1"},
		},
	}
	if IsSimpleSubquery(sub, tableUniqueKeys) {
		t.Fatal("expected NOT simple")
	}
}

func TestIsSimpleSubquery_AnyUniqueKeySatisfied(t *testing.T) {
	tableUniqueKeys := map[string][][]string{
		"users": {{"id"}, {"email", "tenant"}},
	}
	sub := AST{
		Table: "users",
		Where: &Condition{
			Type: "and",
			Conditions: []Condition{
				{
					Type:  "simple",
					Op:    "=",
					Left:  &ValuePos{Type: "column", Name: "email"},
					Right: &ValuePos{Type: "literal", Value: "a@b.com"},
				},
				{
					Type:  "simple",
					Op:    "=",
					Left:  &ValuePos{Type: "column", Name: "tenant"},
					Right: &ValuePos{Type: "literal", Value: "t1"},
				},
			},
		},
	}
	if !IsSimpleSubquery(sub, tableUniqueKeys) {
		t.Fatal("expected simple (composite key fully covered)")
	}
}

// --- ResolveSimpleScalarSubqueries ---

func TestResolve_SimpleScalarToLiteralCondition(t *testing.T) {
	tableUniqueKeys := map[string][][]string{"users": {{"id"}}}
	ast := AST{
		Table: "issues",
		Where: &Condition{
			Type:   "correlatedSubquery",
			Op:     "EXISTS",
			Scalar: true,
			Related: &CorrelatedSubquery{
				Correlation: Correlation{
					ParentField: []string{"ownerId"},
					ChildField:  []string{"name"},
				},
				Subquery: AST{
					Table: "users",
					Where: &Condition{
						Type:  "simple",
						Op:    "=",
						Left:  &ValuePos{Type: "column", Name: "id"},
						Right: &ValuePos{Type: "literal", Value: "0001"},
					},
				},
			},
		},
	}

	result := ResolveSimpleScalarSubqueries(
		ast, tableUniqueKeys,
		func(_ AST, _ string) (interface{}, bool) { return "Alice", true },
	)

	if result.AST.Where == nil ||
		result.AST.Where.Type != "simple" ||
		result.AST.Where.Op != "=" ||
		result.AST.Where.Left.Type != "column" ||
		result.AST.Where.Left.Name != "ownerId" ||
		result.AST.Where.Right.Type != "literal" ||
		result.AST.Where.Right.Value != "Alice" {
		t.Fatalf("unexpected resolved where: %+v", result.AST.Where)
	}
	if len(result.Companions) != 1 ||
		result.Companions[0].AST.Table != "users" ||
		result.Companions[0].ChildField != "name" ||
		result.Companions[0].ResolvedValue != "Alice" ||
		!result.Companions[0].Matched {
		t.Fatalf("unexpected companions: %+v", result.Companions)
	}
}

func TestResolve_NotExistsScalarUsesIsNot(t *testing.T) {
	tableUniqueKeys := map[string][][]string{"users": {{"id"}}}
	ast := AST{
		Table: "issues",
		Where: &Condition{
			Type:   "correlatedSubquery",
			Op:     "NOT EXISTS",
			Scalar: true,
			Related: &CorrelatedSubquery{
				Correlation: Correlation{
					ParentField: []string{"ownerId"},
					ChildField:  []string{"name"},
				},
				Subquery: AST{
					Table: "users",
					Where: &Condition{
						Type:  "simple",
						Op:    "=",
						Left:  &ValuePos{Type: "column", Name: "id"},
						Right: &ValuePos{Type: "literal", Value: "0001"},
					},
				},
			},
		},
	}
	result := ResolveSimpleScalarSubqueries(
		ast, tableUniqueKeys,
		func(_ AST, _ string) (interface{}, bool) { return "Alice", true },
	)
	if result.AST.Where == nil || result.AST.Where.Op != "IS NOT" {
		t.Fatalf("expected IS NOT op, got: %+v", result.AST.Where)
	}
}

func TestResolve_NoMatchReturnsAlwaysFalse(t *testing.T) {
	tableUniqueKeys := map[string][][]string{"users": {{"id"}}}
	ast := AST{
		Table: "issues",
		Where: &Condition{
			Type:   "correlatedSubquery",
			Op:     "EXISTS",
			Scalar: true,
			Related: &CorrelatedSubquery{
				Correlation: Correlation{
					ParentField: []string{"ownerId"},
					ChildField:  []string{"name"},
				},
				Subquery: AST{
					Table: "users",
					Where: &Condition{
						Type:  "simple",
						Op:    "=",
						Left:  &ValuePos{Type: "column", Name: "id"},
						Right: &ValuePos{Type: "literal", Value: "ghost"},
					},
				},
			},
		},
	}
	result := ResolveSimpleScalarSubqueries(
		ast, tableUniqueKeys,
		func(_ AST, _ string) (interface{}, bool) { return nil, false },
	)
	w := result.AST.Where
	if w == nil ||
		w.Type != "simple" || w.Op != "=" ||
		w.Left.Type != "literal" || w.Left.Value != float64(1) ||
		w.Right.Type != "literal" || w.Right.Value != float64(0) {
		t.Fatalf("expected ALWAYS_FALSE (1=0), got: %+v", w)
	}
	// Companion must still be recorded — even unmatched, the resolver
	// needs the caller to know which subquery was evaluated so a future
	// row insert can re-trigger the EXISTS check.
	if len(result.Companions) != 1 || result.Companions[0].Matched {
		t.Fatalf("expected one unmatched companion, got: %+v", result.Companions)
	}
}

// TestResolve_MatchedNullReturnsAlwaysFalse proves the matched-but-NULL case
// bakes the SAME ALWAYS_FALSE predicate as the no-match case — the refutation
// of the review's "no-match→matched-NULL regression" claim.
//
// resolve_scalar.go:171 collapses both states via `!matched || value == nil`.
// In SQL `parentField = NULL` is false, exactly like the no-row case, so TS
// collapses both to ALWAYS_FALSE too (resolve-scalar-subqueries.ts:171) and the
// engine's resolvedValue==nil (engine.go:618) makes the two indistinguishable
// downstream. The only surviving difference is the companion's Matched flag,
// which is redundant with value==nil for output purposes.
func TestResolve_MatchedNullReturnsAlwaysFalse(t *testing.T) {
	tableUniqueKeys := map[string][][]string{"users": {{"id"}}}
	mkAST := func() AST {
		return AST{
			Table: "issues",
			Where: &Condition{
				Type:   "correlatedSubquery",
				Op:     "EXISTS",
				Scalar: true,
				Related: &CorrelatedSubquery{
					Correlation: Correlation{
						ParentField: []string{"ownerId"},
						ChildField:  []string{"name"},
					},
					Subquery: AST{
						Table: "users",
						Where: &Condition{
							Type:  "simple",
							Op:    "=",
							Left:  &ValuePos{Type: "column", Name: "id"},
							Right: &ValuePos{Type: "literal", Value: "u1"},
						},
					},
				},
			},
		}
	}

	// matched=true but value=nil (row exists, child field is NULL).
	matchedNull := ResolveSimpleScalarSubqueries(
		mkAST(), tableUniqueKeys,
		func(_ AST, _ string) (interface{}, bool) { return nil, true },
	)
	w := matchedNull.AST.Where
	if w == nil || w.Type != "simple" || w.Op != "=" ||
		w.Left.Type != "literal" || w.Left.Value != float64(1) ||
		w.Right.Type != "literal" || w.Right.Value != float64(0) {
		t.Fatalf("matched-NULL must bake ALWAYS_FALSE (1=0), got: %+v", w)
	}
	// Companion records the match (distinct from no-match) but with nil value.
	if len(matchedNull.Companions) != 1 ||
		!matchedNull.Companions[0].Matched ||
		matchedNull.Companions[0].ResolvedValue != nil {
		t.Fatalf("expected one matched companion with nil value, got: %+v", matchedNull.Companions)
	}

	// The baked predicate must be byte-identical to the no-match case.
	noMatch := ResolveSimpleScalarSubqueries(
		mkAST(), tableUniqueKeys,
		func(_ AST, _ string) (interface{}, bool) { return nil, false },
	)
	if !reflect.DeepEqual(noMatch.AST.Where, matchedNull.AST.Where) {
		t.Fatalf("no-match and matched-NULL must bake identical predicates:\n no-match=%+v\n matched-NULL=%+v",
			noMatch.AST.Where, matchedNull.AST.Where)
	}
}

func TestResolve_NonSimpleScalarLeftAlone(t *testing.T) {
	// Subquery WHERE doesn't fully cover any unique key — resolver must
	// leave the EXISTS in place so the regular join machinery handles it.
	tableUniqueKeys := map[string][][]string{"issueLabel": {{"issueId", "labelId"}}}
	original := AST{
		Table: "issues",
		Where: &Condition{
			Type:   "correlatedSubquery",
			Op:     "EXISTS",
			Scalar: true,
			Related: &CorrelatedSubquery{
				Correlation: Correlation{
					ParentField: []string{"id"},
					ChildField:  []string{"issueId"},
				},
				Subquery: AST{
					Table: "issueLabel",
					Where: &Condition{
						Type:  "simple",
						Op:    "=",
						Left:  &ValuePos{Type: "column", Name: "issueId"},
						Right: &ValuePos{Type: "literal", Value: "1"},
					},
				},
			},
		},
	}
	calls := 0
	result := ResolveSimpleScalarSubqueries(
		original, tableUniqueKeys,
		func(_ AST, _ string) (interface{}, bool) {
			calls++
			return nil, false
		},
	)
	if calls != 0 {
		t.Fatalf("executor must not be called for non-simple subquery, calls=%d", calls)
	}
	if result.AST.Where == nil || result.AST.Where.Type != "correlatedSubquery" {
		t.Fatalf("expected unchanged correlatedSubquery, got: %+v", result.AST.Where)
	}
	if len(result.Companions) != 0 {
		t.Fatalf("expected no companions, got: %+v", result.Companions)
	}
}
