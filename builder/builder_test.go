package builder

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// mockSource implements Source for testing.
type mockSource struct {
	tableName string
}

func (ms *mockSource) Connect(opts ConnectOptions) ivm.Input {
	src := ivm.NewMemorySource(ms.tableName, map[string]string{
		"id":   "string",
		"name": "string",
		"age":  "number",
	}, []string{"id"})
	sort := opts.Sort
	if sort == nil {
		sort = ivm.Ordering{{"id", "asc"}}
	}
	return src.Connect(sort, nil, nil)
}

func (ms *mockSource) PrimaryKey() []string {
	return []string{"id"}
}

// NormalizeRow — no-op for tests; production sources delegate to
// MemorySource/TableSource's NormalizeRow.
func (ms *mockSource) NormalizeRow(_ ivm.Row) {}

// mockDelegate implements Delegate for testing.
type mockDelegate struct {
	sources map[string]*mockSource
}

func (md *mockDelegate) GetSource(tableName string) Source {
	if s, ok := md.sources[tableName]; ok {
		return s
	}
	return nil
}

func (md *mockDelegate) CreateStorage(name string) ivm.TakeStorage {
	return ivm.NewMemoryTakeStorage()
}

func TestBuildSimplePipeline(t *testing.T) {
	delegate := &mockDelegate{
		sources: map[string]*mockSource{
			"users": {tableName: "users"},
		},
	}

	ast := AST{
		Table:   "users",
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	p := BuildPipeline(ast, delegate)
	if p == nil {
		t.Fatal("expected non-nil pipeline")
	}
	if p.Input == nil {
		t.Fatal("expected non-nil input")
	}
	if p.Name != "users" {
		t.Fatalf("expected name 'users', got %q", p.Name)
	}
}

func TestBuildPipelineWithLimit(t *testing.T) {
	delegate := &mockDelegate{
		sources: map[string]*mockSource{
			"users": {tableName: "users"},
		},
	}

	limit := 10
	ast := AST{
		Table:   "users",
		OrderBy: ivm.Ordering{{"id", "asc"}},
		Limit:   &limit,
	}

	p := BuildPipeline(ast, delegate)
	if p == nil {
		t.Fatal("expected non-nil pipeline")
	}
	// Should have at least the source→take edge
	if len(p.Edges) < 1 {
		t.Fatalf("expected at least 1 edge, got %d", len(p.Edges))
	}
}

func TestBuildPipelineWithFilter(t *testing.T) {
	delegate := &mockDelegate{
		sources: map[string]*mockSource{
			"users": {tableName: "users"},
		},
	}

	ast := AST{
		Table:   "users",
		OrderBy: ivm.Ordering{{"id", "asc"}},
		Where: &Condition{
			Type:  "simple",
			Op:    "=",
			Left:  &ValuePos{Type: "column", Name: "name"},
			Right: &ValuePos{Type: "literal", Value: "Alice"},
		},
	}

	p := BuildPipeline(ast, delegate)
	if p == nil {
		t.Fatal("expected non-nil pipeline")
	}
	// With fullyAppliedFilters optimization, simple filters are applied at the source level
	// (via FilterPredicate), so no FilterStart/FilterEnd operators are in the chain.
	// The pipeline is valid with 0 edges when only a source + simple filter exist.
	if p.Input == nil {
		t.Fatal("expected non-nil pipeline input")
	}
}

func TestBuildPipelineWithRelated(t *testing.T) {
	delegate := &mockDelegate{
		sources: map[string]*mockSource{
			"users": {tableName: "users"},
			"posts": {tableName: "posts"},
		},
	}

	ast := AST{
		Table:   "users",
		OrderBy: ivm.Ordering{{"id", "asc"}},
		Related: []CorrelatedSubquery{
			{
				Correlation: Correlation{
					ParentField: []string{"id"},
					ChildField:  []string{"userId"},
				},
				Subquery: AST{
					Table:   "posts",
					Alias:   "posts",
					OrderBy: ivm.Ordering{{"id", "asc"}},
				},
			},
		},
	}

	p := BuildPipeline(ast, delegate)
	if p == nil {
		t.Fatal("expected non-nil pipeline")
	}
	// Should have edges for source + join
	if len(p.Edges) < 2 {
		t.Fatalf("expected at least 2 edges for join, got %d", len(p.Edges))
	}
}

func TestFilterPredicate(t *testing.T) {
	tests := []struct {
		name   string
		cond   *Condition
		row    ivm.Row
		expect bool
	}{
		{
			name: "simple equals match",
			cond: &Condition{
				Type:  "simple",
				Op:    "=",
				Left:  &ValuePos{Type: "column", Name: "name"},
				Right: &ValuePos{Type: "literal", Value: "Alice"},
			},
			row:    ivm.Row{"name": "Alice"},
			expect: true,
		},
		{
			name: "simple equals no match",
			cond: &Condition{
				Type:  "simple",
				Op:    "=",
				Left:  &ValuePos{Type: "column", Name: "name"},
				Right: &ValuePos{Type: "literal", Value: "Alice"},
			},
			row:    ivm.Row{"name": "Bob"},
			expect: false,
		},
		{
			name: "AND both match",
			cond: &Condition{
				Type: "and",
				Conditions: []Condition{
					{Type: "simple", Op: "=", Left: &ValuePos{Type: "column", Name: "name"}, Right: &ValuePos{Type: "literal", Value: "Alice"}},
					{Type: "simple", Op: ">", Left: &ValuePos{Type: "column", Name: "age"}, Right: &ValuePos{Type: "literal", Value: float64(20)}},
				},
			},
			row:    ivm.Row{"name": "Alice", "age": float64(25)},
			expect: true,
		},
		{
			name: "OR one matches",
			cond: &Condition{
				Type: "or",
				Conditions: []Condition{
					{Type: "simple", Op: "=", Left: &ValuePos{Type: "column", Name: "name"}, Right: &ValuePos{Type: "literal", Value: "Alice"}},
					{Type: "simple", Op: "=", Left: &ValuePos{Type: "column", Name: "name"}, Right: &ValuePos{Type: "literal", Value: "Bob"}},
				},
			},
			row:    ivm.Row{"name": "Bob"},
			expect: true,
		},
		{
			name: "LIKE pattern",
			cond: &Condition{
				Type:  "simple",
				Op:    "LIKE",
				Left:  &ValuePos{Type: "column", Name: "name"},
				Right: &ValuePos{Type: "literal", Value: "Al%"},
			},
			row:    ivm.Row{"name": "Alice"},
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred := BuildPredicate(tt.cond)
			got := pred(tt.row)
			if got != tt.expect {
				t.Fatalf("expected %v, got %v", tt.expect, got)
			}
		})
	}
}

// TestCrossTypeOrderedComparison covers operators MED-8: a numeric column
// compared against a numeric-string literal (or vice versa) must coerce and
// order numerically — mirroring the TS path that runs the filter through
// SQLite's implicit cast — instead of panicking in ivm.CompareValues. Pairs
// with the =/!= coercion HIGH-2 already shipped via valuesIdentical.
func TestCrossTypeOrderedComparison(t *testing.T) {
	tests := []struct {
		name   string
		op     string
		left   ivm.Value // column value
		right  ivm.Value // literal value
		expect bool
	}{
		// number column vs numeric-string literal.
		{"num > numstr true", ">", float64(10), "5", true},
		{"num > numstr false", ">", float64(3), "5", false},
		{"num >= numstr equal", ">=", float64(5), "5", true},
		{"num < numstr true", "<", float64(3), "5", true},
		{"num <= numstr equal", "<=", float64(5), "5", true},
		// numeric-string column vs number literal (inverse direction).
		{"numstr < num true", "<", "3", float64(5), true},
		{"numstr > num false", ">", "3", float64(5), false},
		// int64 (PK-shaped) vs numeric-string still coerces.
		{"int64 > numstr", ">", int64(10), "5", true},
		// same-type strings still order lexically (no numeric coercion).
		{"string < string lexical", "<", "apple", "banana", true},
		{"string > string lexical", ">", "apple", "banana", false},
		// same-type numbers unaffected.
		{"num < num", "<", float64(1), float64(2), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := &Condition{
				Type:  "simple",
				Op:    tt.op,
				Left:  &ValuePos{Type: "column", Name: "v"},
				Right: &ValuePos{Type: "literal", Value: tt.right},
			}
			got := BuildPredicate(cond)(ivm.Row{"v": tt.left})
			if got != tt.expect {
				t.Fatalf("%s %v %v = %v, want %v", tt.op, tt.left, tt.right, got, tt.expect)
			}
		})
	}
}

// TestInCoercion covers types MED-8: IN/NOT IN must coerce numeric↔
// numeric-string per element, exactly like =/!= (valuesIdentical), so
// `count IN ('5')` agrees with `count = '5'`.
func TestInCoercion(t *testing.T) {
	tests := []struct {
		name   string
		op     string
		left   ivm.Value
		list   ivm.Value
		expect bool
	}{
		{"num IN numeric-string list match", "IN", float64(5), []interface{}{"5", "6"}, true},
		{"num IN numeric-string list no match", "IN", float64(7), []interface{}{"5", "6"}, false},
		{"int64 IN numeric-string list", "IN", int64(5), []interface{}{"4", "5"}, true},
		{"numeric-string IN number list", "IN", "5", []float64{5, 6}, true},
		{"NOT IN numeric-string excludes", "NOT IN", float64(5), []interface{}{"5", "6"}, false},
		{"NOT IN numeric-string includes", "NOT IN", float64(9), []interface{}{"5", "6"}, true},
		{"string IN string list exact", "IN", "ab", []string{"ab", "cd"}, true},
		{"string IN string list miss", "IN", "zz", []string{"ab", "cd"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := &Condition{
				Type:  "simple",
				Op:    tt.op,
				Left:  &ValuePos{Type: "column", Name: "v"},
				Right: &ValuePos{Type: "literal", Value: tt.list},
			}
			got := BuildPredicate(cond)(ivm.Row{"v": tt.left})
			if got != tt.expect {
				t.Fatalf("%s %v in %v = %v, want %v", tt.op, tt.left, tt.list, got, tt.expect)
			}
		})
	}
}

func TestStripCSQConditions(t *testing.T) {
	cond := &Condition{
		Type: "and",
		Conditions: []Condition{
			{Type: "simple", Op: "=", Left: &ValuePos{Type: "column", Name: "x"}, Right: &ValuePos{Type: "literal", Value: 1}},
			{Type: "correlatedSubquery", Op: "EXISTS"},
		},
	}

	stripped := stripCSQConditions(cond)
	if stripped == nil {
		t.Fatal("expected non-nil")
	}
	// Should have only the simple condition left (unwrapped from AND since only 1 remaining)
	if stripped.Type != "simple" {
		t.Fatalf("expected simple, got %s", stripped.Type)
	}
}
