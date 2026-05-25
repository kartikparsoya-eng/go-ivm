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
			Type: "simple",
			Op:   "=",
			Left: &ValuePos{Type: "column", Name: "name"},
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
