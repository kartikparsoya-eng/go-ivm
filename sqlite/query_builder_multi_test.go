package sqlite

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// SQL-shape tests for the multiConstraints batched-IN clauses
// (TS zqlite/query-builder.ts multiConstraintToSQL, zero 1.7.0 #5928).

func mcCols() map[string]ColumnSchema {
	return map[string]ColumnSchema{
		"id":  {Type: "string"},
		"org": {Type: "string"},
		"n":   {Type: "number"},
		"b":   {Type: "boolean"},
	}
}

func TestBuildSelectQuery_SingleColumnMultiConstraint(t *testing.T) {
	q := BuildSelectQuery("t", mcCols(), nil, nil, nil, false, nil,
		[]ivm.MultiConstraint{{{"id": "a"}, {"id": "b"}, {"id": "c"}}})

	if !strings.Contains(q.SQL, `"id" IN (?,?,?)`) {
		t.Fatalf("SQL = %q, want single-column IN form", q.SQL)
	}
	if len(q.Params) != 3 || q.Params[0] != "a" || q.Params[1] != "b" || q.Params[2] != "c" {
		t.Fatalf("params = %v, want [a b c]", q.Params)
	}
}

func TestBuildSelectQuery_CompoundMultiConstraint(t *testing.T) {
	// Compound keys: sorted column list (Go determinism deviation, see
	// multiConstraintToSQL), row-value VALUES form, params in key order per
	// entry.
	q := BuildSelectQuery("t", mcCols(), nil, nil, nil, false, nil,
		[]ivm.MultiConstraint{{
			{"org": "o1", "id": "a"},
			{"org": "o2", "id": "b"},
		}})

	if !strings.Contains(q.SQL, `("id", "org") IN (VALUES (?, ?),(?, ?))`) {
		t.Fatalf("SQL = %q, want compound VALUES form with sorted keys", q.SQL)
	}
	want := []interface{}{"a", "o1", "b", "o2"}
	if len(q.Params) != len(want) {
		t.Fatalf("params = %v, want %v", q.Params, want)
	}
	for i := range want {
		if q.Params[i] != want[i] {
			t.Fatalf("params = %v, want %v", q.Params, want)
		}
	}
}

func TestBuildSelectQuery_MultiANDsWithConstraintAndFilters(t *testing.T) {
	constraint := ivm.Constraint{"b": true}
	filters := &Condition{
		Type:  "simple",
		Op:    "=",
		Left:  ValuePos{Type: "column", Name: "org"},
		Right: ValuePos{Type: "literal", Value: "o"},
	}
	q := BuildSelectQuery("t", mcCols(), &constraint, filters,
		ivm.Ordering{{"id", "asc"}}, false, nil,
		[]ivm.MultiConstraint{{{"id": "a"}}, {{"n": float64(5)}}})

	// Both multis present, ANDed between constraint and filters.
	for _, frag := range []string{`"b" = ?`, `"id" IN (?)`, `"n" IN (?)`, `"org" = ?`, " AND "} {
		if !strings.Contains(q.SQL, frag) {
			t.Fatalf("SQL = %q missing %q", q.SQL, frag)
		}
	}
	if len(q.Params) != 4 {
		t.Fatalf("params = %v, want 4 (b, id, n, org)", q.Params)
	}
}

func TestBuildSelectQuery_EmptyMultiConstraintSkipped(t *testing.T) {
	// TS: `if (mc.length > 0)` — an empty entry adds NO clause (it is not
	// an empty IN list, which would be no-match).
	q := BuildSelectQuery("t", mcCols(), nil, nil, nil, false, nil,
		[]ivm.MultiConstraint{{}})
	if strings.Contains(q.SQL, "WHERE") {
		t.Fatalf("SQL = %q, want no WHERE for empty multi", q.SQL)
	}
	q = BuildSelectQuery("t", mcCols(), nil, nil, nil, false, nil, nil)
	if strings.Contains(q.SQL, "WHERE") {
		t.Fatalf("SQL = %q, want no WHERE for nil multis", q.SQL)
	}
}

func TestBuildSelectQuery_HeterogeneousMultiPanicsDataError(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("heterogeneous multi entries must panic (TS asserts)")
		}
		if _, ok := r.(*ivm.DataError); !ok {
			t.Fatalf("panic value = %T, want *ivm.DataError", r)
		}
	}()
	BuildSelectQuery("t", mcCols(), nil, nil, nil, false, nil,
		[]ivm.MultiConstraint{{{"id": "a"}, {"org": "o"}}})
}
