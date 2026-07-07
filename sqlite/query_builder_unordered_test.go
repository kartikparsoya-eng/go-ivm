package sqlite

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Unordered fetches (order == nil — the Cap/EXISTS-child connect) must emit
// NO ORDER BY, letting SQLite pick any plan and never build a temp b-tree.
// TS: buildSelectQuery only appends orderByToSQL when `order && order.length`
// (zqlite/query-builder.ts:63-66). Pre-fix, the tablesource fetch paths
// force-defaulted a nil connection sort to PK-ASC, so every "unordered"
// fetch still carried ORDER BY.
func TestBuildSelectQuery_NilOrderOmitsOrderBy(t *testing.T) {
	cols := map[string]ColumnSchema{
		"id":      {Type: "string"},
		"issueID": {Type: "string"},
	}
	constraint := ivm.Constraint{"issueID": "i1"}

	q := BuildSelectQuery("comment", cols, &constraint, nil, nil, false, nil, nil)

	if strings.Contains(q.SQL, "ORDER BY") {
		t.Fatalf("nil order must omit ORDER BY, got %q", q.SQL)
	}
	if !strings.Contains(q.SQL, "WHERE") {
		t.Fatalf("constraint missing from SQL: %q", q.SQL)
	}
}

// Reverse without an order is inert — TS's orderByToSQL is the only reverse
// consumer, and it is skipped when order is undefined.
func TestBuildSelectQuery_NilOrderIgnoresReverse(t *testing.T) {
	cols := map[string]ColumnSchema{"id": {Type: "string"}}

	q := BuildSelectQuery("t", cols, nil, nil, nil, true, nil, nil)

	if strings.Contains(q.SQL, "ORDER BY") || strings.Contains(q.SQL, "DESC") {
		t.Fatalf("nil order + reverse must not order, got %q", q.SQL)
	}
}

// TS query-builder.ts:50-53 — assert(order !== undefined, 'start requires
// ordering'): a start cursor on an unordered fetch is a caller bug and must
// fail loud. Pre-fix, Go silently dropped the cursor (start != nil &&
// order != nil gate), returning an UNWINDOWED scan.
func TestBuildSelectQuery_StartWithoutOrderPanics(t *testing.T) {
	cols := map[string]ColumnSchema{"id": {Type: "string"}}
	start := &ivm.Start{Row: ivm.Row{"id": "x"}, Basis: "at"}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic 'start requires ordering'")
		}
		if s, _ := r.(string); s != "start requires ordering" {
			t.Fatalf("panic = %v, want 'start requires ordering'", r)
		}
	}()
	BuildSelectQuery("t", cols, nil, nil, nil, false, start, nil)
}
