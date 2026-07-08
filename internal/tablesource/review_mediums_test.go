package tablesource

// Consolidated regression tests for the napi-review mediums + user's-audit
// items that land in this package:
//
//   M3  — filter literals typed by LITERAL JS-type (convertFilter no longer
//         stamps the column's schema type onto literal sides).
//   M4  — poolReader's prepared-stmt cache is bounded (IN-length variance).
//   M9b — compound multiConstraints exercised through Source.Fetch itself
//         (previously only the generated SQL was executed, never the
//         Source.Fetch plumbing that binds and scans it).
//   a4  — FromSQLiteType string/number branches follow TS's fromSQLiteType
//         exactly (no numeric-string parsing; ints in string columns become
//         JS numbers) — covered in sqlite's coercion tests via the exported
//         function; the convertFilter side is here.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// TestConvertFilterLiteralTypedByLiteral (M3): a string literal filtered
// against a json column must reach SQLite as the BARE string, exactly as TS
// binds it (query-builder.ts valuePositionToSQL → toSQLiteType(v,
// getJsType(v))). Pre-fix, convertSimpleCondition stamped ColType="json"
// from the column schema and the sqlite layer marshaled the literal to its
// JSON encoding ('"Payment Failures"') — silently comparing different
// values than TS for the identical AST.
func TestConvertFilterLiteralTypedByLiteral(t *testing.T) {
	src, db := newJSONSource(t)
	defer db.Close()

	cond := &builder.Condition{
		Type: "simple",
		Op:   "=",
		Left: &builder.ValuePos{Type: "column", Name: "payload"},
		Right: &builder.ValuePos{
			Type: "literal", Value: "Payment Failures",
		},
	}
	sc := src.convertFilter(cond)
	if sc == nil {
		t.Fatal("convertFilter returned nil for a simple condition")
	}
	q := sqlite.BuildSelectQuery("docs", src.columns, nil, sc,
		ivm.Ordering{{"id", "asc"}}, false, nil, nil)
	if len(q.Params) != 1 {
		t.Fatalf("params = %v, want exactly the literal", q.Params)
	}
	if got, want := q.Params[0], "Payment Failures"; got != want {
		t.Fatalf("json-column string literal bound as %#v; TS binds %#v (bare string)", got, want)
	}
}

// TestPoolReaderStmtCacheBounded (M4): IN-clause shapes vary by list length
// (`IN (?,?)` vs `IN (?,?,?)`), so a batched flipped-join hydrate mints one
// distinct SQL text per length — each pinning a compiled sqlite3_stmt on
// the C heap for the reader's whole life. Pre-fix the map was unbounded;
// post-fix it holds ≤ stmtCachePerConnCap entries and evicts the coldest
// quarter, with hot shapes surviving. Exercised through the Option B
// checkout/return cycle (checkout removes from the map; RETURN is the
// insert — and thus the eviction trigger).
func TestPoolReaderStmtCacheBounded(t *testing.T) {
	path := newWALFixture(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Construct the reader directly (the pool's converge loop needs a full
	// replica with _zero.replicationState; the cache under test is purely
	// per-reader and identical either way).
	dc, err := rawOpenReaderConn(db)
	if err != nil {
		t.Fatalf("rawOpenReaderConn: %v", err)
	}
	r := &poolReader{dc: dc, stmts: map[string]*poolStmt{}}
	defer r.close(context.Background())

	prepare := func(q string) {
		t.Helper()
		st, err := r.checkoutStmt(context.Background(), q)
		if err != nil {
			t.Fatalf("checkoutStmt(%q): %v", q, err)
		}
		r.returnStmt(q, st, true)
	}

	// A hot shape, re-touched throughout: must survive every eviction.
	hot := `SELECT id FROM t WHERE id = ?`
	prepare(hot)

	// Simulate IN-length variance well past the cap.
	for n := 1; n <= stmtCachePerConnCap+128; n++ {
		q := `SELECT id FROM t WHERE id IN (?`
		for i := 1; i < n%7+1; i++ {
			q += ",?"
		}
		q += fmt.Sprintf(") AND s != 'shape-%d'", n) // force distinct texts
		prepare(q)
		prepare(hot) // keep the hot shape recent
	}

	if got := len(r.stmts); got > stmtCachePerConnCap {
		t.Fatalf("poolReader stmt cache grew to %d entries; cap is %d (unbounded pre-M4)",
			got, stmtCachePerConnCap)
	}
	if _, ok := r.stmts[hot]; !ok {
		t.Fatal("hot (recently-used) shape was evicted; eviction must drop coldest first")
	}
	// Cached stmts must still be usable after evictions.
	st, err := r.checkoutStmt(context.Background(), hot)
	if err != nil {
		t.Fatalf("checkoutStmt(hot) after evictions: %v", err)
	}
	rows, err := queryStmt(context.Background(), st, []any{"nope"})
	if err != nil {
		r.returnStmt(hot, st, false)
		t.Fatalf("hot stmt query: %v", err)
	}
	if err := rows.Next(make([]driver.Value, 1)); !errors.Is(err, io.EOF) {
		rows.Close()
		r.returnStmt(hot, st, false)
		t.Fatalf("hot stmt rows: err = %v, want io.EOF (no rows)", err)
	}
	rows.Close()
	r.returnStmt(hot, st, true)
}

// TestSourceFetchCompoundMultiConstraints (M9b): the compound
// `(a,b) IN (VALUES …)` form was only ever executed as raw SQL in tests;
// this drives it through Source.Fetch itself — Connect → Fetch with
// MultiConstraints — proving the binding, scan, and ordering plumbing.
func TestSourceFetchCompoundMultiConstraints(t *testing.T) {
	path := newWALFixture(t)
	seed, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if _, err := seed.Exec(
		`INSERT INTO t (id, s) VALUES (1,'o1'), (2,'o1'), (3,'o2'), (4,'o2'), (5,'o3')`,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	seed.Close()

	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })
	src, err := New(db, wdb, "t", map[string]sqlite.ColumnSchema{
		"id": {Type: "number"},
		"s":  {Type: "string"},
	}, []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{
		MultiConstraints: []ivm.MultiConstraint{{
			{"id": float64(2), "s": "o1"},
			{"id": float64(3), "s": "o2"},
			{"id": float64(5), "s": "WRONG"}, // tuple compare — must not match
		}},
	}))
	var ids []float64
	for _, n := range nodes {
		ids = append(ids, n.Row["id"].(float64))
	}
	if want := []float64{2, 3}; !slices.Equal(ids, want) {
		t.Fatalf("compound multiConstraints via Source.Fetch = %v, want %v", ids, want)
	}
}
