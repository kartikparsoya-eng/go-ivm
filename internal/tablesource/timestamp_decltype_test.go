package tablesource

// Regression for the ART G15 true positive (2026-07-07): any value in a
// NULLABLE timestamp/datetime/date column with |epoch-ms| <= 1e12 (i.e.
// earlier than 2001-09-09T01:46:40Z, negatives included) hydrated from Go
// as value*1000; TS ships the raw integer.
//
// Chain (all verified against source):
//  1. the replica stores epoch-ms INTEGER (e.g. 1 = 1970-01-01T00:00:00.001Z);
//  2. mattn/go-sqlite3 converts INTEGER result columns whose declared type
//     is exactly "timestamp"/"datetime"/"date" into time.Time — only
//     NULLABLE ones (NOT-null columns are declared "timestamp|NOT_NULL",
//     which dodges the exact-string decltype match);
//  3. mattn's magnitude heuristic (sqlite3.go:2583-2588 @ v1.14.44):
//     |v| <= 1e12 ⇒ SECONDS (time.Unix(v, 0)), else ms;
//  4. FromSQLiteType reversed with t.UnixMilli() ⇒ v*1000 for the whole
//     seconds window. A smarter reversal is provably impossible — stored
//     2e9 (seconds path) and 2e12 (ms path) produce the identical
//     time.Time.
//
// Fix: BuildSelectQuery wraps every result column in SQLite's unary `+`
// no-op (`+"col" AS "col"`) — expressions carry no declared type, so the
// driver conversion never fires and the raw cell ships, byte-identical to
// TS's better-sqlite3 read. This test drives the REAL fetch path (Source →
// BuildSelectQuery → mattn scan → FromSQLiteType) against a replica-shaped
// table and pins the raw values for the full edge matrix; it fails pre-fix
// with 1000/-1000/1e15/-1e15 for the in-window cases.
//
// Runs under BOTH tag sets (bundled 3.53 + wal2 3.51): unary + is core SQL
// semantics, and the decltype-stripping behavior must hold on the
// production library too.

import (
	"database/sql"
	"path/filepath"
	"slices"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// seedTemporalReplica mirrors the replica's DDL shape for temporal columns:
// nullable ones declared with the BARE type ("timestamp"/"datetime"/"date"),
// NOT-null ones with the "|NOT_NULL"-suffixed type (quoted — SQLite accepts
// any quoted identifier as a type name, which is exactly how zero-cache's
// lite schema writes it).
func seedTemporalReplica(t *testing.T, tsValues []any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE "events" (
			"id" "varchar|NOT_NULL" PRIMARY KEY,
			"ts" timestamp,
			"dt" datetime,
			"d" date,
			"tsnn" "timestamp|NOT_NULL",
			"_0_version" "varchar|NOT_NULL"
		)`,
	}
	for _, s := range stmts {
		if _, err := w.Exec(s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	for i, v := range tsValues {
		// Same value in every temporal column; tsnn gets a non-NULL stand-in
		// when v is NULL (the column is conceptually NOT NULL).
		nn := v
		if nn == nil {
			nn = int64(0)
		}
		if _, err := w.Exec(
			`INSERT INTO "events" VALUES (?,?,?,?,?,?)`,
			string(rune('a'+i)), v, v, v, nn, "v1",
		); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	return path
}

func temporalSchema() map[string]sqlite.ColumnSchema {
	return map[string]sqlite.ColumnSchema{
		"id":         {Type: "string"},
		"ts":         {Type: "number", Optional: true},
		"dt":         {Type: "number", Optional: true},
		"d":          {Type: "number", Optional: true},
		"tsnn":       {Type: "number"},
		"_0_version": {Type: "string"},
	}
}

func TestFetchShipsRawEpochMsForNullableTemporalColumns(t *testing.T) {
	// The edge matrix. want == raw stored value for EVERY case — TS's
	// better-sqlite3 has no decltype conversion, so raw is the reference.
	// preFix documents what the UnixMilli reversal used to ship.
	cases := []struct {
		name   string
		stored any     // INTEGER cell (or nil)
		want   ivm.Value // shipped value ("number" logical type → float64)
	}{
		{"epoch-ms 1 (1970-01-01T00:00:00.001Z; pre-fix 1000)", int64(1), float64(1)},
		{"negative -1 (pre-fix -1000)", int64(-1), float64(-1)},
		{"epoch 0 (in-window fixpoint 0*1000=0 — why G12's old list missed the bug)", int64(0), float64(0)},
		{"1e12 boundary (mattn: > 1e12 is ms, so 1e12 EXACTLY is seconds; pre-fix 1e15)", int64(1_000_000_000_000), float64(1_000_000_000_000)},
		{"-1e12 boundary (pre-fix -1e15)", int64(-1_000_000_000_000), float64(-1_000_000_000_000)},
		{"1e12+1 first ms-path value (UnixMilli round-trips; unaffected pre-fix)", int64(1_000_000_000_001), float64(1_000_000_000_001)},
		{"realistic post-2001 (unaffected pre-fix)", int64(1751900000000), float64(1751900000000)},
		{"NULL stays NULL", nil, nil},
	}

	stored := make([]any, len(cases))
	for i, c := range cases {
		stored[i] = c.stored
	}
	path := seedTemporalReplica(t, stored)

	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })
	src, err := New(db, wdb, "events", temporalSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close()

	conn := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	nodes := slices.Collect(conn.Fetch(ivm.FetchRequest{}))
	if len(nodes) != len(cases) {
		t.Fatalf("fetched %d rows, want %d", len(nodes), len(cases))
	}

	for i, c := range cases {
		row := nodes[i].Row
		// Nullable temporal columns — all three decltypes mattn matches.
		for _, col := range []string{"ts", "dt", "d"} {
			if got := row[col]; got != c.want {
				t.Errorf("%s: row[%q] = %#v (%T), want raw %#v — nullable temporal column diverged from the stored cell",
					c.name, col, got, got, c.want)
			}
		}
		// NOT-null column: raw pre- AND post-fix ("timestamp|NOT_NULL"
		// decltype never matched mattn). Pin no regression.
		wantNN := c.want
		if wantNN == nil {
			wantNN = float64(0)
		}
		if got := row["tsnn"]; got != wantNN {
			t.Errorf("%s: row[\"tsnn\"] = %#v, want %#v — NOT_NULL temporal column regressed", c.name, got, wantNN)
		}
	}
}
