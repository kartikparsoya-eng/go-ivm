package testharness

// Synthetic round-trip test for the snapshot replay harness (#2). Pins that the
// capture format (VACUUM INTO .db + schema + AST) round-trips: a captured .db is
// re-hydrated by RunTestCaseFromSnapshot and the result matches the known-good
// hydrate the in-memory RunTestCase produces from the same rows.
//
// No PII: the .db is built in a temp dir from a hand-rolled items schema (safe
// to commit to the public repo). The real captured bundles from prod are
// never checked in.

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// itemsSchema is the hand-rolled schema shared by the writer, the snapshot
// loader, and the in-memory reference RunTestCase.
var itemsSchema = map[string]TableSchema{
	"items": {
		Columns: map[string]ColumnDef{
			"id":    {Type: "string"},
			"name":  {Type: "string"},
			"price": {Type: "number"},
		},
		PrimaryKey: []string{"id"},
	},
}

// itemsRows is the POST state the captured .db holds.
var itemsRows = []ivm.Row{
	{"id": "1", "name": "Apple", "price": 1.5},
	{"id": "2", "name": "Banana", "price": 0.5},
	{"id": "3", "name": "Cherry", "price": 3.0},
}

// itemsAST is the captured query: main-table scan, ordered by name then id.
var itemsAST = builder.AST{
	Table:   "items",
	OrderBy: ivm.Ordering{{"name", "asc"}, {"id", "asc"}},
}

// TestSnapshotRoundTrip builds a synthetic .db that mimics a #captureDivergence
// VACUUM INTO bundle, re-hydrates it via RunTestCaseFromSnapshot, and asserts
// the result matches the known-good hydrate RunTestCase produces in-memory.
//
// This is the end-to-end pin for #2: it fails if the .db read path, the
// FromSQLiteType normalization, or the main-table comparison regresses.
func TestSnapshotRoundTrip(t *testing.T) {
	// 1. Build a synthetic captured .db in a temp dir.
	snapshotFile := filepath.Join(t.TempDir(), "snapshot.db")
	writeItemsDB(t, snapshotFile, itemsRows)

	// 2. Compute the expected hydrate from the known-good in-memory path.
	//    This is what the offline oracle would diff the re-hydrate against.
	ref := mustRunTestCase(t, itemsSchema, itemsRows, itemsAST)

	// 3. Re-hydrate from the captured .db.
	got, err := RunTestCaseFromSnapshot(SnapshotTestCase{
		Schema:            itemsSchema,
		AST:               itemsAST,
		SnapshotFile:      snapshotFile,
		ExpectedHydration: ref.Hydration,
	})
	if err != nil {
		t.Fatalf("RunTestCaseFromSnapshot: %v", err)
	}

	// 4. The loader's re-hydrate must match the in-memory reference.
	if !got.Match {
		t.Fatalf("round-trip mismatch: %s\nhydration=%v", got.Diff, got.Hydration)
	}
	if len(got.Hydration) != len(itemsRows) {
		t.Fatalf("row count: got %d want %d", len(got.Hydration), len(itemsRows))
	}
}

// TestSnapshotRoundTripFilter repeats the round trip with a WHERE clause, so
// the captured query isn't a trivial full scan — exercises that the loader's
// AST-driven hydrate honors the filter against rows read from the .db.
func TestSnapshotRoundTripFilter(t *testing.T) {
	ast := builder.AST{
		Table:   "items",
		OrderBy: ivm.Ordering{{"name", "asc"}, {"id", "asc"}},
		Where: &builder.Condition{
			Type:  "simple",
			Op:    ">",
			Left:  &builder.ValuePos{Type: "column", Name: "price"},
			Right: &builder.ValuePos{Type: "literal", Value: 1.0},
		},
	}

	snapshotFile := filepath.Join(t.TempDir(), "snapshot.db")
	writeItemsDB(t, snapshotFile, itemsRows)

	ref := mustRunTestCase(t, itemsSchema, itemsRows, ast)

	got, err := RunTestCaseFromSnapshot(SnapshotTestCase{
		Schema:            itemsSchema,
		AST:               ast,
		SnapshotFile:      snapshotFile,
		ExpectedHydration: ref.Hydration,
	})
	if err != nil {
		t.Fatalf("RunTestCaseFromSnapshot: %v", err)
	}
	if !got.Match {
		t.Fatalf("round-trip mismatch (filter): %s\nhydration=%v", got.Diff, got.Hydration)
	}
	// price > 1.0 ⇒ Apple (1.5) + Cherry (3.0); Banana (0.5) excluded.
	if len(got.Hydration) != 2 {
		t.Fatalf("filtered row count: got %d want 2", len(got.Hydration))
	}
}

// TestSnapshotDivergenceDetected asserts the comparison is not vacuous: a
// mismatched ExpectedHydration (different row count) must report Match=false
// with a non-empty Diff. This pins that the loader actually compares, not just
// echoes the input.
func TestSnapshotDivergenceDetected(t *testing.T) {
	snapshotFile := filepath.Join(t.TempDir(), "snapshot.db")
	writeItemsDB(t, snapshotFile, itemsRows)

	// Expected = only one row; the .db has three → must not match.
	wrongExpected := []*CaughtNode{
		{Row: ivm.Row{"id": "1", "name": "Apple", "price": 1.5}, Relationships: map[string][]*CaughtNode{}},
	}

	got, err := RunTestCaseFromSnapshot(SnapshotTestCase{
		Schema:            itemsSchema,
		AST:               itemsAST,
		SnapshotFile:      snapshotFile,
		ExpectedHydration: wrongExpected,
	})
	if err != nil {
		t.Fatalf("RunTestCaseFromSnapshot: %v", err)
	}
	if got.Match {
		t.Fatal("expected Match=false for a mismatched expected hydration, got true")
	}
	if got.Diff == "" {
		t.Fatal("expected a non-empty Diff for a mismatched expected hydration")
	}
}

// TestSnapshotFromJSON drives the JSON entry point with a serialized case,
// pinning the wire shape the #captureDivergence .json metadata would carry.
func TestSnapshotFromJSON(t *testing.T) {
	snapshotFile := filepath.Join(t.TempDir(), "snapshot.db")
	writeItemsDB(t, snapshotFile, itemsRows)

	ref := mustRunTestCase(t, itemsSchema, itemsRows, itemsAST)

	// Marshal the case to JSON (the format an offline replay tool would read),
	// then run it through the JSON entry point.
	tc := SnapshotTestCase{
		Schema:            itemsSchema,
		AST:               itemsAST,
		SnapshotFile:      snapshotFile,
		ExpectedHydration: ref.Hydration,
	}
	// Round-trip through JSON to mimic a capture file on disk.
	payload, err := json.Marshal(tc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	out, err := RunTestCaseFromSnapshotJSON(payload)
	if err != nil {
		t.Fatalf("RunTestCaseFromSnapshotJSON: %v", err)
	}

	var res SnapshotResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("unmarshal result: %v\nraw: %s", err, string(out))
	}
	if !res.Match {
		t.Fatalf("JSON round-trip mismatch: %s", res.Diff)
	}
}

// --- helpers ---

// writeItemsDB creates a standalone .db (no WAL — mirrors a VACUUM INTO copy)
// and inserts the given rows into an `items` table matching itemsSchema.
func writeItemsDB(t *testing.T, path string, rows []ivm.Row) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open writer db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(
		`CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, price REAL)`,
	); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO items (id, name, price) VALUES (?, ?, ?)`,
			r["id"], r["name"], r["price"],
		); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
}

// mustRunTestCase runs the in-memory RunTestCase on the same schema/rows/AST and
// returns its result — the known-good reference hydrate.
func mustRunTestCase(t *testing.T, schema map[string]TableSchema, rows []ivm.Row, ast builder.AST) *HarnessResult {
	t.Helper()
	res, err := RunTestCase(TestCase{
		Schema:      schema,
		InitialData: map[string][]ivm.Row{"items": rows},
		AST:         ast,
	})
	if err != nil {
		t.Fatalf("reference RunTestCase: %v", err)
	}
	return res
}
