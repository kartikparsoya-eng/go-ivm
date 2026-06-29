package testharness

// Snapshot-based replay harness (#2). Counterpart to RunTestCase for divergences
// captured by the shadow comparator's #captureDivergence hook (see
// pipeline-driver.ts #captureDivergence): a captured bundle is a VACUUM INTO
// copy of the SQLite replica (.db) plus a .json metadata file {ast, queryID,
// operation, tsChanges, goChanges, sqlVerdict, stateVersion}.
//
// This loader opens the captured .db, reads the AST's MAIN table into a
// MemorySource (reusing the same FromSQLiteType normalization as production),
// builds the pipeline from the captured AST, and hydrates — producing a
// HarnessResult the caller compares to the captured goChanges (the offline
// oracle, run separately, substitutes for the TS side since mono is corporate
// / non-portable). The existing InitialData-based RunTestCase is untouched.
//
// NEVER push a real prod capture to the public go-ivm repo (PII). The
// accompanying test builds a synthetic .db in a temp dir — no PII, safe to
// commit.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// SnapshotTestCase is the captured-replay variant of TestCase. Instead of
// InitialData + Mutations it carries a path to a captured .db snapshot
// (VACUUM INTO copy) plus the Go changes captured at divergence time. The
// loader re-hydrates from the .db and the caller compares to ExpectedHydration
// (the captured goChanges) to reproduce the divergence offline.
type SnapshotTestCase struct {
	Schema       map[string]TableSchema `json:"schema"`
	AST          builder.AST            `json:"ast"`
	SnapshotFile string                 `json:"snapshotFile"`
	// ExpectedHydration is the captured Go side's hydrate output (the
	// goChanges from the .json metadata, as CaughtNodes). The loader's
	// re-hydrate is compared against this to reproduce the divergence.
	ExpectedHydration []*CaughtNode `json:"expectedHydration"`
}

// SnapshotResult is the output of RunTestCaseFromSnapshot: the re-hydrated
// nodes from the captured .db, plus whether they match ExpectedHydration.
type SnapshotResult struct {
	Hydration []*CaughtNode `json:"hydration"`
	// Match is true iff the re-hydrate matches ExpectedHydration (same length
	// + same rows, order-insensitive on the main table — hydrate order is
	// the AST's orderBy, which the re-hydrate reproduces from the .db).
	Match bool `json:"match"`
	// Diff is a human-readable summary of the first divergence (empty if Match).
	Diff string `json:"diff,omitempty"`
}

// RunTestCaseFromSnapshot opens the captured .db, reads the AST's main table
// into a MemorySource, builds the pipeline, and hydrates. The result's
// `Hydration` is the re-produced state; `Match` compares it to
// `tc.ExpectedHydration`.
func RunTestCaseFromSnapshot(tc SnapshotTestCase) (*SnapshotResult, error) {
	mainTable := tc.AST.Table

	// Build sources for every schema table (the AST may join related tables;
	// the .db has them all). Only the main table is read into the source —
	// related tables are read too so a related-table AST can hydrate its
	// fan-out. (For a main-table-only hydrate the extra reads are cheap no-ops.)
	sources := make(map[string]*ivm.MemorySource)
	for tableName, schema := range tc.Schema {
		columns := make(map[string]string)
		for colName, colDef := range schema.Columns {
			columns[colName] = colDef.Type
		}
		sources[tableName] = ivm.NewMemorySourceWithConverter(
			tableName, columns, schema.PrimaryKey,
			func(v interface{}, colType string) ivm.Value {
				return sqlite.FromSQLiteType(v, colType)
			},
		)
	}

	// Open the captured .db (VACUUM INTO copy — a standalone file, no WAL).
	db, err := sql.Open("sqlite", tc.SnapshotFile)
	if err != nil {
		return nil, fmt.Errorf("open snapshot db: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// Read each schema table's rows from the .db into its MemorySource. The
	// captured .db is the POST state, so this reproduces the hydrate exactly.
	for tableName, schema := range tc.Schema {
		source := sources[tableName]
		rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %q`, tableName))
		if err != nil {
			// A table in the schema but absent from the .db (e.g. an internal
			// table the capture filtered out) is fine — skip it.
			continue
		}
		colNames, err := rows.Columns()
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("columns for %s: %w", tableName, err)
		}
		raw := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		for rows.Next() {
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan %s: %w", tableName, err)
			}
			row := make(ivm.Row, len(colNames))
			for i, c := range colNames {
				colDef, ok := schema.Columns[c]
				if !ok {
					continue
				}
				row[c] = sqlite.FromSQLiteType(raw[i], colDef.Type)
			}
			source.Push(ivm.MakeSourceChangeAdd(row))
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("rows %s: %w", tableName, err)
		}
	}

	// Build pipeline + collect hydrate output — same path as RunTestCase.
	delegate := &memoryDelegate{sources: sources}
	pipeline := builder.BuildPipeline(tc.AST, delegate)
	collector := &changeCollector{}
	pipeline.Input.SetOutput(collector)
	nodes := slices.Collect(pipeline.Input.Fetch(ivm.FetchRequest{}))
	hydration := make([]*CaughtNode, 0, len(nodes))
	for _, node := range nodes {
		hydration = append(hydration, expandNode(node))
	}

	res := &SnapshotResult{Hydration: hydration}
	res.Match, res.Diff = compareHydration(hydration, tc.ExpectedHydration, mainTable)
	return res, nil
}

// RunTestCaseFromSnapshotJSON runs a snapshot test case from JSON input (the
// captured .json metadata file, possibly augmented with ExpectedHydration).
func RunTestCaseFromSnapshotJSON(input []byte) ([]byte, error) {
	var tc SnapshotTestCase
	if err := json.Unmarshal(input, &tc); err != nil {
		return nil, fmt.Errorf("parse snapshot test case: %w", err)
	}
	result, err := RunTestCaseFromSnapshot(tc)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(result, "", "  ")
}

// compareHydration compares the re-hydrate to the captured expected output on
// the MAIN table (order-insensitive — a captured hydrate's order is the AST's
// orderBy, which the re-hydrate reproduces, but comparing as a set is the
// robust check the offline oracle wants). Returns (match, diffSummary).
func compareHydration(
	got []*CaughtNode,
	want []*CaughtNode,
	mainTable string,
) (bool, string) {
	// Main-table row signature: stableStringify of the row's PK + content.
	// The .Row is ivm.Row (map[string]Value); marshal to JSON for a stable key.
	keyOf := func(n *CaughtNode) string {
		if n == nil {
			return "<nil>"
		}
		b, _ := json.Marshal(n.Row)
		return string(b)
	}
	if len(got) != len(want) {
		return false, fmt.Sprintf(
			"main-table row count: re-hydrate=%d expected=%d",
			len(got), len(want),
		)
	}
	gotSet := make(map[string]int, len(got))
	for _, n := range got {
		gotSet[keyOf(n)]++
	}
	for _, n := range want {
		k := keyOf(n)
		if gotSet[k] == 0 {
			return false, fmt.Sprintf(
				"expected row missing from re-hydrate: %s",
				trunc(k, 120),
			)
		}
		gotSet[k]--
	}
	return true, ""
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
