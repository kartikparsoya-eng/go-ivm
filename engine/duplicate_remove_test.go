package engine

// Regression test for the intra-batch duplicate-Remove DriftError.
//
// Root cause: Go's diff.Collect() eagerly materializes ALL changes from the
// clean prev conn BEFORE AdvanceStream applies them. TS's Diff iterates lazily
// during #advance — reading prevValues from the prev conn AFTER each
// writeChange mutates it. TS's no-op filter (prevValues.length === 0 &&
// !nextFound → skip) catches the case where a row was already deleted by a
// previous writeChange in the same batch. Go's eager Collect misses this.
//
// When a row appears as BOTH a unique-conflict removal (from another row's
// opSet) AND its own opDel in the change-log, Go emits two Removes for the
// same row. The first Remove's writeChange deletes it; the second Remove's
// driftCheckLocked finds it missing → false DriftError → pipeline reset →
// full re-hydration (the pre-prod "slow sync / manual reload" symptom).
//
// Fix: skip the duplicate Remove in genPushAndWrite instead of panicking,
// matching TS's no-op filter behavior.

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// TestAdvance_DuplicateRemoveNoDrift_MemorySource verifies that two
// SnapshotChanges producing duplicate Removes for the same row in a single
// Advance batch do NOT raise a DriftError. The first change simulates a
// unique-conflict removal (opSet of rowA conflicts with rowB → Remove(rowB)
// + Add(rowA)); the second simulates rowB's own opDel (Remove(rowB)). Without
// the fix, the second Remove panics with DriftError because writeChange from
// the first Remove already deleted rowB from the in-memory data.
func TestAdvance_DuplicateRemoveNoDrift_MemorySource(t *testing.T) {
	src := ivm.NewMemorySource(
		"stage_transitions",
		map[string]string{"id": "string", "stage_id": "string", "transition_id": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "stage_transitions"}); err != nil {
		t.Fatal(err)
	}

	rowB := ivm.Row{"id": "B", "stage_id": "s1", "transition_id": "X"}
	rowA := ivm.Row{"id": "A", "stage_id": "s2", "transition_id": "X"}

	// Hydrate: add rowB so the source has it.
	hydrateResult := eng.Advance([]SnapshotChange{
		{Table: "stage_transitions", NextValue: rowB},
	})
	if hydrateResult.Drift != nil {
		t.Fatalf("hydrate drift: %v", hydrateResult.Drift)
	}

	// Advance with two changes that produce duplicate Removes for rowB:
	//   Change 1 (opSet of rowA, unique-conflict with rowB):
	//     PrevValues=[rowB], NextValue=rowA
	//     → snapshotToSourceChanges: Remove(rowB) + Add(rowA)
	//   Change 2 (opDel of rowB):
	//     PrevValues=[rowB], NextValue=nil
	//     → snapshotToSourceChanges: Remove(rowB)
	//
	// The engine pushes Remove(rowB), Add(rowA), Remove(rowB).
	// The first Remove(rowB) succeeds and writeChange deletes rowB.
	// The second Remove(rowB) must be skipped (not panic with DriftError).
	result := eng.Advance([]SnapshotChange{
		{
			Table:      "stage_transitions",
			PrevValues: []ivm.Row{rowB},
			NextValue:  rowA,
		},
		{
			Table:      "stage_transitions",
			PrevValues: []ivm.Row{rowB},
			NextValue:  nil,
		},
	})

	if result.Drift != nil {
		t.Fatalf("expected no drift for duplicate Remove, got DriftError: %s "+
			"(table=%s op=%s pk=%v has_count=%d)",
			result.Drift.Error(), result.Drift.Table, result.Drift.Op,
			result.Drift.PK, result.Drift.HasCount)
	}
}

// TestAdvance_DuplicateRemoveNoDrift_TableSource is the same scenario but
// through the production TableSource (SQLite-backed) path. The TableSource's
// driftCheckLocked queries the prev SQLite and writeChangeLocked mutates it
// via DELETE/INSERT — so the second Remove's existsLocked finds the row
// already deleted by the first Remove's writeChangeLocked.
func TestAdvance_DuplicateRemoveNoDrift_TableSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE stage_transitions (
		id TEXT PRIMARY KEY,
		stage_id TEXT,
		transition_id TEXT
	)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(
		`INSERT INTO stage_transitions VALUES (?, ?, ?)`,
		"B", "s1", "X",
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "stage_transitions",
		map[string]sqlite.ColumnSchema{
			"id":            {Type: "string"},
			"stage_id":      {Type: "string"},
			"transition_id": {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "stage_transitions"}); err != nil {
		t.Fatal(err)
	}

	rowB := ivm.Row{"id": "B", "stage_id": "s1", "transition_id": "X"}
	rowA := ivm.Row{"id": "A", "stage_id": "s2", "transition_id": "X"}

	// Same duplicate-Remove scenario as the MemorySource test.
	// Change 1: opSet of rowA with prevValues=[rowB] (unique-conflict removal).
	// Change 2: opDel of rowB with prevValues=[rowB].
	result := eng.Advance([]SnapshotChange{
		{
			Table:      "stage_transitions",
			PrevValues: []ivm.Row{rowB},
			NextValue:  rowA,
		},
		{
			Table:      "stage_transitions",
			PrevValues: []ivm.Row{rowB},
			NextValue:  nil,
		},
	})

	if result.Drift != nil {
		t.Fatalf("expected no drift for duplicate Remove (TableSource), got DriftError: %s "+
			"(table=%s op=%s pk=%v has_count=%d)",
			result.Drift.Error(), result.Drift.Table, result.Drift.Op,
			result.Drift.PK, result.Drift.HasCount)
	}
}

// TestAdvance_DuplicateRemoveThreeRows verifies the fix handles multiple
// duplicate Removes in a single batch. Three rows (B, C, D) each appear as
// both a unique-conflict removal from rowA's opSet and their own opDel.
func TestAdvance_DuplicateRemoveThreeRows(t *testing.T) {
	src := ivm.NewMemorySource(
		"items",
		map[string]string{"id": "string", "sku": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "items"}); err != nil {
		t.Fatal(err)
	}

	// Hydrate three rows.
	hydrate := []SnapshotChange{
		{Table: "items", NextValue: ivm.Row{"id": "B", "sku": "X"}},
		{Table: "items", NextValue: ivm.Row{"id": "C", "sku": "X"}},
		{Table: "items", NextValue: ivm.Row{"id": "D", "sku": "X"}},
	}
	if r := eng.Advance(hydrate); r.Drift != nil {
		t.Fatalf("hydrate drift: %v", r.Drift)
	}

	rowA := ivm.Row{"id": "A", "sku": "X"}
	rowB := ivm.Row{"id": "B", "sku": "X"}
	rowC := ivm.Row{"id": "C", "sku": "X"}
	rowD := ivm.Row{"id": "D", "sku": "X"}

	// Change 1: opSet of rowA, unique-conflict with B, C, D.
	// Change 2: opDel of B.
	// Change 3: opDel of C.
	// Change 4: opDel of D.
	result := eng.Advance([]SnapshotChange{
		{
			Table:      "items",
			PrevValues: []ivm.Row{rowB, rowC, rowD},
			NextValue:  rowA,
		},
		{Table: "items", PrevValues: []ivm.Row{rowB}, NextValue: nil},
		{Table: "items", PrevValues: []ivm.Row{rowC}, NextValue: nil},
		{Table: "items", PrevValues: []ivm.Row{rowD}, NextValue: nil},
	})

	if result.Drift != nil {
		t.Fatalf("expected no drift for 3 duplicate Removes, got: %s "+
			"(table=%s op=%s pk=%v)",
			result.Drift.Error(), result.Drift.Table,
			result.Drift.Op, result.Drift.PK)
	}
}

// TestAdvance_GenuineRemoveDrift_StillDetected verifies the fix does NOT
// mask genuine drift. A Remove for a row that was never in the source (not
// from a previous writeChange in this batch, but truly absent) should still
// raise DriftError. This distinguishes the fix from a blanket "ignore all
// missing-row Removes" which would be incorrect.
//
// The key safety argument: if the change was collected by Collect(), the row
// was in the source at collection time. If driftCheckLocked later finds it
// missing, it MUST be because a previous writeChange in this batch removed
// it. A Remove for a row that was NEVER in the source would have been caught
// by the no-op filter in Each() (diff.go:197: len(prevValues)==0 → skip) and
// never collected. We simulate this by directly calling Advance with a
// Remove for a row that was never hydrated — the MemorySource has no such
// row, and the change was not preceded by a writeChange that removed it.
func TestAdvance_GenuineRemoveDrift_StillDetected(t *testing.T) {
	src := ivm.NewMemorySource(
		"tasks",
		map[string]string{"id": "string", "title": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "tasks"}); err != nil {
		t.Fatal(err)
	}

	// No hydration — the source is empty. A Remove for a row that was never
	// added should still raise DriftError (genuine drift, not intra-batch
	// duplicate). The fix must not mask this case.
	result := eng.Advance([]SnapshotChange{
		{
			Table:      "tasks",
			PrevValues: []ivm.Row{{"id": "ghost", "title": "never existed"}},
			NextValue:  nil,
		},
	})

	if result.Drift == nil {
		t.Fatalf("expected DriftError for genuine missing-row Remove, got nil drift")
	}
	if result.Drift.Op != "Remove" {
		t.Errorf("expected drift.Op=Remove, got %q", result.Drift.Op)
	}
	if result.Drift.Table != "tasks" {
		t.Errorf("expected drift.Table=tasks, got %q", result.Drift.Table)
	}
}

// ---------------------------------------------------------------------------
// BUG 1b: Edit-against-batch-removed-row
//
// A row appears as BOTH a unique-conflict removal (from another row's opSet)
// AND its own opSet (non-PK update) in one batch. Go's eager Collect gets
// prevValues=[rowB] for both entries. The first entry's Remove(rowB) deletes
// rowB via writeChange. The second entry's Edit(rowB', rowB) then finds rowB
// missing → false DriftError.
//
// Fix: convert Edit→Add when OldRow was removed earlier in this batch (matching
// TS's lazy iteration: TS re-reads prevValues from the prev conn after the
// first writeChange deleted rowB, gets empty prevValues, and emits Add(rowB')).
// ---------------------------------------------------------------------------

func TestAdvance_EditAfterBatchRemoveNoDrift_MemorySource(t *testing.T) {
	src := ivm.NewMemorySource(
		"stage_transitions",
		map[string]string{"id": "string", "stage_id": "string", "transition_id": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "stage_transitions"}); err != nil {
		t.Fatal(err)
	}

	rowB := ivm.Row{"id": "B", "stage_id": "s1", "transition_id": "X"}
	rowA := ivm.Row{"id": "A", "stage_id": "s2", "transition_id": "X"}
	rowBUpdated := ivm.Row{"id": "B", "stage_id": "s3", "transition_id": "X"}

	if r := eng.Advance([]SnapshotChange{
		{Table: "stage_transitions", NextValue: rowB},
	}); r.Drift != nil {
		t.Fatalf("hydrate drift: %v", r.Drift)
	}

	result := eng.Advance([]SnapshotChange{
		{
			Table:      "stage_transitions",
			PrevValues: []ivm.Row{rowB},
			NextValue:  rowA,
		},
		{
			Table:      "stage_transitions",
			PrevValues: []ivm.Row{rowB},
			NextValue:  rowBUpdated,
		},
	})

	if result.Drift != nil {
		t.Fatalf("expected no drift for Edit after batch Remove (MemorySource), got: %s "+
			"(table=%s op=%s pk=%v has_count=%d)",
			result.Drift.Error(), result.Drift.Table, result.Drift.Op,
			result.Drift.PK, result.Drift.HasCount)
	}
}

func TestAdvance_EditAfterBatchRemoveNoDrift_TableSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE stage_transitions (
		id TEXT PRIMARY KEY,
		stage_id TEXT,
		transition_id TEXT
	)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(
		`INSERT INTO stage_transitions VALUES (?, ?, ?)`,
		"B", "s1", "X",
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "stage_transitions",
		map[string]sqlite.ColumnSchema{
			"id":            {Type: "string"},
			"stage_id":      {Type: "string"},
			"transition_id": {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "stage_transitions"}); err != nil {
		t.Fatal(err)
	}

	rowB := ivm.Row{"id": "B", "stage_id": "s1", "transition_id": "X"}
	rowA := ivm.Row{"id": "A", "stage_id": "s2", "transition_id": "X"}
	rowBUpdated := ivm.Row{"id": "B", "stage_id": "s3", "transition_id": "X"}

	result := eng.Advance([]SnapshotChange{
		{
			Table:      "stage_transitions",
			PrevValues: []ivm.Row{rowB},
			NextValue:  rowA,
		},
		{
			Table:      "stage_transitions",
			PrevValues: []ivm.Row{rowB},
			NextValue:  rowBUpdated,
		},
	})

	if result.Drift != nil {
		t.Fatalf("expected no drift for Edit after batch Remove (TableSource), got: %s "+
			"(table=%s op=%s pk=%v has_count=%d)",
			result.Drift.Error(), result.Drift.Table, result.Drift.Op,
			result.Drift.PK, result.Drift.HasCount)
	}
}

// ---------------------------------------------------------------------------
// BUG 1c: Duplicate-Add within batch
//
// A row is INSERTed then UPDATEd in the same batch, but was NOT in the prev
// snapshot. Go's eager Collect gets prevValues=[] for both entries, producing
// Add(rowA_v1) then Add(rowA_v2). The second Add finds rowA_v1 already in the
// source → false DriftError{Op:"Add"}.
//
// Fix: track addedInBatch; when a second Add arrives for a PK already added in
// this batch, convert it to Edit(rowA_v2, rowA_v1) (matching TS's lazy
// iteration: TS's prev.getRows sees the just-INSERTed row and emits an Edit).
// ---------------------------------------------------------------------------

func TestAdvance_DuplicateAddNoDrift_MemorySource(t *testing.T) {
	src := ivm.NewMemorySource(
		"items",
		map[string]string{"id": "string", "name": "string", "sku": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "items"}); err != nil {
		t.Fatal(err)
	}

	itemV1 := ivm.Row{"id": "1", "name": "widget", "sku": "A"}
	itemV2 := ivm.Row{"id": "1", "name": "widget", "sku": "B"}

	result := eng.Advance([]SnapshotChange{
		{Table: "items", NextValue: itemV1},
		{Table: "items", NextValue: itemV2},
	})

	if result.Drift != nil {
		t.Fatalf("expected no drift for duplicate Add (MemorySource), got: %s "+
			"(table=%s op=%s pk=%v has_count=%d)",
			result.Drift.Error(), result.Drift.Table, result.Drift.Op,
			result.Drift.PK, result.Drift.HasCount)
	}
}

func TestAdvance_DuplicateAddNoDrift_TableSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE items (
		id TEXT PRIMARY KEY,
		name TEXT,
		sku TEXT
	)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "items",
		map[string]sqlite.ColumnSchema{
			"id":   {Type: "string"},
			"name": {Type: "string"},
			"sku":  {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "items"}); err != nil {
		t.Fatal(err)
	}

	itemV1 := ivm.Row{"id": "1", "name": "widget", "sku": "A"}
	itemV2 := ivm.Row{"id": "1", "name": "widget", "sku": "B"}

	result := eng.Advance([]SnapshotChange{
		{Table: "items", NextValue: itemV1},
		{Table: "items", NextValue: itemV2},
	})

	if result.Drift != nil {
		t.Fatalf("expected no drift for duplicate Add (TableSource), got: %s "+
			"(table=%s op=%s pk=%v has_count=%d)",
			result.Drift.Error(), result.Drift.Table, result.Drift.Op,
			result.Drift.PK, result.Drift.HasCount)
	}
}

// ---------------------------------------------------------------------------
// Safety: genuine drift must still be detected after BUG 1b / BUG 1c fixes
// ---------------------------------------------------------------------------

func TestAdvance_GenuineEditDrift_StillDetected(t *testing.T) {
	src := ivm.NewMemorySource(
		"tasks",
		map[string]string{"id": "string", "title": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "tasks"}); err != nil {
		t.Fatal(err)
	}

	result := eng.Advance([]SnapshotChange{
		{
			Table:      "tasks",
			PrevValues: []ivm.Row{{"id": "ghost", "title": "old"}},
			NextValue:  ivm.Row{"id": "ghost", "title": "new"},
		},
	})

	if result.Drift == nil {
		t.Fatalf("expected DriftError for genuine missing-row Edit, got nil drift")
	}
	if result.Drift.Op != "Edit" {
		t.Errorf("expected drift.Op=Edit, got %q", result.Drift.Op)
	}
	if result.Drift.Table != "tasks" {
		t.Errorf("expected drift.Table=tasks, got %q", result.Drift.Table)
	}
}

func TestAdvance_GenuineAddDrift_StillDetected(t *testing.T) {
	src := ivm.NewMemorySource(
		"products",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "products"}); err != nil {
		t.Fatal(err)
	}

	existing := ivm.Row{"id": "P1", "name": "gadget"}
	if r := eng.Advance([]SnapshotChange{
		{Table: "products", NextValue: existing},
	}); r.Drift != nil {
		t.Fatalf("hydrate drift: %v", r.Drift)
	}

	result := eng.Advance([]SnapshotChange{
		{Table: "products", NextValue: ivm.Row{"id": "P1", "name": "gadget-v2"}},
	})

	if result.Drift == nil {
		t.Fatalf("expected DriftError for genuine cross-batch duplicate Add, got nil drift")
	}
	if result.Drift.Op != "Add" {
		t.Errorf("expected drift.Op=Add, got %q", result.Drift.Op)
	}
}

func TestAdvance_DuplicateAddThreeVersions(t *testing.T) {
	src := ivm.NewMemorySource(
		"metrics",
		map[string]string{"id": "string", "val": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(src)

	if _, _, err := eng.AddQuery("q", builder.AST{Table: "metrics"}); err != nil {
		t.Fatal(err)
	}

	result := eng.Advance([]SnapshotChange{
		{Table: "metrics", NextValue: ivm.Row{"id": "M", "val": "1"}},
		{Table: "metrics", NextValue: ivm.Row{"id": "M", "val": "2"}},
		{Table: "metrics", NextValue: ivm.Row{"id": "M", "val": "3"}},
	})

	if result.Drift != nil {
		t.Fatalf("expected no drift for 3 duplicate Adds, got: %s "+
			"(table=%s op=%s pk=%v)",
			result.Drift.Error(), result.Drift.Table,
			result.Drift.Op, result.Drift.PK)
	}

	data := src.Data()
	if len(data) != 1 {
		t.Fatalf("expected 1 row in source, got %d", len(data))
	}
	if v, ok := data[0]["val"].(string); !ok || v != "3" {
		t.Errorf("expected val=3, got %v", data[0]["val"])
	}
}
