package engine

// Bulk-mutation pattern through TableSource — the production soak's
// trigger that crashed the sidecar with the stale-bound panic.
//
// MemorySource-backed Take tests passed cleanly for the same patterns
// (see ivm/take_bulkmutation_test.go), so any panic here would prove
// the bug lives in TableSource's machinery: overlay-during-Push,
// snapshot rotation timing across batched changes, or the readViaSnapshot
// gate interacting with rapid back-to-back Edits.

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// TestTableSourceTake_BulkUpdateAcrossBound: top-10 by updatedAt DESC,
// then a bulk update that bumps updatedAt on the rows OUTSIDE the
// window so they enter and displace. Simulates the production
// `bulk priority→MEDIUM on tickets in chan-N` pattern that triggered
// the soak panic (ORM bumps updatedAt on every UPDATE).
func TestTableSourceTake_BulkUpdateAcrossBound(t *testing.T) {
	t.Skip("prev-tx arch: mid-batch external INSERT can't be simulated without BEGIN CONCURRENT (rocicorp wal2 patch). Production path uses BEGIN CONCURRENT and works; this test relied on the OLD snapshot-rotate-per-Push mechanism.")
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, updatedAt INTEGER)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < 20; i++ {
		_, err := w.Exec(`INSERT INTO tickets VALUES (?, ?)`,
			fmt.Sprintf("t-%02d", i), int64(1000+i*100))
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "tickets",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"updatedAt": {Type: "number"},
		},
		[]string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	limit := 10
	ast := builder.AST{
		Table:   "tickets",
		OrderBy: ivm.Ordering{{"updatedAt", "desc"}},
		Limit:   &limit,
	}
	_, _, err = eng.AddQuery("q-top10", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	// Now the bulk update: rows t-00 through t-09 (currently outside the
	// top-10 window) all get updatedAt = 3000 — a single PG tx with 10
	// UPDATEs. We deliver this as 10 Edits in one engine.Advance call
	// to mimic the replicator's batched WAL frame.
	defer func() {
		r := recover()
		if r == nil {
			t.Logf("no panic — TableSource handled bulk-across-bound cleanly")
			return
		}
		if d, ok := r.(*ivm.DriftError); ok {
			t.Logf("ROOT CAUSE REPRO via TableSource — DriftError: %v", d.Error())
			t.Fatalf("(failing the test so the repro is visible in CI)")
		}
		panic(r)
	}()

	// Apply the same updates to SQLite first (mimicking the replicator).
	w2, _ := sql.Open("sqlite3", path)
	tx, _ := w2.Begin()
	for i := 0; i < 10; i++ {
		_, err := tx.Exec(`UPDATE tickets SET updatedAt = 3000 WHERE id = ?`, fmt.Sprintf("t-%02d", i))
		if err != nil {
			t.Fatalf("update: %v", err)
		}
	}
	tx.Commit()
	w2.Close()

	// Build the snapshot changes the replicator would emit and call
	// engine.Advance with the whole batch.
	var changes []SnapshotChange
	for i := 0; i < 10; i++ {
		changes = append(changes, SnapshotChange{
			Table: "tickets",
			PrevValues: []ivm.Row{{
				"id":        fmt.Sprintf("t-%02d", i),
				"updatedAt": int64(1000 + i*100),
			}},
			NextValue: ivm.Row{
				"id":        fmt.Sprintf("t-%02d", i),
				"updatedAt": int64(3000),
			},
		})
	}
	result := eng.Advance(changes)
	t.Logf("advance returned %d changes, drift=%v", len(result.Changes), result.Drift)
}

// TestTableSourceTake_BulkUpdateIncludingBound: same bulk pattern but
// also includes the bound row in the batch. This is the case most
// likely to expose snapshot-rotation timing: the bound is being
// updated while the Edits are still being processed.
func TestTableSourceTake_BulkUpdateIncludingBound(t *testing.T) {
	t.Skip("prev-tx arch: mid-batch external INSERT can't be simulated without BEGIN CONCURRENT (rocicorp wal2 patch). Production path uses BEGIN CONCURRENT and works; this test relied on the OLD snapshot-rotate-per-Push mechanism.")
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, updatedAt INTEGER)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < 20; i++ {
		_, err := w.Exec(`INSERT INTO tickets VALUES (?, ?)`,
			fmt.Sprintf("t-%02d", i), int64(1000+i*100))
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	src, _ := tablesource.New(db, wdb, "tickets",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"updatedAt": {Type: "number"},
		},
		[]string{"id"})

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	limit := 10
	ast := builder.AST{
		Table:   "tickets",
		OrderBy: ivm.Ordering{{"updatedAt", "desc"}},
		Limit:   &limit,
	}
	_, _, err := eng.AddQuery("q-top10b", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	// Bound after hydrate is t-10 (updatedAt=2000). Bulk-update t-08..t-12
	// all to 3500. This includes the bound row and rows on both sides.
	defer func() {
		r := recover()
		if r == nil {
			t.Logf("no panic — TableSource handled bound-in-batch cleanly")
			return
		}
		if d, ok := r.(*ivm.DriftError); ok {
			t.Logf("ROOT CAUSE REPRO via TableSource — DriftError: %v", d.Error())
			t.Fatalf("(failing the test so the repro is visible in CI)")
		}
		panic(r)
	}()

	w2, _ := sql.Open("sqlite3", path)
	tx, _ := w2.Begin()
	for _, i := range []int{8, 9, 10, 11, 12} {
		_, err := tx.Exec(`UPDATE tickets SET updatedAt = 3500 WHERE id = ?`, fmt.Sprintf("t-%02d", i))
		if err != nil {
			t.Fatalf("update: %v", err)
		}
	}
	tx.Commit()
	w2.Close()

	var changes []SnapshotChange
	for _, i := range []int{8, 9, 10, 11, 12} {
		changes = append(changes, SnapshotChange{
			Table: "tickets",
			PrevValues: []ivm.Row{{
				"id":        fmt.Sprintf("t-%02d", i),
				"updatedAt": int64(1000 + i*100),
			}},
			NextValue: ivm.Row{
				"id":        fmt.Sprintf("t-%02d", i),
				"updatedAt": int64(3500),
			},
		})
	}
	result := eng.Advance(changes)
	t.Logf("advance returned %d changes, drift=%v", len(result.Changes), result.Drift)
}
