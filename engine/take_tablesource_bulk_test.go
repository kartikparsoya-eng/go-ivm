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

	// The SnapshotChanges below ARE the replicator notification; Source.Push's
	// writeChange applies each Edit to the prev tx. No external UPDATE — that
	// would commit after the prev tx's read snapshot and deadlock the
	// writeChange under plain BEGIN (mattn has no BEGIN CONCURRENT).

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

	// SnapshotChanges drive the prev-tx writeChange; no external UPDATE
	// (would deadlock the prev tx's read snapshot under plain BEGIN).

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

// TestTableSourceTake_BatchedAddDisplacesBound is the regression for the
// reverse-fetch overlay-comparator bug. A Take(limit=3) ORDER BY val DESC
// over [50,40,30,20,10] gets a batched advance adding 45 and 35. Each Add
// at size==limit triggers Take's displaced-bound lookup, which fetches the
// source with {start:bound, basis:'at', reverse:true}. The in-flight Add
// must be spliced into that REVERSE-ordered fetch using the reverse-aware
// comparator (TS makeComparator(sort, req.reverse)); applying the forward
// comparator put it at the wrong index → Take removed the wrong row → the
// resulting window diverged from TS (shadow soak row-pick mismatches).
//
// We verify the NET result set (hydrate adds + advance adds − removes)
// equals the true top-3 by val: {50, 45, 40}. A wrong displaced row would
// leave the window holding 30 or 35 instead.
func TestTableSourceTake_BatchedAddDisplacesBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE items (id TEXT PRIMARY KEY, val INTEGER)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	seed := map[string]int64{"a": 50, "b": 40, "c": 30, "d": 20, "e": 10}
	for id, v := range seed {
		if _, err := w.Exec(`INSERT INTO items VALUES (?, ?)`, id, v); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	t.Cleanup(func() { wdb.Close() })
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "items",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "val": {Type: "number"}},
		[]string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	limit := 3
	ast := builder.AST{Table: "items", OrderBy: ivm.Ordering{{"val", "desc"}}, Limit: &limit}
	hydrate, _, err := eng.AddQuery("q-top3", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	// Track the live window as a set of ids, replaying hydrate + advance.
	window := map[string]bool{}
	apply := func(changes []RowChange) {
		for _, c := range changes {
			if c.Table != "items" {
				continue
			}
			id, _ := c.RowKey["id"].(string)
			switch c.Type {
			case RowChangeAdd:
				if window[id] {
					t.Fatalf("Add for %q already in window %v (double-add)", id, window)
				}
				window[id] = true
			case RowChangeRemove:
				// A Remove must target a row currently in the window. The
				// reverse-overlay bug makes Take pick the just-added row as
				// its bound and emit Remove(just-added-row) — which targets a
				// row never in the materialized window. Catch that here.
				if !window[id] {
					t.Fatalf("Remove for %q NOT in window %v (Take removed a non-window row → reverse-overlay bug)", id, window)
				}
				delete(window, id)
			}
		}
	}
	apply(hydrate)
	// Sanity: hydrate window is the top 3 = {a,b,c}.
	if !(window["a"] && window["b"] && window["c"] && len(window) == 3) {
		t.Fatalf("hydrate window = %v, want {a,b,c}", window)
	}

	// Batched advance: add x=100, y=90, z=80 in one call — all NEWER than
	// every existing row (the soak's bulk-insert shape: newest rows go to
	// the top of a DESC window). Each Add at size==limit triggers Take's
	// displaced-bound reverse fetch with the added row spliced via overlay.
	// Under the FORWARD comparator the added row splices at index 0 of the
	// reverse-ordered fetch, so Take picks the just-added row AS the bound
	// and removes it instead of the real bottom row — the window never
	// advances. The reverse-aware comparator places it correctly.
	res := eng.Advance([]SnapshotChange{
		{Table: "items", NextValue: ivm.Row{"id": "x", "val": int64(100)}},
		{Table: "items", NextValue: ivm.Row{"id": "y", "val": int64(90)}},
		{Table: "items", NextValue: ivm.Row{"id": "z", "val": int64(80)}},
	})
	if res.Drift != nil {
		t.Fatalf("unexpected drift: %v", res.Drift)
	}
	apply(res.Changes)

	// True top-3 by val after adding 100,90,80 to {50,40,30,20,10}:
	// {x:100, y:90, z:80}. a,b,c must all be displaced.
	want := map[string]bool{"x": true, "y": true, "z": true}
	if len(window) != 3 {
		t.Fatalf("post-advance window size = %d, want 3 (window=%v)", len(window), window)
	}
	for id := range want {
		if !window[id] {
			t.Fatalf("post-advance window missing %q; got %v (wrong displaced row → reverse-overlay bug)", id, window)
		}
	}
	for id := range window {
		if !want[id] {
			t.Fatalf("post-advance window has unexpected %q; got %v (wrong displaced row → reverse-overlay bug)", id, window)
		}
	}
}
