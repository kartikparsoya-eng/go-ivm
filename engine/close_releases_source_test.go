package engine

// Regression for the writable conn + open prev-tx leak on group teardown.
//
// Before the fix, the engine.Source interface had no Close(), so Engine.Close
// destroyed the pipeline inputs (which only disconnects the leaf's connection
// from its slice) but never released the tablesource leaf's dedicated writable
// *sql.Conn or rolled back its prev tx. Every group teardown / re-init therefore
// leaked one writable conn + one open tx per queried table; under sustained
// client-group churn this exhausted the writable pool (the reconnect-flood root
// cause that pool-256 + the 30s acquire timeout only deferred).
//
// The fix adds Close() to engine.Source and has Engine.Close() invoke it on every
// registered source. This test pins the contract end-to-end: after a hydrate has
// caused the tablesource leaf to check a writable conn out of the pool, closing
// the engine must return it (InUse back to 0).
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

func TestEngineClose_ReleasesTableSourceConn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, updatedAt INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, s := range []struct {
		id string
		ts int64
	}{{"ticket-1", 100}, {"ticket-2", 200}, {"ticket-3", 300}} {
		if _, err := w.Exec(`INSERT INTO tickets VALUES (?, ?)`, s.id, s.ts); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("tablesource.Open: %v", err)
	}
	defer db.Close()
	// wdb is the long-lived shared writable pool (the production analogue is
	// getReplicaWritableDB()). It deliberately outlives the engine: re-init
	// reuses it, so the engine MUST return its checked-out conn on Close
	// rather than relying on the pool being torn down.
	wdb, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("tablesource.OpenWritable: %v", err)
	}
	defer wdb.Close()

	src, err := tablesource.New(db, wdb, "tickets",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"updatedAt": {Type: "number"},
		},
		[]string{"id"})
	if err != nil {
		t.Fatalf("tablesource.New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: filepath.Join(t.TempDir(), "storage.db")})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	eng.RegisterSource(src)

	// Hydrate the query — this drives the leaf's first Fetch, which lazily
	// checks a writable conn out of wdb and opens the prev tx on it.
	ast := builder.AST{
		Table:   "tickets",
		OrderBy: ivm.Ordering{{"updatedAt", "desc"}},
	}
	if _, _, err := eng.AddQuery("q-hydrate", ast); err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	// Precondition: the hydrate must actually have a conn checked out, else
	// this test would pass vacuously even with the leak present.
	if got := wdb.Stats().InUse; got == 0 {
		t.Fatalf("expected the hydrate to hold a writable conn (InUse>=1), got InUse=0 — "+
			"test no longer exercises the leak path")
	}

	if err := eng.Close(); err != nil {
		t.Fatalf("eng.Close: %v", err)
	}

	// Postcondition (the fix): the writable conn is returned to the pool.
	// Pre-fix this stayed at 1 (leaked) forever.
	if got := wdb.Stats().InUse; got != 0 {
		t.Fatalf("engine.Close leaked a writable conn: wdb InUse=%d, want 0", got)
	}
}
