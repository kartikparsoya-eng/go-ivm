package engine

// TS↔Go lifecycle + parallelization correctness on the PROD source path
// (internal/tablesource, not MemorySource). The TS view-syncer churns query
// lifecycle constantly — TTL expiry removes queries, auth maintenance
// retransforms them, clients re-issue them — all interleaved with advances.
// Every RPC below maps 1:1 onto that churn: addQueriesStream / removeQuery /
// advance(Stream). These pin that the Go engine's state stays correct across
// the churn: removed queries emit nothing, writeChange-applied advances are
// visible to a later re-hydrate, re-added queries re-wire their outputs, and
// the whole lifecycle is race-clean under concurrent callers (e.mu is the
// contract; run with -race).

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// newTicketsTableEngine seeds a WAL replica with `seedRows` tickets rows and
// returns an engine with the tickets TableSource registered.
func newTicketsTableEngine(t *testing.T, seedRows int) *Engine {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, updatedAt INTEGER)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < seedRows; i++ {
		if _, err := w.Exec(`INSERT INTO tickets VALUES (?, ?)`,
			fmt.Sprintf("t-%03d", i), int64(1000+i)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	wdb, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenWritable: %v", err)
	}
	t.Cleanup(func() { db.Close(); wdb.Close() })

	src, err := tablesource.New(db, wdb, "tickets",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"updatedAt": {Type: "number"},
		},
		[]string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)
	return eng
}

func ticketsAST() builder.AST {
	return builder.AST{Table: "tickets", OrderBy: ivm.Ordering{{"id", "asc"}}}
}

// ticketAdd builds the SnapshotChange the replicator would emit for one new row.
func ticketAdd(i int) SnapshotChange {
	return SnapshotChange{
		Table:     "tickets",
		NextValue: ivm.Row{"id": fmt.Sprintf("t-%03d", i), "updatedAt": int64(2000 + i)},
	}
}

// hydrateStream runs AddQueriesStream for one query and returns (rows, finals).
func hydrateStream(t *testing.T, eng *Engine, queryID string) (int, int) {
	t.Helper()
	rows, finals := 0, 0
	err := eng.AddQueriesStream(
		[]QuerySpec{{QueryID: queryID, AST: ticketsAST()}},
		func(r QueryResult) {
			rows += len(r.Changes)
			if r.Final {
				finals++
			}
		})
	if err != nil {
		t.Fatalf("AddQueriesStream(%s): %v", queryID, err)
	}
	return rows, finals
}

// advanceStream runs AdvanceStream and returns total emitted RowChanges.
func advanceStream(t *testing.T, eng *Engine, changes []SnapshotChange) int {
	t.Helper()
	total := 0
	err := eng.AdvanceStream(changes, func(p AdvanceStreamPartial) {
		if p.Drift != nil {
			t.Fatalf("unexpected drift: %v", p.Drift)
		}
		total += len(p.Changes)
	})
	if err != nil {
		t.Fatalf("AdvanceStream: %v", err)
	}
	return total
}

// The core TS-driven lifecycle: hydrate → advance → remove (TTL expiry) →
// advance while removed → re-add (client re-issues) → advance again.
//
// TableSource state model being pinned here: writeChange applies each batch's
// changes to an UNCOMMITTED prev-tx only; OnAdvanceEnd ROLLS THAT BACK and
// re-pins at the file's head (source.go OnAdvanceEnd — port of TS
// Snapshotter.resetToHead). The replica FILE is ground truth: in prod the
// replicator has already committed each batch before the advance RPC arrives,
// so the rollback discards nothing real. These synthetic advances have no
// replicator behind them, so their rows deliberately do NOT survive the batch
// — which is exactly what the re-hydrate and repeat-Add assertions verify.
func TestTableSourceLifecycle_AddAdvanceRemoveReAdd(t *testing.T) {
	const seed = 5
	eng := newTicketsTableEngine(t, seed)

	// Hydrate.
	if rows, finals := hydrateStream(t, eng, "q1"); rows != seed || finals != 1 {
		t.Fatalf("hydrate: rows=%d finals=%d, want %d/1", rows, finals, seed)
	}

	// Advance with a live query → 1 add emitted.
	if got := advanceStream(t, eng, []SnapshotChange{ticketAdd(100)}); got != 1 {
		t.Fatalf("advance with live query emitted %d, want 1", got)
	}

	// Remove (TS: query TTL expired / client dropped it).
	eng.RemoveQuery("q1")

	// Advance while removed → nothing emitted, no panic. (An orphan pipeline
	// emitting here is the exact leak the batch-build unwind fix closed.)
	if got := advanceStream(t, eng, []SnapshotChange{ticketAdd(101)}); got != 0 {
		t.Fatalf("advance after remove emitted %d, want 0 (orphan output)", got)
	}

	// Re-add the same queryID (TS: client re-issues the query). The hydrate
	// reads the pinned replica frame — the batches above were rolled back at
	// each OnAdvanceEnd (no replicator committed them), so ground truth is
	// still the seeded file. Seeing 6 or 7 here would mean prev-tx state
	// LEAKED across the batch boundary (rows Go invented that the replica
	// never had = drift against SQL ground truth).
	if rows, finals := hydrateStream(t, eng, "q1"); rows != seed || finals != 1 {
		t.Fatalf("re-hydrate: rows=%d finals=%d, want %d/1 (prev-tx state leaked across batches)",
			rows, finals, seed)
	}

	// The re-wired output must emit on the next advance — and re-pushing the
	// SAME row as batch 1 must be a clean Add, not a duplicate-Add drift,
	// precisely BECAUSE batch 1 was rolled back.
	if got := advanceStream(t, eng, []SnapshotChange{ticketAdd(100)}); got != 1 {
		t.Fatalf("advance after re-add emitted %d, want 1 (output not re-wired, or rollback leaked)", got)
	}
}

// File-as-ground-truth + rotation visibility: rows committed to the replica
// by an external writer (the replicator) become visible to hydrates exactly
// at the NEXT snapshot rotation (OnAdvanceEnd's ROLLBACK + re-pin), and not
// before. Both directions matter:
//   - BEFORE rotation the committed row must be INVISIBLE — hydrate reads the
//     pinned prev frame (activeConn → prevConn); leaking a mid-frame commit
//     into a hydrate would tear the frame the CVR was built against.
//   - AFTER rotation it must be VISIBLE — a re-pin that misses replicator
//     commits = permanent staleness.
// This is the Go port of TS Snapshotter.resetToHead timing (snapshotter.ts).
func TestTableSourceLifecycle_ExternalCommitVisibleToRehydrate(t *testing.T) {
	const seed = 5
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, updatedAt INTEGER)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < seed; i++ {
		if _, err := w.Exec(`INSERT INTO tickets VALUES (?, ?)`,
			fmt.Sprintf("t-%03d", i), int64(1000+i)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	t.Cleanup(func() { w.Close() })

	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wdb, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); wdb.Close() })
	src, err := tablesource.New(db, wdb, "tickets",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"updatedAt": {Type: "number"},
		},
		[]string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	if rows, _ := hydrateStream(t, eng, "q1"); rows != seed {
		t.Fatalf("hydrate rows=%d, want %d", rows, seed)
	}

	// Replicator commits a row. WAL: the pinned prev read-tx doesn't block it.
	if _, err := w.Exec(`INSERT INTO tickets VALUES ('t-100', 2100)`); err != nil {
		t.Fatalf("external commit: %v", err)
	}

	// BEFORE rotation: the hydrate frame is still pinned pre-commit — the new
	// row must NOT appear (frame tearing if it does).
	eng.RemoveQuery("q1")
	if rows, _ := hydrateStream(t, eng, "q1"); rows != seed {
		t.Fatalf("pre-rotation re-hydrate rows=%d, want %d (mid-frame commit leaked into pinned frame)",
			rows, seed)
	}

	// Rotation: an empty advance still runs signalAdvanceEnd → OnAdvanceEnd →
	// ROLLBACK + re-pin at head (which now includes t-100). This is the moment
	// the replicator's commit becomes the IVM's world.
	if got := advanceStream(t, eng, nil); got != 0 {
		t.Fatalf("empty advance emitted %d changes", got)
	}

	// AFTER rotation: visible.
	eng.RemoveQuery("q1")
	if rows, _ := hydrateStream(t, eng, "q1"); rows != seed+1 {
		t.Fatalf("post-rotation re-hydrate rows=%d, want %d (re-pin missed the replicator commit)",
			rows, seed+1)
	}

	// And the next real advance works against the new frame: Add t-101 is
	// clean (no false drift), emitted once.
	if got := advanceStream(t, eng, []SnapshotChange{ticketAdd(101)}); got != 1 {
		t.Fatalf("advance after rotation emitted %d, want 1", got)
	}
}

// Remove of an unknown queryID and double-remove are no-ops (TS can race a
// TTL expiry against an explicit drop → removeQuery arrives twice).
func TestTableSourceLifecycle_RemoveUnknownAndDoubleRemove(t *testing.T) {
	eng := newTicketsTableEngine(t, 3)

	eng.RemoveQuery("never-existed") // must not panic

	if rows, _ := hydrateStream(t, eng, "q1"); rows != 3 {
		t.Fatalf("hydrate rows=%d, want 3", rows)
	}
	eng.RemoveQuery("q1")
	eng.RemoveQuery("q1") // double remove — no-op

	if got := advanceStream(t, eng, []SnapshotChange{ticketAdd(50)}); got != 0 {
		t.Fatalf("advance after double-remove emitted %d, want 0", got)
	}
}

// Parallel hydrate lanes over TableSource leaves — the prod cold-start shape
// (lanes=4 default). Streaming results arrive interleaved across queries from
// multiple goroutines; per-query chunk sequences must stay contiguous, every
// query must end with exactly one Final, and totals must match the serial
// path. Run with -race: this pins the lane fan-out + shared streamW callback
// against data races.
func TestTableSourceParallel_StreamingHydrateLanes(t *testing.T) {
	const (
		nTables  = 4
		rowsEach = 200
		perTable = 2 // 8 queries over 4 lanes → lanes are re-used and contended
	)
	path := seedMultiTableReplica(t, nTables, rowsEach)
	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wdb, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); wdb.Close() })
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	for i := 0; i < nTables; i++ {
		src, err := tablesource.New(db, wdb, fmt.Sprintf("t%d", i), benchColumns(), []string{"id"})
		if err != nil {
			t.Fatalf("New t%d: %v", i, err)
		}
		eng.RegisterSource(src)
	}

	// Force multi-chunk per query so per-query chunkIndex ordering is
	// actually exercised across lanes.
	savedChunk := hydrateChunkSize
	hydrateChunkSize = 64
	defer func() { hydrateChunkSize = savedChunk }()

	var specs []QuerySpec
	for tb := 0; tb < nTables; tb++ {
		for q := 0; q < perTable; q++ {
			specs = append(specs, QuerySpec{
				QueryID: fmt.Sprintf("q-t%d-%d", tb, q),
				AST: builder.AST{
					Table:   fmt.Sprintf("t%d", tb),
					OrderBy: ivm.Ordering{{"id", "asc"}},
				},
			})
		}
	}

	var mu sync.Mutex
	rowsPerQuery := map[string]int{}
	finalsPerQuery := map[string]int{}
	nextChunk := map[string]int{}
	if err := eng.AddQueriesStream(specs, func(r QueryResult) {
		mu.Lock()
		defer mu.Unlock()
		if r.ChunkIndex != nextChunk[r.QueryID] {
			t.Errorf("%s: chunkIndex %d out of order (want %d)", r.QueryID, r.ChunkIndex, nextChunk[r.QueryID])
		}
		nextChunk[r.QueryID]++
		rowsPerQuery[r.QueryID] += len(r.Changes)
		if r.Final {
			finalsPerQuery[r.QueryID]++
		}
	}); err != nil {
		t.Fatalf("AddQueriesStream: %v", err)
	}

	if len(rowsPerQuery) != nTables*perTable {
		t.Fatalf("queries seen = %d, want %d", len(rowsPerQuery), nTables*perTable)
	}
	for _, spec := range specs {
		if rowsPerQuery[spec.QueryID] != rowsEach {
			t.Errorf("%s: rows=%d, want %d", spec.QueryID, rowsPerQuery[spec.QueryID], rowsEach)
		}
		if finalsPerQuery[spec.QueryID] != 1 {
			t.Errorf("%s: finals=%d, want exactly 1", spec.QueryID, finalsPerQuery[spec.QueryID])
		}
	}
}

// Concurrent lifecycle callers: one goroutine advancing, one churning
// remove/re-add of the same query — the TS view-syncer's advance loop racing
// TTL-expiry/re-issue traffic. e.mu must serialize them with no data race
// (run with -race), no drift, and no state leak: each advance batch is rolled
// back at OnAdvanceEnd (no replicator behind it), so whatever the
// interleaving, a final fresh hydrate must see exactly the seeded replica —
// more rows would mean prev-tx state leaked across a batch boundary under
// concurrency.
func TestTableSourceLifecycle_ConcurrentAdvanceAndQueryChurn(t *testing.T) {
	const (
		seed     = 5
		advances = 20
		churns   = 10
	)
	eng := newTicketsTableEngine(t, seed)
	if rows, _ := hydrateStream(t, eng, "q1"); rows != seed {
		t.Fatalf("hydrate rows=%d, want %d", rows, seed)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	driftSeen := make(chan string, advances)
	go func() {
		defer wg.Done()
		for i := 0; i < advances; i++ {
			_ = eng.AdvanceStream([]SnapshotChange{ticketAdd(200 + i)},
				func(p AdvanceStreamPartial) {
					if p.Drift != nil {
						driftSeen <- p.Drift.Error()
					}
				})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < churns; i++ {
			eng.RemoveQuery("q1")
			_ = eng.AddQueriesStream(
				[]QuerySpec{{QueryID: "q1", AST: ticketsAST()}},
				func(QueryResult) {})
		}
	}()
	wg.Wait()
	close(driftSeen)
	for d := range driftSeen {
		t.Errorf("advance drifted under churn: %s", d)
	}

	// Rollback-per-batch: final ground truth is the seeded file, regardless
	// of interleaving.
	if rows, finals := hydrateStream(t, eng, "q-final"); rows != seed || finals != 1 {
		t.Fatalf("final hydrate rows=%d finals=%d, want %d/1 (prev-tx leak or lost rotation under churn)",
			rows, finals, seed)
	}
}
