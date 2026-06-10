package engine

// F6 (VERIFY): shared-conn parallelism in drive mode.
//
// In drive mode buildSnapshotterLocked (cmd/sidecar/advance_to_head.go) sticky-
// binds EVERY tablesource leaf to the snapshotter's single *sql.Conn via
// engine.BindTableSourcesToConn(snap.Current().Conn()). AddQueriesStream then
// hydrates queries on PARALLEL goroutines (engine.go:753 — one goroutine per
// query). Distinct tables have distinct per-Source mutexes, so their hydrate
// Fetches do NOT serialize against each other in our code: they issue concurrent
// QueryContext calls on the ONE shared conn (tablesource/source.go:896 via
// activeConn()). database/sql serializes use of a single *sql.Conn, so this is
// expected to be correct-but-serial — within-CG hydrate parallelism is
// effectively nil in drive mode (a perf ceiling worth knowing, not a bug).
//
// These tests PIN the no-error + correct-rows behaviour rather than trusting a
// recollection of stdlib semantics. Run with -race to also catch a data race on
// the shared conn. (seedMultiTableReplica / benchColumns are reused from
// tablesource_bench_test.go — tables t0..tN-1 (id INTEGER PK, name, val), rows
// id=1..rows.)

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// bindAllSourcesToSharedConn acquires ONE *sql.Conn from the writable pool and
// pins it with BEGIN + a warm-up read — the exact shape buildSnapshotterLocked
// hands to BindTableSourcesToConn (snap.Current().Conn()), minus the snapshotter
// wrapper (whose beginAndPin reads the replica's replication-state table, absent
// from a hand-seeded fixture). Binds every registered source to it.
func bindAllSourcesToSharedConn(t *testing.T, eng *Engine, wdb *sql.DB) {
	t.Helper()
	ctx := context.Background()
	shared, err := wdb.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire shared conn: %v", err)
	}
	t.Cleanup(func() { shared.Close() })
	if _, err := shared.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN on shared conn: %v", err)
	}
	if _, err := shared.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatalf("warm-up read on shared conn: %v", err)
	}
	eng.BindTableSourcesToConn(shared)
	t.Cleanup(func() { eng.UnbindTableSources() })
}

// newSharedConnEngine builds an engine with nTables tablesource leaves over a
// freshly-seeded multi-table replica, all sticky-bound to one shared conn.
// Returns the engine; the seeded row count per table is `rowsEach`.
func newSharedConnEngine(t *testing.T, nTables, rowsEach int) *Engine {
	t.Helper()
	path := seedMultiTableReplica(t, nTables, rowsEach)

	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("tablesource.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("tablesource.OpenWritable: %v", err)
	}
	t.Cleanup(func() { wdb.Close() })

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	for i := 0; i < nTables; i++ {
		name := fmt.Sprintf("t%d", i)
		src, err := tablesource.New(db, wdb, name, benchColumns(), []string{"id"})
		if err != nil {
			t.Fatalf("tablesource.New %s: %v", name, err)
		}
		eng.RegisterSource(src)
		eng.SetTableUniqueKeys(name, [][]string{{"id"}})
	}

	bindAllSourcesToSharedConn(t, eng, wdb)
	return eng
}

func allRowsQueries(nTables int) []QuerySpec {
	queries := make([]QuerySpec, nTables)
	for i := 0; i < nTables; i++ {
		name := fmt.Sprintf("t%d", i)
		queries[i] = QuerySpec{
			QueryID: "q-" + name,
			AST:     builder.AST{Table: name, OrderBy: ivm.Ordering{{"id", "asc"}}},
		}
	}
	return queries
}

// TestSharedConnParallelHydrate_NoError: N tables all sticky-bound to ONE conn,
// hydrated by AddQueriesStream's parallel goroutines, must return no error and
// every query's full row set — no "database is locked" / busy / dropped rows
// from concurrent QueryContext on the shared conn.
func TestSharedConnParallelHydrate_NoError(t *testing.T) {
	// Small chunk size → multiple flush cycles per query, widening the window in
	// which each goroutine holds the shared conn mid-fetch (more overlap).
	withChunkSize(t, 64)

	const (
		nTables  = 6
		rowsEach = 2000
	)
	eng := newSharedConnEngine(t, nTables, rowsEach)

	var mu sync.Mutex
	perQueryRows := make(map[string]int)
	// Verify completeness (not just count) for t0: collect the distinct ids so a
	// drop AND a compensating duplicate can't cancel out in a bare count.
	t0ids := make(map[string]struct{})
	err := eng.AddQueriesStream(allRowsQueries(nTables), func(r QueryResult) {
		mu.Lock()
		perQueryRows[r.QueryID] += len(r.Changes)
		if r.QueryID == "q-t0" {
			for _, c := range r.Changes {
				t0ids[fmt.Sprint(c.RowKey["id"])] = struct{}{}
			}
		}
		mu.Unlock()
	})

	// The crux of F6: concurrent QueryContext on one shared *sql.Conn must NOT
	// error.
	if err != nil {
		t.Fatalf("AddQueriesStream over a shared conn returned error: %v", err)
	}
	for i := 0; i < nTables; i++ {
		qid := fmt.Sprintf("q-t%d", i)
		if got := perQueryRows[qid]; got != rowsEach {
			t.Fatalf("%s hydrated %d rows, want %d (a dropped/garbled row under "+
				"shared-conn contention)", qid, got, rowsEach)
		}
	}
	if len(t0ids) != rowsEach {
		t.Fatalf("q-t0 yielded %d DISTINCT ids, want %d (rows lost or duplicated "+
			"under concurrent QueryContext on the shared conn)", len(t0ids), rowsEach)
	}
}

// TestSharedConnParallelHydrate_RepeatedIsStable: re-hydrating the same bound
// conn repeatedly (re-register churns pipelines, as a restart-driven reinit
// does) stays error-free and stable — pins that the shared conn does not
// accumulate a busted statement / leaked lock across rounds.
func TestSharedConnParallelHydrate_RepeatedIsStable(t *testing.T) {
	withChunkSize(t, 128)

	const (
		nTables  = 4
		rowsEach = 800
		rounds   = 5
	)
	eng := newSharedConnEngine(t, nTables, rowsEach)
	queries := allRowsQueries(nTables)

	for round := 0; round < rounds; round++ {
		var mu sync.Mutex
		total := 0
		err := eng.AddQueriesStream(queries, func(r QueryResult) {
			mu.Lock()
			total += len(r.Changes)
			mu.Unlock()
		})
		if err != nil {
			t.Fatalf("round %d: AddQueriesStream error: %v", round, err)
		}
		if want := nTables * rowsEach; total != want {
			t.Fatalf("round %d: hydrated %d rows total, want %d", round, total, want)
		}
	}
}
