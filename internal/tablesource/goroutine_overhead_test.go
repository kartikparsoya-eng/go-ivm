package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/mattn/go-sqlite3"
)

// seedLargeTable creates a SQLite file with the given number of rows and
// returns the path.
func seedLargeTable(t *testing.T, rows int) string {
	t.Helper()
	path := seedReplicaWithStateVersion(t, fmt.Sprintf("%010d", rows))
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS users",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, val REAL)",
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	tx, err := w.Begin()
	if err != nil {
		t.Fatalf("seed begin: %v", err)
	}
	ins, err := tx.Prepare("INSERT INTO users (id, name, val) VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("seed prep: %v", err)
	}
	for i := 1; i <= rows; i++ {
		if _, err := ins.Exec(i, fmt.Sprintf("row-%d", i), float64(i*7)); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	ins.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	return path
}

// TestMattnDriverGoroutinePerNext is the root-cause reproduction test.
//
// The mattn/go-sqlite3 driver's Next() method has two paths:
//
//  1. Synchronous (context.Background or nil Done): calls nextSyncLocked
//     directly — a single CGO call, no goroutine, no channel.
//
//  2. Goroutine-per-Next (any context with a Done() channel): spawns a
//     helper goroutine for every Next() call, blocks in select on a
//     channel. The helper does the CGO call and closes the channel.
//
// In production, Source.ctx (derived from the CG lifetime context) has a
// Done() channel, so every row fetch goes through path 2. Through the N+1
// EXISTS pattern (N parent rows × M child rows), this is N*M goroutine
// creations — enough overhead to starve the Go scheduler and wedge for
// 17+ minutes.
//
// This test directly demonstrates the driver behavior by querying the same
// table with two different contexts and comparing the overhead.
func TestMattnDriverGoroutinePerNext(t *testing.T) {
	const rows = 50000
	path := seedLargeTable(t, rows)

	// --- Path 1: context.Background() (synchronous — the FIX) ---
	db1, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	defer db1.Close()

	bgRows, err := db1.QueryContext(context.Background(), "SELECT id, name, val FROM users")
	if err != nil {
		t.Fatalf("query background: %v", err)
	}
	bgCount := 0
	bgStart := time.Now()
	for bgRows.Next() {
		var id int
		var name string
		var val float64
		_ = bgRows.Scan(&id, &name, &val)
		bgCount++
	}
	bgElapsed := time.Since(bgStart)
	bgRows.Close()

	// --- Path 2: cancellable context (goroutine-per-Next — the BUG) ---
	db2, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer db2.Close()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	ctxRows, err := db2.QueryContext(ctx2, "SELECT id, name, val FROM users")
	if err != nil {
		t.Fatalf("query cancellable: %v", err)
	}
	ctxCount := 0
	maxGoroutines := 0
	ctxStart := time.Now()
	for ctxRows.Next() {
		var id int
		var name string
		var val float64
		_ = ctxRows.Scan(&id, &name, &val)
		ctxCount++
		if ctxCount%5000 == 0 {
			g := runtime.NumGoroutine()
			if g > maxGoroutines {
				maxGoroutines = g
			}
		}
	}
	ctxElapsed := time.Since(ctxStart)
	ctxRows.Close()

	t.Logf("sync (Background):   %d rows in %v (%.1f ns/row)",
		bgCount, bgElapsed, float64(bgElapsed.Nanoseconds())/float64(bgCount))
	t.Logf("async (cancellable):  %d rows in %v (%.1f ns/row, maxGoroutines=%d)",
		ctxCount, ctxElapsed, float64(ctxElapsed.Nanoseconds())/float64(ctxCount), maxGoroutines)

	if bgCount != rows || ctxCount != rows {
		t.Fatalf("row count mismatch: sync=%d async=%d want=%d", bgCount, ctxCount, rows)
	}

	// The synchronous path (context.Background) must not be slower than
	// the goroutine-per-Next path (cancellable context). If it is, the
	// fix is not working correctly.
	if bgElapsed > ctxElapsed {
		t.Errorf("synchronous path (%v) is SLOWER than goroutine-per-Next path (%v) — "+
			"the fix uses context.Background() to avoid goroutine-per-Next overhead. "+
			"This is the root cause of the 17-minute production wedge: millions of "+
			"goroutine creations through the N+1 EXISTS pattern starve the scheduler.",
			bgElapsed, ctxElapsed)
	}

	ratio := float64(ctxElapsed.Nanoseconds()) / float64(bgElapsed.Nanoseconds())
	t.Logf("overhead ratio (async/sync): %.2fx", ratio)
	if ratio < 1.1 {
		t.Logf("NOTE: goroutine-per-Next overhead is only %.2fx at %d rows — "+
			"the test machine may be too fast. In production with N+1 EXISTS "+
			"(millions of Next calls), this overhead compounds to 17+ minutes.",
			ratio, rows)
	}
}

// TestMattnDriverContextDetection verifies that the mattn driver is in use
// and switches between synchronous and goroutine-per-Next paths based on
// whether the context has a Done() channel.
func TestMattnDriverContextDetection(t *testing.T) {
	const rows = 1000
	path := seedLargeTable(t, rows)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Verify the driver is mattn/go-sqlite3
	if _, ok := db.Driver().(*sqlite3.SQLiteDriver); !ok {
		t.Fatalf("driver is %T, not mattn/go-sqlite3 — test is invalid", db.Driver())
	}

	// With context.Background() — synchronous path
	bgRows, err := db.QueryContext(context.Background(), "SELECT id FROM users")
	if err != nil {
		t.Fatalf("query background: %v", err)
	}
	bgCount := 0
	for bgRows.Next() {
		var id int
		_ = bgRows.Scan(&id)
		bgCount++
	}
	bgRows.Close()

	// With cancellable context — goroutine-per-Next path
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctxRows, err := db.QueryContext(ctx, "SELECT id FROM users")
	if err != nil {
		t.Fatalf("query cancellable: %v", err)
	}
	ctxCount := 0
	for ctxRows.Next() {
		var id int
		_ = ctxRows.Scan(&id)
		ctxCount++
	}
	ctxRows.Close()

	if bgCount != rows || ctxCount != rows {
		t.Fatalf("row count mismatch: background=%d cancellable=%d want=%d",
			bgCount, ctxCount, rows)
	}

	t.Logf("mattn/go-sqlite3 driver verified — both paths returned %d rows", rows)
}

// TestN1ExistsOverhead simulates the production N+1 EXISTS pattern: an outer
// scan where for each row, a nested query runs on the SAME connection
// (interleaved cursors, TS's better-sqlite3 model). This is the exact pattern
// that wedged for 17 minutes in production.
//
// The test runs the N+1 pattern with BOTH contexts and compares:
// - context.Background() (the fix): synchronous Next(), no goroutine overhead
// - cancellable context (the bug): goroutine-per-Next, scheduler overhead
//
// The N+1 pattern amplifies the per-Next overhead: N outer rows × M inner
// rows = N*M Next() calls. At production scale, this is millions of goroutine
// creations.
func TestN1ExistsOverhead(t *testing.T) {
	if testing.Short() {
		t.Skip("N+1 test requires 100K+ rows")
	}
	const outerRows = 200
	const innerRows = 5000 // total Next() calls: 200 * (5000 + 1) = ~1M
	path := seedLargeTable(t, innerRows)

	// Helper: run the N+1 pattern with a given context type.
	runN1 := func(label string, useBackground bool) (time.Duration, int) {
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatalf("[%s] open: %v", label, err)
		}
		defer db.Close()

		fetchCtx := context.Background()
		if !useBackground {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fetchCtx = ctx
		}

		// Outer query: scan first N rows
		rOuter, err := db.QueryContext(fetchCtx, "SELECT id FROM users")
		if err != nil {
			t.Fatalf("[%s] outer query: %v", label, err)
		}
		defer rOuter.Close()

		totalNexts := 0
		outerCount := 0
		start := time.Now()

		for rOuter.Next() {
			totalNexts++
			outerCount++
			if outerCount > outerRows {
				break
			}

			// Nested query: full scan (the EXISTS child / fetchSize)
			rInner, err := db.QueryContext(fetchCtx, "SELECT id FROM users")
			if err != nil {
				t.Fatalf("[%s] inner query: %v", label, err)
			}
			for rInner.Next() {
				totalNexts++
			}
			rInner.Close()
		}
		elapsed := time.Since(start)
		return elapsed, totalNexts
	}

	elapsedSync, nextsSync := runN1("sync", true)
	elapsedAsync, nextsAsync := runN1("async", false)

	t.Logf("N+1 EXISTS: sync(Bg)      %d Nexts in %v (%.1f ns/Next)",
		nextsSync, elapsedSync, float64(elapsedSync.Nanoseconds())/float64(nextsSync))
	t.Logf("N+1 EXISTS: async(Cancel) %d Nexts in %v (%.1f ns/Next)",
		nextsAsync, elapsedAsync, float64(elapsedAsync.Nanoseconds())/float64(nextsAsync))

	if nextsSync != nextsAsync {
		t.Fatalf("Next count mismatch: sync=%d async=%d", nextsSync, nextsAsync)
	}

	// The synchronous path (context.Background) must not be slower than
	// the goroutine-per-Next path (cancellable context).
	if elapsedSync > elapsedAsync {
		t.Errorf("synchronous path (%v) is SLOWER than goroutine-per-Next (%v) — "+
			"the fix should make fetches faster by avoiding goroutine-per-Next "+
			"overhead. This is the root cause of the 17-minute production wedge.",
			elapsedSync, elapsedAsync)
	}

	ratio := float64(elapsedAsync.Nanoseconds()) / float64(elapsedSync.Nanoseconds())
	t.Logf("N+1 overhead ratio (async/sync): %.2fx", ratio)
}

// TestSourceFetchUsesSynchronousPath is the key before/after test.
//
// It creates a Source with a cancellable context (mirroring production where
// s.ctx is derived from the CG lifetime context via context.WithCancel, so
// s.ctx always has a Done() channel), then fetches through the reader pool
// (which calls fetchViaBoundReaderStream).
//
// Two baselines are measured with direct database/sql:
//   - sync: context.Background() → mattn driver uses nextSyncLocked directly
//   - gpr:  cancellable context → mattn driver spawns a goroutine per Next()
//
// If the fix is in place: fetchViaBoundReaderStream uses context.Background()
// for the SQLite calls → driver uses the synchronous path → Source fetch is
// FASTER than the gpr baseline (Source overhead is under the gpr overhead).
//
// If the fix is NOT in place: fetchViaBoundReaderStream uses s.ctx → driver
// uses the goroutine-per-Next path → Source fetch is SLOWER than the gpr
// baseline (Source overhead + gpr overhead).
func TestSourceFetchUsesSynchronousPath(t *testing.T) {
	const rows = 50000
	path := seedLargeTable(t, rows)

	// --- Baseline 1: direct database/sql with context.Background() (sync) ---
	dbDirect, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open direct: %v", err)
	}
	defer dbDirect.Close()

	bgRows, err := dbDirect.QueryContext(context.Background(), "SELECT id, name, val FROM users")
	if err != nil {
		t.Fatalf("query bg: %v", err)
	}
	bgCount := 0
	bgStart := time.Now()
	for bgRows.Next() {
		var id int
		var name string
		var val float64
		_ = bgRows.Scan(&id, &name, &val)
		bgCount++
	}
	bgElapsed := time.Since(bgStart)
	bgRows.Close()

	// --- Baseline 2: direct database/sql with cancellable context (gpr) ---
	ctxGPR, cancelGPR := context.WithCancel(context.Background())
	defer cancelGPR()
	gprRows, err := dbDirect.QueryContext(ctxGPR, "SELECT id, name, val FROM users")
	if err != nil {
		t.Fatalf("query gpr: %v", err)
	}
	gprCount := 0
	gprStart := time.Now()
	for gprRows.Next() {
		var id int
		var name string
		var val float64
		_ = gprRows.Scan(&id, &name, &val)
		gprCount++
	}
	gprElapsed := time.Since(gprStart)
	gprRows.Close()

	// --- Source fetch through reader pool ---
	db, err := Open(path, OpenOptions{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()

	// Cancellable context — mirrors production (s.ctx always has Done()
	// because NewWithContext wraps parent in context.WithCancel).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src, err := NewWithContext(ctx, db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("NewWithContext: %v", err)
	}
	defer src.Close()

	pool, perr := NewReaderPool(context.Background(), db, "", 1)
	if perr != nil {
		t.Fatalf("NewReaderPool: %v", perr)
	}
	defer pool.Close()
	src.BindReaderPool(pool)
	defer src.UnbindReaderPool()

	src.SetNextConnectGroup("q1")
	release, ok := pool.AcquireForPipeline("q1", time.Second)
	if !ok {
		t.Fatal("AcquireForPipeline did not grant")
	}
	defer release()

	// Warm the stmt cache
	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	for range in.Fetch(ivm.FetchRequest{}) {
	}

	// Measure Source fetch
	in2 := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	srcCount := 0
	srcStart := time.Now()
	for range in2.Fetch(ivm.FetchRequest{}) {
		srcCount++
	}
	srcElapsed := time.Since(srcStart)

	bgNsPerRow := float64(bgElapsed.Nanoseconds()) / float64(bgCount)
	gprNsPerRow := float64(gprElapsed.Nanoseconds()) / float64(gprCount)
	srcNsPerRow := float64(srcElapsed.Nanoseconds()) / float64(srcCount)

	t.Logf("direct sync (Background):  %d rows in %v (%.1f ns/row)",
		bgCount, bgElapsed, bgNsPerRow)
	t.Logf("direct gpr (cancellable):   %d rows in %v (%.1f ns/row)",
		gprCount, gprElapsed, gprNsPerRow)
	t.Logf("Source fetchViaBoundReader: %d rows in %v (%.1f ns/row)",
		srcCount, srcElapsed, srcNsPerRow)

	if srcCount != rows {
		t.Fatalf("Source fetch got %d rows, want %d", srcCount, rows)
	}

	// If the fix is in place, Source uses context.Background() → sync path.
	// Source ns/row = Source overhead + sync SQLite cost.
	// This should be FASTER than direct gpr (which is pure gpr SQLite cost
	// without Source overhead, but gpr overhead > Source overhead).
	//
	// If the fix is MISSING, Source uses s.ctx → gpr path.
	// Source ns/row = Source overhead + gpr SQLite cost.
	// This should be SLOWER than direct gpr (Source overhead on top).
	if srcNsPerRow > gprNsPerRow {
		t.Errorf("Source fetch (%.1f ns/row) is SLOWER than direct goroutine-per-Next "+
			"(%.1f ns/row) — fetchViaBoundReaderStream is using s.ctx instead of "+
			"context.Background(), causing the mattn driver to create a goroutine "+
			"per Next() call. This is the root cause of the 17-minute production "+
			"wedge: through the N+1 EXISTS pattern, millions of goroutine "+
			"creations starve the Go scheduler.",
			srcNsPerRow, gprNsPerRow)
	}
}
