package tablesource

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
	_ "github.com/mattn/go-sqlite3"
)

// seedReplicaLarge builds a WAL-mode database with `rows` rows so bench
// runs aren't entirely dominated by page-cache cold misses on a 3-row
// fixture. PK is INTEGER so SQLite uses the rowid index directly.
func seedReplicaLarge(b *testing.B, rows int) string {
	b.Helper()
	path := filepath.Join(b.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		b.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
	} {
		if _, err := w.Exec(stmt); err != nil {
			b.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	tx, err := w.Begin()
	if err != nil {
		b.Fatalf("seed begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO t (id, name, val) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("seed prep: %v", err)
	}
	for i := 1; i <= rows; i++ {
		if _, err := stmt.Exec(i, fmt.Sprintf("row-%d", i), i*7); err != nil {
			b.Fatalf("seed insert %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatalf("seed commit: %v", err)
	}
	return path
}

// BenchmarkOpenAndClose measures cold-start cost — what the sidecar pays
// at startup or on shared-sidecar reconnect. Includes WAL assertion.
func BenchmarkOpenAndClose(b *testing.B) {
	path := seedReplicaLarge(b, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := Open(path, OpenOptions{})
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		db.Close()
	}
}

// BenchmarkPointReadHotCache measures per-row leaf cost with pages
// warm — the main number to compare vs MemorySource's ~50ns map lookup.
// Reads roll across distinct ids so we stay in the page cache but don't
// alias one row's plan path repeatedly.
func BenchmarkPointReadHotCache(b *testing.B) {
	const rows = 1000
	path := seedReplicaLarge(b, rows)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Warm the pages so we measure steady-state, not first-touch I/O.
	var sink int
	for i := 1; i <= rows; i++ {
		_ = db.QueryRow("SELECT val FROM t WHERE id = ?", i).Scan(&sink)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := (i % rows) + 1
		if err := db.QueryRow("SELECT val FROM t WHERE id = ?", id).Scan(&sink); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

// BenchmarkRangeScan reflects a typical IVM query — ORDER BY + LIMIT
// over a small slice of rows. Tests the cost of a real Scan loop, not
// just one round-trip.
func BenchmarkRangeScan(b *testing.B) {
	const rows = 1000
	const limit = 100
	path := seedReplicaLarge(b, rows)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()
	// Warm.
	if _, err := db.Exec("SELECT COUNT(*) FROM t"); err != nil {
		b.Fatalf("warm: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rs, err := db.Query("SELECT id, name, val FROM t ORDER BY id LIMIT ?", limit)
		if err != nil {
			b.Fatalf("query: %v", err)
		}
		var id, val int
		var name string
		n := 0
		for rs.Next() {
			if err := rs.Scan(&id, &name, &val); err != nil {
				rs.Close()
				b.Fatalf("scan: %v", err)
			}
			n++
		}
		rs.Close()
		if n != limit {
			b.Fatalf("scanned %d, want %d", n, limit)
		}
	}
}

// BenchmarkConcurrentReaders uses b.RunParallel to scale across cores.
// If WAL or the pool were misconfigured, ns/op rises with goroutine
// count (or worse, the bench deadlocks). With them right, ns/op should
// stay roughly flat or improve.
func BenchmarkConcurrentReaders(b *testing.B) {
	const rows = 1000
	path := seedReplicaLarge(b, rows)
	db, err := Open(path, OpenOptions{MaxOpenConns: 16, MaxIdleConns: 16})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Warm.
	var sink int
	for i := 1; i <= rows; i++ {
		_ = db.QueryRow("SELECT val FROM t WHERE id = ?", i).Scan(&sink)
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var local int
		i := 0
		for pb.Next() {
			id := (i % rows) + 1
			if err := db.QueryRow("SELECT val FROM t WHERE id = ?", id).Scan(&local); err != nil {
				b.Fatalf("read: %v", err)
			}
			i++
		}
	})
}

// TxCache benchmark removed in Phase 7b — TxCache is retired in favor
// of the snapshot-pinning model (see DESIGN-snapshot-open.md). Per-
// Source mutex contention was the bottleneck identified by the Phase 6
// profile; the new model replaces it with per-call conn from the pool
// pinned via sqlite3_snapshot_open, eliminating the mutex entirely.

// BenchmarkSourceFetchRepeated drives the REAL Source.fetchForConn read path
// (prev-tx conn + BuildSelectQuery + Scan loop + FromSQLiteType) rather than a
// raw *sql.DB. The live read-path profile put fetchForConn at 63% of sidecar
// CPU, with sqlite3_prepare_v2 = 14.6% of cgo time because database/sql's
// one-shot QueryContext re-compiles the statement on every call. Repeated
// identical-shape fetches model the steady-state advance/hydrate cadence (the
// SQL text is constant — BuildSelectQuery parameterizes all values), so this
// is the benchmark that exposes whether that prepare is amortized by a
// per-(conn,SQL) prepared-statement cache. The prev conn is stable across all
// iterations (no OnAdvanceEnd between fetches), exactly like the Snapshotter's
// leapfrogged frame conn in production.
func BenchmarkSourceFetchRepeated(b *testing.B) {
	const rows = 1000
	path := seedReplicaLarge(b, rows)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb, err := OpenWritable(path, OpenOptions{})
	if err != nil {
		b.Fatalf("OpenWritable: %v", err)
	}
	defer wdb.Close()

	cols := map[string]sqlite.ColumnSchema{
		"id":   {Type: "number"},
		"name": {Type: "string"},
		"val":  {Type: "number"},
	}
	src, err := New(db, wdb, "t", cols, []string{"id"})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer src.Close()
	in := src.Connect(nil, nil, nil, nil)

	// Sweep fetch sizes: the prepared-statement cache eliminates a CONSTANT
	// per-fetch sqlite3_prepare_v2, so its relative win grows as the per-row
	// work shrinks. limit=1 models the high-frequency small-advance path;
	// limit=100 the hydrate/dashboard path. req.Limit early-breaks the Scan
	// loop Go-side (source.go), so a small limit genuinely scans fewer rows.
	for _, limit := range []int{1, 10, 100} {
		req := ivm.FetchRequest{Limit: limit}
		// Establish the prev tx + warm pages + (for the cached variant) prime
		// the statement cache so we measure steady state, not first-touch.
		if got := len(in.Fetch(req)); got != limit {
			b.Fatalf("warm fetch got %d rows, want %d", got, limit)
		}
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := len(in.Fetch(req)); got != limit {
					b.Fatalf("fetch got %d rows, want %d", got, limit)
				}
			}
		})
	}
}
