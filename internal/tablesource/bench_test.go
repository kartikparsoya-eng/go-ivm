package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

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

// BenchmarkTxCacheAcquire measures the per-CG-tx overhead (mutex + map
// lookup, no SQL). Same epoch / same CG every call — should be cheap
// since the tx is reused.
func BenchmarkTxCacheAcquire(b *testing.B) {
	path := seedReplicaLarge(b, 10)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cache := NewTxCache(db)
	defer cache.Close()
	ctx := context.Background()

	// Prime the cache so the first Acquire (which BEGINs a tx) doesn't
	// pollute the measurement.
	_, rel, err := cache.Acquire(ctx, "cg-a", 1)
	if err != nil {
		b.Fatalf("prime: %v", err)
	}
	rel()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rel, err := cache.Acquire(ctx, "cg-a", 1)
		if err != nil {
			b.Fatalf("Acquire: %v", err)
		}
		rel()
	}
}
