package engine

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// BenchmarkPerQueryOverhead_Go measures the raw cost of executing a single
// prepared SELECT in Go via database/sql + CGO + mattn/go-sqlite3. This is
// the per-channel probe in the N+1 EXISTS pattern.
//
// The benchmark creates a table with 1000 rows, prepares a parameterized
// SELECT, and executes it N times (once per "channel") — measuring the
// per-query overhead (CGO boundary + database/sql wrapper + sqlite3_step).
func BenchmarkPerQueryOverhead_Go(b *testing.B) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT)`)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO t (id, val) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("ch-%04d", i), fmt.Sprintf("val-%d", i))
	}
	stmt.Close()
	tx.Commit()

	// Prepare the probe query (what EXISTS does internally)
	probe, err := db.Prepare("SELECT 1 FROM t WHERE id = ? LIMIT 1")
	if err != nil {
		b.Fatalf("prepare probe: %v", err)
	}
	defer probe.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		channelIdx := i % 1000
		var dummy int
		row := probe.QueryRow(fmt.Sprintf("ch-%04d", channelIdx))
		row.Scan(&dummy)
	}
}

// TestPerQueryOverhead_Go reports the per-query cost as a test (not benchmark)
// so the number is visible without -bench flags.
func TestPerQueryOverhead_Go(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO t (id, val) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("ch-%04d", i), fmt.Sprintf("val-%d", i))
	}
	stmt.Close()
	tx.Commit()

	probe, err := db.Prepare("SELECT 1 FROM t WHERE id = ? LIMIT 1")
	if err != nil {
		t.Fatalf("prepare probe: %v", err)
	}
	defer probe.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		var dummy int
		probe.QueryRow(fmt.Sprintf("ch-%04d", i)).Scan(&dummy)
	}

	// Measure 1000 queries (the N+1 pattern for 1000 channels)
	N := 1000
	start := time.Now()
	for i := 0; i < N; i++ {
		var dummy int
		probe.QueryRow(fmt.Sprintf("ch-%04d", i)).Scan(&dummy)
	}
	elapsed := time.Since(start)
	perQuery := elapsed / time.Duration(N)

	t.Logf("Go per-query overhead (database/sql + CGO):")
	t.Logf("  %d queries in %v", N, elapsed)
	t.Logf("  per-query: %v (%.3fms)", perQuery, float64(perQuery.Microseconds())/1000.0)
}
