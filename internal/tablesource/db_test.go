package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// seedReplica creates a WAL-mode SQLite file at a temp path, writes a
// few rows, and returns the path. Mirrors what the TS replicator would
// have produced before our reader connects.
func seedReplica(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	writer, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer writer.Close()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
	} {
		if _, err := writer.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	return path
}

func TestOpenSucceedsOnWALReplica(t *testing.T) {
	path := seedReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
}

func TestOpenRejectsNonWALDatabase(t *testing.T) {
	// Build a journal-mode database — no WAL switch.
	path := filepath.Join(t.TempDir(), "rollback.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if _, err := w.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	w.Close()

	if _, err := Open(path, OpenOptions{}); err == nil {
		t.Fatalf("Open succeeded on non-WAL database; expected error")
	}
}

func TestOpenRejectsEmptyPath(t *testing.T) {
	if _, err := Open("", OpenOptions{}); err == nil {
		t.Fatalf("Open with empty path succeeded; expected error")
	}
}

func TestQueryOnlyBlocksWrites(t *testing.T) {
	path := seedReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("INSERT INTO t (id, name) VALUES (99, 'x')"); err == nil {
		t.Fatalf("write succeeded against query_only pool; expected SQLITE_READONLY")
	}
}

// TestConcurrentReaders is the parallelization smoke test for the design's
// hard constraint. If WAL or the pool were misconfigured, this would
// either deadlock or serialize. It should fan out without contention.
func TestConcurrentReaders(t *testing.T) {
	path := seedReplica(t)
	db, err := Open(path, OpenOptions{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const goroutines = 8
	const iters = 200
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				var n int
				if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
					errs <- err
					return
				}
				if n != 3 {
					errs <- fmt.Errorf("count = %d, want 3", n)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent reader failed: %v", err)
	}
}

// TestConnMaxIdleReclaimsConnections is the regression guard for the ART
// memory-growth finding: without SetConnMaxIdleTime, a load burst that
// fans the pool out to many conns leaves up to MaxIdleConns of them parked
// FOREVER (each pinning an fd + C-side page cache), so RSS never comes back
// down. With a short ConnMaxIdle the pool cleaner must close the idle
// surplus. We assert via database/sql's own Stats().Idle (proxy for the
// live-conn/fd/page-cache count) so the test needs no /proc introspection.
func TestConnMaxIdleReclaimsConnections(t *testing.T) {
	path := seedReplica(t)
	db, err := Open(path, OpenOptions{
		MaxOpenConns: 16,
		MaxIdleConns: 16,
		ConnMaxIdle:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Burst: open several conns concurrently (each blocks holding a conn
	// until all have started), forcing the pool to grow past 1.
	var wg sync.WaitGroup
	release := make(chan struct{})
	const burst = 8
	started := make(chan struct{}, burst)
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := db.Conn(context.Background())
			if err != nil {
				return
			}
			defer conn.Close()
			started <- struct{}{}
			<-release // hold the conn so the pool must open a fresh one per goroutine
		}()
	}
	for i := 0; i < burst; i++ {
		<-started
	}
	close(release)
	wg.Wait()

	if got := db.Stats().Idle; got == 0 {
		t.Fatalf("expected idle conns parked after the burst, got 0")
	}

	// The pool cleaner runs on ~ConnMaxIdle cadence; give it a few cycles.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db.Stats().Idle <= 1 {
			return // reclaimed down to the steady-state floor
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("idle conns not reclaimed: Stats().Idle=%d after 5s (ConnMaxIdle leak)", db.Stats().Idle)
}

// TestConnMaxIdleDisabledParksConnections pins the opt-out (negative
// ConnMaxIdle): the pre-fix behavior where idle conns are retained. This is
// what the leak looked like — kept as an explicit A/B anchor.
func TestConnMaxIdleDisabledParksConnections(t *testing.T) {
	path := seedReplica(t)
	db, err := Open(path, OpenOptions{
		MaxOpenConns: 8,
		MaxIdleConns: 8,
		ConnMaxIdle:  -1, // disable the deadline
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := db.Conn(context.Background())
			if err != nil {
				return
			}
			defer conn.Close()
			started <- struct{}{}
			<-release
		}()
	}
	for i := 0; i < 4; i++ {
		<-started
	}
	close(release)
	wg.Wait()

	// With the deadline disabled, the idle conns stay parked.
	time.Sleep(500 * time.Millisecond)
	if db.Stats().Idle == 0 {
		t.Fatalf("with ConnMaxIdle disabled, idle conns should remain parked, got 0")
	}
}
