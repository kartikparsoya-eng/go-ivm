package main

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestBUG6_GetReplicaDB_SuccessfulCallDoesNotPanic verifies that a successful
// getReplicaDB() call does NOT panic from a double-close of the probe channel.
//
// Before the fix: the LEAK-1 fix added `defer close(probe)` in Phase 2, but
// Phase 3 ALSO called `close(probe)` → double close panic → crashes every
// CG init (every successful getReplicaDB call panics).
//
// This test creates a valid replica file, calls getReplicaDB, and asserts:
//  1. No panic occurs (the double-close would panic)
//  2. The returned *sql.DB is non-nil
//  3. A second call returns the cached DB (no re-probe)
func TestBUG6_GetReplicaDB_SuccessfulCallDoesNotPanic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.db")
	seedReplicaForBUG6(t, path)

	// Override the open timeout so a failure surfaces quickly instead of
	// retrying for 60s (the default).
	oldTimeout := replicaOpenTimeout
	oldBackoff := replicaOpenInitialBackoff
	replicaOpenTimeout = 5 * time.Second
	replicaOpenInitialBackoff = 100 * time.Millisecond
	t.Cleanup(func() {
		replicaOpenTimeout = oldTimeout
		replicaOpenInitialBackoff = oldBackoff
	})

	srv := NewServer(path)
	t.Cleanup(srv.closeAll)

	// The double-close bug would panic here on EVERY successful call.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BUG-6: getReplicaDB panicked on successful call: %v "+
				"(double-close of probe channel)", r)
		}
	}()

	db, err := srv.getReplicaDB()
	if err != nil {
		t.Fatalf("getReplicaDB returned error: %v", err)
	}
	if db == nil {
		t.Fatal("getReplicaDB returned nil db")
	}

	// Second call should return the cached DB (no re-probe, no panic).
	db2, err := srv.getReplicaDB()
	if err != nil {
		t.Fatalf("second getReplicaDB returned error: %v", err)
	}
	if db2 != db {
		t.Errorf("second call returned different *sql.DB (expected cached)")
	}
}

// TestBUG6_GetReplicaDB_ConcurrentCallersNoPanic verifies that concurrent
// callers to getReplicaDB don't trigger a double-close panic. The probe
// channel is shared: the prober closes it via defer, and if Phase 3 also
// closed it, the double-close would race across goroutines.
func TestBUG6_GetReplicaDB_ConcurrentCallersNoPanic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.db")
	seedReplicaForBUG6(t, path)

	oldTimeout := replicaOpenTimeout
	oldBackoff := replicaOpenInitialBackoff
	replicaOpenTimeout = 5 * time.Second
	replicaOpenInitialBackoff = 100 * time.Millisecond
	t.Cleanup(func() {
		replicaOpenTimeout = oldTimeout
		replicaOpenInitialBackoff = oldBackoff
	})

	srv := NewServer(path)
	t.Cleanup(srv.closeAll)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BUG-6: panic in concurrent getReplicaDB: %v", r)
		}
	}()

	// Spin up 4 concurrent callers. The first becomes the prober; the rest
	// wait on the probe channel. With the double-close bug, the prober's
	// defer + Phase 3 both close the channel → panic.
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, err := srv.getReplicaDB()
			errs <- err
		}()
	}
	for i := 0; i < 4; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent getReplicaDB returned error: %v", err)
		}
	}
}

// seedReplicaForBUG6 creates a minimal WAL-mode SQLite replica file with
// the _zero.replicationState table so tablesource.Open and OpenWritable
// succeed (WAL mode is asserted by both Open functions).
func seedReplicaForBUG6(t *testing.T, path string) {
	t.Helper()
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL)`,
		`INSERT INTO "_zero.replicationState" VALUES ('v1')`,
		`CREATE TABLE "_zero.changeLog2" (
			"stateVersion" TEXT NOT NULL,
			"pos" INT NOT NULL,
			"table" TEXT NOT NULL,
			"rowKey" TEXT NOT NULL,
			"op" TEXT NOT NULL,
			PRIMARY KEY("stateVersion","pos")
		)`,
		`CREATE TABLE dummy (id INTEGER PRIMARY KEY, _0_version TEXT)`,
	}
	for _, s := range stmts {
		if _, err := w.Exec(s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
}
