package snapshotter

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// seedReplicaForSnapshotter creates a WAL-mode SQLite file with the
// _zero.replicationState and _zero.changeLog2 tables the Snapshotter
// needs, plus a simple "items" table. Returns the file path.
func seedReplicaForSnapshotter(t *testing.T, stateVersion string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL)`,
		`INSERT INTO "_zero.replicationState" VALUES ('` + stateVersion + `')`,
		`CREATE TABLE "_zero.changeLog2" (
			stateVersion TEXT NOT NULL,
			"table"       TEXT NOT NULL,
			rowKey        TEXT NOT NULL,
			op            TEXT NOT NULL,
			pos           INTEGER NOT NULL
		)`,
		`CREATE TABLE items (
			id          INTEGER PRIMARY KEY,
			name        TEXT,
			_0_version  TEXT
		)`,
	}
	for _, s := range stmts {
		if _, err := w.Exec(s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	return path
}

// openWritableForSnap opens a writable pool on the replica path, suitable
// for passing to snapshotter.New.
func openWritableForSnap(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenWritable: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// dropReplicationState drops the _zero.replicationState table from the
// replica file via a fresh raw connection. This causes selectStateVersion
// to fail on the next resetToHead call.
func dropReplicationState(t *testing.T, path string) {
	t.Helper()
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("drop open: %v", err)
	}
	defer w.Close()
	if _, err := w.Exec(`DROP TABLE "_zero.replicationState"`); err != nil {
		t.Fatalf("drop replicationState: %v", err)
	}
}

// --- BUG-4: resetToHead selectStateVersion failure orphans transaction ---

// TestBUG4_ResetToHead_SelectStateVersionFailure_ClosesConn verifies that
// when selectStateVersion fails inside resetToHead (e.g. the table was
// dropped), the connection is CLOSED and set to nil rather than left with
// an open BEGIN transaction that leaks a writable pool conn.
//
// Before the fix: BEGIN succeeded but selectStateVersion failed → conn left
// with open tx, not closed → leaked writable pool conn + next resetToHead
// silently ROLLBACKs it (only if called; Destroy would also ROLLBACK, but
// the conn itself was never returned to the pool).
func TestBUG4_ResetToHead_SelectStateVersionFailure_ClosesConn(t *testing.T) {
	path := seedReplicaForSnapshotter(t, "v1")
	wdb := openWritableForSnap(t, path)

	snap, err := New(wdb, "testapp")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := snap.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { snap.Destroy() })

	// First Advance creates prev and populates s.prev.
	specs := map[string]*TableSpec{
		"items": {
			Name: "items",
			Columns: map[string]sqlite.ColumnSchema{
				"id":         {Type: "integer"},
				"name":       {Type: "text"},
				"_0_version": {Type: "text"},
			},
			UniqueKeys: [][]string{{"id"}},
		},
	}
	allNames := map[string]bool{"items": true}
	if _, err := snap.Advance(specs, allNames); err != nil {
		t.Fatalf("first Advance: %v", err)
	}

	// Verify prev exists and has a live conn.
	snap.mu.Lock()
	prevConn := snap.prev.conn
	snap.mu.Unlock()
	if prevConn == nil {
		t.Fatal("expected prev.conn to be non-nil after first Advance")
	}

	// Drop the replicationState table so selectStateVersion will fail.
	dropReplicationState(t, path)

	// Second Advance calls resetToHead on prev → ROLLBACK + BEGIN succeed
	// but selectStateVersion fails.
	_, err = snap.Advance(specs, allNames)
	if err == nil {
		t.Fatal("expected Advance to fail after dropping replicationState")
	}

	// BUG-4 assertion: the conn should be CLOSED (nil), not left with an
	// open BEGIN. Before the fix, s.prev.conn was non-nil with an open tx.
	snap.mu.Lock()
	connAfter := snap.prev.conn
	snap.mu.Unlock()
	if connAfter != nil {
		t.Errorf("BUG-4: expected prev.conn to be nil after selectStateVersion "+
			"failure (conn should be closed), but it is non-nil — "+
			"orphaned BEGIN transaction leaks a writable pool conn")
	}
}

// --- BUG-5: Advance nil-dereferences conn after resetToHead failure ---

// TestBUG5_AdvanceAfterResetToHeadFailure_DoesNotPanic verifies that after
// a resetToHead failure sets s.prev.conn = nil, the next Advance call does
// NOT nil-dereference the conn. Instead, it should treat prev as needing a
// fresh connection (freshConn=true) and create a new snapshot.
//
// Before the fix: s.prev.conn == nil → resetToHead called on nil conn →
// nil pointer dereference → panic → CG teardown loop on retry.
func TestBUG5_AdvanceAfterResetToHeadFailure_DoesNotPanic(t *testing.T) {
	path := seedReplicaForSnapshotter(t, "v1")
	wdb := openWritableForSnap(t, path)

	snap, err := New(wdb, "testapp")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := snap.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { snap.Destroy() })

	specs := map[string]*TableSpec{
		"items": {
			Name: "items",
			Columns: map[string]sqlite.ColumnSchema{
				"id":         {Type: "integer"},
				"name":       {Type: "text"},
				"_0_version": {Type: "text"},
			},
			UniqueKeys: [][]string{{"id"}},
		},
	}
	allNames := map[string]bool{"items": true}

	// First Advance: creates prev.
	if _, err := snap.Advance(specs, allNames); err != nil {
		t.Fatalf("first Advance: %v", err)
	}

	// Drop replicationState to make resetToHead fail on the next Advance.
	dropReplicationState(t, path)

	// Second Advance: resetToHead fails, s.prev.conn set to nil.
	_, err = snap.Advance(specs, allNames)
	if err == nil {
		t.Fatal("expected second Advance to fail (replicationState dropped)")
	}

	// Restore replicationState so a new snapshot CAN pin.
	// We need to recreate the table and re-seed it.
	restoreReplicationState(t, path, "v2")

	// BUG-5 assertion: third Advance must NOT panic.
	// Before the fix, s.prev.conn == nil → resetToHead(nil) → panic.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("BUG-5: Advance panicked after resetToHead failure: %v "+
				"(expected self-healing via freshConn)", r)
		}
	}()
	// This should succeed (or fail with a diff error, but NOT panic).
	_, err = snap.Advance(specs, allNames)
	// An error is acceptable here (e.g. diff construction issue); the test
	// asserts NO PANIC, which is the BUG-5 regression condition.
	if err != nil {
		t.Logf("third Advance returned error (acceptable, BUG-5 is about panic): %v", err)
	}
}

// restoreReplicationState recreates the _zero.replicationState table
// after it was dropped, so a new snapshot can pin.
func restoreReplicationState(t *testing.T, path, stateVersion string) {
	t.Helper()
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("restore open: %v", err)
	}
	defer w.Close()
	stmts := []string{
		`CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL)`,
		`INSERT INTO "_zero.replicationState" VALUES ('` + stateVersion + `')`,
	}
	for _, s := range stmts {
		if _, err := w.Exec(s); err != nil {
			t.Fatalf("restore exec %q: %v", s, err)
		}
	}
}

// --- BUG-4 + BUG-5 combined: full self-healing cycle ---

// TestResetToHeadFailure_SelfHeals verifies the complete self-healing
// path: resetToHead fails → conn closed → next Advance creates fresh conn.
// This is the integration of BUG-4 (conn closed) and BUG-5 (no panic).
func TestResetToHeadFailure_SelfHeals(t *testing.T) {
	path := seedReplicaForSnapshotter(t, "v1")
	wdb := openWritableForSnap(t, path)

	snap, err := New(wdb, "testapp")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := snap.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { snap.Destroy() })

	specs := map[string]*TableSpec{
		"items": {
			Name: "items",
			Columns: map[string]sqlite.ColumnSchema{
				"id":         {Type: "integer"},
				"name":       {Type: "text"},
				"_0_version": {Type: "text"},
			},
			UniqueKeys: [][]string{{"id"}},
		},
	}
	allNames := map[string]bool{"items": true}

	// First Advance: creates prev with a live conn.
	if _, err := snap.Advance(specs, allNames); err != nil {
		t.Fatalf("first Advance: %v", err)
	}

	// Break resetToHead by dropping replicationState.
	dropReplicationState(t, path)

	// Second Advance: fails (selectStateVersion error).
	_, err = snap.Advance(specs, allNames)
	if err == nil {
		t.Fatal("expected second Advance to fail")
	}

	// Verify conn was closed (BUG-4).
	snap.mu.Lock()
	connAfterFail := snap.prev.conn
	snap.mu.Unlock()
	if connAfterFail != nil {
		t.Errorf("BUG-4: prev.conn should be nil after failure, got non-nil")
	}

	// Restore replicationState.
	restoreReplicationState(t, path, "v2")

	// Third Advance: should self-heal (BUG-5 — no panic, fresh conn).
	// The snapshotter should treat s.prev as freshConn (conn is nil) and
	// create a new snapshot.
	_, err = snap.Advance(specs, allNames)
	// Error is acceptable (diff issues); panic is not.
	if err != nil {
		t.Logf("third Advance returned error (acceptable): %v", err)
	}

	// Verify the snapshotter is still usable — curr should have a live conn.
	snap.mu.Lock()
	currConn := snap.curr.conn
	snap.mu.Unlock()
	if currConn == nil {
		t.Error("expected curr.conn to be non-nil after self-healing Advance")
	}
}
