//go:build libsqlite3

package main

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/internal/snapshotter"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
)

// TestBuildReaderPool_FallsBackOnNonWal2 is the DECISION-level companion to
// tablesource.TestCoRead_NonWal2Errors (which isolates the primitive trigger).
// It proves buildReaderPoolLocked's coread-fast / converge-fallback contract:
// when the anchor connection is a plain-wal (non-wal2) frame, the co-read
// capture errors — the wal layer guards with
//
//	if( !isWalMode2(pAnchor) ) return SQLITE_ERROR;   // co-read is wal2-only
//
// (sqlite3.c, sqlite3WalCoReadGet) — and buildReaderPoolLocked MUST fall
// through to the shipped NewReaderPool (converge-upward) rather than crash or
// hand back a pool bogusly pinned to a frame the DB cannot co-read.
//
// Load-bearing assertion: the returned *CoRead is nil. buildReaderPoolLocked
// returns a non-nil coread ONLY on the coread-fast success path
// (advance_to_head.go:153); EVERY fallback returns nil. So coread==nil is
// exactly "we fell back," while pool!=nil && err==nil is "the fallback still
// built a working pool." A non-nil coread here would mean K readers were
// mis-pinned to a wal2 frame on a database that has none.
//
// The build tag is deliberate: this must link the real libsqlite3 co-read
// symbols so CaptureCoReadFromConn exercises the actual C guard, not the
// !libsqlite3 stub (which errors unconditionally and so could not tell a
// genuine non-wal2 rejection apart from a missing implementation). It runs via
// scripts/test-coread-local.sh alongside the tablesource co-read suite.
func TestBuildReaderPool_FallsBackOnNonWal2(t *testing.T) {
	// makeReplica (advance_to_head_test.go) seeds a plain-WAL replica
	// (_journal_mode=WAL, NOT WAL2) at version 0000000001 — exactly the mode
	// the fallback exists for. The writer handle is unused here.
	path, _ := makeReplica(t)

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	// hydrateReaders MUST be >1, else buildReaderPoolLocked short-circuits to
	// (nil,nil,nil) "feature off" and never reaches the coread/fallback branch
	// at all — the test would pass vacuously.
	srv.hydrateReaders = 8
	t.Cleanup(srv.closeAll)

	// Open + cache the read and writable pools the same way production does, so
	// buildReaderPoolLocked's internal getReplicaDB() hits the cache.
	if _, err := srv.getReplicaDB(); err != nil {
		t.Fatalf("getReplicaDB: %v", err)
	}
	writableDB := srv.getReplicaWritableDB()
	if writableDB == nil {
		t.Fatal("getReplicaWritableDB: nil writable pool after successful getReplicaDB")
	}

	// Build a real, NON-NIL anchor snapshot over the plain-wal replica. Its
	// Conn() holds a pinned read tx (plain BEGIN — beginAndPin falls back off
	// BEGIN CONCURRENT on non-wal2). Passing a non-nil cur is what makes
	// buildReaderPoolLocked ENTER the coread-fast path and actually call
	// CaptureCoReadFromConn(cur.Conn()) — i.e. exercise the fall-through, not
	// skip the whole branch (which a nil cur would do).
	snap, err := snapshotter.New(writableDB, "myapp")
	if err != nil {
		t.Fatalf("snapshotter.New: %v", err)
	}
	t.Cleanup(snap.Destroy)
	if err := snap.Init(); err != nil {
		t.Fatalf("snapshotter.Init: %v", err)
	}
	cur, err := snap.Current()
	if err != nil {
		t.Fatalf("snapshotter.Current: %v", err)
	}
	// Sanity: the anchor really is pinned at the v1 head. A drifted anchor
	// could mask the fallback assertion behind an unrelated version mismatch.
	if got := cur.Version(); got != "0000000001" {
		t.Fatalf("anchor pinned at %q, want 0000000001", got)
	}

	pool, coread, err := srv.buildReaderPoolLocked(cur)
	if err != nil {
		t.Fatalf("buildReaderPoolLocked: unexpected error (should fall back cleanly): %v", err)
	}
	if pool == nil {
		t.Fatal("buildReaderPoolLocked returned nil pool — fallback NewReaderPool did not build one")
	}
	t.Cleanup(pool.Close)

	// THE load-bearing assertion: coread==nil proves we took a fallback path,
	// NOT the coread-fast success path (the only place a non-nil coread is
	// returned, advance_to_head.go:153).
	if coread != nil {
		coread.Free()
		t.Fatal("buildReaderPoolLocked returned a non-nil coread on a non-wal2 DB — " +
			"the co-read capture should have errored and fallen back to NewReaderPool")
	}

	// The fallback pool still converges to the replica head (v1). Guards against
	// a vacuous pass where the fallback hands back an unusable/empty pool.
	if got := pool.Version(); got != "0000000001" {
		t.Fatalf("fallback pool at version %q, want 0000000001 (replica head)", got)
	}
}
