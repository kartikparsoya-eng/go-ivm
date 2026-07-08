//go:build libsqlite3

package tablesource

// The coread half of the gen-crossing frame red-proof (the converge half
// lives in reader_cache_test.go): shells cached from a torn-down COREAD pool
// must re-arm cleanly onto a NEW anchor's frame. The C-level guarantee is
// that sqlite3_wal2_coread_open is arm→begin→disarm ATOMIC inside the call
// (sqlite3.c:193001-193008) — pWal->pCoRead never survives it — so a cached
// shell carries no dangling arm to a freed handle. This test pins that
// guarantee at the Go level so it survives a wal2-fork rebase.

import (
	"context"
	"database/sql"
	"testing"
)

func TestCoReadPool_CachedShellsRepinToNewAnchorFrame(t *testing.T) {
	path := seedWal2Replica(t, "v1")
	skipIfNotWal2(t, path)
	ctx := context.Background()

	poolDB, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer poolDB.Close()
	EnableReaderShellCache(poolDB, 8)
	t.Cleanup(func() { CloseReaderShellCache(poolDB) })

	// Generation 1: anchor pinned at v1, coread pool armed to it.
	anchor1DB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("anchor1 open: %v", err)
	}
	defer anchor1DB.Close()
	a1, err := anchor1DB.Conn(ctx)
	if err != nil {
		t.Fatalf("anchor1 conn: %v", err)
	}
	defer a1.Close()
	if _, err := a1.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("anchor1 BEGIN: %v", err)
	}
	var n int
	if err := a1.QueryRowContext(ctx, "SELECT count(*) FROM users").Scan(&n); err != nil {
		t.Fatalf("anchor1 warm: %v", err)
	}
	cr1, err := CaptureCoReadFromConn(a1)
	if err != nil {
		t.Fatalf("capture gen-1: %v", err)
	}
	gen1, err := NewCoReadReaderPool(ctx, poolDB, cr1, 3)
	if err != nil {
		cr1.Free()
		t.Fatalf("gen-1 NewCoReadReaderPool: %v", err)
	}
	if gen1.Version() != "v1" {
		t.Fatalf("gen-1 version = %q, want v1", gen1.Version())
	}

	// Teardown-to-cache, in production order: pool first (shells rolled
	// back + cached), then the coread handle, then the anchor's tx.
	gen1.Close()
	if got := ReaderShellCacheSize(poolDB); got != 3 {
		t.Fatalf("cache size after gen-1 Close = %d, want 3", got)
	}
	cr1.Free()
	if _, err := a1.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("anchor1 ROLLBACK: %v", err)
	}

	// Head advances past the gen-1 frame.
	writer, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("writer open: %v", err)
	}
	defer writer.Close()
	if _, err := writer.Exec(`UPDATE "_zero.replicationState" SET stateVersion='v2'`); err != nil {
		t.Fatalf("writer bump: %v", err)
	}
	if _, err := writer.Exec(`INSERT INTO users VALUES (4, 'dave', 60)`); err != nil {
		t.Fatalf("writer insert: %v", err)
	}

	// Generation 2: NEW anchor pinned at v2, pool built FROM the cached
	// shells and armed to the new frame.
	anchor2DB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("anchor2 open: %v", err)
	}
	defer anchor2DB.Close()
	a2, err := anchor2DB.Conn(ctx)
	if err != nil {
		t.Fatalf("anchor2 conn: %v", err)
	}
	defer a2.Close()
	if _, err := a2.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("anchor2 BEGIN: %v", err)
	}
	if err := a2.QueryRowContext(ctx, "SELECT count(*) FROM users").Scan(&n); err != nil {
		t.Fatalf("anchor2 warm: %v", err)
	}
	cr2, err := CaptureCoReadFromConn(a2)
	if err != nil {
		t.Fatalf("capture gen-2: %v", err)
	}
	defer cr2.Free()

	hits0, _ := ReaderShellCacheCounters()
	gen2, err := NewCoReadReaderPool(ctx, poolDB, cr2, 3)
	if err != nil {
		t.Fatalf("gen-2 NewCoReadReaderPool (from cached shells): %v", err)
	}
	defer gen2.Close()
	hits1, _ := ReaderShellCacheCounters()
	if d := hits1 - hits0; d < 3 {
		t.Fatalf("cache hits during gen-2 build = %d, want >= 3", d)
	}

	// THE pin: every cached shell re-armed onto the NEW anchor's frame —
	// no arm/tx residue from generation 1.
	if gen2.Version() != "v2" {
		t.Fatalf("gen-2 pinned %q, want v2 — a cached shell carried gen-1 arm/tx residue", gen2.Version())
	}
	if cnt := rawScalarInt(t, gen2.all[0], "SELECT count(*) FROM users"); cnt != 4 {
		t.Fatalf("gen-2 reader saw %d users, want 4 (v2 frame)", cnt)
	}
}
