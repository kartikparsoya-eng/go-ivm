package tablesource

// Pins for the reader-shell cache (reader_cache.go) — conn + stmt-cache
// reuse across pool GENERATIONS, the change that deleted the build-slot
// gate and its routine skip-to-serial:
//   - gen-crossing frame safety (converge variant): shells cached from a
//     torn-down pool re-pin to the CURRENT head on the next build — no tx
//     residue can hold them at the old frame (the coread variant lives in
//     reader_cache_coread_test.go, wal2-only);
//   - the prepared-stmt cache survives generations (the steady-state win —
//     compiled bytecode outlives txs and pools);
//   - a leaked-tx shell (the hazard ROLLBACK-before-cache exists for) fails
//     its next BEGIN closed and self-heals to a fresh conn — never a
//     silent mis-pin;
//   - cap overflow closes, TTL sweep closes, the borrowed-readers BUG path
//     never caches.

import (
	"context"
	"testing"
	"time"
)

// bumpReplicaHead advances the seeded replica: stateVersion → newVersion and
// one extra users row, so a stale-frame read is OBSERVABLE (5 rows vs 6).
func bumpReplicaHead(t *testing.T, path, newVersion string) {
	t.Helper()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()
	if _, err := wdb.Exec(`UPDATE "_zero.replicationState" SET stateVersion=?`, newVersion); err != nil {
		t.Fatalf("bump stateVersion: %v", err)
	}
	if _, err := wdb.Exec(`INSERT INTO users VALUES (6, 'frank', 60, 1)`); err != nil {
		t.Fatalf("bump insert: %v", err)
	}
}

// TestReaderShellCache_ReuseAcrossGenerations_ConvergeRepinsToHead is the
// converge half of the gen-crossing red-proof: gen-1 pool → Close (shells
// cached, txs rolled back) → head advances → gen-2 pool builds FROM the
// cached shells and must pin the NEW head — a fresh BEGIN on a reused conn
// lands on the latest frame; any tx residue would hold it at v1. Also pins
// the stmt-cache survival (the cache's steady-state win) and the hit
// accounting.
func TestReaderShellCache_ReuseAcrossGenerations_ConvergeRepinsToHead(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v1")
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	EnableReaderShellCache(db, 8)
	t.Cleanup(func() { CloseReaderShellCache(db) })

	gen1, err := NewReaderPool(context.Background(), db, "", 3)
	if err != nil {
		t.Fatalf("gen-1 NewReaderPool: %v", err)
	}
	if gen1.Version() != "v1" {
		t.Fatalf("gen-1 version = %q, want v1", gen1.Version())
	}
	// Warm a distinctive stmt shape on one reader — survival across the
	// generation crossing is the cache's steady-state win.
	const warmSQL = `SELECT name FROM users WHERE id = ?`
	st, err := gen1.all[0].checkoutStmt(context.Background(), warmSQL)
	if err != nil {
		t.Fatalf("warm checkout: %v", err)
	}
	gen1.all[0].returnStmt(warmSQL, st, true)

	gen1.Close()
	if got := ReaderShellCacheSize(db); got != 3 {
		t.Fatalf("cache size after gen-1 Close = %d, want 3 (healthy teardown must cache every shell)", got)
	}

	bumpReplicaHead(t, path, "v2")

	hits0, _ := ReaderShellCacheCounters()
	gen2, err := NewReaderPool(context.Background(), db, "", 3)
	if err != nil {
		t.Fatalf("gen-2 NewReaderPool: %v", err)
	}
	defer gen2.Close()
	hits1, _ := ReaderShellCacheCounters()
	if d := hits1 - hits0; d < 3 {
		t.Fatalf("cache hits during gen-2 build = %d, want >= 3 (build did not reuse cached shells)", d)
	}
	if got := ReaderShellCacheSize(db); got != 0 {
		t.Fatalf("cache size after gen-2 build = %d, want 0", got)
	}

	// THE gen-crossing frame assertion.
	if gen2.Version() != "v2" {
		t.Fatalf("gen-2 pinned %q, want v2 — a reused shell carried frame/tx residue across generations", gen2.Version())
	}
	// Stmt cache survived — check BEFORE any helper runs its own SQL.
	found := false
	for _, r := range gen2.all {
		if _, ok := r.stmts[warmSQL]; ok {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("warmed stmt shape did not survive the generation crossing — shells must carry their stmt caches")
	}
	// Data check: gen-2 sees the post-bump row set.
	if n := rawScalarInt(t, gen2.all[0], "SELECT count(*) FROM users"); n != 6 {
		t.Fatalf("gen-2 reader saw %d users, want 6 (stale frame?)", n)
	}
}

// TestReaderShellCache_LeakedTxShellFailsClosedAndSelfHeals: a shell cached
// WITH an open, frame-pinning read tx — the exact hazard Close's
// ROLLBACK-before-cache exists to prevent — must fail its next BEGIN closed
// ("cannot start a transaction within a transaction") and be replaced by a
// fresh conn (provisionReader's self-heal), never silently mis-pin the
// stale frame.
func TestReaderShellCache_LeakedTxShellFailsClosedAndSelfHeals(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v1")
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	EnableReaderShellCache(db, 4)
	t.Cleanup(func() { CloseReaderShellCache(db) })

	// Forge the hazard: a shell with an ESTABLISHED read tx (BEGIN + read —
	// the read is what pins the frame) pushed into the cache uncleaned.
	dc, err := rawOpenReaderConn(db)
	if err != nil {
		t.Fatalf("rawOpenReaderConn: %v", err)
	}
	leaked := &poolReader{dc: dc, stmts: map[string]*poolStmt{}}
	if err := leaked.rawExec(context.Background(), "BEGIN"); err != nil {
		t.Fatalf("leak BEGIN: %v", err)
	}
	if ver, err := leaked.readStateVersion(context.Background()); err != nil || ver != "v1" {
		t.Fatalf("leak pin read: ver=%q err=%v", ver, err)
	}
	shellCacheFor(db).put(leaked)

	bumpReplicaHead(t, path, "v2")

	pool, err := NewReaderPool(context.Background(), db, "", 1)
	if err != nil {
		t.Fatalf("build must self-heal past the leaked-tx shell, got: %v", err)
	}
	defer pool.Close()
	if pool.Version() != "v2" {
		t.Fatalf("pool pinned %q, want v2 — the leaked-tx shell mis-pinned the stale frame instead of failing closed", pool.Version())
	}
	if got := ReaderShellCacheSize(db); got != 0 {
		t.Fatalf("cache size = %d, want 0 (the poisoned shell must be consumed and closed)", got)
	}
}

// TestReaderShellCache_CapOverflowAndTTLSweep pins the two bounds: a Close
// past the cap closes the overflow instead of growing, and the TTL sweep
// (reaper tick) closes idle shells — maxAge<=0 drains everything
// (closeAll's drop-all shape).
func TestReaderShellCache_CapOverflowAndTTLSweep(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v1")
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	EnableReaderShellCache(db, 2)
	t.Cleanup(func() { CloseReaderShellCache(db) })

	pool, err := NewReaderPool(context.Background(), db, "", 4)
	if err != nil {
		t.Fatalf("NewReaderPool: %v", err)
	}
	pool.Close()
	if got := ReaderShellCacheSize(db); got != 2 {
		t.Fatalf("cache size = %d, want 2 (cap must close the overflow)", got)
	}
	if n := SweepReaderShellCache(db, time.Hour); n != 0 {
		t.Fatalf("fresh shells swept by a 1h TTL: %d", n)
	}
	if n := SweepReaderShellCache(db, 0); n != 2 {
		t.Fatalf("drop-all sweep closed %d, want 2", n)
	}
	if got := ReaderShellCacheSize(db); got != 0 {
		t.Fatalf("cache size after drop-all = %d, want 0", got)
	}
	// The cache stays usable after a sweep.
	pool2, err := NewReaderPool(context.Background(), db, "", 1)
	if err != nil {
		t.Fatalf("post-sweep build: %v", err)
	}
	pool2.Close()
	if got := ReaderShellCacheSize(db); got != 1 {
		t.Fatalf("cache size after post-sweep Close = %d, want 1", got)
	}
}

// TestReaderShellCache_BorrowedBugPathNeverCaches: Close with a reader still
// borrowed is a lifecycle BUG — everything closes outright; nothing may
// enter the cache (a cached shell must be provably idle).
func TestReaderShellCache_BorrowedBugPathNeverCaches(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v1")
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	EnableReaderShellCache(db, 4)
	t.Cleanup(func() { CloseReaderShellCache(db) })

	pool, err := NewReaderPool(context.Background(), db, "", 2)
	if err != nil {
		t.Fatalf("NewReaderPool: %v", err)
	}
	if _, ok := pool.AcquireForPipeline("q1", time.Second); !ok {
		t.Fatal("AcquireForPipeline did not grant")
	}
	// Deliberately no release — the BUG shape.
	pool.Close()
	if got := ReaderShellCacheSize(db); got != 0 {
		t.Fatalf("BUG-path Close cached %d shell(s) — a borrowed pool must close outright", got)
	}
}
