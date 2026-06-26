//go:build libsqlite3

package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// seedWal2Replica seeds a wal2-mode replica with the _zero.replicationState
// table and a users table. Returns the file path. Fails if the SQLite engine
// doesn't support wal2 (will return "wal" instead of "wal2" — the test should
// call skipIfNotWal2 and skip rather than fail).
func seedWal2Replica(t *testing.T, stateVersion string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	stmts := []string{
		"PRAGMA journal_mode=wal2",
		`CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL)`,
		`INSERT INTO "_zero.replicationState" VALUES ('` + stateVersion + `')`,
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`,
		`INSERT INTO users VALUES (1, 'alice', 90), (2, 'bob', 80), (3, 'carol', 70)`,
	}
	for _, s := range stmts {
		if _, err := w.Exec(s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	return path
}

// skipIfNotWal2 checks that the database at path is in wal2 journal mode and
// skips the test if it's not (system SQLite without the rocicorp wal2 patch).
func skipIfNotWal2(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("skip check open: %v", err)
	}
	defer db.Close()
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("skip check pragma: %v", err)
	}
	if mode != "wal2" {
		t.Skipf("database is in %q mode, not wal2 — coread requires the rocicorp wal2 patch", mode)
	}
}

// TestCoReadPool_PinsAllToAnchorFrame: a coread pool built from an anchor's
// frame opens K readers, all latched to the same wal2 frame. After a writer
// advances the head, all K readers still see the anchor's snapshot (3 rows,
// not 4). Also runs PRAGMA integrity_check on a reader.
func TestCoReadPool_PinsAllToAnchorFrame(t *testing.T) {
	path := seedWal2Replica(t, "v1")
	skipIfNotWal2(t, path)

	poolDB, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer poolDB.Close()

	// Anchor: separate writable DB, conn with a read tx.
	anchorDB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("anchor open: %v", err)
	}
	defer anchorDB.Close()

	anchorConn, err := anchorDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("anchor conn: %v", err)
	}
	defer anchorConn.Close()

	if _, err := anchorConn.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatalf("anchor BEGIN: %v", err)
	}
	// Warm the read tx so SQLite assigns a wal2 frame.
	var n int
	if err := anchorConn.QueryRowContext(context.Background(),
		"SELECT count(*) FROM users").Scan(&n); err != nil {
		t.Fatalf("anchor warm: %v", err)
	}
	if n != 3 {
		t.Fatalf("anchor saw %d rows, want 3", n)
	}

	// Capture coread from the anchor conn.
	cr, err := CaptureCoReadFromConn(anchorConn)
	if err != nil {
		t.Fatalf("CaptureCoReadFromConn: %v", err)
	}
	defer cr.Free()

	// Build the coread reader pool.
	const k = 4
	pool, err := NewCoReadReaderPool(context.Background(), poolDB, cr, k)
	if err != nil {
		t.Fatalf("NewCoReadReaderPool: %v", err)
	}
	defer pool.Close()

	if len(pool.all) != k {
		t.Fatalf("pool has %d readers, want %d", len(pool.all), k)
	}

	// Writer advances the head: insert a 4th row.
	writerDB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("writer open: %v", err)
	}
	defer writerDB.Close()
	if _, err := writerDB.Exec("INSERT INTO users VALUES (4, 'dave', 60)"); err != nil {
		t.Fatalf("writer insert: %v", err)
	}

	// All K readers should still see 3 rows (anchor's frame), not 4.
	borrowed := make([]*poolReader, 0, k)
	for i := 0; i < k; i++ {
		r, aerr := pool.acquire(context.Background())
		if aerr != nil {
			t.Fatalf("acquire %d: %v", i, aerr)
		}
		var count int
		if qerr := r.conn.QueryRowContext(context.Background(),
			"SELECT count(*) FROM users").Scan(&count); qerr != nil {
			t.Fatalf("reader %d query: %v", i, qerr)
		}
		if count != 3 {
			t.Fatalf("reader %d saw %d users, want 3 (anchor's frame)", i, count)
		}
		borrowed = append(borrowed, r)
	}
	for _, r := range borrowed {
		pool.release(r)
	}

	// PRAGMA integrity_check on one reader.
	r, _ := pool.acquire(context.Background())
	defer pool.release(r)
	var integrity string
	if err := r.conn.QueryRowContext(context.Background(),
		"PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity_check = %q, want ok", integrity)
	}
}

// TestCoReadPool_ConcurrentReads: many goroutines fetch through the coread
// pool concurrently, all getting the correct result (3 rows from the anchor's
// frame). Run under -race to prove the coread read path has no data races.
func TestCoReadPool_ConcurrentReads(t *testing.T) {
	path := seedWal2Replica(t, "v7")
	skipIfNotWal2(t, path)

	poolDB, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer poolDB.Close()

	anchorDB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("anchor open: %v", err)
	}
	defer anchorDB.Close()

	anchorConn, err := anchorDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("anchor conn: %v", err)
	}
	defer anchorConn.Close()

	if _, err := anchorConn.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatalf("anchor BEGIN: %v", err)
	}
	var n int
	if err := anchorConn.QueryRowContext(context.Background(),
		"SELECT count(*) FROM users").Scan(&n); err != nil {
		t.Fatalf("anchor warm: %v", err)
	}

	cr, err := CaptureCoReadFromConn(anchorConn)
	if err != nil {
		t.Fatalf("CaptureCoReadFromConn: %v", err)
	}
	defer cr.Free()

	const k = 8
	pool, err := NewCoReadReaderPool(context.Background(), poolDB, cr, k)
	if err != nil {
		t.Fatalf("NewCoReadReaderPool: %v", err)
	}
	defer pool.Close()

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, aerr := pool.acquire(context.Background())
			if aerr != nil {
				errs[idx] = aerr
				return
			}
			defer pool.release(r)
			var count int
			if qerr := r.conn.QueryRowContext(context.Background(),
				"SELECT count(*) FROM users").Scan(&count); qerr != nil {
				errs[idx] = qerr
				return
			}
			if count != 3 {
				errs[idx] = fmt.Errorf("goroutine %d saw %d rows, want 3", idx, count)
			}
		}(g)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
}

// TestCoRead_CaptureOpenFree: basic capture→open→free chain via the public
// Go binding (CaptureCoRead + OpenAtCoRead + Free), mirroring the C smoke
// test. Proves the Go cgo wrapper correctly forwards to the C API.
func TestCoRead_CaptureOpenFree(t *testing.T) {
	path := seedWal2Replica(t, "v1")
	skipIfNotWal2(t, path)

	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Capture: opens its own conn + read tx, warms, captures coread.
	cr, err := CaptureCoRead(context.Background(), db)
	if err != nil {
		t.Fatalf("CaptureCoRead: %v", err)
	}
	defer cr.Free()

	// OpenAtCoRead: opens a new conn, BEGINs, arms onto the coread's frame.
	tx, conn, err := OpenAtCoRead(context.Background(), db, cr)
	if err != nil {
		t.Fatalf("OpenAtCoRead: %v", err)
	}
	// Rollback must happen before conn.Close, so use a single defer.
	defer func() {
		_ = tx.Rollback()
		_ = conn.Close()
	}()

	// The armed reader should see the anchored data (3 rows).
	var count int
	if err := tx.QueryRowContext(context.Background(),
		"SELECT count(*) FROM users").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 3 {
		t.Fatalf("OpenAtCoRead saw %d rows, want 3", count)
	}
}

// seedWal2ReplicaCrossTable seeds a wal2 replica with _zero.replicationState
// (stateVersion), a users table (3 rows) and an orders table (2 rows). Used by
// the cold-hydrate cross-table pin test to prove all K readers latch a single
// consistent cut spanning BOTH tables — the skew a single-table test can't see.
func seedWal2ReplicaCrossTable(t *testing.T, stateVersion string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	stmts := []string{
		"PRAGMA journal_mode=wal2",
		`CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL)`,
		`INSERT INTO "_zero.replicationState" VALUES ('` + stateVersion + `')`,
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO users VALUES (1,'alice'),(2,'bob'),(3,'carol')`,
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amt INTEGER)`,
		`INSERT INTO orders VALUES (10,1,100),(20,2,200)`,
	}
	for _, s := range stmts {
		if _, err := w.Exec(s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	return path
}

// TestCoReadPool_ColdHydrateCrossTablePin is the faithful reproduction of the
// production cold-hydrate seam (refreshSnapForInitialHydrateLocked →
// buildReaderPoolLocked coread-fast path). Unlike the primitive tests above, it
// exercises the three things the real wiring depends on:
//
//  1. CAPTURE-FROM-ANCHOR: the coread is captured from a conn pinned exactly
//     like the snapshotter's curr — BEGIN CONCURRENT plus a load-bearing read of
//     _zero.replicationState (the read, not BEGIN CONCURRENT, is what assigns the
//     wal2 frame). This is the literal CaptureCoReadFromConn(cur.Conn()) call.
//  2. ALIGNMENT GATE: refreshSnapForInitialHydrateLocked binds the pool ONLY when
//     pool.Version() == cur.Version(); we assert that equality holds.
//  3. CROSS-TABLE PIN BEHIND HEAD: the head is advanced across BOTH tables (and
//     stateVersion bumped) BEFORE the pool is built. A converge-upward pool built
//     at this point lands on the NEW head (asserted via the negative control);
//     the coread pool instead latches the anchor's OLDER frame, so every one of
//     the K readers observes the anchor's consistent cut (users=3 AND orders=2
//     AND stateVersion=v1). This isolates the TOCTOU coread-fast eliminates — a
//     single-table test, or one that writes after the pool is built, can't see it.
func TestCoReadPool_ColdHydrateCrossTablePin(t *testing.T) {
	const anchorVersion = "v1"
	path := seedWal2ReplicaCrossTable(t, anchorVersion)
	skipIfNotWal2(t, path)
	ctx := context.Background()

	// Read pool — proxy for s.getReplicaDB() (the _query_only read pool the
	// coread readers are drawn from).
	poolDB, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open pool: %v", err)
	}
	defer poolDB.Close()

	// Anchor conn — proxy for the snapshotter's curr. BEGIN CONCURRENT (wal2),
	// falling back to plain BEGIN, then a load-bearing read of the state version.
	anchorDB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("anchor open: %v", err)
	}
	defer anchorDB.Close()
	anchorConn, err := anchorDB.Conn(ctx)
	if err != nil {
		t.Fatalf("anchor conn: %v", err)
	}
	defer anchorConn.Close()
	if _, err := anchorConn.ExecContext(ctx, "BEGIN CONCURRENT"); err != nil {
		if _, err2 := anchorConn.ExecContext(ctx, "BEGIN"); err2 != nil {
			t.Fatalf("anchor BEGIN: concurrent=%v plain=%v", err, err2)
		}
	}
	var anchorVer string
	if err := anchorConn.QueryRowContext(ctx, stateVersionSQL).Scan(&anchorVer); err != nil {
		t.Fatalf("anchor read stateVersion: %v", err)
	}
	if anchorVer != anchorVersion {
		t.Fatalf("anchor stateVersion = %q, want %q", anchorVer, anchorVersion)
	}

	// Advance the head ACROSS BOTH TABLES in one transaction and bump the state
	// version — BEFORE building any pool. The anchor stays pinned at v1; head is
	// now v2. This is the window the old converge-upward path raced on.
	writerDB, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("writer open: %v", err)
	}
	defer writerDB.Close()
	wconn, err := writerDB.Conn(ctx)
	if err != nil {
		t.Fatalf("writer conn: %v", err)
	}
	defer wconn.Close()
	for _, s := range []string{
		"BEGIN",
		`UPDATE "_zero.replicationState" SET stateVersion='v2'`,
		`INSERT INTO users VALUES (4,'dave')`,
		`INSERT INTO orders VALUES (30,3,300)`,
		"COMMIT",
	} {
		if _, err := wconn.ExecContext(ctx, s); err != nil {
			t.Fatalf("writer %q: %v", s, err)
		}
	}

	// NEGATIVE CONTROL: a converge-upward pool built NOW lands on the new head
	// (v2), NOT the anchor's v1 — proving the head really moved and that the
	// coread result below is a meaningful difference, not a vacuous pass.
	convPool, err := NewReaderPool(ctx, poolDB, "", 2)
	if err != nil {
		t.Fatalf("NewReaderPool (negative control): %v", err)
	}
	if convPool.Version() != "v2" {
		convPool.Close()
		t.Fatalf("negative control: converge pool at %q, want v2 (head moved)", convPool.Version())
	}
	convPool.Close()

	// COREAD-FAST: capture from the anchor (still pinned at v1) — the exact
	// buildReaderPoolLocked call — and build the K-reader pool. Despite head
	// being at v2, these readers must latch the anchor's OLDER v1 frame.
	cr, err := CaptureCoReadFromConn(anchorConn)
	if err != nil {
		t.Fatalf("CaptureCoReadFromConn: %v", err)
	}
	defer cr.Free()

	const k = 8 // GO_IVM_HYDRATE_READERS
	pool, err := NewCoReadReaderPool(ctx, poolDB, cr, k)
	if err != nil {
		t.Fatalf("NewCoReadReaderPool: %v", err)
	}
	defer pool.Close()

	// (2) Alignment gate: the coread pool's version must equal the anchor's
	// (v1) — NOT the v2 head the converge pool landed on. In production this
	// equality is what lets refreshSnapForInitialHydrateLocked bind the pool.
	if pool.Version() != anchorVer {
		t.Fatalf("alignment gate failed: coread pool at %q, anchor=%q (want match, not head v2)",
			pool.Version(), anchorVer)
	}
	if len(pool.all) != k {
		t.Fatalf("pool has %d readers, want %d", len(pool.all), k)
	}

	// (3) Every reader must see the anchor's consistent cross-table cut
	// (users=3, orders=2, stateVersion=v1) — NOT the writer's v2/4/3.
	borrowed := make([]*poolReader, 0, k)
	for i := 0; i < k; i++ {
		r, aerr := pool.acquire(ctx)
		if aerr != nil {
			t.Fatalf("acquire %d: %v", i, aerr)
		}
		var users, orders int
		var ver string
		if err := r.conn.QueryRowContext(ctx, "SELECT count(*) FROM users").Scan(&users); err != nil {
			t.Fatalf("reader %d users: %v", i, err)
		}
		if err := r.conn.QueryRowContext(ctx, "SELECT count(*) FROM orders").Scan(&orders); err != nil {
			t.Fatalf("reader %d orders: %v", i, err)
		}
		if err := r.conn.QueryRowContext(ctx, stateVersionSQL).Scan(&ver); err != nil {
			t.Fatalf("reader %d version: %v", i, err)
		}
		if users != 3 || orders != 2 || ver != anchorVersion {
			t.Fatalf("reader %d saw users=%d orders=%d ver=%q; want 3/2/%q (skew or unpinned frame)",
				i, users, orders, ver, anchorVersion)
		}
		borrowed = append(borrowed, r)
	}
	for _, r := range borrowed {
		pool.release(r)
	}
}

// TestCoRead_NonWal2Errors proves the coread-fast TRIGGER fails closed on a
// plain-wal (non-wal2) database. The wal layer guards with
//
//	if( !isWalMode2(pAnchor) ) return SQLITE_ERROR;   // co-read is wal2-only
//
// (sqlite3.c, sqlite3WalCoReadGet). buildReaderPoolLocked relies on exactly this
// error to fall through from coread-fast to the converge-upward pool. Both
// capture entry points (CaptureCoRead opens its own tx; CaptureCoReadFromConn
// uses an existing one) must surface the error rather than hand back a bogus
// handle that would silently mis-pin every reader. The companion test
// TestBuildReaderPool_FallsBackOnNonWal2 (cmd/sidecar) asserts the resulting
// fallback DECISION; this one isolates the primitive that drives it.
func TestCoRead_NonWal2Errors(t *testing.T) {
	// seedReplicaWithStateVersion (reader_pool_test.go) seeds journal_mode=WAL,
	// i.e. plain wal — the mode mattn's bundled SQLite produces and the one the
	// fallback exists for.
	path := seedReplicaWithStateVersion(t, "v1")

	// Sanity: confirm the replica really is plain wal, not wal2. If a future
	// engine bump silently upgraded the seed to wal2 this test would otherwise
	// pass vacuously, so assert the precondition explicitly.
	{
		probe, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatalf("probe open: %v", err)
		}
		var mode string
		if err := probe.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
			probe.Close()
			t.Fatalf("probe pragma: %v", err)
		}
		probe.Close()
		if mode != "wal" {
			t.Fatalf("seed is in %q mode, want plain wal for this test", mode)
		}
	}

	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// (a) CaptureCoRead opens its own conn + read tx, warms, then captures.
	// On non-wal2 the capture must error.
	if cr, err := CaptureCoRead(context.Background(), db); err == nil {
		cr.Free()
		t.Fatal("CaptureCoRead: expected error on non-wal2 DB, got nil (would mis-pin readers)")
	}

	// (b) CaptureCoReadFromConn captures from an existing, already-warmed read
	// tx — the production path (buildReaderPoolLocked passes cur.Conn()). Must
	// error the same way.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	var n int
	if err := conn.QueryRowContext(context.Background(),
		"SELECT count(*) FROM users").Scan(&n); err != nil {
		t.Fatalf("warm: %v", err)
	}
	if cr, err := CaptureCoReadFromConn(conn); err == nil {
		cr.Free()
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		t.Fatal("CaptureCoReadFromConn: expected error on non-wal2 DB, got nil (would mis-pin readers)")
	}
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
}
