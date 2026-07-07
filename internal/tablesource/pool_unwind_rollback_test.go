package tablesource

// Pin for the unwind-path ROLLBACK contract: when a bounded pool build
// times out mid-acquisition, the conns it already held must be returned to
// the pool with their read txs ROLLED BACK — close(expiredCtx) skipped the
// ROLLBACK (ExecContext fails instantly on an expired ctx), handing back
// WAL-frame-pinning conns that deferred wal2's WAL switch until mattn's
// ResetSession discarded them at next checkout (2026-07-06 latency
// forensics follow-up).

import (
	"context"
	"testing"
	"time"
)

// TestNewReaderPool_TimeoutUnwindRollsBackReadTxs: exhaust a 3-conn pool
// with 2 held conns, force a K=2 build to time out after acquiring its
// first reader, then verify the WAL frame that reader pinned is actually
// released — a subsequent TRUNCATE checkpoint must fully checkpoint,
// which is impossible while any pooled conn still holds an open read tx.
func TestNewReaderPool_TimeoutUnwindRollsBackReadTxs(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 3, MaxIdleConns: 3})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	setPoolAcquireTimeout(t, 400*time.Millisecond)

	// Hold 2 of 3 conns: the K=2 build acquires reader 1 (the last free
	// conn, BEGINs a read tx on it) then times out waiting for reader 2.
	release := holdConns(t, db, 2)
	if _, perr := NewReaderPool(context.Background(), db, "", 2); perr == nil {
		t.Fatal("build should have timed out with only 1 free conn")
	}
	release()

	// Write + TRUNCATE-checkpoint through a separate writable handle. If
	// the aborted build's reader went back to the pool with its read tx
	// open, the frame pin blocks full checkpointing (busy != 0 or the WAL
	// retains frames).
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })
	if _, err := wdb.Exec(`INSERT INTO users VALUES (99, 'zed', 1, 1)`); err != nil {
		t.Fatalf("write: %v", err)
	}
	var busy, logFrames, ckpted int
	if err := wdb.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &ckpted); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if busy != 0 {
		t.Fatalf("TRUNCATE checkpoint reported busy=%d — a pooled conn still pins a WAL read tx", busy)
	}
}
