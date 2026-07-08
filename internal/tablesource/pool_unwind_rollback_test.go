package tablesource

// Pin for the unwind-path contract: when a bounded pool build fails
// mid-construction, the raw readers it already opened must be fully
// released — read txs rolled back, conns closed — so no WAL frame stays
// pinned. Historical trigger: close(expiredCtx) skipped the ROLLBACK
// (ExecContext fails instantly on an expired ctx) and handed WAL-pinning
// conns back to the shared pool (2026-07-06 latency forensics follow-up).
// Option B readers are raw driver conns CLOSED outright on unwind (never
// returned to any pool), so the exposure shrank to the instant before
// dc.Close — but the contract stays pinned: an aborted build must leave
// nothing that defers wal2's WAL switch.

import (
	"context"
	"testing"
	"time"
)

// TestNewReaderPool_AbortedBuildReleasesFrames: force the build deadline to
// expire mid-construction (PoolAcquireTimeout≈0 makes the first BEGIN /
// stateVersion read fail on the expired ctx), then verify no reader the
// aborted build opened still pins a WAL frame — a TRUNCATE checkpoint must
// fully checkpoint, which is impossible while any conn holds an open read tx.
func TestNewReaderPool_AbortedBuildReleasesFrames(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 3, MaxIdleConns: 3})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// A microscopic build budget: raw opens may succeed, but the BEGIN /
	// converge reads run against an already-expired ctx → error → unwind.
	setPoolAcquireTimeout(t, time.Nanosecond)
	if pool, perr := NewReaderPool(context.Background(), db, "", 2); perr == nil {
		pool.Close()
		t.Skip("build finished inside a nanosecond budget — cannot exercise the unwind on this machine")
	}

	// Write + TRUNCATE-checkpoint through a separate writable handle. If
	// the aborted build left a reader's read tx open, the frame pin blocks
	// full checkpointing (busy != 0).
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
		t.Fatalf("TRUNCATE checkpoint reported busy=%d — an aborted build's reader still pins a WAL read tx", busy)
	}
}
