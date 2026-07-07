package tablesource

// Pins for the bounded pool-acquisition contract (PoolAcquireTimeout) — the
// 2026-07-06 ART incident fix. Under read-pool exhaustion:
//   - reader-pool builds must fail within the bound and unwind every held
//     conn (callers then fall back to serial hydrate), instead of
//     hold-and-wait deadlocking against other builders forever;
//   - the Source constructor's presence probe must fail within the bound
//     (handleInit surfaces a fast rpcError), instead of wedging the CG
//     worker until the TS-side 120s RPC timeout and leaking the goroutine.

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// holdConns checks out n conns from db and returns a release func. Fails the
// test if the pool can't supply them promptly (it must be sized ≥ n).
func holdConns(t *testing.T, db *sql.DB, n int) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	held := make([]*sql.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			for _, h := range held {
				_ = h.Close()
			}
			t.Fatalf("holdConns: acquire %d/%d: %v", i+1, n, err)
		}
		held = append(held, c)
	}
	return func() {
		for _, h := range held {
			_ = h.Close()
		}
	}
}

func setPoolAcquireTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	saved := PoolAcquireTimeout
	PoolAcquireTimeout = d
	t.Cleanup(func() { PoolAcquireTimeout = saved })
}

// TestNewReaderPool_ExhaustedPoolFailsFastAndUnwinds: with every pool conn
// held elsewhere, the build must error within the bound (not block), and a
// retry after release must succeed with the full K — proving the failed
// build released everything it had acquired.
func TestNewReaderPool_ExhaustedPoolFailsFastAndUnwinds(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 2, MaxIdleConns: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	setPoolAcquireTimeout(t, 300*time.Millisecond)

	release := holdConns(t, db, 2)
	start := time.Now()
	pool, perr := NewReaderPool(context.Background(), db, "", 2)
	elapsed := time.Since(start)
	if perr == nil {
		pool.Close()
		release()
		t.Fatal("NewReaderPool succeeded against a fully-held pool")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("build blocked %v — the acquire bound did not apply", elapsed)
	}
	release()

	// Recovery: the full K must be acquirable again (nothing leaked).
	pool, perr = NewReaderPool(context.Background(), db, "", 2)
	if perr != nil {
		t.Fatalf("post-release build failed (leaked conns from the aborted build?): %v", perr)
	}
	if pool.Size() != 2 {
		t.Fatalf("pool size = %d, want 2", pool.Size())
	}
	pool.Close()
}

// TestNewSource_ProbeFailsFastUnderPoolExhaustion: the constructor's
// presence probe must surface exhaustion as a bounded, diagnosable error —
// the shape handleInit converts to a fast rpcError.
func TestNewSource_ProbeFailsFastUnderPoolExhaustion(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 2, MaxIdleConns: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })
	setPoolAcquireTimeout(t, 300*time.Millisecond)

	cols := map[string]sqlite.ColumnSchema{
		"id":   {Type: "INTEGER"},
		"name": {Type: "TEXT"},
	}

	release := holdConns(t, db, 2)
	start := time.Now()
	_, serr := New(db, wdb, "users", cols, []string{"id"})
	elapsed := time.Since(start)
	release()
	if serr == nil {
		t.Fatal("New succeeded against a fully-held read pool")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("probe blocked %v — the acquire bound did not apply", elapsed)
	}
	if !strings.Contains(serr.Error(), "presence probe timed out") {
		t.Fatalf("error %q should identify the probe timeout (not 'table not found')", serr)
	}

	// Recovery after release.
	src, serr := New(db, wdb, "users", cols, []string{"id"})
	if serr != nil {
		t.Fatalf("post-release New failed: %v", serr)
	}
	src.Close()
}
