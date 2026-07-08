package tablesource

// Pins for the bounded-acquisition contract (PoolAcquireTimeout) and the
// Option B decoupling of pool builds from the shared read pool:
//   - reader-pool builds open RAW driver conns (rawOpenReaderConn) that do
//     not queue on database/sql, so a build must SUCCEED even when every
//     pooled conn is held elsewhere — the structural fix for the 2026-07-06
//     ART incident (concurrent builders hold-and-wait deadlocking on the
//     shared pool and starving handleInit's probe);
//   - the Source constructor's presence probe still rides the shared pool
//     and must fail within the bound (handleInit surfaces a fast rpcError),
//     instead of wedging the CG worker until the TS-side 120s RPC timeout.

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

// TestNewReaderPool_BuildsDespiteExhaustedSharedPool: with every pooled conn
// held elsewhere, the build must SUCCEED — raw driver opens are invisible to
// database/sql's MaxOpenConns, so pool builds no longer compete with probes
// or other pooled work. (Pre-Option-B this same shape deadlocked builders
// against each other and could only fail fast; the whole class is gone.)
func TestNewReaderPool_BuildsDespiteExhaustedSharedPool(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 2, MaxIdleConns: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	setPoolAcquireTimeout(t, 2*time.Second)

	release := holdConns(t, db, 2)
	defer release()
	pool, perr := NewReaderPool(context.Background(), db, "", 4)
	if perr != nil {
		t.Fatalf("NewReaderPool against a fully-held shared pool must succeed "+
			"(raw opens bypass database/sql): %v", perr)
	}
	if pool.Size() != 4 {
		t.Fatalf("pool size = %d, want 4", pool.Size())
	}
	if pool.Version() != "0000000001" {
		t.Fatalf("pool version = %q, want the seeded stateVersion", pool.Version())
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
