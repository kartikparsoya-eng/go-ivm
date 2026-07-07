package tablesource

// Pins for the presence-probe cache (2026-07-07 soak incident): the
// constructor's `SELECT 1 FROM x LIMIT 0` probe was the only hard-fail
// read-pool acquisition on the CG-init path. Under sustained CG churn,
// warm-hydrate reader pools (K conns held for the full hydrate) saturate
// the read pool, so per-init probes queued behind hydrates timed out →
// "Go backend init failed" → TS view-syncer death → client Rehome storm.
// Presence is a property of the replica file, not the CG, so a SUCCESSFUL
// probe is cached per (pool, table) and later inits skip the pool
// entirely. Negative results are never cached (mid-run migrations can
// create tables), and the cache never leaks across pools.

import (
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// TestPresenceProbeCachedSkipsPoolUnderSaturation: once a (pool, table) has
// probed successfully, constructing another Source for the same table must
// NOT touch the read pool — a fully-held pool and a short acquire bound
// must not fail it. Pre-fix this failed with "presence probe timed out"
// (the exact production init-failure signature).
func TestPresenceProbeCachedSkipsPoolUnderSaturation(t *testing.T) {
	path := seedTypedReplica(t)
	db, err := Open(path, OpenOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })

	// First init probes and caches.
	src1, err := New(db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	t.Cleanup(func() { src1.Close() })

	// Saturate: the pool's only conn is held elsewhere (the production
	// shape: warm-hydrate reader pools pinning every read conn).
	release := holdConns(t, db, 1)
	defer release()
	setPoolAcquireTimeout(t, 200*time.Millisecond)

	start := time.Now()
	src2, err := New(db, wdb, "users", userSchema(), []string{"id"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("cached-presence New failed under pool saturation "+
			"(the per-init probe is back): %v", err)
	}
	t.Cleanup(func() { src2.Close() })
	if elapsed > 2*time.Second {
		t.Fatalf("cached-presence New took %v — it must not queue on the read pool", elapsed)
	}
}

// TestPresenceProbeNegativeNotCached: a failed probe (missing table) must
// not poison the cache — after the table is created (a mid-run schema
// migration), the next init must probe again and succeed; and THAT success
// must then be cached (skipping the pool under saturation).
func TestPresenceProbeNegativeNotCached(t *testing.T) {
	path := seedTypedReplica(t)
	db, err := Open(path, OpenOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })

	cols := map[string]sqlite.ColumnSchema{"id": {Type: "INTEGER"}}

	// Missing table → clean "table not found", nothing cached.
	if _, err := New(db, wdb, "ghosts", cols, []string{"id"}); err == nil {
		t.Fatal("New on a missing table succeeded")
	} else if !strings.Contains(err.Error(), "table not found") {
		t.Fatalf("error %q should identify the missing table", err)
	}

	// The migration lands.
	if _, err := wdb.Exec(`CREATE TABLE ghosts (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Re-init probes again (negative was not cached) and succeeds.
	src, err := New(db, wdb, "ghosts", cols, []string{"id"})
	if err != nil {
		t.Fatalf("post-migration New failed — negative probe result was cached: %v", err)
	}
	t.Cleanup(func() { src.Close() })

	// And that success is now cached: saturate + short bound → still fine.
	release := holdConns(t, db, 1)
	defer release()
	setPoolAcquireTimeout(t, 200*time.Millisecond)
	src2, err := New(db, wdb, "ghosts", cols, []string{"id"})
	if err != nil {
		t.Fatalf("cached-presence New failed under saturation: %v", err)
	}
	t.Cleanup(func() { src2.Close() })
}

// TestPresenceProbeKeyedByPool: the cache must not leak across pools — a
// table probed on pool A is unknown to pool B (a different pool may be
// wired to a different replica file), so B's first probe still queues on
// B's conns and still surfaces exhaustion.
func TestPresenceProbeKeyedByPool(t *testing.T) {
	path := seedTypedReplica(t)
	dbA, err := Open(path, OpenOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	t.Cleanup(func() { dbA.Close() })
	dbB, err := Open(path, OpenOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	t.Cleanup(func() { dbB.Close() })
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })

	// Cache "users" on pool A.
	srcA, err := New(dbA, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New on pool A: %v", err)
	}
	t.Cleanup(func() { srcA.Close() })

	// Pool B fully held: its first probe must still time out — A's cache
	// entry must not vouch for B.
	release := holdConns(t, dbB, 1)
	defer release()
	setPoolAcquireTimeout(t, 200*time.Millisecond)
	_, err = New(dbB, wdb, "users", userSchema(), []string{"id"})
	if err == nil {
		t.Fatal("New on exhausted pool B succeeded — presence cache leaked across pools")
	}
	if !strings.Contains(err.Error(), "presence probe timed out") {
		t.Fatalf("error %q should be the probe timeout", err)
	}
}
