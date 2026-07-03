package main

// Cold-pool TTL (scale review): a CG that cold-hydrates and then never
// advances kept its cold-start reader pool — K readers pinned at the
// init-time WAL frame — bound until group teardown. The idle reaper never
// fired for it when the client kept the group alive with non-advance RPCs
// (lastUsedNs refreshed on every arrival), so on a busy replica wal2 could
// not checkpoint past the pinned frame: unbounded WAL growth. The reaper
// now also sweeps cold pools whose bind is older than coldPoolTTL.

import (
	"context"
	"testing"
	"time"
)

// TestReaper_TearsDownStaleColdPool is the end-to-end regression: with the
// TTL and reaper interval env-tuned to 1s, a bound cold pool must be gone
// shortly after, while the GROUP survives (only the pool is dropped). Uses
// only pre-fix identifiers, so it compiles — and demonstrably fails — on
// the pre-fix tree (where GO_IVM_COLD_POOL_TTL_SEC is consumed nowhere and
// the pool stays bound forever).
func TestReaper_TearsDownStaleColdPool(t *testing.T) {
	t.Setenv("GO_IVM_REAPER_INTERVAL_SEC", "1")
	t.Setenv("GO_IVM_COLD_POOL_TTL_SEC", "1")

	srv, group := warmTestServer(t)
	group.mu.Lock()
	bound := group.readerPool != nil
	group.mu.Unlock()
	if !bound {
		t.Skip("cold pool not bound on this build (serial cold hydrate); TTL not exercisable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.runReaper(ctx)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		group.mu.Lock()
		gone := group.readerPool == nil
		group.mu.Unlock()
		if gone {
			if srv.getGroup("cg1", false) == nil {
				t.Fatal("group reaped along with its pool — the TTL sweep must drop ONLY the pool")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("cold reader pool still bound after TTL — the pinned WAL frame blocks checkpointing indefinitely")
}

// TestReapStaleColdPools_Contract pins the sweep's guards at the unit
// level: fresh binds are kept; a busy group (in-flight handler, or group.mu
// held) is skipped rather than waited on; a stale bind on an idle group is
// dropped, leaving the group alive.
func TestReapStaleColdPools_Contract(t *testing.T) {
	srv, group := warmTestServer(t)
	group.mu.Lock()
	bound := group.readerPool != nil
	group.mu.Unlock()
	if !bound {
		t.Skip("cold pool not bound on this build (serial cold hydrate)")
	}
	const ttl = 5 * time.Minute

	// Fresh bind: kept.
	if n := srv.reapStaleColdPools(time.Now(), ttl); n != 0 {
		t.Fatalf("fresh cold pool dropped by the sweep (%d)", n)
	}

	// Stale bind, but group.mu held (a hydrate in progress): skipped, never
	// blocked on. Run the sweep from this goroutine while holding the lock —
	// TryLock fails and the sweep must return immediately.
	group.mu.Lock()
	group.readerPoolBoundAt = time.Now().Add(-time.Hour)
	if n := srv.reapStaleColdPools(time.Now(), ttl); n != 0 {
		group.mu.Unlock()
		t.Fatalf("sweep touched a group whose mu is held (%d)", n)
	}
	group.mu.Unlock()

	// Stale bind, worker mid-handler: skipped via the inFlight guard.
	group.inFlight.Store(true)
	if n := srv.reapStaleColdPools(time.Now(), ttl); n != 0 {
		t.Fatalf("sweep dropped the pool of an in-flight group (%d)", n)
	}
	group.inFlight.Store(false)

	// Stale bind on an idle group: dropped; the group itself survives.
	if n := srv.reapStaleColdPools(time.Now(), ttl); n != 1 {
		t.Fatalf("stale cold pool not dropped (n=%d)", n)
	}
	group.mu.Lock()
	still := group.readerPool != nil
	zeroed := group.readerPoolBoundAt.IsZero()
	group.mu.Unlock()
	if still {
		t.Fatal("readerPool non-nil after the TTL sweep")
	}
	if !zeroed {
		t.Fatal("readerPoolBoundAt not zeroed by tearDownReaderPool")
	}
	if srv.getGroup("cg1", false) == nil {
		t.Fatal("group destroyed by the pool sweep")
	}
	// Idempotent.
	if n := srv.reapStaleColdPools(time.Now(), ttl); n != 0 {
		t.Fatalf("second sweep dropped %d pools; want 0", n)
	}
}
