package main

// Pins for the sidecar wiring of the reader-shell cache (reader_cache.go)
// and the POOL-SERIAL incident marker — the pair that replaced the
// build-slot gate (deleted with build_slots_test.go: its premise, bounding
// K-fresh-opens-per-build, dissolved when builds started provisioning from
// the cache).

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
)

// TestColdPoolTeardownFeedsShellCache: getReplicaDB enables the cache
// (default cap 32), and tearing down a bound cold pool returns its K shells
// — the next CG's build pops them instead of opening fresh conns.
func TestColdPoolTeardownFeedsShellCache(t *testing.T) {
	srv, group := warmTestServer(t)
	if group.readerPool == nil {
		t.Skip("cold pool not bound on this build (serial cold hydrate); teardown-to-cache not exercisable")
	}
	k := group.readerPool.Size()
	db, err := srv.getReplicaDB()
	if err != nil {
		t.Fatalf("getReplicaDB: %v", err)
	}
	before := tablesource.ReaderShellCacheSize(db)
	srv.tearDownReaderPool(group)
	if got := tablesource.ReaderShellCacheSize(db); got != before+k {
		t.Fatalf("cache size %d→%d after tearing down a K=%d pool — healthy teardown must return every shell",
			before, got, k)
	}
}

// TestWarmCoreadFailureLogsPoolSerialIncident: a warm build whose co-read
// capture fails (the stub on plain builds; a wal-mode replica under wal2
// tags — either way the failure-serial shape) must emit the greppable
// [GO-IVM][POOL-SERIAL] incident marker. Routine-serial no longer exists;
// the marker is what lets a soak gate distinguish "feature degraded" from
// "infra broke" without a human reading PERF-POOL ratios.
func TestWarmCoreadFailureLogsPoolSerialIncident(t *testing.T) {
	buf := &syncBuf{}
	saved := poolSerialLogW
	poolSerialLogW = buf
	t.Cleanup(func() { poolSerialLogW = saved })

	srv, group := warmTestServer(t)
	srv.tearDownReaderPool(group)
	pool, cr := srv.buildWarmReaderPoolLocked(group, "cg-marker")
	if pool != nil || cr != nil {
		if cr != nil {
			cr.Free()
		}
		if pool != nil {
			pool.Close()
		}
		t.Fatal("warm build unexpectedly produced a pool on a non-wal2 replica")
	}
	s := buf.String()
	if !strings.Contains(s, "[GO-IVM][POOL-SERIAL] cg=cg-marker path=warm reason=coread-capture") {
		t.Fatalf("failure-serial did not emit the incident marker; log:\n%s", s)
	}
}

// TestBuildConcurrency_NoSlotGate: with the slot gate deleted, concurrent
// warm builds must ALL attempt the pool (no skip-to-serial) — on this
// replica they all fail at co-read capture (marker path), but none may
// short-circuit before the attempt. Pinned via the marker count: every
// build reaches the capture site.
func TestBuildConcurrency_NoSlotGate(t *testing.T) {
	buf := &syncBuf{}
	saved := poolSerialLogW
	poolSerialLogW = buf
	t.Cleanup(func() { poolSerialLogW = saved })

	srv, group := warmTestServer(t)
	srv.tearDownReaderPool(group)
	const n = 6 // 3× the old slot cap — all must attempt
	for i := 0; i < n; i++ {
		pool, cr := srv.buildWarmReaderPoolLocked(group, "cg-conc")
		if pool != nil || cr != nil {
			if cr != nil {
				cr.Free()
			}
			if pool != nil {
				pool.Close()
			}
			t.Fatal("unexpected pool on non-wal2 replica")
		}
	}
	if got := strings.Count(buf.String(), "reason=coread-capture"); got != n {
		t.Fatalf("capture attempts = %d, want %d — something short-circuited before the build attempt (a slot gate resurrected?)", got, n)
	}
}
