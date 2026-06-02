package main

// Regression coverage for the C13 getReplicaDB singleflight refactor.
//
// Pre-fix getReplicaDB held replicaMu across the entire 60-second retry
// loop. N concurrent first-init calls serialized behind the first; each
// failed attempt forced the next waiter to redo a fresh 60s loop from
// scratch. Under chronic replica-unreachable conditions this multiplied
// init latency by N and saturated the mutex.
//
// The fix uses a probe-channel singleflight: exactly one goroutine
// performs the slow open; others wait on `replicaProbe` (closed when
// the probe completes) and then read the shared result. The mutex is
// only held for tiny critical sections.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
)

// TestGetReplicaDB_NoCacheMissBlock confirms that when no replica path is
// configured, the error path returns quickly under the cache-miss branch
// without entering the retry loop.
func TestGetReplicaDB_NoCacheMissBlock(t *testing.T) {
	s := &Server{sourceMode: tablesource.ModeTable}
	// replicaPath is empty — should fail fast, not enter the 60s loop.
	start := time.Now()
	_, err := s.getReplicaDB()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error for empty replicaPath, got nil")
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("empty-path failure took %v — should fail fast", elapsed)
	}
}

// TestGetReplicaDB_ConcurrentCallersShareProbe is the core C13 contract:
// N concurrent callers do NOT each restart the 60s retry loop. The first
// caller probes; the rest wait on the probe channel. With a bad path
// that consistently fails, ALL N should observe the same single ~60s
// delay (well, we shorten by using an invalid path; the loop exits
// after its deadline regardless).
//
// We can't easily set a custom timeout without exposing internals, so
// we focus on the structural assertion: the second caller observes the
// SAME failure as the first within a small grace window, not a fresh
// retry loop. We measure that the second caller doesn't start its own
// probe by checking that BOTH callers finish within a tight window of
// each other.
func TestGetReplicaDB_ConcurrentCallersShareProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ~60s singleflight test under -short")
	}
	// Use a non-existent path so the probe will fail. We override the
	// open timeout indirectly: the test would take 60s with the real
	// timeout, so this test is opportunistic — we abort early once we
	// observe the singleflight behavior.
	//
	// Lighter approach: spin up two goroutines, let the first one
	// start probing, give the second a head start to join via
	// replicaProbe wait, then check that they finish nearly together.
	s := &Server{
		sourceMode:  tablesource.ModeTable,
		replicaPath: "/nonexistent/path/that/will/fail.db",
	}

	const N = 4
	finished := make([]time.Time, N)
	var wg sync.WaitGroup
	var probeRegistrations atomic.Int32

	// Watcher goroutine: when it observes that replicaProbe has been
	// registered, increment counter. If counter ever exceeds 1 (with
	// the second caller's probe replacing the first AFTER the first
	// completed), we'd see the regression — non-shared probes.
	//
	// For our purposes, the cleaner pin is timing: all callers should
	// finish within a small window of each other.
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.getReplicaDB()
			finished[idx] = time.Now()
		}(0)
		// Stagger the callers slightly so the first one wins the probe.
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()

	// All N should finish within a small window of each other (the
	// time it takes the probe channel to broadcast). Pre-fix, each
	// caller would serialize and the gap would be ~60s × (N-1).
	var minT, maxT time.Time
	for _, ts := range finished {
		if minT.IsZero() || ts.Before(minT) {
			minT = ts
		}
		if ts.After(maxT) {
			maxT = ts
		}
	}
	gap := maxT.Sub(minT)
	if gap > 2*time.Second {
		t.Errorf("expected concurrent callers to share probe (finish within ~probe-broadcast-time); "+
			"gap=%v (would be ~%v×N if not shared)", gap, 60*time.Second)
	}
	_ = probeRegistrations // (unused — kept for future tightening)
}

// TestGetReplicaDB_NextCallerAfterFailureCanProbe confirms that after a
// failed probe, the next caller is allowed to start a NEW probe. The
// fix must not deadlock subsequent callers on the cleared probe state.
func TestGetReplicaDB_NextCallerAfterFailureCanProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ~120s singleflight cleanup test under -short")
	}
	s := &Server{
		sourceMode:  tablesource.ModeTable,
		replicaPath: "/nonexistent/will/fail.db",
	}

	// First call: probes, fails.
	_, err1 := s.getReplicaDB()
	if err1 == nil {
		t.Fatal("expected first call to fail")
	}

	// Second call: should be allowed to probe (replicaProbe was cleared
	// in the finally), and should fail too — not hang waiting for a
	// stale probe channel.
	done := make(chan error, 1)
	go func() {
		_, e := s.getReplicaDB()
		done <- e
	}()
	select {
	case err2 := <-done:
		if err2 == nil {
			t.Fatal("expected second call to fail too")
		}
	case <-time.After(70 * time.Second):
		t.Fatal("second call hung — probe-channel cleanup is broken")
	}
}
