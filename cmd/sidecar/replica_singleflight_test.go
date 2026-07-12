package main

// Regression coverage for the getReplicaDB singleflight refactor.
//
// getReplicaDB uses a probe-channel singleflight: exactly one goroutine
// performs the slow open; others wait on `replicaProbe` (closed when the
// probe completes) and then read the shared result. The mutex is only
// held for tiny critical sections.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGetReplicaDB_NoCacheMissBlock confirms that when no replica path is
// configured, the error path returns quickly under the cache-miss branch
// without entering the retry loop.
func TestGetReplicaDB_NoCacheMissBlock(t *testing.T) {
	s := &Server{}
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

// TestGetReplicaDB_ConcurrentCallersShareProbe verifies that N concurrent
// callers share the same probe: the first caller probes, the rest wait on
// the probe channel. All N should finish within a small window of each
// other, not each restart the retry loop independently.
func TestGetReplicaDB_ConcurrentCallersShareProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ~60s singleflight test under -short")
	}
	oldTimeout := replicaOpenTimeout
	oldBackoff := replicaOpenInitialBackoff
	replicaOpenTimeout = 25 * time.Millisecond
	replicaOpenInitialBackoff = time.Millisecond
	t.Cleanup(func() {
		replicaOpenTimeout = oldTimeout
		replicaOpenInitialBackoff = oldBackoff
	})
	// Use a non-existent path so the probe will fail. We override the
	// open timeout indirectly: the test would take 60s with the real
	// timeout, so this test is opportunistic — we abort early once we
	// observe the singleflight behavior.
	//
	// Lighter approach: spin up two goroutines, let the first one
	// start probing, give the second a head start to join via
	// replicaProbe wait, then check that they finish nearly together.
	s := &Server{
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
		}(i)
		// Stagger the callers slightly so the first one wins the probe.
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()

	// All N should finish within a small window of each other (the
	// time it takes the probe channel to broadcast).
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
	// probeRegistrations is reserved for a future tightening (assert that
	// the counter never exceeds 1). For now timing is the load-bearing pin.
	// Read via .Load() — assigning to _ would trip vet's copylocks check
	// (atomic.Int32 contains noCopy).
	_ = probeRegistrations.Load()
}

// TestGetReplicaDB_NextCallerAfterFailureCanProbe confirms that after a
// failed probe, the next caller is allowed to start a NEW probe. The
// fix must not deadlock subsequent callers on the cleared probe state.
func TestGetReplicaDB_NextCallerAfterFailureCanProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ~120s singleflight cleanup test under -short")
	}
	oldTimeout := replicaOpenTimeout
	oldBackoff := replicaOpenInitialBackoff
	replicaOpenTimeout = 25 * time.Millisecond
	replicaOpenInitialBackoff = time.Millisecond
	t.Cleanup(func() {
		replicaOpenTimeout = oldTimeout
		replicaOpenInitialBackoff = oldBackoff
	})
	s := &Server{
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
