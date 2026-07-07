package main

// Pins for the reader-pool build-slot gate (2026-07-07 latency forensics):
// at most 2 concurrent pool builds per engine; a WARM build finding both
// slots busy must skip to the serial fallback IMMEDIATELY (it runs with
// group.mu held — waiting there convoys the CG's whole request stream),
// and a COLD build waits at most ~1s before its own serial fallback.

import (
	"database/sql"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func occupyBuildSlots(t *testing.T, s *Server) func() {
	t.Helper()
	for i := 0; i < cap(s.readerPoolBuildSlots); i++ {
		select {
		case s.readerPoolBuildSlots <- struct{}{}:
		default:
			t.Fatal("build slots unexpectedly occupied")
		}
	}
	return func() {
		for i := 0; i < cap(s.readerPoolBuildSlots); i++ {
			<-s.readerPoolBuildSlots
		}
	}
}

// warmBuildFixture inits a drive-mode server with one hydrated pipeline —
// the preconditions buildWarmReaderPoolLocked requires (snap armed, engine
// live, PipelineCount > 0).
func warmBuildFixture(t *testing.T) (*Server, *ClientGroup, *sql.DB) {
	t.Helper()
	path, db := makeReplica(t)
	srv := NewServer(path)
	srv.appID = "myapp"
	// Set explicitly: production wires this from GO_IVM_WARM_HYDRATE_POOL in
	// abi.go (default ON); NewServer alone leaves it false and every warm
	// build would early-return at the kill-switch gate — before the slot
	// try-acquire these tests exist to exercise.
	srv.warmHydratePoolEnabled = true
	t.Cleanup(srv.closeAll)
	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)
	hydrateOneStreamOK(t, srv, "cg1", "q1",
		builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		group.initEpoch.Load())
	return srv, group, db
}

// TestNewServer_BuildSlotsSized pins that NewServer actually allocates the
// slot channel. A nil channel passes BOTH busy-path tests below (nil send
// never proceeds → try-acquire always "fails") while silently disabling
// every pool build in production: warm adds all skip to serial and every
// cold hydrate eats the full coldBuildSlotWait before ITS serial fallback.
func TestNewServer_BuildSlotsSized(t *testing.T) {
	srv := NewServer("unused")
	if srv.readerPoolBuildSlots == nil {
		t.Fatal("readerPoolBuildSlots is nil — every pool build silently degrades to serial")
	}
	if got := cap(srv.readerPoolBuildSlots); got != readerPoolBuildSlotCap {
		t.Fatalf("build slot cap=%d, want %d", got, readerPoolBuildSlotCap)
	}
}

// TestWarmBuild_ReleasesSlot: whatever the build outcome (real pool under
// wal2, serial fallback under plain tags), the slot must be free again when
// buildWarmReaderPoolLocked returns — a leaked slot halves build concurrency
// forever and two leaks disable pooling entirely.
func TestWarmBuild_ReleasesSlot(t *testing.T) {
	srv, group, _ := warmBuildFixture(t)

	group.mu.Lock()
	// Model the production warm shape: the first advance tears down the
	// cold pool; warm adds build against a pool-less live CG. Without this
	// the fixture's cold pool (K ≥ warm k) short-circuits at "cold pool
	// covers this batch's demand" and the slot path never runs.
	srv.tearDownReaderPool(group)
	pool, cr := srv.buildWarmReaderPoolLocked(group, 2)
	group.mu.Unlock()
	if pool != nil {
		srv.tearDownWarmReaderPool(group, pool, cr)
	}
	if n := len(srv.readerPoolBuildSlots); n != 0 {
		t.Fatalf("%d build slot(s) still held after build returned — slot leak", n)
	}
}

func TestWarmBuild_SkipsToSerialWhenSlotsBusy(t *testing.T) {
	srv, group, _ := warmBuildFixture(t)
	releaseSlots := occupyBuildSlots(t, srv)
	defer releaseSlots()

	group.mu.Lock()
	defer group.mu.Unlock()
	// See TestWarmBuild_ReleasesSlot: drop the fixture's cold pool so the
	// build takes the warm path (slot try-acquire) instead of the
	// cold-pool-covers/rebuild early-outs.
	srv.tearDownReaderPool(group)
	skipsBefore := metrics.readerPoolBuildSlotSkips.Load()
	start := time.Now()
	pool, cr := srv.buildWarmReaderPoolLocked(group, 2)
	elapsed := time.Since(start)
	if pool != nil || cr != nil {
		srv.tearDownWarmReaderPool(group, pool, cr)
		t.Fatal("warm build returned a pool despite both slots busy")
	}
	// Non-blocking contract: no build-slot wait, no PoolAcquireTimeout —
	// the CG worker must not stall behind the optimization.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("warm build blocked %v with slots busy — must skip instantly", elapsed)
	}
	if metrics.readerPoolBuildSlotSkips.Load() <= skipsBefore {
		t.Fatal("slot skip not recorded in metrics")
	}
}

func TestColdBuild_BoundedWaitThenSerialWhenSlotsBusy(t *testing.T) {
	srv, group, _ := warmBuildFixture(t)
	releaseSlots := occupyBuildSlots(t, srv)
	defer releaseSlots()

	group.mu.Lock()
	cur, cerr := group.snap.Current()
	group.mu.Unlock()
	if cerr != nil {
		t.Fatalf("snap.Current: %v", cerr)
	}
	start := time.Now()
	pool, cr, err := srv.buildReaderPoolLocked(cur, 2)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("cold build errored (want clean serial fallback): %v", err)
	}
	if pool != nil {
		pool.Close()
		if cr != nil {
			cr.Free()
		}
		t.Fatal("cold build returned a pool despite both slots busy")
	}
	if elapsed < 500*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("cold build waited %v — want ~1s bounded wait then serial", elapsed)
	}
}
