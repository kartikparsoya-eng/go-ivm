package main

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// warmTestServer brings a CG to the post-cold-hydrate, pre-warm-add state that
// buildWarmReaderPoolLocked is designed for: drive mode armed, one live
// pipeline (PipelineCount==1), and the cold reader pool already torn down (as
// the first advance would do) so group.readerPool==nil. It deliberately runs
// under the DEFAULT build, where CaptureCoReadFromConn is the stub that errors
// unconditionally — the exact "co-read unavailable" condition (non-wal2 /
// capture failure) the warm path must survive by staying SERIAL, never by
// converging to head. Returns the server and its group.
func warmTestServer(t *testing.T) (*Server, *ClientGroup) {
	t.Helper()
	path, _ := makeReplica(t)

	srv := NewServer(path)
	srv.appID = "myapp"
	srv.hydrateReaders = 8
	srv.warmHydratePoolEnabled = true
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, initParams{
		ClientGroupID: "cg1",
		Tables: map[string]tableSchemaParams{
			"issue": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "title": {Type: "string"}, "number": {Type: "number"}, "_0_version": {Type: "string"}},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}},
			},
		},
	})}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)
	if group == nil || group.snap == nil || group.eng == nil {
		t.Fatalf("group not armed after init: %v", group)
	}

	// Cold hydrate one query — establishes a live pipeline (PipelineCount==1)
	// and, on this first add, builds the cold reader pool (converge-fallback
	// under the stub). Read-only, so it works on the default build.
	hydrateOneStreamOK(t, srv, "cg1", "q1",
		builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		group.initEpoch.Load())
	if group.eng.PipelineCount() != 1 {
		t.Fatalf("PipelineCount = %d, want 1 after cold hydrate", group.eng.PipelineCount())
	}
	return srv, group
}

// TestBuildWarmReaderPool_NonWal2StaysSerial is the safety-landmine test: when
// co-read is unavailable (here the default-build stub errors, mirroring a
// non-wal2 replica), a WARM hydrate MUST fall back to the serial single-conn
// read of curr.Conn() — it must NOT converge-upward to head, which would pin
// the new query to a NEWER frame than the live pipelines and desync it.
//
// Proof: buildWarmReaderPoolLocked returns (nil,nil) — the only non-serial
// return is a co-read pool, and there is no NewReaderPool (converge) call on
// the warm path at all — and the warm-serial metric (not warm-coread) ticks.
func TestBuildWarmReaderPool_NonWal2StaysSerial(t *testing.T) {
	srv, group := warmTestServer(t)

	// Simulate what the first advance does: drop the cold pool so readerPool==nil
	// (otherwise the warm path correctly reuses the still-bound cold pool).
	srv.tearDownReaderPool(group)
	if group.readerPool != nil {
		t.Fatal("tearDownReaderPool left readerPool non-nil")
	}

	beforeSerial := metrics.readerPoolWarmSerial.Load()
	beforeCoread := metrics.readerPoolWarmCoread.Load()

	pool, cr := srv.buildWarmReaderPoolLocked(group, 1)
	if pool != nil || cr != nil {
		if cr != nil {
			cr.Free()
		}
		if pool != nil {
			pool.Close()
		}
		t.Fatal("warm pool built on a non-wal2 (co-read-unavailable) replica — " +
			"the warm path MUST stay serial here, never converge to head")
	}
	if got := metrics.readerPoolWarmSerial.Load() - beforeSerial; got != 1 {
		t.Errorf("warm-serial metric +%d, want +1", got)
	}
	if got := metrics.readerPoolWarmCoread.Load() - beforeCoread; got != 0 {
		t.Errorf("warm-coread metric +%d, want +0 (co-read should have failed)", got)
	}
}

// TestBuildWarmReaderPool_Guards covers the early-return guards: each should
// return (nil,nil) WITHOUT recording a bind outcome (the function never reached
// the co-read attempt).
func TestBuildWarmReaderPool_Guards(t *testing.T) {
	t.Run("feature off", func(t *testing.T) {
		srv, group := warmTestServer(t)
		srv.tearDownReaderPool(group)
		srv.warmHydratePoolEnabled = false
		assertWarmNoop(t, srv, group)
	})

	t.Run("pool size <=1", func(t *testing.T) {
		srv, group := warmTestServer(t)
		srv.tearDownReaderPool(group)
		srv.hydrateReaders = 1
		srv.hydrateLanes = 1
		assertWarmNoop(t, srv, group)
	})

	t.Run("cold pool still bound is reused not rebuilt", func(t *testing.T) {
		// No tearDownReaderPool: the cold pool is still bound (no advance yet).
		// The warm add's fetches already run through it, so warm is a no-op.
		srv, group := warmTestServer(t)
		if group.readerPool == nil {
			t.Skip("cold pool not bound on this build (serial cold hydrate); guard not exercisable")
		}
		assertWarmNoop(t, srv, group)
	})
}

// assertWarmNoop asserts buildWarmReaderPoolLocked returns (nil,nil) and records
// NEITHER bind outcome — i.e. it short-circuited at a guard before attempting a
// co-read (distinguishing a guard from the serial-fallback path, which DOES
// record warm-serial).
func assertWarmNoop(t *testing.T, srv *Server, group *ClientGroup) {
	t.Helper()
	beforeSerial := metrics.readerPoolWarmSerial.Load()
	beforeCoread := metrics.readerPoolWarmCoread.Load()
	pool, cr := srv.buildWarmReaderPoolLocked(group, 1)
	if pool != nil || cr != nil {
		if cr != nil {
			cr.Free()
		}
		if pool != nil {
			pool.Close()
		}
		t.Fatal("guard should have returned (nil,nil)")
	}
	if got := metrics.readerPoolWarmSerial.Load() - beforeSerial; got != 0 {
		t.Errorf("warm-serial metric +%d, want +0 (guard, not a serial fallback)", got)
	}
	if got := metrics.readerPoolWarmCoread.Load() - beforeCoread; got != 0 {
		t.Errorf("warm-coread metric +%d, want +0", got)
	}
}

// TestBuildWarmReaderPool_UndersizedColdPoolNotReused is the scale-review C2
// regression test: a second addQueriesStream arriving BEFORE the first advance
// routes its hydrate through the still-bound cold pool, whose K was sized for
// the FIRST batch's Cmax. If the new batch's joins are deeper
// (P × Cmax(new) > K), every hydrate lane can hold parent readers while
// blocked in acquire for a child — a resource deadlock held under group.mu +
// the engine lock, unkillable because acquire's only unblock (CG teardown)
// itself needs group.mu.
//
// Pre-fix, buildWarmReaderPoolLocked returned (nil,nil) unconditionally when
// group.readerPool != nil, leaving the undersized pool bound. Post-fix the
// invariant is: after the call, EITHER the bound pool covers the new demand
// (rebuilt at the same frame with a larger K), OR no pool is bound at all
// (serial fallback) — never an undersized bound pool.
func TestBuildWarmReaderPool_UndersizedColdPoolNotReused(t *testing.T) {
	srv, group := warmTestServer(t)
	// Cold pool from warmTestServer's single-table q1 (cmax=1):
	// K1 = max(hydrateReaders=8, hydrateLanes=4 × 1) = 8.
	if group.readerPool == nil {
		t.Skip("cold pool not bound on this build (serial cold hydrate); C2 window not exercisable")
	}
	if got := group.readerPool.Size(); got != 8 {
		t.Fatalf("precondition: cold pool size = %d, want 8 (max(readers=8, lanes=4×cmax=1))", got)
	}

	// Batch 2, pre-first-advance, deeper joins: cmax=3 → demand
	// K = max(8, 4×3) = 12 > 8. The 8-reader pool can deadlock: 4 lanes ×
	// (3−1) held readers = 8 = K, all lanes still needing one more.
	const cmax2 = 3
	need := srv.hydrateReaders
	if lc := srv.hydrateLanes * cmax2; lc > need {
		need = lc
	}

	pool, cr := srv.buildWarmReaderPoolLocked(group, cmax2)
	if pool != nil || cr != nil {
		if cr != nil {
			cr.Free()
		}
		if pool != nil {
			pool.Close()
		}
		t.Fatal("warm path must not return an ephemeral pool while the cold slot is occupied")
	}

	// THE C2 invariant.
	if group.readerPool == nil {
		t.Log("undersized cold pool torn down (serial fallback) — deadlock-free")
		return
	}
	if got := group.readerPool.Size(); got < need {
		t.Fatalf("undersized cold pool still bound: K=%d < demand=%d (P=%d × Cmax=%d) — "+
			"second-batch hydrate can deadlock in acquire", got, need, srv.hydrateLanes, cmax2)
	}
	// A rebuilt pool must sit on curr's frame — the frame the live pipelines
	// hydrated at — or the new queries desync.
	cur, cerr := group.snap.Current()
	if cerr != nil {
		t.Fatalf("snap.Current: %v", cerr)
	}
	if group.readerPool.Version() != cur.Version() {
		t.Fatalf("rebuilt pool pinned to frame %s, want curr's frame %s",
			group.readerPool.Version(), cur.Version())
	}
	t.Logf("cold pool rebuilt 8→%d at curr's frame %s — covers P×Cmax=%d",
		group.readerPool.Size(), cur.Version(), need)
}
