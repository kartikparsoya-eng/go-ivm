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

	pool, cr := srv.buildWarmReaderPoolLocked(group, "cg1")
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
	pool, cr := srv.buildWarmReaderPoolLocked(group, "cg-noop")
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

// TestBuildWarmReaderPool_BoundColdPoolReusedForAnyBatch replaces the
// scale-review C2 resize test (TestBuildWarmReaderPool_UndersizedColdPoolNotReused).
// C2's premise — a still-bound cold pool sized K < P × Cmax(new batch) lets
// hydrate lanes hold parent readers while blocked acquiring child readers —
// DISSOLVED with Option B: a pipeline needs exactly ONE reader regardless of
// join depth (nested fetches interleave cursors on it), acquired while
// holding nothing, so "undersized" no longer exists; a batch wider than K
// queues at admission. The invariant flips accordingly: a second
// addQueriesStream arriving pre-first-advance must REUSE the still-bound
// cold pool untouched — no rebuild, no teardown, same frame — for ANY
// batch shape.
func TestBuildWarmReaderPool_BoundColdPoolReusedForAnyBatch(t *testing.T) {
	srv, group := warmTestServer(t)
	if group.readerPool == nil {
		t.Skip("cold pool not bound on this build (serial cold hydrate); reuse window not exercisable")
	}
	boundBefore := group.readerPool
	sizeBefore := boundBefore.Size()
	frameBefore := boundBefore.Version()

	pool, cr := srv.buildWarmReaderPoolLocked(group, "cg1")
	if pool != nil || cr != nil {
		if cr != nil {
			cr.Free()
		}
		if pool != nil {
			pool.Close()
		}
		t.Fatal("warm path must not return an ephemeral pool while the cold slot is occupied")
	}

	// THE Option B invariant: the bound pool is untouched.
	if group.readerPool != boundBefore {
		t.Fatal("still-bound cold pool was replaced — Option B reuses it for any batch")
	}
	if got := group.readerPool.Size(); got != sizeBefore {
		t.Fatalf("cold pool resized %d→%d — resize machinery should be gone", sizeBefore, got)
	}
	if got := group.readerPool.Version(); got != frameBefore {
		t.Fatalf("cold pool frame moved %s→%s — must stay on the live pipelines' frame", frameBefore, got)
	}
	// And it still sits on curr's frame — the frame the live pipelines
	// hydrated at.
	cur, cerr := group.snap.Current()
	if cerr != nil {
		t.Fatalf("snap.Current: %v", cerr)
	}
	if group.readerPool.Version() != cur.Version() {
		t.Fatalf("cold pool pinned to frame %s, want curr's frame %s",
			group.readerPool.Version(), cur.Version())
	}
}
