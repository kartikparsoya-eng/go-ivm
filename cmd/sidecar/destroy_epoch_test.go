package main

// Destroy epoch guard tests.
//
// handleDestroy checks initEpoch like every other mutating RPC. A stale
// destroy from a torn-down view-syncer whose RPC raced past a fresh init
// for the same cgID must be rejected with rpcCodeStaleInitEpoch instead
// of tearing down the live successor's engine.

import (
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// initTestServer creates a replica-backed server, calls handleInit for a
// single issue-table CG, and returns the server + the post-init epoch.
func initTestServer(t *testing.T) (*Server, string, uint64) {
	t.Helper()
	srv, _ := newIssueServer(t)
	cgID := "cg-destroy-test"
	epoch := initIssueCG(t, srv, cgID)
	return srv, cgID, epoch
}

// TestDestroy_MatchingEpochSucceeds verifies that a destroy carrying the
// correct initEpoch tears down the group normally.
func TestDestroy_MatchingEpochSucceeds(t *testing.T) {
	srv, cgID, epoch := initTestServer(t)

	resp := srv.handleDestroy(RPCRequest{
		Method: "destroy", ID: 2,
		Params: mustMarshal(t, destroyParams{ClientGroupID: cgID, InitEpoch: epoch}),
	})
	if resp.Error != nil {
		t.Fatalf("destroy with matching epoch failed: %+v", resp.Error)
	}

	// Group must be gone.
	if g := srv.getGroup(cgID, false); g != nil {
		t.Fatal("group still exists after matching-epoch destroy")
	}
}

// TestDestroy_StaleEpochRejected verifies that a destroy carrying a stale
// (wrong) epoch is rejected with rpcCodeStaleInitEpoch and does NOT tear
// down the group.
func TestDestroy_StaleEpochRejected(t *testing.T) {
	srv, cgID, epoch := initTestServer(t)

	staleEpoch := epoch + 999
	resp := srv.handleDestroy(RPCRequest{
		Method: "destroy", ID: 2,
		Params: mustMarshal(t, destroyParams{ClientGroupID: cgID, InitEpoch: staleEpoch}),
	})
	if resp.Error == nil {
		t.Fatal("expected stale-epoch error, got success")
	}
	if resp.Error.Code != rpcCodeStaleInitEpoch {
		t.Fatalf("expected error code %d (rpcCodeStaleInitEpoch), got %d: %s",
			rpcCodeStaleInitEpoch, resp.Error.Code, resp.Error.Message)
	}

	// Group must STILL exist — the stale destroy must not have torn it down.
	if g := srv.getGroup(cgID, false); g == nil {
		t.Fatal("group was torn down by stale-epoch destroy (should still exist)")
	}
}

// TestDestroy_ZeroEpochRejected verifies that a destroy that omits initEpoch
// (defaults to 0) is rejected when the group has a non-zero epoch. This
// simulates a pre-protocolRev-9 client that doesn't know about the epoch
// guard.
func TestDestroy_ZeroEpochRejected(t *testing.T) {
	srv, cgID, _ := initTestServer(t)

	resp := srv.handleDestroy(RPCRequest{
		Method: "destroy", ID: 2,
		Params: mustMarshal(t, destroyParams{ClientGroupID: cgID}),
	})
	if resp.Error == nil {
		t.Fatal("expected stale-epoch error for zero epoch, got success")
	}
	if resp.Error.Code != rpcCodeStaleInitEpoch {
		t.Fatalf("expected error code %d, got %d: %s",
			rpcCodeStaleInitEpoch, resp.Error.Code, resp.Error.Message)
	}

	// Group must still exist.
	if g := srv.getGroup(cgID, false); g == nil {
		t.Fatal("group was torn down by zero-epoch destroy (should still exist)")
	}
}

// TestDestroy_NonExistentGroupSucceeds verifies that a destroy for a cgID
// that doesn't exist returns ok (no-op) rather than an error. There's
// nothing to guard — the epoch check is skipped when the group is absent.
func TestDestroy_NonExistentGroupSucceeds(t *testing.T) {
	srv, _ := newIssueServer(t)

	resp := srv.handleDestroy(RPCRequest{
		Method: "destroy", ID: 1,
		Params: mustMarshal(t, destroyParams{ClientGroupID: "never-existed", InitEpoch: 42}),
	})
	if resp.Error != nil {
		t.Fatalf("destroy for non-existent group should succeed, got error: %+v", resp.Error)
	}
}

// TestDestroy_DefaultClientGroupID verifies that an empty clientGroupID
// defaults to "default" and the epoch guard works for that group.
func TestDestroy_DefaultClientGroupID(t *testing.T) {
	srv, _ := newIssueServer(t)

	// Init with empty cgID (defaults to "default").
	params := issueInitParams("")
	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, params)}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}

	g := srv.getGroup("default", false)
	if g == nil {
		t.Fatal("default group not found after init")
	}
	epoch := g.initEpoch.Load()

	// Destroy with empty cgID + correct epoch should succeed.
	resp := srv.handleDestroy(RPCRequest{
		Method: "destroy", ID: 2,
		Params: mustMarshal(t, destroyParams{ClientGroupID: "", InitEpoch: epoch}),
	})
	if resp.Error != nil {
		t.Fatalf("destroy with matching epoch for default group failed: %+v", resp.Error)
	}
	if g2 := srv.getGroup("default", false); g2 != nil {
		t.Fatal("default group still exists after destroy")
	}
}

// TestInitEpoch_SurvivesDestroyReinit verifies that epochs never restart
// across destroy→re-init cycles. Server.lastEpochs (the graveyard)
// survives removeGroup and seeds the re-created group, so gen-2 epochs
// are strictly greater than anything gen 1 handed out. A late mutating
// RPC from the torn-down generation-1 instance must be rejected, not
// applied to generation 2's engine.
func TestInitEpoch_SurvivesDestroyReinit(t *testing.T) {
	srv, cgID, epochGen1 := initTestServer(t)

	// Generation 1 tears down (matching-epoch destroy → removeGroup).
	resp := srv.handleDestroy(RPCRequest{
		Method: "destroy", ID: 2,
		Params: mustMarshal(t, destroyParams{ClientGroupID: cgID, InitEpoch: epochGen1}),
	})
	if resp.Error != nil {
		t.Fatalf("destroy error: %+v", resp.Error)
	}
	if g := srv.getGroup(cgID, false); g != nil {
		t.Fatal("group still exists after destroy")
	}

	// Generation 2: a new view-syncer instance re-inits the SAME cgID.
	initReq := RPCRequest{Method: "init", ID: 3, Params: mustMarshal(t, issueInitParams(cgID))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("re-init error: %+v", resp.Error)
	}
	g2 := srv.getGroup(cgID, false)
	if g2 == nil {
		t.Fatal("group not found after re-init")
	}
	epochGen2 := g2.initEpoch.Load()

	// Epochs must never restart across generations.
	if epochGen2 <= epochGen1 {
		t.Fatalf("epoch restarted across destroy→re-init: gen1=%d gen2=%d — "+
			"a late RPC from the torn-down instance would pass checkInitEpoch",
			epochGen1, epochGen2)
	}

	// End-to-end: generation 1's late mutating RPC (stale epoch;
	// removeQuery is the canonical surviving case) must be rejected, not
	// applied to generation 2's engine.
	rq := srv.handleRemoveQuery(RPCRequest{
		Method: "removeQuery", ID: 4,
		Params: mustMarshal(t, removeQueryParams{
			ClientGroupID: cgID,
			QueryID:       "q-from-gen1",
			InitEpoch:     epochGen1,
		}),
	})
	if rq.Error == nil {
		t.Fatal("stale-epoch removeQuery from the torn-down generation was accepted — " +
			"the old instance mutated the new engine")
	}
	if rq.Error.Code != rpcCodeStaleInitEpoch {
		t.Fatalf("expected rpcCodeStaleInitEpoch (%d), got %d: %s",
			rpcCodeStaleInitEpoch, rq.Error.Code, rq.Error.Message)
	}
	t.Logf("epochs: gen1=%d gen2=%d; stale gen1 removeQuery rejected", epochGen1, epochGen2)
}

// TestInitEpoch_GraveyardSurvivesReaper verifies the reaper's deletion path
// also persists the epoch: a group reaped for idleness, then re-created by a
// reconnecting client, must not restart at epoch 1.
func TestInitEpoch_GraveyardSurvivesReaper(t *testing.T) {
	srv, cgID, epochGen1 := initTestServer(t)

	// Reap: pretend the group has been idle past the cutoff.
	g := srv.getGroup(cgID, false)
	g.lastUsedNs.Store(0) // epoch start — ancient
	if n := srv.reapIdleGroups(time.Now()); n != 1 {
		t.Fatalf("reapIdleGroups reaped %d groups, want 1", n)
	}
	if g := srv.getGroup(cgID, false); g != nil {
		t.Fatal("group still exists after reap")
	}

	// Re-init the same cgID.
	initReq := RPCRequest{Method: "init", ID: 3, Params: mustMarshal(t, issueInitParams(cgID))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("re-init error: %+v", resp.Error)
	}
	epochGen2 := srv.getGroup(cgID, false).initEpoch.Load()
	if epochGen2 <= epochGen1 {
		t.Fatalf("epoch restarted across reap→re-init: gen1=%d gen2=%d", epochGen1, epochGen2)
	}
}
