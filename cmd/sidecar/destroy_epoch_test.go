package main

// D2: destroy epoch guard tests.
//
// handleDestroy now checks initEpoch like every other mutating RPC.
// A stale destroy from a torn-down view-syncer whose RPC raced past a
// fresh init for the same cgID must be rejected with rpcCodeStaleInitEpoch
// instead of tearing down the live successor's engine.

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// initTestServer is a helper that creates a Memory-mode server, calls
// handleInit for a single empty-table CG, and returns the server + the
// post-init epoch.
func initTestServer(t *testing.T) (*Server, string, uint64) {
	t.Helper()
	srv := NewServer(tablesource.ModeMemory, "")
	t.Cleanup(srv.closeAll)

	cgID := "cg-destroy-test"
	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, initParams{
		ClientGroupID: cgID,
		Tables: map[string]tableSchemaParams{
			"t": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}},
			},
		},
	})}
	resp := srv.handleInit(initReq)
	if resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}

	g := srv.getGroup(cgID, false)
	if g == nil {
		t.Fatal("group not found after init")
	}
	g.mu.Lock()
	epoch := g.initEpoch
	g.mu.Unlock()

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
	srv := NewServer(tablesource.ModeMemory, "")
	t.Cleanup(srv.closeAll)

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
	srv := NewServer(tablesource.ModeMemory, "")
	t.Cleanup(srv.closeAll)

	// Init with empty cgID (defaults to "default").
	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, initParams{
		ClientGroupID: "",
		Tables: map[string]tableSchemaParams{
			"t": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}},
			},
		},
	})}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}

	g := srv.getGroup("default", false)
	if g == nil {
		t.Fatal("default group not found after init")
	}
	g.mu.Lock()
	epoch := g.initEpoch
	g.mu.Unlock()

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
