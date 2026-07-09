package main

// Gen-6 pin (CVR version-skew teardown): handleInit must report the
// snapshotter's pinned stateVersion. TS stamps its CVR hydrate updater at
// max(tsVersion, THIS); without the field, a reconnect whose queries have
// unchanged transformation hashes stamps the updater at TS's OWN (earlier)
// snapshot version — equal to the committed CVR version — while Go hydrates
// rows written after that pin, and cvr.ts:778 ("Expected CVR version to have
// been bumped above original") tears the client group down.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// initResultVersion extracts the "version" field from a handleInit response.
func initResultVersion(t *testing.T, resp RPCResponse) string {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	m, ok := resp.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("init result is %T, want map", resp.Result)
	}
	v, ok := m["version"].(string)
	if !ok {
		t.Fatalf("init result carries no string version: %#v", m)
	}
	return v
}

// The init response version must equal the replica's stateVersion at pin
// time — the frame the first hydrate reads at.
func TestHandleInit_ReportsSnapshotterPinVersion(t *testing.T) {
	srv, _ := newIssueServer(t)

	resp := srv.handleInit(RPCRequest{
		Method: "init", ID: 1,
		Params: mustMarshal(t, issueInitParams("cg-pin")),
	})
	if got := initResultVersion(t, resp); got != "0000000001" {
		t.Fatalf("init version = %q, want %q (replica stateVersion at pin)", got, "0000000001")
	}

	// The reported version must match the live snapshotter pin exactly —
	// the value the first hydrate's frame carries.
	group := srv.getGroup("cg-pin", false)
	if group == nil {
		t.Fatal("group missing after init")
	}
	cur, err := group.snap.Current()
	if err != nil {
		t.Fatalf("snap.Current: %v", err)
	}
	if cur.Version() != "0000000001" {
		t.Fatalf("snapshotter pin = %q, want %q", cur.Version(), "0000000001")
	}
}

// Re-init after the replica advanced re-pins at the NEW head and must report
// the new version — a stale init version would re-open the CVR skew window.
func TestHandleInit_ReInitReportsFreshPin(t *testing.T) {
	srv, db := newIssueServer(t)

	resp := srv.handleInit(RPCRequest{
		Method: "init", ID: 1,
		Params: mustMarshal(t, issueInitParams("cg-repin")),
	})
	if got := initResultVersion(t, resp); got != "0000000001" {
		t.Fatalf("first init version = %q, want 0000000001", got)
	}

	// Replica advances (a write lands between sessions).
	mustExec(t, db,
		`INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	resp = srv.handleInit(RPCRequest{
		Method: "init", ID: 2,
		Params: mustMarshal(t, issueInitParams("cg-repin")),
	})
	if got := initResultVersion(t, resp); got != "0000000002" {
		t.Fatalf("re-init version = %q, want 0000000002 (fresh pin)", got)
	}
}

func TestHandleInit_FailedReInitLeavesCommittedGenerationLive(t *testing.T) {
	srv, _ := newIssueServer(t)
	const cgID = "cg-init-atomic"

	resp := srv.handleInit(RPCRequest{
		Method: "init", ID: 1,
		Params: mustMarshal(t, issueInitParams(cgID)),
	})
	if resp.Error != nil {
		t.Fatalf("initial init error: %+v", resp.Error)
	}
	group := srv.getGroup(cgID, false)
	if group == nil {
		t.Fatal("group missing after initial init")
	}
	epoch := group.initEpoch.Load()
	eng := group.eng
	snap := group.snap
	if epoch == 0 || eng == nil || snap == nil {
		t.Fatalf("initial generation incomplete: epoch=%d eng=%p snap=%p", epoch, eng, snap)
	}

	bad := issueInitParams(cgID)
	bad.Tables["missing_table"] = bad.Tables["issue"]
	resp = srv.handleInit(RPCRequest{
		Method: "init", ID: 2,
		Params: mustMarshal(t, bad),
	})
	if resp.Error == nil {
		t.Fatalf("bad re-init unexpectedly succeeded: %#v", resp.Result)
	}

	group = srv.getGroup(cgID, false)
	if group == nil {
		t.Fatal("group missing after failed re-init")
	}
	if got := group.initEpoch.Load(); got != epoch {
		t.Fatalf("failed re-init changed epoch: got %d, want %d", got, epoch)
	}
	if group.eng != eng {
		t.Fatalf("failed re-init replaced engine: got %p, want %p", group.eng, eng)
	}
	if group.snap != snap {
		t.Fatalf("failed re-init replaced snapshotter: got %p, want %p", group.snap, snap)
	}
	if _, err := group.snap.Current(); err != nil {
		t.Fatalf("committed snapshotter not live after failed re-init: %v", err)
	}

	hydrateOneStreamOK(t, srv, cgID, "q-after-failed-init",
		builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		epoch)
}
