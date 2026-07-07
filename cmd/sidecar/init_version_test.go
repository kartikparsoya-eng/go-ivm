package main

// Gen-6 pin (CVR version-skew teardown): handleInit must report the
// snapshotter's pinned stateVersion. TS stamps its CVR hydrate updater at
// max(tsVersion, THIS); without the field, a reconnect whose queries have
// unchanged transformation hashes stamps the updater at TS's OWN (earlier)
// snapshot version — equal to the committed CVR version — while Go hydrates
// rows written after that pin, and cvr.ts:778 ("Expected CVR version to have
// been bumped above original") tears the client group down.

import "testing"

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
