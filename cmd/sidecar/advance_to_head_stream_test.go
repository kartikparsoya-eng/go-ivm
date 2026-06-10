package main

// Tests for handleAdvanceToHeadStream — the streaming variant of advanceToHead
// for DRIVE mode (finding F5). It chunks the engine's RowChanges over
// advanceChunkSize-sized partial frames so a bulk backfill / mass UPDATE can't
// blow the 64MB single-frame cap that the TS receive loop SKIPS (orphaning the
// RPC into a timeout).
//
// Reuses makeReplica / mustExec / mustMarshal / beginConcurrentSupported from
// advance_to_head_test.go (same package).

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// collectAdvanceToHeadStreamFrames returns a streamWriter that appends every
// partial frame the handler emits into the returned slice pointer.
func collectAdvanceToHeadStreamFrames() (streamWriter, *[]advanceToHeadStreamPartial) {
	frames := &[]advanceToHeadStreamPartial{}
	w := streamWriter(func(_ interface{}, partial interface{}) {
		if p, ok := partial.(advanceToHeadStreamPartial); ok {
			*frames = append(*frames, p)
		}
	})
	return w, frames
}

// assertStreamFrameInvariants checks the frame invariants common to every
// successful advanceToHeadStream: ≥1 frame, monotonic chunkIndex from 0,
// exactly one Final=true and it's the LAST frame, version/numChanges only on
// the final.
func assertStreamFrameInvariants(t *testing.T, frames []advanceToHeadStreamPartial) {
	t.Helper()
	if len(frames) == 0 {
		t.Fatalf("no frames emitted")
	}
	finals := 0
	for i, f := range frames {
		if f.ChunkIndex != i {
			t.Errorf("frame %d: chunkIndex = %d, want %d (must be monotonic from 0)", i, f.ChunkIndex, i)
		}
		if f.Final {
			finals++
			if i != len(frames)-1 {
				t.Errorf("Final=true on frame %d but it is not the last (of %d)", i, len(frames))
			}
		} else {
			// version / numChanges ride the Final frame ONLY.
			if f.Version != "" {
				t.Errorf("non-final frame %d carries Version=%q (must be empty)", i, f.Version)
			}
			if f.NumChanges != 0 {
				t.Errorf("non-final frame %d carries NumChanges=%d (must be 0)", i, f.NumChanges)
			}
		}
	}
	if finals != 1 {
		t.Errorf("got %d Final frames, want exactly 1", finals)
	}
}

// issueInitParams builds an init for a single issue table (id PK).
func issueInitParams(cg string) initParams {
	return initParams{
		ClientGroupID: cg,
		Tables: map[string]tableSchemaParams{
			"issue": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "title": {Type: "string"}, "number": {Type: "number"}, "_0_version": {Type: "string"}},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}},
			},
		},
	}
}

// F5 drive-only contract: the streaming variant carries the engine's
// RowChanges, which only exist when driving. In non-drive mode it must REFUSE
// rather than silently drop the derived diff. Runs everywhere (no engine write
// → no BEGIN CONCURRENT needed).
func TestAdvanceToHeadStream_RequiresDriveMode(t *testing.T) {
	path, _ := makeReplica(t)

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	srv.advanceDriveEnabled = false // snapshotter armed, but NOT driving
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)
	if group == nil || group.snap == nil {
		t.Fatalf("snapshotter not armed after init (group=%v)", group)
	}

	w, frames := collectAdvanceToHeadStreamFrames()
	req := RPCRequest{Method: "advanceToHeadStream", ID: 2, Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch,
	})}
	resp := srv.handleAdvanceToHeadStream(req, w)

	if resp.Error == nil {
		t.Fatalf("expected an error in non-drive mode, got result %+v", resp.Result)
	}
	if !strings.Contains(resp.Error.Message, "drive mode") {
		t.Errorf("error message = %q, want it to mention drive mode", resp.Error.Message)
	}
	if len(*frames) != 0 {
		t.Errorf("expected no partial frames on rejection, got %d", len(*frames))
	}
}

// A TRUNCATE aborts the diff at Collect() BEFORE the engine apply, so the
// handler emits a single Final frame carrying the reset + version (no
// RowChanges) and the caller re-hydrates. This path takes no engine write, so
// it runs everywhere (no BEGIN CONCURRENT needed) even though it is drive mode.
func TestAdvanceToHeadStream_DriveTruncateEmitsResetFrame(t *testing.T) {
	path, db := makeReplica(t)

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	srv.advanceDriveEnabled = true
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	// V2: a table-wide TRUNCATE of issue (pos=-1 sorts first within its version,
	// op='t'). The snapshotter Diff aborts with a truncation ResetSignal.
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',-1,'issue','0000000002','t')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	w, frames := collectAdvanceToHeadStreamFrames()
	req := RPCRequest{Method: "advanceToHeadStream", ID: 2, Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch,
	})}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error != nil {
		t.Fatalf("advanceToHeadStream(truncate) error: %+v", resp.Error)
	}
	if resp.Result != "done" {
		t.Errorf("result = %v, want \"done\"", resp.Result)
	}

	if len(*frames) != 1 {
		t.Fatalf("want exactly 1 (reset) frame, got %d: %+v", len(*frames), *frames)
	}
	f := (*frames)[0]
	if !f.Final {
		t.Errorf("reset frame must be Final")
	}
	if f.Reset == nil {
		t.Fatalf("want a Reset on the frame, got nil")
	}
	if f.Reset.Reason != "truncation" {
		t.Errorf("reset reason = %q, want truncation", f.Reset.Reason)
	}
	if f.Version != "0000000002" {
		t.Errorf("version = %q, want 0000000002", f.Version)
	}
	if len(f.Changes) != 0 {
		t.Errorf("reset frame must carry no RowChanges, got %+v", f.Changes)
	}
}

// Full drive streaming path: apply Go's own derived diff to the engine and
// stream the resulting RowChanges. Reassembling the frames must reproduce the
// non-streaming advanceToHead result (version + RowChanges), with version +
// numChanges on the Final frame. Skips without BEGIN CONCURRENT (the apply
// writes into a past-pinned snapshot) — same constraint as
// TestAdvanceToHead_DriveProducesRowChanges; the full path is validated by the
// rust-test soak.
func TestAdvanceToHeadStream_DriveReassembles(t *testing.T) {
	path, db := makeReplica(t)
	if !beginConcurrentSupported(t, db) {
		t.Skip("drive mode writes into a past-pinned snapshot — requires BEGIN CONCURRENT (wal2/libsqlite3 build); validated via the rust-test soak")
	}

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	srv.advanceDriveEnabled = true
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	// Hydrate a query so the advance produces RowChanges for it.
	addReq := RPCRequest{Method: "addQuery", ID: 2, Params: mustMarshal(t, addQueryParams{
		ClientGroupID: "cg1",
		QueryID:       "q1",
		AST:           builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		InitEpoch:     group.initEpoch,
	})}
	if resp := srv.handleAddQuery(addReq); resp.Error != nil {
		t.Fatalf("addQuery error: %+v", resp.Error)
	}

	// V2: add issue id=2.
	mustExec(t, db, `INSERT INTO "issue" VALUES ('2','two',2,'0000000002')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',0,'issue','{"id":"2"}','s')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	w, frames := collectAdvanceToHeadStreamFrames()
	req := RPCRequest{Method: "advanceToHeadStream", ID: 3, Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch,
	})}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error != nil {
		t.Fatalf("advanceToHeadStream(drive) error: %+v", resp.Error)
	}
	if resp.Result != "done" {
		t.Errorf("result = %v, want \"done\"", resp.Result)
	}

	assertStreamFrameInvariants(t, *frames)

	final := (*frames)[len(*frames)-1]
	if final.Version != "0000000002" {
		t.Errorf("final version = %q, want 0000000002", final.Version)
	}
	if final.NumChanges != 1 {
		t.Errorf("final numChanges = %d, want 1", final.NumChanges)
	}
	if final.Reset != nil {
		t.Errorf("unexpected reset: %+v", final.Reset)
	}
	if final.Drift != nil {
		t.Errorf("unexpected drift: %+v", final.Drift)
	}

	// Reassemble: concatenating every frame's Changes reproduces the
	// non-streaming RowChanges (one add of id=2 for q1).
	var reassembled []engineRowChangeAlias
	for _, f := range *frames {
		for _, rc := range f.Changes {
			reassembled = append(reassembled, engineRowChangeAlias{rc.Type, rc.QueryID, rc.Table, rc.RowKey})
		}
	}
	if len(reassembled) != 1 {
		t.Fatalf("reassembled %d RowChanges, want 1: %+v", len(reassembled), reassembled)
	}
	rc := reassembled[0]
	if rc.Type != 0 || rc.QueryID != "q1" || rc.Table != "issue" || rc.RowKey["id"] != "2" {
		t.Errorf("reassembled RowChange wrong: %+v", rc)
	}
}

// engineRowChangeAlias captures just the fields we assert on, so the test does
// not depend on the full engine.RowChange row payload shape.
type engineRowChangeAlias struct {
	Type    int
	QueryID string
	Table   string
	RowKey  map[string]interface{}
}
