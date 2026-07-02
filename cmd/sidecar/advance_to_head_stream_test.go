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
	if len(f.Rows) != 0 {
		t.Errorf("reset frame must carry no RowChanges, got %+v", f.Rows)
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
		for _, rc := range fromPositional(positionalChanges{Dict: f.Dict, Rows: f.Rows}) {
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

// Row-mode (NAPI) advanceToHeadStream: with abiDeliver set and rowMode=true,
// the engine's RowChanges must cross as per-row records (kind 2/3 on the
// delivery queue) with the terminal Final frame (kind 1) carrying
// Version/NumChanges — and NOTHING on streamW (the ordering invariant: one
// RPC's output on ONE queue). Skips without BEGIN CONCURRENT — same
// constraint as TestAdvanceToHeadStream_DriveReassembles.
func TestAdvanceToHeadStream_RowMode(t *testing.T) {
	path, db := makeReplica(t)
	if !beginConcurrentSupported(t, db) {
		t.Skip("drive mode writes into a past-pinned snapshot — requires BEGIN CONCURRENT (wal2/libsqlite3 build); validated via the rust-test soak")
	}

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	srv.advanceDriveEnabled = true
	t.Cleanup(srv.closeAll)

	// Arm the row plane exactly as the NAPI host does (abi.go wires
	// server.abiDeliver to the addon's delivery callback).
	col := newSinkCollector()
	srv.abiDeliver = col.sink

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

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
	req := RPCRequest{Method: "advanceToHeadStream", ID: float64(3), Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch, RowMode: true,
	})}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error != nil {
		t.Fatalf("advanceToHeadStream(rowMode) error: %+v", resp.Error)
	}
	if resp.Result != "done" {
		t.Errorf("result = %v, want \"done\"", resp.Result)
	}

	// The ordering invariant: nothing rides streamW in row mode.
	if len(*frames) != 0 {
		t.Fatalf("row mode must not emit streamW frames, got %d: %+v", len(*frames), *frames)
	}

	// Handler calls deliver synchronously — no waiting needed.
	col.mu.Lock()
	entries := append([]sinkEntry(nil), col.entries...)
	col.mu.Unlock()

	var def *decodedGroupDef
	var rowIDs []string
	var finalSeen bool
	for _, e := range entries {
		switch e.kind {
		case abiKindGroupDef:
			d := decodeGroupDef(t, e.payload)
			if d.reqID != 3 || d.queryID != "q1" || d.table != "issue" {
				t.Fatalf("groupDef content: %+v", d)
			}
			def = &d
		case abiKindRow:
			if def == nil {
				t.Fatal("ORDERING VIOLATION: row record before its groupDef")
			}
			if finalSeen {
				t.Fatal("ORDERING VIOLATION: row record after final frame")
			}
			dr := decodeRowRecord(t, e.payload, len(def.cols))
			if dr.reqID != 3 {
				t.Fatalf("row for wrong req: %+v", dr)
			}
			for i, c := range def.cols {
				if c == "id" {
					rowIDs = append(rowIDs, dr.values[i].(string))
				}
			}
		case abiKindFrame:
			respF := decodeResp(t, e.payload)
			if id, ok := toFloat(respF.ID); !ok || id != 3 {
				continue
			}
			m, ok := respF.Result.(map[string]interface{})
			if !ok {
				t.Fatalf("kind-1 frame result not a map: %#v", respF.Result)
			}
			if fin, _ := m["final"].(bool); !fin {
				t.Fatalf("only the Final partial may ship as a frame in row mode, got: %#v", m)
			}
			finalSeen = true
			if v, _ := m["version"].(string); v != "0000000002" {
				t.Errorf("final version = %q, want 0000000002", v)
			}
			nc, _ := toFloat(m["numChanges"])
			if nc != 1 {
				t.Errorf("final numChanges = %v, want 1", m["numChanges"])
			}
			// The single add row rode the record plane; the final frame
			// must carry no fallback rows.
			if rows, exists := m["r"]; exists && rows != nil {
				t.Errorf("final frame carries fallback rows: %#v", rows)
			}
		}
	}
	if !finalSeen {
		t.Fatal("no Final kind-1 frame delivered")
	}
	if len(rowIDs) != 1 || rowIDs[0] != "2" {
		t.Errorf("row records = %v, want [2]", rowIDs)
	}
}

// Row-mode + reset (TRUNCATE): the diff aborts BEFORE the engine apply, so no
// row records exist and the single Final reset frame legitimately rides
// streamW (ordering trivially preserved — nothing else in flight for the id).
// Runs everywhere (no engine write → no BEGIN CONCURRENT needed).
func TestAdvanceToHeadStream_RowModeTruncateResetViaStreamW(t *testing.T) {
	path, db := makeReplica(t)

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	srv.advanceDriveEnabled = true
	t.Cleanup(srv.closeAll)

	col := newSinkCollector()
	srv.abiDeliver = col.sink

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',-1,'issue','0000000002','t')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	w, frames := collectAdvanceToHeadStreamFrames()
	req := RPCRequest{Method: "advanceToHeadStream", ID: float64(2), Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch, RowMode: true,
	})}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error != nil {
		t.Fatalf("advanceToHeadStream(rowMode truncate) error: %+v", resp.Error)
	}
	if resp.Result != "done" {
		t.Errorf("result = %v, want \"done\"", resp.Result)
	}

	if len(*frames) != 1 {
		t.Fatalf("want exactly 1 (reset) streamW frame, got %d", len(*frames))
	}
	f := (*frames)[0]
	if !f.Final || f.Reset == nil || f.Reset.Reason != "truncation" || f.Version != "0000000002" {
		t.Errorf("reset frame wrong: %+v", f)
	}
	// No records were produced (the abort precedes the engine apply).
	col.mu.Lock()
	defer col.mu.Unlock()
	for _, e := range col.entries {
		if e.kind == abiKindRow || e.kind == abiKindGroupDef {
			t.Fatalf("unexpected record delivery on the reset path: kind=%d", e.kind)
		}
	}
}

// Stale initEpoch + rowMode: the epoch guard runs BEFORE the row plane is
// created, so a torn-down caller's advance must produce ONE error frame and
// ZERO row-plane records (a leaked record for a dead RPC would be dropped by
// TS, but a leaked groupDef would poison the registry for a reused id).
// Runs everywhere (rejected before any engine write).
func TestAdvanceToHeadStream_RowModeStaleEpochNoRecords(t *testing.T) {
	path, _ := makeReplica(t)

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	srv.advanceDriveEnabled = true
	t.Cleanup(srv.closeAll)

	col := newSinkCollector()
	srv.abiDeliver = col.sink

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	w, frames := collectAdvanceToHeadStreamFrames()
	req := RPCRequest{Method: "advanceToHeadStream", ID: float64(2), Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch + 99, RowMode: true,
	})}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error == nil {
		t.Fatalf("stale epoch must error, got %+v", resp.Result)
	}
	if len(*frames) != 0 {
		t.Errorf("stale epoch must emit no partial frames, got %d", len(*frames))
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	for _, e := range col.entries {
		if e.kind == abiKindRow || e.kind == abiKindGroupDef {
			t.Fatalf("stale epoch leaked a row-plane record (kind=%d)", e.kind)
		}
	}
}
