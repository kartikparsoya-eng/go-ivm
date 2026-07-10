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
	"fmt"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
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
// successful advanceToHeadStream: ≥1 frame, at most one leading Header,
// monotonic data/final chunkIndex from 0, exactly one Final=true and it's the
// LAST frame, version/numChanges only on the header/final metadata frames.
func assertStreamFrameInvariants(t *testing.T, frames []advanceToHeadStreamPartial) {
	t.Helper()
	if len(frames) == 0 {
		t.Fatalf("no frames emitted")
	}
	finals := 0
	dataIndex := 0
	for i, f := range frames {
		if f.Header {
			if i != 0 {
				t.Errorf("Header frame at index %d, want leading frame only", i)
			}
			if f.Final {
				t.Errorf("Header frame must not be Final")
			}
			continue
		}
		if f.ChunkIndex != dataIndex {
			t.Errorf("frame %d: chunkIndex = %d, want data index %d (must be monotonic from 0)", i, f.ChunkIndex, dataIndex)
		}
		dataIndex++
		if f.Final {
			finals++
			if i != len(frames)-1 {
				t.Errorf("Final=true on frame %d but it is not the last (of %d)", i, len(frames))
			}
		} else {
			// version / numChanges ride metadata frames only.
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

// A change for a replicated-but-non-syncable table is skipped (not an
// error), thanks to allTableNames being read from sqlite_master. NumChanges
// (the raw changelog count) still reports it on the Final frame. No engine
// write happens (the only change is skipped), so it runs everywhere.
func TestAdvanceToHeadStream_SkipsNonSyncableTable(t *testing.T) {
	path, db := makeReplica(t)
	// A replicated table the sidecar does NOT serve.
	mustExec(t, db, `CREATE TABLE "lmids" ("clientID" TEXT PRIMARY KEY, "lmid" INTEGER, "_0_version" TEXT)`)

	srv := NewServer(path)
	srv.appID = "myapp"
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)
	if group == nil || group.snap == nil {
		t.Fatalf("snapshotter not armed after init (group=%v)", group)
	}

	// V2: a change ONLY to the non-syncable lmids table.
	mustExec(t, db, `INSERT INTO "lmids" VALUES ('client-a',7,'0000000002')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',0,'lmids','{"clientID":"client-a"}','s')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	w, frames := collectAdvanceToHeadProdFrames(t, srv, 2)
	req := RPCRequest{Method: "advanceToHeadStream", ID: 2, Params: mustMarshal(t,
		prodAdvanceParams("cg1", group.initEpoch.Load()))}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error != nil {
		t.Fatalf("advanceToHeadStream error (non-syncable should be skipped, not errored): %+v", resp.Error)
	}
	if len(*frames) != 2 {
		t.Fatalf("want header + empty Final frames, got %d: %+v", len(*frames), *frames)
	}
	if !(*frames)[0].Header {
		t.Fatalf("first frame must be Header, got %+v", (*frames)[0])
	}
	f := (*frames)[1]
	if !f.Final || len(f.Rows) != 0 {
		t.Fatalf("want an empty Final frame (lmids skipped), got %+v", f)
	}
	if f.Version != "0000000002" {
		t.Errorf("version = %q, want 0000000002", f.Version)
	}
	if f.NumChanges != 1 {
		t.Errorf("NumChanges (raw changelog count) = %d, want 1", f.NumChanges)
	}
}

// A TRUNCATE aborts the diff at Collect() BEFORE the engine apply, so the
// handler emits a single Final frame carrying the reset + version (no
// RowChanges) and the caller re-hydrates. This path takes no engine write, so
// it runs everywhere (no BEGIN CONCURRENT needed) even though it is drive mode.
func TestAdvanceToHeadStream_DriveTruncateEmitsResetFrame(t *testing.T) {
	path, db := makeReplica(t)

	srv := NewServer(path)
	srv.appID = "myapp"
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

	w, frames := collectAdvanceToHeadProdFrames(t, srv, 2)
	req := RPCRequest{Method: "advanceToHeadStream", ID: 2, Params: mustMarshal(t,
		prodAdvanceParams("cg1", group.initEpoch.Load()))}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error != nil {
		t.Fatalf("advanceToHeadStream(truncate) error: %+v", resp.Error)
	}
	if resp.Result != "done" {
		t.Errorf("result = %v, want \"done\"", resp.Result)
	}

	if len(*frames) != 2 {
		t.Fatalf("want header + reset frame, got %d: %+v", len(*frames), *frames)
	}
	if !(*frames)[0].Header {
		t.Fatalf("first frame must be Header, got %+v", (*frames)[0])
	}
	f := (*frames)[1]
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

	srv := NewServer(path)
	srv.appID = "myapp"
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	// Hydrate a query so the advance produces RowChanges for it.
	hydrateOneStreamOK(t, srv, "cg1", "q1",
		builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		group.initEpoch.Load())

	// V2: add issue id=2.
	mustExec(t, db, `INSERT INTO "issue" VALUES ('2','two',2,'0000000002')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',0,'issue','{"id":"2"}','s')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	w, frames := collectAdvanceToHeadProdFrames(t, srv, 3)
	req := RPCRequest{Method: "advanceToHeadStream", ID: 3, Params: mustMarshal(t,
		prodAdvanceParams("cg1", group.initEpoch.Load()))}
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

	srv := NewServer(path)
	srv.appID = "myapp"
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

	hydrateOneStreamOK(t, srv, "cg1", "q1",
		builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		group.initEpoch.Load())

	// V2: add issue id=2.
	mustExec(t, db, `INSERT INTO "issue" VALUES ('2','two',2,'0000000002')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',0,'issue','{"id":"2"}','s')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	// Clear hydrate entries from the collector — we only inspect
	// advance entries below. hydrateOneStreamOK uses reqID=2; without
	// clearing, its groupDef (reqID=2) fails the reqID!=3 check.
	col.mu.Lock()
	col.entries = col.entries[:0]
	col.mu.Unlock()

	w, frames := collectAdvanceToHeadStreamFrames()
	req := RPCRequest{Method: "advanceToHeadStream", ID: float64(3), Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch.Load(), RowMode: true, PullMode: true, PullWindow: 1024,
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
			if header, _ := m["header"].(bool); header {
				continue
			}
			if fin, _ := m["final"].(bool); !fin {
				t.Fatalf("only Header/Final partials may ship as frames in row mode, got: %#v", m)
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
// row records exist and the Header + Final reset frames ride the row plane's
// NAPI queue. Runs everywhere (no engine write → no BEGIN CONCURRENT needed).
func TestAdvanceToHeadStream_RowModeTruncateResetViaStreamW(t *testing.T) {
	path, db := makeReplica(t)

	srv := NewServer(path)
	srv.appID = "myapp"
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
		ClientGroupID: "cg1", InitEpoch: group.initEpoch.Load(), RowMode: true, PullMode: true, PullWindow: 1024,
	})}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error != nil {
		t.Fatalf("advanceToHeadStream(rowMode truncate) error: %+v", resp.Error)
	}
	if resp.Result != "done" {
		t.Errorf("result = %v, want \"done\"", resp.Result)
	}

	if len(*frames) != 0 {
		t.Fatalf("row-mode reset must not emit streamW frames, got %d: %+v", len(*frames), *frames)
	}

	// No row records were produced (the abort precedes the engine apply).
	col.mu.Lock()
	defer col.mu.Unlock()
	var headerSeen, resetSeen bool
	for _, e := range col.entries {
		if e.kind == abiKindRow || e.kind == abiKindGroupDef {
			t.Fatalf("unexpected record delivery on the reset path: kind=%d", e.kind)
		}
		if e.kind != abiKindFrame {
			continue
		}
		respF := decodeResp(t, e.payload)
		if id, ok := toFloat(respF.ID); !ok || id != 2 {
			continue
		}
		f, ok := advancePartialFromResult(respF.Result)
		if !ok {
			t.Fatalf("decode row-plane reset frame: %#v", respF.Result)
		}
		if f.Header {
			headerSeen = true
			continue
		}
		if f.Final {
			resetSeen = true
			if f.Reset == nil || f.Reset.Reason != "truncation" || f.Version != "0000000002" {
				t.Errorf("reset frame wrong: %+v", f)
			}
		}
	}
	if !headerSeen || !resetSeen {
		t.Fatalf("row-plane reset frames missing: header=%v reset=%v", headerSeen, resetSeen)
	}
}

// Stale initEpoch + rowMode: the epoch guard runs BEFORE the row plane is
// created, so a torn-down caller's advance must produce ONE error frame and
// ZERO row-plane records (a leaked record for a dead RPC would be dropped by
// TS, but a leaked groupDef would poison the registry for a reused id).
// Runs everywhere (rejected before any engine write).
func TestAdvanceToHeadStream_RowModeStaleEpochNoRecords(t *testing.T) {
	path, _ := makeReplica(t)

	srv := NewServer(path)
	srv.appID = "myapp"
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
		ClientGroupID: "cg1", InitEpoch: group.initEpoch.Load() + 99, RowMode: true, PullMode: true, PullWindow: 1024,
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

// TestPerfMetrics_AdvanceToHeadStreamCountsAsAdvance pins the [GO-IVM][PERF]
// accounting contract for drive mode: advanceToHeadStream REPLACES
// advanceStream there, so the worker-loop metric switch must count it in the
// advances segment. Before this was fixed, drive deployments reported
// advances=0 in every 10s PERF window while the real advance traffic was
// visible only as PERF-CHUNKS row counts — advance latency was structurally
// invisible in exactly the deployments (napi drive) being perf-tuned.
//
// Dispatches through trySendReq → g.worker (the REAL path with the metric
// switch), not a direct handler call. The handler errors (advanceToHead not
// armed on this bare server) — deliberate and load-bearing: like
// advanceStream, an errored advance still records into the count/latency
// metrics, because the metric is dispatch-level, not success-level.
func TestPerfMetrics_AdvanceToHeadStreamCountsAsAdvance(t *testing.T) {
	s := NewServer(makeReplicaPathOnly(t))
	t.Cleanup(s.closeAll)
	s.abiDeliver = newSinkCollector().sink
	g := s.getGroup("cg-perf-a2h", true)

	// metrics is package-global; tests in this package never run in
	// parallel (no t.Parallel), so a delta assertion is race-free. The
	// 10s reporter that Swap(0)s these is not started in unit tests.
	beforeCount := metrics.advanceCount.Load()
	beforeInFlight := metrics.advancesInFlight.Load()

	respCh := make(chan RPCResponse, 1)
	ok := g.trySendReq(clientGroupReq{
		req: RPCRequest{
			Method: "advanceToHeadStream",
			ID:     float64(1),
			Params: mustMarshal(t, prodAdvanceParams("cg-perf-a2h", g.initEpoch.Load())),
		},
		respCh:  respCh,
		streamW: func(_ interface{}, _ interface{}) {},
	})
	if !ok {
		t.Fatal("trySendReq refused the request")
	}

	select {
	case resp := <-respCh:
		if resp.Error == nil {
			t.Fatalf("expected not-armed error from bare server, got result %+v", resp.Result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no terminal response within 10s")
	}

	if got := metrics.advanceCount.Load() - beforeCount; got != 1 {
		t.Fatalf("advanceCount delta = %d, want 1 — advanceToHeadStream not counted in the PERF advances segment", got)
	}
	if got := metrics.advancesInFlight.Load(); got != beforeInFlight {
		t.Fatalf("advancesInFlight = %d after completion, want %d — inc/dec unbalanced", got, beforeInFlight)
	}
}

// D9 (DESIGN-duplex-streaming): the streaming handler feeds the engine from
// the changelog cursor LAZILY — no diff materialization, no size cap (the
// old GO_IVM_MAX_DIFF_CHANGES guard belonged to the deleted unary
// advanceToHead, which had to materialize its single-frame response). A
// large diff must stream to completion instead of erroring into the
// caller's reset path. Skips without BEGIN CONCURRENT (drive apply writes
// into a past-pinned snapshot) — same constraint as
// TestAdvanceToHeadStream_DriveReassembles.
func TestAdvanceToHeadStream_OversizedDiffStreamsWithoutCap(t *testing.T) {
	path, db := makeReplica(t)
	if !beginConcurrentSupported(t, db) {
		t.Skip("drive mode writes into a past-pinned snapshot — requires BEGIN CONCURRENT (wal2/libsqlite3 build)")
	}

	srv := NewServer(path)
	srv.appID = "myapp"
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	hydrateOneStreamOK(t, srv, "cg1", "q1",
		builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		group.initEpoch.Load())

	// V2: 20 new issues (4x the shrunken cap).
	const n = 20
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("d9-%03d", i)
		mustExec(t, db, `INSERT INTO "issue" VALUES (?,?,?,'0000000002')`, id, "t-"+id, 100+i)
		mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',?, 'issue', ?, 's')`,
			i, fmt.Sprintf(`{"id":%q}`, id))
	}
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	w, frames := collectAdvanceToHeadProdFrames(t, srv, 3)
	req := RPCRequest{Method: "advanceToHeadStream", ID: 3, Params: mustMarshal(t,
		prodAdvanceParams("cg1", group.initEpoch.Load()))}
	resp := srv.handleAdvanceToHeadStream(req, w)
	if resp.Error != nil {
		t.Fatalf("oversized diff errored despite lazy streaming (cap resurrected?): %+v", resp.Error)
	}
	assertStreamFrameInvariants(t, *frames)

	rows := 0
	for _, f := range *frames {
		rows += len(f.Rows)
	}
	if rows != n {
		t.Fatalf("streamed %d rows, want %d (full oversized diff)", rows, n)
	}
	final := (*frames)[len(*frames)-1]
	if final.NumChanges != n {
		t.Fatalf("final NumChanges = %d, want %d", final.NumChanges, n)
	}
}
