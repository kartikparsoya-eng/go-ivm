package main

// User's-audit items in the row plane + advance handlers:
//
//   a7 — remove-first groups must regain the row-record fast path: the
//        first add/edit after a remove-first def mints a REPLACEMENT group
//        (fresh id, full columns) instead of pinning the (queryID,table) to
//        the msgpack frame plane for the rest of the RPC.
//   a3 — advanceToHead[Stream] enforce GO_IVM_ADVANCE_BUDGET_MS so a slow
//        advance cannot pin the WAL frame indefinitely (TS suppresses its
//        own breaker in Go-primary mode).

import (
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestRowPlaneRemoveFirstGroupRegainsRecords (a7): pre-fix, a group whose
// FIRST change was a remove interned cols=nil forever — the remove itself
// encoded fine (PK-only def), but EVERY later add/edit for that
// (queryID,table) fell back to a frame for the rest of the RPC. Post-fix
// the first add mints a replacement def and rides the record plane.
func TestRowPlaneRemoveFirstGroupRegainsRecords(t *testing.T) {
	col := newSinkCollector()
	rp := rowPlaneForTest(t, col, 77)

	// chunkSize=1 partials, exactly like the production rowMode path.
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcRemove("q1", "a")},
	}, "0000000002", 1)
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcAdd("q1", "b")},
	}, "0000000002", 1)
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcAdd("q1", "c")},
	}, "0000000002", 1)

	defs, rows, frames := countKinds(col)
	// def#1 (PK-only, remove-first) + def#2 (full columns, minted by the
	// first add). Pre-fix: defs=1, rows=1 (the remove), frames=2 (both adds
	// fell back).
	if defs != 2 {
		t.Fatalf("groupDefs = %d, want 2 (PK-only + full-column replacement)", defs)
	}
	if rows != 3 {
		t.Fatalf("row records = %d, want 3 (remove + BOTH adds as records; "+
			"pre-a7 the adds fell back to frames forever)", rows)
	}
	if frames != 0 {
		t.Fatalf("fallback frames = %d, want 0 (non-final partials with "+
			"encodable rows emit no frame)", frames)
	}
}

// TestRowPlaneRemoveOnlyGroupStaysOnRecords: removes for a never-added
// group keep working against the PK-only def (regression guard around the
// a7 change).
func TestRowPlaneRemoveOnlyGroupStaysOnRecords(t *testing.T) {
	col := newSinkCollector()
	rp := rowPlaneForTest(t, col, 78)
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcRemove("q1", "a")},
	}, "0000000002", 1)
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcRemove("q1", "b")},
	}, "0000000002", 1)
	defs, rows, frames := countKinds(col)
	if defs != 1 || rows != 2 || frames != 0 {
		t.Fatalf("remove-only group: defs=%d rows=%d frames=%d, want 1/2/0", defs, rows, frames)
	}
}

// TestAdvanceBudgetChecker (a3): the per-phase/per-partial budget check
// panics a PLAIN error (never a DataError — the TS classifier must file it
// 'unclassified' → reset, not 'data-error' → teardown) once the deadline
// passes, and stays silent before it / when disabled.
func TestAdvanceBudgetChecker(t *testing.T) {
	// Before the deadline: no panic.
	checkAdvanceBudget(time.Now().Add(time.Hour), true, "apply", "cg1")
	// Disabled: no panic even with an expired deadline.
	checkAdvanceBudget(time.Now().Add(-time.Hour), false, "apply", "cg1")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expired budget did not panic")
		}
		if _, isDataErr := r.(*ivm.DataError); isDataErr {
			t.Fatalf("budget panic is a DataError — TS would classify 'data-error' " +
				"(teardown, never reset); it must be the typed abort → reset")
		}
		// The typed abort maps to rpcCodeAdvanceAborted → TS resets via
		// ResetPipelinesSignal('advancement-timeout'). A plain string would
		// surface as -32000 'unclassified' — which now RETHROWS (CG teardown)
		// under the follow-TS failure model, the wrong disposition for a
		// deliberate time bound.
		aerr, ok := r.(*advanceAbortedError)
		if !ok {
			t.Fatalf("budget panic value = %T, want *advanceAbortedError", r)
		}
		msg := aerr.Error()
		for _, needle := range []string{"GO_IVM_ADVANCE_BUDGET_MS", "apply", "cg1", "reset"} {
			if !strings.Contains(msg, needle) {
				t.Fatalf("budget panic %q missing %q", msg, needle)
			}
		}
	}()
	checkAdvanceBudget(time.Now().Add(-time.Millisecond), true, "apply", "cg1")
}

// TestAdvanceToHeadStreamBudgetExceeded (a3, handler-level): with a 1ms
// budget and a 2000-row derived diff, the derive+Collect phase alone
// overruns the deadline, so the "collect" checkpoint (which runs BEFORE the
// engine apply — no BEGIN CONCURRENT needed, so this runs everywhere)
// panics and handleStreamWithRecover surfaces it as an RPC error — the
// exact path production takes through the dispatch loop.
func TestAdvanceToHeadStreamBudgetExceeded(t *testing.T) {
	saved := advanceBudgetMs
	advanceBudgetMs = 1
	defer func() { advanceBudgetMs = saved }()

	path, db := makeReplica(t)
	srv := NewServer(path)
	srv.appID = "myapp"
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	// 30000 changes: deriving + Collecting this diff takes well over 1ms of
	// SQLite work EVERYWHERE, so the deadline (anchored at handler entry) is
	// expired by the collect checkpoint. Sized up from 2000 (flaked 6/20 in
	// isolation on an M-series: sub-millisecond derive → collect checkpoint
	// passed → the apply path's fixture-only "database is locked" panic
	// preempted the apply checkpoint and failed the message assertion).
	mustExec(t, db, `INSERT INTO issue (id, title, number, _0_version)
		SELECT 'i'||value, 't', value, '0000000002'
		FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM c WHERE value < 30000) SELECT value FROM c)`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op")
		SELECT '0000000002', value, 'issue', '{"id":"i'||value||'"}', 's'
		FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM c WHERE value < 30000) SELECT value FROM c)`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	w, _ := collectAdvanceToHeadStreamFrames()
	req := RPCRequest{Method: "advanceToHeadStream", ID: 2, Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch.Load(),
	})}
	resp := srv.handleStreamWithRecover(req, w, srv.handleAdvanceToHeadStream)
	if resp.Error == nil {
		t.Fatalf("expected budget-exceeded error, got result %+v", resp.Result)
	}
	if !strings.Contains(resp.Error.Message, "GO_IVM_ADVANCE_BUDGET_MS") {
		t.Fatalf("error = %q, want it to mention GO_IVM_ADVANCE_BUDGET_MS", resp.Error.Message)
	}
}
