package main

// Pins for the TS economic advancement-abort port (advance_abort.go) and its
// wire contract. The formula, message, and recovery are TS's own
// (#shouldAdvanceYieldMaybeAbortAdvance → ResetPipelinesSignal
// 'advancement-timeout'); these tests pin that Go reproduces them exactly and
// that the legacy / disarmed paths are untouched.

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// abortTestSetup seeds a replica at v1, inits a drive-mode server, and stages
// nChanges logged changes at v2 (NOT yet advanced). Returns the server, the
// CG's initEpoch, and the seeding db handle (for beginConcurrentSupported
// gates — tests that COMPLETE the apply write into a past-pinned snapshot,
// which needs BEGIN CONCURRENT, i.e. the wal2/libsqlite3 build; abort-path
// tests stop before the apply and run everywhere).
func abortTestSetup(t *testing.T, nChanges int) (*Server, uint64, *sql.DB) {
	t.Helper()
	path, db := makeReplica(t)
	srv := NewServer(path)
	srv.appID = "myapp"
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg1"))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	// Hydrate one pipeline before staging changes — the established apply
	// pattern in this suite (the engine's writeChange targets the armed
	// prev-tx; without it the fixture hits a same-process "database is
	// locked" on the first applied INSERT — see the budget-test deflake).
	hydrateOneStreamOK(t, srv, "cg1", "q1",
		builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		group.initEpoch.Load())

	mustExec(t, db, `INSERT INTO issue (id, title, number, _0_version)
		SELECT 'i'||value, 't', value, '0000000002'
		FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM c WHERE value < ?1+1) SELECT value FROM c WHERE value <= ?1)`, nChanges)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op")
		SELECT '0000000002', value, 'issue', '{"id":"i'||value||'"}', 's'
		FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM c WHERE value < ?1+1) SELECT value FROM c WHERE value <= ?1)`, nChanges)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)
	return srv, group.initEpoch.Load(), db
}

func advanceToHeadStreamReq(t *testing.T, epoch uint64, total *float64, suppress bool) RPCRequest {
	t.Helper()
	return RPCRequest{Method: "advanceToHeadStream", ID: 2, Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID:        "cg1",
		InitEpoch:            epoch,
		TotalHydrationTimeMs: total,
		SuppressAbort:        suppress,
	})}
}

// TestAdvanceToHeadStream_EconomicAbort: a request-armed budget below the
// advance's real cost must abort with rpcCodeAdvanceAborted and the
// byte-identical TS message. Pre-fix (params ignored) this advance completed
// successfully — the abort is the new contract.
func TestAdvanceToHeadStream_EconomicAbort(t *testing.T) {
	savedMin := minAdvancementTimeLimitMs
	minAdvancementTimeLimitMs = 0 // deterministic: any elapsed exceeds the min gate
	defer func() { minAdvancementTimeLimitMs = savedMin }()

	srv, epoch, _ := abortTestSetup(t, 20)
	total := 0.000001 // far below the leapfrog's cost → abort at the first check
	w, frames := collectAdvanceToHeadStreamFrames()
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, &total, false), w, srv.handleAdvanceToHeadStream)

	if resp.Error == nil {
		t.Fatalf("expected economic abort, got success %+v", resp.Result)
	}
	if resp.Error.Code != rpcCodeAdvanceAborted {
		t.Fatalf("code = %d, want %d (rpcCodeAdvanceAborted): %s",
			resp.Error.Code, rpcCodeAdvanceAborted, resp.Error.Message)
	}
	// Byte-shape of the TS template literal (pipeline-driver.ts:6062-6066).
	re := regexp.MustCompile(`^Advancement exceeded timeout at \d+ of 20 changes after [0-9.]+ ms\. ` +
		`Advancement time limited based on total hydration time of 0\.000001 ms\.$`)
	if !re.MatchString(resp.Error.Message) {
		t.Fatalf("message %q does not match the TS advancement-timeout shape", resp.Error.Message)
	}
	// The abort rode the seq error slot before any partial: no Final frame
	// may have settled the stream (a half-applied diff must never look done).
	for _, fr := range *frames {
		if fr.Final {
			t.Fatalf("abort emitted a Final frame: %+v", fr)
		}
	}
}

// TestAdvanceToHeadStream_SuppressAbort mirrors TS's suppressAbort flag: the
// same starvation budget with suppress=true must complete normally.
func TestAdvanceToHeadStream_SuppressAbort(t *testing.T) {
	savedMin := minAdvancementTimeLimitMs
	minAdvancementTimeLimitMs = 0
	defer func() { minAdvancementTimeLimitMs = savedMin }()

	srv, epoch, db := abortTestSetup(t, 20)
	if !beginConcurrentSupported(t, db) {
		t.Skip("completing the apply requires BEGIN CONCURRENT (wal2/libsqlite3 build)")
	}
	total := 0.000001
	w, frames := collectAdvanceToHeadStreamFrames()
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, &total, true), w, srv.handleAdvanceToHeadStream)
	if resp.Error != nil {
		t.Fatalf("suppressAbort advance failed: %+v", resp.Error)
	}
	assertAbortTestAdvanceCompleted(t, frames)
}

// TestAdvanceToHeadStream_GenerousBudgetNoAbort: with budget ≥ cost the
// formula never fires — the steady-state production shape.
func TestAdvanceToHeadStream_GenerousBudgetNoAbort(t *testing.T) {
	srv, epoch, db := abortTestSetup(t, 20)
	if !beginConcurrentSupported(t, db) {
		t.Skip("completing the apply requires BEGIN CONCURRENT (wal2/libsqlite3 build)")
	}
	total := 60_000.0
	w, frames := collectAdvanceToHeadStreamFrames()
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, &total, false), w, srv.handleAdvanceToHeadStream)
	if resp.Error != nil {
		t.Fatalf("generous-budget advance failed: %+v", resp.Error)
	}
	assertAbortTestAdvanceCompleted(t, frames)
}

// TestAdvanceToHeadStream_AbortDisarmedWhenParamAbsent pins old-client
// compatibility: no totalHydrationTimeMs → no abort, regardless of the min
// gate (the pre-existing env-budget behavior is pinned separately by
// TestAdvanceToHeadStreamBudgetExceeded).
func TestAdvanceToHeadStream_AbortDisarmedWhenParamAbsent(t *testing.T) {
	savedMin := minAdvancementTimeLimitMs
	minAdvancementTimeLimitMs = 0
	defer func() { minAdvancementTimeLimitMs = savedMin }()

	srv, epoch, db := abortTestSetup(t, 20)
	if !beginConcurrentSupported(t, db) {
		t.Skip("completing the apply requires BEGIN CONCURRENT (wal2/libsqlite3 build)")
	}
	w, frames := collectAdvanceToHeadStreamFrames()
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, nil, false), w, srv.handleAdvanceToHeadStream)
	if resp.Error != nil {
		t.Fatalf("param-absent advance failed: %+v", resp.Error)
	}
	assertAbortTestAdvanceCompleted(t, frames)
}

func assertAbortTestAdvanceCompleted(t *testing.T, frames *[]advanceToHeadStreamPartial) {
	t.Helper()
	finals := 0
	for _, fr := range *frames {
		if fr.Final {
			finals++
			if fr.Version != "0000000002" {
				t.Fatalf("final version = %q, want 0000000002", fr.Version)
			}
			if fr.NumChanges != 20 {
				t.Fatalf("numChanges = %d, want 20", fr.NumChanges)
			}
		}
	}
	if finals != 1 {
		t.Fatalf("final frames = %d, want 1", finals)
	}
}

// TestAdvanceToHeadStream_CleanRetryableOnSnapshotterFailure pins the -32104
// contract: a failure before any state moved (here: the snapshotter torn
// down under the handler) is tagged retryable so TS retries in place
// instead of resetting.
func TestAdvanceToHeadStream_CleanRetryableOnSnapshotterFailure(t *testing.T) {
	srv, epoch, _ := abortTestSetup(t, 3)
	group := srv.getGroup("cg1", false)
	group.snap.Destroy() // force Advance → "snapshotter: not initialized"

	w, _ := collectAdvanceToHeadStreamFrames()
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, nil, false), w, srv.handleAdvanceToHeadStream)
	if resp.Error == nil {
		t.Fatal("expected clean-retryable error, got success")
	}
	if resp.Error.Code != rpcCodeAdvanceCleanRetryable {
		t.Fatalf("code = %d, want %d (rpcCodeAdvanceCleanRetryable): %s",
			resp.Error.Code, rpcCodeAdvanceCleanRetryable, resp.Error.Message)
	}
	if !strings.Contains(resp.Error.Message, "not initialized") {
		t.Fatalf("message %q should carry the underlying cause", resp.Error.Message)
	}
}

// TestAdvanceAbortMessage_TSByteShape pins byte-identity of the rendered
// message against the exact output of TS's template literal
// (pipeline-driver.ts:6062-6066) for representative values, including the
// JS number rendering (no trailing ".0" on integral floats, shortest
// round-trip decimals otherwise).
func TestAdvanceAbortMessage_TSByteShape(t *testing.T) {
	a := &advanceAbort{armed: true, totalHydrationTimeMs: 120.5, numChanges: 30000, pos: 1499}
	got := (&advanceAbortedError{msg: renderAdvanceAbortMessage(a.pos, a.numChanges, 234.56789, a.totalHydrationTimeMs)}).Error()
	want := "Advancement exceeded timeout at 1499 of 30000 changes after 234.56789 ms. " +
		"Advancement time limited based on total hydration time of 120.5 ms."
	if got != want {
		t.Fatalf("message mismatch:\n got %q\nwant %q", got, want)
	}
	// Integral values must render like JS `${120}` — no ".0".
	if s := jsNum(120); s != "120" {
		t.Fatalf("jsNum(120) = %q, want \"120\"", s)
	}
	if s := jsNum(0.000001); s != "0.000001" {
		t.Fatalf("jsNum(0.000001) = %q, want \"0.000001\"", s)
	}
}

// TestPanicMapping_AdvanceAborted pins the sink-site abort path: a typed
// abort panic must surface as rpcCodeAdvanceAborted with the BARE message
// (no "panic: " prefix — TS surfaces it as the ResetPipelinesSignal message).
func TestPanicMapping_AdvanceAborted(t *testing.T) {
	e := &advanceAbortedError{msg: "Advancement exceeded timeout at 5 of 10 changes after 99 ms. " +
		"Advancement time limited based on total hydration time of 50 ms."}
	if code := panicErrorCode(e); code != rpcCodeAdvanceAborted {
		t.Fatalf("panicErrorCode = %d, want %d", code, rpcCodeAdvanceAborted)
	}
	if msg := panicErrorMessage(e); msg != e.Error() {
		t.Fatalf("panicErrorMessage = %q, want bare %q", msg, e.Error())
	}
	if msg := panicErrorMessage("boom"); msg != "panic: boom" {
		t.Fatalf("generic panic message = %q, want prefixed", msg)
	}
}
