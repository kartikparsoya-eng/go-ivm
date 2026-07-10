package main

// Pins for the TS economic advancement-abort port (advance_abort.go) and its
// wire contract. The formula, message, and recovery are TS's own
// (#shouldAdvanceYieldMaybeAbortAdvance → ResetPipelinesSignal
// 'advancement-timeout'); these tests pin that Go reproduces them exactly and
// that the legacy / disarmed paths are untouched.

import (
	"database/sql"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/procclock"
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
	p := prodAdvanceParams("cg1", epoch)
	p.TotalHydrationTimeMs = total
	p.SuppressAbort = suppress
	return RPCRequest{Method: "advanceToHeadStream", ID: 2, Params: mustMarshal(t, p)}
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
	w, frames := collectAdvanceToHeadProdFrames(t, srv, 2)
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, &total, false), w, srv.handleAdvanceToHeadStream)

	if resp.Error == nil {
		t.Fatalf("expected economic abort, got success %+v", resp.Result)
	}
	if resp.Error.Code != rpcCodeAdvanceAborted {
		t.Fatalf("code = %d, want %d (rpcCodeAdvanceAborted): %s",
			resp.Error.Code, rpcCodeAdvanceAborted, resp.Error.Message)
	}
	// Byte-shape of the TS template literal (pipeline-driver.ts:2657-2659).
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
	w, frames := collectAdvanceToHeadProdFrames(t, srv, 2)
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, &total, true), w, srv.handleAdvanceToHeadStream)
	if resp.Error != nil {
		t.Fatalf("suppressAbort advance failed: %+v", resp.Error)
	}
	assertAbortTestAdvanceCompleted(t, frames, 20)
}

// TestAdvanceToHeadStream_GenerousBudgetNoAbort: with budget ≥ cost the
// formula never fires — the steady-state production shape.
func TestAdvanceToHeadStream_GenerousBudgetNoAbort(t *testing.T) {
	srv, epoch, db := abortTestSetup(t, 20)
	if !beginConcurrentSupported(t, db) {
		t.Skip("completing the apply requires BEGIN CONCURRENT (wal2/libsqlite3 build)")
	}
	total := 60_000.0
	w, frames := collectAdvanceToHeadProdFrames(t, srv, 2)
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, &total, false), w, srv.handleAdvanceToHeadStream)
	if resp.Error != nil {
		t.Fatalf("generous-budget advance failed: %+v", resp.Error)
	}
	assertAbortTestAdvanceCompleted(t, frames, 20)
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
	w, frames := collectAdvanceToHeadProdFrames(t, srv, 2)
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, nil, false), w, srv.handleAdvanceToHeadStream)
	if resp.Error != nil {
		t.Fatalf("param-absent advance failed: %+v", resp.Error)
	}
	assertAbortTestAdvanceCompleted(t, frames, 20)
}

func assertAbortTestAdvanceCompleted(t *testing.T, frames *[]advanceToHeadStreamPartial, wantChanges int) {
	t.Helper()
	finals := 0
	for _, fr := range *frames {
		if fr.Final {
			finals++
			if fr.Version != "0000000002" {
				t.Fatalf("final version = %q, want 0000000002", fr.Version)
			}
			if fr.NumChanges != wantChanges {
				t.Fatalf("numChanges = %d, want %d", fr.NumChanges, wantChanges)
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
	group.snap.Destroy() // force Advance → "snapshotter: destroyed" (L13)

	w, _ := collectAdvanceToHeadProdFrames(t, srv, 2)
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, nil, false), w, srv.handleAdvanceToHeadStream)
	if resp.Error == nil {
		t.Fatal("expected clean-retryable error, got success")
	}
	if resp.Error.Code != rpcCodeAdvanceCleanRetryable {
		t.Fatalf("code = %d, want %d (rpcCodeAdvanceCleanRetryable): %s",
			resp.Error.Code, rpcCodeAdvanceCleanRetryable, resp.Error.Message)
	}
	if !strings.Contains(resp.Error.Message, "destroyed") {
		t.Fatalf("message %q should carry the underlying cause", resp.Error.Message)
	}
}

// TestAdvanceAbortMessage_TSByteShape pins byte-identity of the rendered
// message against the exact output of TS's template literal
// (pipeline-driver.ts:2657-2659) for representative values, including the
// JS number rendering (no trailing ".0" on integral floats, shortest
// round-trip decimals otherwise).
func TestAdvanceAbortMessage_TSByteShape(t *testing.T) {
	got := (&advanceAbortedError{msg: renderAdvanceAbortMessage(1499, 30000, 234.56789, 120.5)}).Error()
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

// TestAdvanceAbortFormula_InjectedClock pins the ported condition itself
// (previously covered only end-to-end) with a deterministic elapsed source,
// at the default 50ms floor.
func TestAdvanceAbortFormula_InjectedClock(t *testing.T) {
	cases := []struct {
		name    string
		elapsed float64
		hyd     float64
		pos     int
		num     int
		abort   bool
	}{
		{"under floor guards tiny budgets", 49, 10, 0, 10, false},
		{"over floor and over budget", 51, 10, 9, 10, true},
		{"over floor, under half budget", 60, 200, 0, 10, false},
		{"half budget spent, behind schedule", 120, 200, 5, 10, true},
		{"half budget spent, ahead of schedule", 120, 200, 6, 10, false},
		{"full budget spent, ahead of schedule", 201, 200, 9, 10, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &advanceAbort{
				armed:                true,
				totalHydrationTimeMs: tc.hyd,
				elapsedMsForTest:     func() float64 { return tc.elapsed },
			}
			a.setNumChanges(tc.num)
			a.pos.Store(int64(tc.pos))
			err := a.check()
			if tc.abort && err == nil {
				t.Fatalf("elapsed=%v hyd=%v pos=%d/%d: want abort, got nil",
					tc.elapsed, tc.hyd, tc.pos, tc.num)
			}
			if !tc.abort && err != nil {
				t.Fatalf("elapsed=%v hyd=%v pos=%d/%d: unexpected abort: %v",
					tc.elapsed, tc.hyd, tc.pos, tc.num, err)
			}
		})
	}
	// Unarmed zero value: every check passes.
	if err := (&advanceAbort{}).check(); err != nil {
		t.Fatalf("unarmed check aborted: %v", err)
	}
}

// spinThreadCPU burns the calling thread's CPU until its thread-CPU clock
// advances by d. Caller must be locked to its OS thread (Begin does this).
// Measuring the spin against the same clock the abort reads makes the
// wall-vs-CPU pin below deterministic.
func spinThreadCPU(d time.Duration) {
	start := procclock.ThreadCPUNS()
	if start < 0 {
		return
	}
	for procclock.ThreadCPUNS()-start < int64(d) { //nolint:revive // hot loop
	}
}

// TestAdvanceAbortMeasurement_WallAbortsCPUDoesNot is the measurement pin
// for the 2026-07-06 ART finding: an advance that did ~2ms of real work but
// sat descheduled for 80ms must NOT abort under the production CPU clock —
// while the same scenario measured by wall (the pre-fix semantics, driven
// through the test seam) provably WOULD have. Both halves are deterministic:
// the spin is measured by the same clock the abort reads, and the sleep is
// pure wall.
func TestAdvanceAbortMeasurement_WallAbortsCPUDoesNot(t *testing.T) {
	savedMin := minAdvancementTimeLimitMs
	minAdvancementTimeLimitMs = 10 // floor below the 80ms stall, above the 2ms work
	defer func() { minAdvancementTimeLimitMs = savedMin }()

	// Production semantics: per-thread CPU. 2ms work + 80ms stall < 10ms floor.
	a := &advanceAbort{armed: true, totalHydrationTimeMs: 30, clk: &procclock.Accumulator{}}
	a.setNumChanges(4)
	stop := a.beginProcessing()
	spinThreadCPU(2 * time.Millisecond)
	time.Sleep(80 * time.Millisecond) // scheduler-queueing / GC-wait analog
	if err := a.check(); err != nil {
		t.Fatalf("CPU-measured abort fired on a stalled-but-idle advance: %v", err)
	}
	stop()

	// Pre-fix semantics (wall from arm time) via the seam: the same 80ms
	// stall spends the whole budget and must abort — proving the scenario
	// discriminates and the old clock was the storm.
	start := time.Now()
	w := &advanceAbort{
		armed:                true,
		totalHydrationTimeMs: 30,
		elapsedMsForTest: func() float64 {
			return float64(time.Since(start)) / float64(time.Millisecond)
		},
	}
	w.setNumChanges(4)
	time.Sleep(80 * time.Millisecond)
	if err := w.check(); err == nil {
		t.Fatal("wall-measured control did not abort — the scenario no longer discriminates")
	}
}

// TestAdvanceToHeadStream_NoAbortUnderSchedulerContention is the end-to-end
// regression pin for the ART storm: 8 workers × concurrent CG advances
// inflated WALL past the 50ms floor while each advance did ~15ms of work —
// 210 false aborts → 49 resets → 5 CG teardowns in ~90s. Here spinners
// saturate the scheduler while a trivial 4-change advance runs armed with a
// 1ms hydration budget: its CPU is far under the (default) 50ms floor, so it
// must complete. Under the old wall clock this same shape aborted
// load-dependently.
func TestAdvanceToHeadStream_NoAbortUnderSchedulerContention(t *testing.T) {
	srv, epoch, db := abortTestSetup(t, 4)
	if !beginConcurrentSupported(t, db) {
		t.Skip("completing the apply requires BEGIN CONCURRENT (wal2/libsqlite3 build)")
	}

	stopSpin := make(chan struct{})
	var spinners sync.WaitGroup
	for i := 0; i < 3*runtime.GOMAXPROCS(0); i++ {
		spinners.Add(1)
		go func() {
			defer spinners.Done()
			for {
				select {
				case <-stopSpin:
					return
				default:
				}
			}
		}()
	}
	defer func() { close(stopSpin); spinners.Wait() }()

	total := 1.0 // 1ms budget: the 50ms floor is the only guard left
	w, frames := collectAdvanceToHeadProdFrames(t, srv, 2)
	resp := srv.handleStreamWithRecover(advanceToHeadStreamReq(t, epoch, &total, false), w, srv.handleAdvanceToHeadStream)
	if resp.Error != nil {
		t.Fatalf("advance failed under scheduler contention (wall leaking into the processing budget?): %+v", resp.Error)
	}
	assertAbortTestAdvanceCompleted(t, frames, 4)
}
