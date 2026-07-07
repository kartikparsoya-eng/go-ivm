package main

// TS economic advancement-abort, ported from
// pipeline-driver.ts #shouldAdvanceYieldMaybeAbortAdvance (:2357-2378).
//
// TS's contract: cancel an advancement — by aborting into a
// ResetPipelinesSignal('advancement-timeout') — when processing has provably
// become MORE EXPENSIVE than the recovery it would trigger:
//
//	elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
//	  (elapsed > totalHydrationTimeMs ||
//	    (elapsed > totalHydrationTimeMs / 2 && pos <= numChanges / 2))
//
// where totalHydrationTimeMs is the measured cost of re-hydrating every
// pipeline in the CG (the price of the reset). This is both a circuit breaker
// for very large transactions AND (in TS) the bound on how long the advance
// pins the inactive WAL file. The economic abort self-stabilizes: the budget
// is the reset's own cost, so aborting is never the more expensive choice,
// and CGs with cheap hydrates (cheap resets) shed load first.
//
// MEASUREMENT (2026-07-06 ART finding — this is the load-bearing part):
// `elapsed` is PROCESSING time, not wall time. TS's advanceTimer is the
// view-syncer Timer whose laps exclude event-loop yields
// (view-syncer.ts:2900 yieldProcess → #stopLap/#startLap); upstream even
// logs the two clocks separately ("process: X ms, wall: Y ms",
// view-syncer.ts:2566-2570). The original port measured wall from handler
// entry, reasoning "the handler is synchronous, so wall IS processing" —
// false under load on three counts: (1) with 8 in-process engines × many
// CGs advancing concurrently, scheduler queueing and GC inflate wall while
// zero work happens; (2) the clock armed BEFORE the WAL leapfrog
// (snap.Advance), counting its lock/IO waits, where TS starts its timer
// only after the diff exists (view-syncer.ts:2544); (3) the per-change
// fanout parks the coordinator in wg.Wait while worker goroutines push.
// The 50c-mutation ART run showed the failure mode: 210 aborts all at
// elapsed 50-56ms (the floor edge, several at 88-96% completion) doing
// ~15ms of real work → 49 resets → 5 CG teardowns in ~90s.
//
// The fix measures the Go analog of TS's lap clock: per-OS-thread CPU time
// (internal/procclock). The handler goroutine registers its thread once the
// diff exists; the parallel push-fanout worker goroutines bracket their
// pushGroup work into the same accumulator (engine
// AdvanceStreamChunkedSeqClocked → tablesource.SetAdvanceClock →
// parallel_fanout.go). The sum is serial-equivalent processing cost:
// scheduler wait, GC stop-the-world on other threads, and wg.Wait parking
// are excluded; SQLite reads and operator compute on the advance's own
// threads are included (GC assist on those threads counts, as it does
// inside TS laps). Every check() publishes the CALLING thread's
// in-progress slice first, so staleness is bounded by OTHER threads' work
// since their last emission — the same blind window TS has between row
// fetches. Advances that are waiting rather than working (lock convoys,
// I/O stalls) are bounded by the GO_IVM_ADVANCE_BUDGET_MS wall backstop
// (default 60s), which keeps the WAL-pin bound TS attributes to this
// abort; the economics are CPU, the runaway/pin guard stays wall.
//
// Faithfulness notes (deliberate deltas, all invisible to clients):
//   - TS checks (1) before each change and (2) on every row fetched during
//     push. Go checks (1) in the changelog feed before yielding each change
//     [advance_to_head.go changesSeq] and (2) per emitted partial from the
//     engine sink — rowMode emits per RowChange, so granularity matches.
//   - The abort surfaces as rpcCodeAdvanceAborted with the byte-identical
//     TS message; the TS side maps it to
//     ResetPipelinesSignal('advancement-timeout') — the SAME signal, reason,
//     and recovery TS's own abort produces.
//   - ivm.MemorySource's GenPushParallel (engine-test fixture, not on the
//     production path) does not bracket its workers; tablesource — the only
//     production leaf — does.

import (
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/kartikparsoya-eng/go-ivm/internal/procclock"
)

// minAdvancementTimeLimitMs mirrors pipeline-driver.ts
// MIN_ADVANCEMENT_TIME_LIMIT_MS (:366). A var (not const) so tests can force
// the abort window deterministically the way advanceBudgetMs tests already do.
var minAdvancementTimeLimitMs = 50.0

// rpcCodeAdvanceAborted: the advance hit TS's economic abort. TS maps it to
// ResetPipelinesSignal('advancement-timeout') — reset + re-hydrate, exactly
// like TS's own abort. State is (potentially) half-advanced, like TS's own
// mid-apply abort; the reset discards and rebuilds it.
const rpcCodeAdvanceAborted = -32103

// rpcCodeAdvanceCleanRetryable: the advance failed BEFORE any state moved —
// snapshotter.Advance is failure-atomic (diff built before the prev/curr swap
// commits) and the engine applied nothing. The RPC is idempotent to retry in
// place (it re-derives from the unchanged position); TS retries with bounded
// backoff instead of resetting. TS-native has no such failure class at all
// (its snapshot advance is infallible in practice) — the retry keeps that
// divergence invisible: transients vanish instead of becoming resets.
const rpcCodeAdvanceCleanRetryable = -32104

// advanceAbortedError carries the TS-identical abort message. Recognized by
// panicErrorCode/panicErrorMessage (sink-site aborts panic — the engine sink
// has no error return; the panic path is proven clean, see
// TestAdvanceStream_PanickingSink_NoDeadlockAndEngineReusable) and by
// finishStream via errors.As (feed-site aborts ride the seq error slot).
type advanceAbortedError struct{ msg string }

func (e *advanceAbortedError) Error() string { return e.msg }

// advanceAbort evaluates the TS formula for one advanceToHeadStream call.
// Zero value / unarmed = every check passes (param absent → old TS client,
// or suppressAbort — mirroring TS's `!suppressAbort &&` gate at :2367;
// TS's internal-replay paths run with suppressAbort=true).
type advanceAbort struct {
	armed                bool
	totalHydrationTimeMs float64
	// numChanges is written once (setNumChanges, before the engine call
	// spawns any goroutine) and read by checks afterwards — ordered by the
	// goroutine-creation edge, so a plain int is race-free.
	numChanges int
	// pos counts fully-processed changes, incremented AFTER each change's
	// push completes — TS increments in the per-change `finally` (:2257) and
	// checks at the TOP of the next iteration (:2204), so at check time pos
	// is the completed count there too. Atomic: the feed goroutine writes it
	// while sink-site checks read it from parallel fanout goroutines (the
	// original plain int was a data race).
	pos atomic.Int64
	// clk accumulates per-thread CPU across the coordinator + fanout
	// workers — the processing-time measure the formula evaluates.
	clk *procclock.Accumulator
	// elapsedMsForTest overrides the elapsed source (unit tests pin the
	// formula and the wall-vs-CPU distinction deterministically). nil in
	// production.
	elapsedMsForTest func() float64
}

// newAdvanceAbort captures the formula parameters. The processing clock is
// NOT running yet — the handler arms it via beginProcessing() once the
// snapshot diff exists (TS parity: its timer starts inside #processChanges,
// after the snapshotter advanced).
func newAdvanceAbort(totalHydrationTimeMs *float64, suppress bool) *advanceAbort {
	if totalHydrationTimeMs == nil || suppress {
		return &advanceAbort{}
	}
	return &advanceAbort{
		armed:                true,
		totalHydrationTimeMs: *totalHydrationTimeMs,
		clk:                  &procclock.Accumulator{},
	}
}

// beginProcessing registers the calling goroutine (the advance coordinator)
// with the processing clock. Returns the matching stop func (defer it); a
// no-op when unarmed.
func (a *advanceAbort) beginProcessing() func() {
	if !a.armed {
		return func() {}
	}
	return a.clk.Begin()
}

// clock exposes the accumulator for the engine's fanout plumbing
// (AdvanceStreamChunkedSeqClocked). nil when unarmed — the plumbing then
// costs nothing.
func (a *advanceAbort) clock() *procclock.Accumulator {
	if !a.armed {
		return nil
	}
	return a.clk
}

func (a *advanceAbort) setNumChanges(n int) { a.numChanges = n }

// check is the ported condition. Returns the typed abort error when the
// advance must stop; nil otherwise. Publishes the calling thread's
// in-progress CPU slice first, so the reading is fresh for whichever
// participating thread (coordinator or fanout worker) hit this checkpoint.
func (a *advanceAbort) check() error {
	if !a.armed {
		return nil
	}
	var elapsed float64
	if a.elapsedMsForTest != nil {
		elapsed = a.elapsedMsForTest()
	} else {
		a.clk.Checkpoint()
		elapsed = float64(a.clk.ElapsedNS()) / 1e6
	}
	pos := int(a.pos.Load())
	if elapsed > minAdvancementTimeLimitMs &&
		(elapsed > a.totalHydrationTimeMs ||
			(elapsed > a.totalHydrationTimeMs/2 && float64(pos) <= float64(a.numChanges)/2)) {
		return &advanceAbortedError{msg: renderAdvanceAbortMessage(pos, a.numChanges, elapsed, a.totalHydrationTimeMs)}
	}
	return nil
}

// renderAdvanceAbortMessage is TS's template literal at
// pipeline-driver.ts:2657-2659, byte-for-byte (numbers rendered as JS
// renders them — see jsNum). Split out so the byte-shape is pin-testable
// with controlled elapsed values.
func renderAdvanceAbortMessage(pos, numChanges int, elapsedMs, totalHydrationTimeMs float64) string {
	return fmt.Sprintf(
		"Advancement exceeded timeout at %d of %d changes after %s ms. "+
			"Advancement time limited based on total hydration time of %s ms.",
		pos, numChanges, jsNum(elapsedMs), jsNum(totalHydrationTimeMs))
}

// jsNum renders a float64 the way a JS template literal does for the value
// ranges this message carries: shortest round-trip decimal, plain notation,
// integer values without a trailing ".0" ('f'/-1 = FormatFloat shortest).
// JS switches to exponent notation only outside ~[1e-7, 1e21) — elapsed and
// hydration milliseconds never leave that range.
func jsNum(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
