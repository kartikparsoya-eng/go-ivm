package main

// TS economic advancement-abort, ported line-faithfully from
// pipeline-driver.ts #shouldAdvanceYieldMaybeAbortAdvance (:6047-6068).
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
// for very large transactions AND the bound on how long the advance pins the
// inactive WAL file (the pin blocks wal2's WAL switch, so an unbounded
// advance makes the WAL grow and compound the slowness — TS's own comment).
//
// This replaces the wall-clock RPC timeout as the ONLY load-coupled abort on
// the in-process advance path. The old shape — fixed 120s timeout →
// 'unclassified' → full re-hydrate — turned systemic load into reset storms:
// slow advance → timeout → re-hydrate UNDER THE SAME LOAD → slower still,
// across every CG at once. The economic abort self-stabilizes instead: the
// budget is the reset's own cost, so aborting is never the more expensive
// choice, and CGs with cheap hydrates (cheap resets) shed load first.
//
// Faithfulness notes (the deliberate deltas, all invisible to clients):
//   - TS's `elapsed` is TimeSliceTimer processing time (excludes yields to
//     the event loop). Go's advance runs the whole handler synchronously on
//     one goroutine, so wall time IS processing time.
//   - TS checks (1) before each change and (2) on every row fetched during
//     push. Go checks (1) in the changelog feed before yielding each change
//     [advance_to_head.go changesSeq] and (2) per emitted partial from the
//     engine sink — rowMode emits per RowChange, so granularity matches.
//   - The abort surfaces as rpcCodeAdvanceAborted with the byte-identical
//     TS message; the TS side maps it to
//     ResetPipelinesSignal('advancement-timeout') — the SAME signal, reason,
//     and recovery TS's own abort produces.

import (
	"fmt"
	"strconv"
	"time"
)

// minAdvancementTimeLimitMs mirrors pipeline-driver.ts:1074
// MIN_ADVANCEMENT_TIME_LIMIT_MS. A var (not const) so tests can force the
// abort window deterministically the way advanceBudgetMs tests already do.
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
// or suppressAbort — mirroring TS's `!suppressAbort &&` gate at :6057;
// TS's shadow paths run with suppressAbort=true and the Go shadow paths use
// the unary advanceToHead which never arms this).
type advanceAbort struct {
	armed                bool
	start                time.Time
	totalHydrationTimeMs float64
	numChanges           int
	// pos counts fully-processed changes, incremented AFTER each change's
	// push completes — TS increments in the per-change `finally` (:5941) and
	// checks at the TOP of the next iteration (:5888), so at check time pos
	// is the completed count there too.
	pos int
}

// newAdvanceAbort starts the clock at handler entry (TS's timer covers the
// whole advance). numChanges isn't known until the diff exists — the handler
// sets it via setNumChanges before the first check can fire.
func newAdvanceAbort(totalHydrationTimeMs *float64, suppress bool) *advanceAbort {
	if totalHydrationTimeMs == nil || suppress {
		return &advanceAbort{}
	}
	return &advanceAbort{
		armed:                true,
		start:                time.Now(),
		totalHydrationTimeMs: *totalHydrationTimeMs,
	}
}

func (a *advanceAbort) setNumChanges(n int) { a.numChanges = n }

// check is the ported condition. Returns the typed abort error when the
// advance must stop; nil otherwise.
func (a *advanceAbort) check() error {
	if !a.armed {
		return nil
	}
	elapsed := float64(time.Since(a.start)) / float64(time.Millisecond)
	if elapsed > minAdvancementTimeLimitMs &&
		(elapsed > a.totalHydrationTimeMs ||
			(elapsed > a.totalHydrationTimeMs/2 && float64(a.pos) <= float64(a.numChanges)/2)) {
		return &advanceAbortedError{msg: renderAdvanceAbortMessage(a.pos, a.numChanges, elapsed, a.totalHydrationTimeMs)}
	}
	return nil
}

// renderAdvanceAbortMessage is TS's template literal at
// pipeline-driver.ts:6062-6066, byte-for-byte (numbers rendered as JS
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
