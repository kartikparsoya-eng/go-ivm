package tablesource

// Parallel advance fanout (DESIGN-streaming-advance follow-up, 2026-07-02).
//
// genPushAndWrite fans one source-level change out to every subscribed
// connection. Serially that is the LAST single-threaded stage of the advance
// path (the engine's per-change loop is inherently ordered — change N+1's
// reads must observe N — but the fanout of ONE change across pipelines is
// not). This file parallelizes it with the QUERY as the serialization unit:
//
//   - Connections are grouped by the pipeline group tag assigned at Connect
//     time (engine threads its queryID through engineSource — see
//     engine.connGroupTagger). Connections of ONE query share spine
//     operators (a query reading a table via two branches — EXISTS leg +
//     related leg — converges at a join/exists and shares Take/Format and
//     the companion state above it), so they MUST push sequentially, in
//     registration order. Connections of DIFFERENT queries share no
//     operators (each query is its own BuildPipeline tree with per-query
//     ClientGroupStorage), so their pushes may run concurrently.
//
// What the concurrent goroutines DO share, and why each is safe:
//
//   - the Source itself: s.overlay is written before the goroutines spawn
//     and cleared after they join (read-only during fanout; same contract
//     ivm.GenPushParallel documents); fetches during push go through the
//     checkout stmt cache (concurrent same-SQL gets take distinct stmts —
//     see checkoutSelectLocked) and interleaved cursor stepping on the one
//     prev-tx conn is mechanically safe (SQLite serializes sqlite3_step on
//     the connection mutex; verified by the E3 experiment + -race suites).
//     conn.lastPushedEpoch is written only by the connection's own group
//     goroutine and read only from fetches on that same goroutine.
//   - the engine's Streamer: Accumulate is mutex-guarded and documents the
//     cross-query ordering contract (D8) — order across concurrently
//     pushing pipelines follows lock acquisition and is non-deterministic;
//     rows WITHIN one query keep their push order. Downstream consumers
//     are per-query (TS routes RowChanges by queryID; the CVR merge keys
//     by (query,row); the shadow comparator sorts), so cross-query
//     interleave is semantically inert.
//   - the AdvanceStream chunkSink: frame emission serializes under flushMu
//     (engine.go) — built parallel-ready for exactly this.
//
// Panic discipline mirrors ivm/parallel.go: a panic on a spawned goroutine
// is FATAL to the process (no outer recover can catch it), so each group
// goroutine recovers into a slot and the caller re-raises on its own
// goroutine after the join — non-drift (programmer bug) prioritized over
// *ivm.DriftError (self-healing re-init), scanning slots in group
// registration order so the surfaced panic is deterministic. Note one
// intentional divergence from the serial path on drift: serially,
// pipelines after the panicking one never ran; in parallel they (and their
// operator-storage writes) may have completed. Both cases funnel into the
// same recovery — the engine emits the partial output and TS re-inits the
// whole client group, discarding all pipeline+storage state — so the
// difference is unobservable past the recovery boundary.
//
// The single-group / knob-off path falls back to the exact serial loop.

import (
	"os"
	"strconv"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// ParallelAdvance gates the per-query parallel fanout. Production default
// ON (the whole point of grouping connections); GO_IVM_PARALLEL_ADVANCE=false
// reverts to the serial loop. Exported as a var so engine-level tests can
// toggle it without env plumbing.
var ParallelAdvance = os.Getenv("GO_IVM_PARALLEL_ADVANCE") != "false"

// ParallelAdvanceWorkers bounds how many query groups push concurrently per
// source-change. Follows the ONE parallelism knob (GO_IVM_PARALLELISM,
// default 4 — same knob that sizes hydrate lanes); 1 disables. Fetch I/O on
// the shared prev-tx conn serializes inside SQLite regardless, so workers
// beyond GOMAXPROCS buy nothing — this bounds Go-side operator compute.
var ParallelAdvanceWorkers = func() int {
	if v := os.Getenv("GO_IVM_PARALLELISM"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 4
}()

// SetNextConnectGroup tags the next Connect call's connection with a
// pipeline group (the engine's queryID). Set-then-Connect is race-free:
// pipeline builds are serialized under Engine.mu and Connect runs
// synchronously inside the build. Connections created without a tag
// (direct source tests, non-engine callers) share the "" group and are
// therefore pushed serially with each other — safe by default.
func (s *Source) SetNextConnectGroup(group string) {
	s.mu.Lock()
	s.nextConnectGroup = group
	s.mu.Unlock()
}

// fanoutPanic captures a recovered panic from one group's goroutine.
// drift and other are mutually exclusive per slot.
type fanoutPanic struct {
	drift *ivm.DriftError
	other any
}

// fanOut pushes one source-level change through every connection, in
// parallel across pipeline groups when enabled. Returns the concatenated
// outputs in GROUP-MAJOR order: groups in first-seen registration order,
// connections in registration order within each. For engine-built
// pipelines this equals plain registration order (a query's connections
// are appended contiguously during its build), and the engine discards
// Push's return value anyway — the real output rides the Streamer — but
// the determinism keeps direct Push callers stable either way.
//
// Called WITHOUT s.mu (fanout must allow recursive Fetch/RefreshSnapshot
// callbacks), after the overlay is set; the caller re-acquires s.mu for
// writeChange after this returns.
func (s *Source) fanOut(change ivm.SourceChange, epoch int, conns []*connection) []ivm.Change {
	// Partition by group, preserving registration order both across groups
	// (first-seen) and within them. output==nil conns (not yet wired) are
	// skipped exactly as the serial loop does.
	var groups [][]*connection
	groupIdx := make(map[string]int, 8)
	for _, conn := range conns {
		if conn.output == nil {
			continue
		}
		gi, ok := groupIdx[conn.group]
		if !ok {
			gi = len(groups)
			groupIdx[conn.group] = gi
			groups = append(groups, nil)
		}
		groups[gi] = append(groups[gi], conn)
	}
	if len(groups) == 0 {
		return nil
	}

	// pushGroup is the EXACT serial per-connection body (epoch bump before
	// filterPush so downstream Fetch's overlay gate matches TS genPush
	// ordering; fresh outputChange per conn).
	pushGroup := func(group []*connection) []ivm.Change {
		var out []ivm.Change
		for _, conn := range group {
			conn.lastPushedEpoch = epoch
			outputChange := sourceChangeToChange(change)
			if outputChange == nil {
				continue
			}
			out = append(out, filterPush(*outputChange, conn)...)
		}
		return out
	}

	workers := ParallelAdvanceWorkers
	if !ParallelAdvance || workers < 2 || len(groups) < 2 {
		// Serial path — behaviorally identical to the pre-parallel loop.
		var out []ivm.Change
		for _, g := range groups {
			out = append(out, pushGroup(g)...)
		}
		return out
	}

	ordered := make([][]ivm.Change, len(groups))
	panics := make([]fanoutPanic, len(groups))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		sem <- struct{}{} // bound in-flight groups to `workers`
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					if d, ok := r.(*ivm.DriftError); ok {
						panics[idx].drift = d
					} else {
						panics[idx].other = r
					}
				}
				<-sem
			}()
			ordered[idx] = pushGroup(groups[idx])
		}(i)
	}
	wg.Wait()

	// Re-raise on the caller's goroutine so the engine's recover (drift →
	// re-init) or the process-abort path (programmer bug) sees the panic in
	// the right scope. Deterministic: scan in group registration order,
	// non-drift first.
	for i := range panics {
		if panics[i].other != nil {
			panic(panics[i].other)
		}
	}
	for i := range panics {
		if panics[i].drift != nil {
			panic(panics[i].drift)
		}
	}

	var out []ivm.Change
	for _, changes := range ordered {
		out = append(out, changes...)
	}
	return out
}
