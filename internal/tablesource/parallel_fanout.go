package tablesource

// Parallel advance fanout.
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
//     cross-query ordering contract — order across concurrently
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
// goroutine after the join, scanning slots in group registration order so
// the surfaced panic is deterministic. Note one intentional divergence from
// the serial path on a mid-fanout panic: serially, pipelines after the
// panicking one never ran; in parallel they (and their operator-storage
// writes) may have completed. Both cases funnel into the same disposition —
// the RPC errors and TS tears the whole client group down, discarding all
// pipeline+storage state — so the difference is unobservable past that
// boundary.
//
// The single-group / knob-off path falls back to the exact serial loop.

import (
	"context"
	"os"
	"strconv"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/internal/procclock"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// ParallelAdvance gates the per-query parallel fanout. Production default is
// ON — controlled by GO_IVM_ADVANCE_PARALLELISM (workers, default 4). Set
// GO_IVM_ADVANCE_PARALLELISM=1 for serial fanout (TS-faithful cross-query
// emission order). GO_IVM_PARALLEL_ADVANCE=false explicitly disables the
// parallel path regardless of worker count.
var ParallelAdvance = os.Getenv("GO_IVM_PARALLEL_ADVANCE") != "false"

// ParallelAdvanceWorkers bounds how many query groups push concurrently per
// source-change. GO_IVM_ADVANCE_PARALLELISM is the advance-specific knob;
// legacy GO_IVM_PARALLELISM remains a fallback. 1 disables. Fetch I/O on the
// shared prev-tx conn serializes inside SQLite regardless, so workers beyond
// GOMAXPROCS buy nothing — this bounds Go-side operator compute.
var ParallelAdvanceWorkers = advanceParallelismFromEnv()

func advanceParallelismFromEnv() int {
	for _, name := range []string{"GO_IVM_ADVANCE_PARALLELISM", "GO_IVM_PARALLELISM"} {
		if v := os.Getenv(name); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return n
			}
		}
	}
	return 4
}

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

// SetAdvanceClock installs (nil: clears) the processing-clock accumulator
// for the advance in flight. Called by the engine (under its own mu, which
// serializes advances) before the first Push of a clocked advance and — via
// defer, so the panic path clears too — after the last. fanOut's parallel
// worker goroutines bracket their pushGroup CPU into it so the sidecar's
// economic advancement-abort budget sees their work; the serial fallback
// path needs no bracket (it runs on the coordinator thread, which the
// sidecar registered itself).
func (s *Source) SetAdvanceClock(clk *procclock.Accumulator) {
	s.advanceClock.Store(clk)
}

// SetAdvanceCtx installs (nil: clears) the advance's wall-clock budget
// context. When set, SQL queries on the advance path use this context
// instead of s.ctx (the CG lifetime context) so that a query blocking on
// WAL contention is interrupted by the budget deadline — the go-sqlite3
// driver calls sqlite3_interrupt() when ctx.Done() fires. Cleared on
// advance completion (via defer, same as the clock).
func (s *Source) SetAdvanceCtx(ctx context.Context) {
	if ctx == nil {
		s.advanceCtx.Store(nil)
	} else {
		s.advanceCtx.Store(&ctx)
	}
}

// advanceQueryCtx returns the advance budget context if installed,
// otherwise s.ctx. Used by advance-path SQL queries (fetchSerial,
// fetchDuringPushStream) so the budget deadline can interrupt a
// blocked query.
func (s *Source) advanceQueryCtx() context.Context {
	if ctx := s.advanceCtx.Load(); ctx != nil {
		return *ctx
	}
	return s.ctx
}

// fanOut pushes one source-level change through every connection, in
// parallel across pipeline groups when enabled. Push output rides the
// engine's Streamer (the terminal sink) — Output.Push is void, so there is
// nothing to collect here; group scheduling only determines EXECUTION
// interleave, which the Streamer's per-query Accumulate contract absorbs
// (see the file header).
//
// Called WITHOUT s.mu (fanout must allow recursive Fetch
// callbacks), after the overlay is set; the caller re-acquires s.mu for
// writeChange after this returns.
func (s *Source) fanOut(change ivm.SourceChange, epoch int, conns []*connection) {
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
		return
	}

	// pushGroup is the EXACT serial per-connection body (epoch bump before
	// filterPush so downstream Fetch's overlay gate matches TS genPush
	// ordering; fresh outputChange per conn).
	pushGroup := func(group []*connection) {
		for _, conn := range group {
			conn.lastPushedEpoch = epoch
			outputChange := sourceChangeToChange(change)
			if outputChange == nil {
				continue
			}
			filterPush(*outputChange, conn)
		}
	}

	workers := ParallelAdvanceWorkers
	if !ParallelAdvance || workers < 2 || len(groups) < 2 {
		// Serial path — behaviorally identical to the pre-parallel loop.
		// workers < 2 covers GO_IVM_ADVANCE_PARALLELISM=1 (explicit serial).
		for _, g := range groups {
			pushGroup(g)
		}
		return
	}

	panics := make([]any, len(groups))
	// Processing clock of the advance in flight (nil outside a clocked
	// advance — Begin on a nil accumulator is a no-op). Loaded once: the
	// engine installs/clears it around the whole advance, never mid-push.
	clk := s.advanceClock.Load()
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		sem <- struct{}{} // bound in-flight groups to `workers`
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics[idx] = r
				}
				<-sem
			}()
			// Bracket this worker's CPU into the advance's processing
			// clock (economic-abort budget). Registered LAST so it
			// publishes FIRST on unwind — a panicking group's partial
			// work still lands before the recover above captures.
			stopClock := clk.Begin()
			defer stopClock()
			pushGroup(groups[idx])
		}(i)
	}
	wg.Wait()

	// Re-raise on the caller's goroutine so the sidecar handler's recover
	// sees the panic in the right scope (a panic on a spawned goroutine
	// would kill the whole process). Deterministic: scan in group
	// registration order.
	for i := range panics {
		if panics[i] != nil {
			panic(panics[i])
		}
	}
}
