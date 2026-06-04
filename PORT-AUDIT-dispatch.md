# Port audit: TS → Go IVM dispatch wiring

Scope: every TS-side decision point that affects what Go sees and how Go's
results are consumed in **Go-primary** mode (`ZERO_GO_SIDECAR_ENABLED=true` +
`ZERO_GO_SIDECAR_SHADOW_MODE=false`). Shadow-mode-only issues are out of
scope unless they leak into a Go-primary code path.

Files audited end-to-end:
- `/Users/kartik.parsoya/Documents/Go-RS/mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts`
- `/Users/kartik.parsoya/Documents/Go-RS/mono/packages/zero-cache/src/services/view-syncer/view-syncer.ts`
- `/Users/kartik.parsoya/Documents/Go-RS/mono/packages/zero-cache/src/services/view-syncer/go-sidecar/go-compute-backend.ts`

Baseline read: `REVIEW-shadow-mode.md`, `REVIEW-ts-integration.md`,
`REVIEW-final.md` — FIXED items not re-listed unless regressed.

---

## Known findings — confirmed with detail

### CRITICAL-1: `getCurrentQueries` callback returns internal queries unfiltered, with raw AST
- **TS-prod behavior**: internal queries (`lmids`, `mutationResults`, queries
  rooted at `<appID>.permissions` / `<appID>_<shard>.clients`) never enter Go's
  data path. `#addQueryDispatch` (pipeline-driver.ts:958-965) routes them to
  TS's `#addQueryImpl` exclusively, and `#currentTablesForGo`
  (pipeline-driver.ts:583-595) excludes their source tables from Go's `init`.
- **Go-primary wiring**: pipeline-driver.ts:521-525 — the callback passed to
  `createGoComputeBackend` is
  `() => Array.from(this.#pipelines.entries(), ([queryID, p]) => ({queryID, ast: p.transformedAst}))`.
  Zero filtering of internal queries. Zero AST planning.
- **Divergence**: on every restart-driven re-registration
  (`#reinitPerCGAndRegisterQueries` in go-compute-backend.ts:688), the
  callback returns every pipeline TS has registered. That set always
  contains `lmids` and `mutationResults` (TS hydrates them through
  `#addQueryImpl` whose tail calls `this.#pipelines.set(queryID, …)` at line
  2031), and may contain queries whose `transformedAst.table` is an
  internal table.
- **Impact**: after any restart (sidecar SIGKILL, drift recovery,
  `advance-engine-not-initialized`, manual `resetEngine`), Go receives
  `addQueries` for `lmids` / `mutationResults` / `<appID>.*` queries. Go
  panics with `no source for table <appID>.permissions` because those
  tables were skipped at `init`. The recovery helper returns `false` →
  `initialized` stays false → Go-primary advance/hydrate falls back to TS
  permanently for that CG until the next sidecar restart races a clean
  pipeline state.
- **Suggested fix**: pipeline-driver.ts:521-525 — wrap the filter to drop
  internal queries AND plan the AST via `#planAstForGo`:
  ```ts
  () =>
    Array.from(this.#pipelines.entries())
      .filter(([qid, p]) =>
        !this.#isInternalQueryID(qid) &&
        !this.#isInternalTable(p.transformedAst.table),
      )
      .map(([queryID, p]) => ({queryID, ast: this.#planAstForGo(p.transformedAst)})),
  ```

### CRITICAL-2: `#goHydrate` (single-query path) sends raw AST + skips TableSource creation
- **TS-prod behavior**: `#addQueryImpl` (pipeline-driver.ts:1857-2041) calls
  `buildPipeline(resolvedQuery, {getSource: name => this.#getSource(name), …})`
  which side-effects `#getSource` for every table reachable in the AST. The
  builder applies `completeOrdering` + cost-model planning during pipeline
  construction, so the AST that materializes the query is canonically
  planned.
- **Go-primary wiring**: pipeline-driver.ts:979-1019 — `#goHydrate`:
  - Line 985: `this.#goBackend!.hydrate(queryID, query)` sends `query`
    (the raw transformedAst from view-syncer) without going through
    `#planAstForGo`.
  - Line 991-1003: stores a stub pipeline entry with no real `Input`.
  - Never calls `#getSource(tableName)` for any table. The user table's
    `TableSource` is never created in `#tables`.
- **Divergence**: two separate gaps from the canonical TS path.
  - (a) Unplanned AST: missing the `flip: true` decorations on
    OR-with-CSQ that `#planAstForGo` would produce (H18-cont class). On a
    query whose planner picks FlippedJoin + UnionFanIn-merge-with-dedup,
    Go's executor of the raw AST takes the plain Join + applyOr branch
    and over-emits the inner-CSQ relationship row(s).
  - (b) No TableSource: `getRow` (pipeline-driver.ts:2098-2112) reads
    from `this.#tables`. If the only path that hydrated a query was
    `#goHydrate`, `#tables` has no entry for its tables. Any subsequent
    CVR catchup that needs `getRow` for those tables fires the
    `must(this.#tables.get(table), …)` panic at line 2106.
- **Impact**: triggered every time view-syncer.ts:1463 (`#hydrateUnchangedQueries`)
  calls `pipelines.addQuery` in Go-primary mode (i.e., every client
  reconnect). The over-emit is silent (clients receive extra rows; CVR
  bloats). The `getRow` panic surfaces as "no TableSource for table=X"
  whenever `#catchupClients` later runs — at high enough connection
  churn this crashes the view-syncer.
  - Partially masked today because view-syncer.ts:1492-1500 unconditionally
    invokes `shadowBatchCompare` after the loop. That dispatch calls
    `#planAstForGo` (pipeline-driver.ts:1303) which side-effect populates
    `#tables`. But the window between the per-query `addQuery` calls and
    `shadowBatchCompare`'s completion is still a window in which a
    parallel `getRow` would fail; and if `shadowBatchCompare` errors
    before all queries are planned, some tables stay uncreated.
- **Suggested fix**: pipeline-driver.ts:979-985 — call
  `const planned = this.#planAstForGo(query);` before the RPC; pass
  `planned` to `hydrate`; store `planned` as `transformedAst` in the
  pipeline entry. The plan side-effect populates `#tables` for all
  reachable tables. Same change needed at `#shadowAddQuery` if
  symmetry across modes is desired.

### CRITICAL-3: `#goHydrate` doesn't run `MeasurePushOperator` decoration
- **TS-prod behavior**: `#addQueryImpl` (pipeline-driver.ts:1911-1917)
  decorates every `SourceInput` with `MeasurePushOperator`, threading
  `inspectorDelegate` + `'query-update-server'` category. This drives the
  `query-update-server` per-query latency metric exposed via
  `InspectorDelegate.addMetric`.
- **Go-primary wiring**: pipeline-driver.ts:991-1003 (single-query path)
  and 1047-1059 / 1198-1210 (batch paths) — the stub pipeline `input` is
  an empty no-op shell. No `MeasurePushOperator` instance exists. No
  pushes ever flow through it (Go owns push processing).
- **Divergence**: the `query-update-server` metric is permanently zero
  for every Go-primary CG. Operator dashboards comparing per-query
  push-time latency across migrations lose all signal once Go-primary
  is enabled.
- **Impact**: observability gap; no functional break. Becomes a HIGH if
  the metric is part of a customer SLO dashboard or an alert.
- **Suggested fix**: surface Go's `timings []TableTiming` from
  `advanceStream`'s final frame (per-(table, op) ms) and feed it via
  `inspectorDelegate.addMetric('query-update-server', ms, queryID)`
  in `#goPrimaryAdvance` after `goResults` resolves. Requires mapping
  `TableTiming.table` → queryID (the same table can be in multiple
  queries; pick the slowest or sum).

### CRITICAL-4: `#goPrimaryAdvance` / `#shadowAdvance` drop `r.timings`
- **TS-prod behavior**: `#advance` (pipeline-driver.ts:2867-2872) records
  per-(table, type) timing into `#advanceTime` histogram with
  `{table, type}` attributes for every processed change. Operator
  dashboards depend on per-table breakdown of advance cost.
- **Go-primary wiring**:
  - `#goPrimaryAdvance` (pipeline-driver.ts:2238-2299): consumes
    `r.changes` from the goPromise but never reads `r.timings`. The
    Go `advanceStream` response carries `TableTiming[]` on the final
    frame; the field is dropped on the TS side.
  - `#shadowAdvance` (pipeline-driver.ts:2504-2506): same pattern;
    `goRaw.timings` is never piped into a histogram.
  - The only TS code that records `#advanceTime` is the TS-native
    `#advance` generator at line 2868 — and in Go-primary, that
    generator only processes internal-query pushes (lmids /
    mutationResults), so the histogram is dominated by 1-2 row
    "internal" advances and misses 100% of user-table cost.
- **Divergence**: histogram cardinality and value distribution flips.
  Pre-Go-primary: thousands of per-table samples per advance. Post:
  one or two `lmids`-sized samples per advance.
- **Impact**: dashboards that alert on per-table p99 advance time get
  no signal once Go-primary is enabled. The fact that REVIEW-final
  MED-CROSS-3 mentions `ivm.advance-go-rpc-time` as a "FIXED" new
  histogram is **regressed/never-implemented**: a fresh grep of the
  TS tree finds no `advance-go-rpc-time` metric anywhere.
- **Suggested fix**: pipeline-driver.ts:2300 (right after
  `goResults = await goPromise`) — pull the timings off the
  `advanceStream` raw response and feed each into `#advanceTime` with
  `{table, type: 'add'|'remove'|'edit'}`. Requires changing
  `#advanceWithRecovery` to surface the raw `AdvanceResult` (currently
  it discards `timings` by mapping to `changes` only at
  pipeline-driver.ts:2239).

### HIGH-1: View-syncer's Go-primary batch streaming path measures wall-time-of-emit-and-flush, not Go-side hydrate
- **TS-prod behavior**: view-syncer.ts:1934-2018 (non-batch path)
  computes `elapsed = timer.stop()` AFTER iterating ALL the
  `RowChange`s. That `elapsed` includes TS-side push iteration, which
  in TS is the actual hydrate work — so it's a good proxy for "time
  this query took to hydrate".
- **Go-primary wiring**: view-syncer.ts:1958-1972 — inside the
  `for await (const {queryID, changes} of goHydrateBatchStream(…))`
  loop:
  ```ts
  const queryStart = performance.now();
  yield* changes as Iterable<RowChange | 'yield'>;
  const elapsed = performance.now() - queryStart;
  ```
  This measures the time the JS generator spends YIELDING `changes`
  — pure conversion + downstream poker flush. Go-side hydrate
  (the actual compute) happened before the chunk arrived. The real
  per-query Go time is in `goResult.timingMs` which `goHydrateBatchStream`
  attaches to the pipeline at pipeline-driver.ts:1206 — but the
  view-syncer never sees it.
- **Divergence**: `query-materialization-server` (per-query
  histogram via `#addQueryMaterializationServerMetric`) and the
  `[shadow][batch-stream] N queries in Xms` log report a fraction of
  the true Go-side hydrate cost. Operators see consistently low
  numbers and assume Go-primary is faster than it actually is — or
  miss slow queries because their elapsed-as-measured is the chunk
  size × consumer cost, not the Go compute cost.
- **Impact**: dashboards lie. Tail-latency analysis underestimates
  Go-primary's true p99 hydrate time, hiding regressions during
  rollout. The `slowHydrateThreshold` warning (see HIGH-2 below)
  never fires for Go-side slowness.
- **Suggested fix**: pipeline-driver.ts:goHydrateBatchStream should
  yield `{queryID, changes, timingMs}` (add `timingMs` to the yield
  shape). View-syncer.ts:1958-1972 should consume `timingMs` and use
  it as the `elapsed` for the metric + slow-hydrate threshold check.
  Keep the `queryStart`-bracketed timing only for the consumer-side
  cost portion.

### HIGH-2: `slowHydrateThreshold` warning fires on wrong elapsed
- **TS-prod behavior**: view-syncer.ts:2008 — `if (elapsed > slowHydrateThreshold) lc.warn?.('Slow query materialization', elapsed, q.ast)`.
  In TS, `elapsed = timer.stop()` covers the actual hydrate cost.
- **Go-primary wiring**: view-syncer.ts:1963-1965 — uses the same
  `elapsed = performance.now() - queryStart` from HIGH-1. That's the
  yield-and-flush time, not Go's compute time.
- **Divergence**: a 5-second Go hydrate that ships chunked results in
  500ms of consumer yields won't trip the threshold (default is
  typically O(seconds) for production).
- **Impact**: slow-query alerts go silent in Go-primary. Operators
  lose the early-warning signal that a query plan went off the rails
  on Go's side.
- **Suggested fix**: same as HIGH-1 — wire `timingMs` through.

### HIGH-3: Go-primary advance has `suppressAbort=true` — no circuit-breaker
- **TS-prod behavior**: `#advance` (pipeline-driver.ts:2782) with
  `suppressAbort=false` calls `#shouldAdvanceYieldMaybeAbortAdvance`
  which throws `ResetPipelinesSignal` when elapsed > totalHydrationTime
  (or half-time at half-progress). This is the bounded-advance
  circuit-breaker that prevents WAL pin starvation from a pathological
  advance.
- **Go-primary wiring**: pipeline-driver.ts:2293 — `this.#advance(replayDiff, timer, numChanges, true)`.
  The `true` is `suppressAbort`. The Go-side advance has no equivalent
  signal-back from the Go sidecar; once dispatched, the RPC runs to
  completion.
- **Divergence**: a Go advance whose actual cost vastly exceeds the
  hydrate-time budget will not abort. The TS-side `#advance`
  generator only emits internal-query changes (lmids), which is
  trivially fast — so the suppress is correct for TS's contribution.
  But there's no equivalent budget enforcement on the Go-RPC
  duration.
- **Impact**: a pathological diff sent to Go could block the
  view-syncer's advance lock for far longer than the bounded-advance
  contract would have allowed in TS-only mode. WAL2 cannot switch
  WALs while the long-running advance holds the read tx, so WAL
  growth is unbounded for that interval. The sidecar's RPC timeout
  (`opts?.timeoutMs ?? DEFAULT_TIMEOUT_MS` = 30s for non-streaming,
  120s for streaming) is the only backstop.
- **Suggested fix**: in `#goPrimaryAdvance`, wrap the goPromise in a
  `Promise.race` against a deadline derived from `totalHydrationTimeMs`
  (same math as `#shouldAdvanceYieldMaybeAbortAdvance`). On timeout,
  throw `ResetPipelinesSignal` so the surrounding view-syncer can
  reset pipelines. Caveat: the Go-side RPC continues running in the
  sidecar; needs a cancel-style cooperation to actually free the
  sidecar's CPU.

---

## New findings — beyond the known eight

### CRITICAL-5: `getCurrentTables` snapshot in `#scheduleGoReset` is captured pre-await
- **TS-prod behavior**: TS has no equivalent — the canonical path always
  reads live SQLite via TableSources.
- **Go-primary wiring**: pipeline-driver.ts:2364 captures
  `const tables = this.#currentTablesForGo()` BEFORE setting
  `this.#goInitPromise = this.#goBackend.resetEngine(tables)`. The
  `resetEngine` call path through `#reinitPerCGAndRegisterQueries` →
  `#doInit` runs schema-only `init` + chunked `loadRows` over a network
  socket; the `tables.rows` payload is the SQLite snapshot at
  schedule-time, not at re-init time.
- **Divergence**: between scheduling and `loadRows` completion,
  arbitrary mutations land on the replica. Go gets re-initialized from
  a snapshot that's already stale by some number of advances. The next
  `#snapshotter.advance()` diff is the difference between Go's
  stale-snapshot state and current SQLite, which works only if Go
  applies the FULL backlog. But TS's `#snapshotter.advance` returns the
  diff since the LAST advance, not since some arbitrary point — so
  Go re-init'd at a different snapshot than TS's advance baseline.
  Subsequent advances apply diffs against a stale Go state → instant
  drift.
- **Impact**: post-reset, the next 1-2 advances may panic with `Row
  not found` (drift signal) because Go's MemorySource doesn't have a
  row TS's diff says was just deleted (because Go was init'd before
  the row was added in the first place). This re-triggers
  `#scheduleGoReset` → loop. The drift-loop circuit breaker
  (go-compute-backend.ts:49-51) catches this after 5 trips in 60s,
  so the impact is bounded to a 5-minute cooldown of Go-fallback-to-TS.
  But the signal-to-noise on alerts goes up — every reset has a
  ~1-in-N chance of triggering a follow-up.
- **Suggested fix**: capture `tables` INSIDE `resetEngine` instead of
  in `#scheduleGoReset`. Move the `this.#currentTablesForGo()` call
  into `GoComputeBackend.resetEngine` or pass a thunk. Even better:
  acquire a snapshot at the same boundary the next advance will
  start from (typically the next `#snapshotter.current().version`).

### CRITICAL-6: `removeQuery` notifies Go for internal queries it never registered
- **TS-prod behavior**: TS's `removeQuery` (pipeline-driver.ts:2047-2059)
  drops the pipeline locally.
- **Go-primary wiring**: pipeline-driver.ts:2058 — `this.#goBackend?.removeQuery(queryID).catch(() => {})`.
  No internal-query filter. Fires for every removed pipeline including
  `lmids` and `mutationResults`.
- **Divergence**: Go never had pipelines for internal queries (filtered
  out at `#addQueryDispatch`:958-965). Go's `removeQuery` handler
  either returns OK silently or errors — either way, the `.catch(() => {})`
  swallows it.
- **Impact**: noise in Go's RPC accounting; possible per-CG semaphore
  acquisition for a no-op. At a steady reconnect rate of N/s this is
  N wasted RPCs/s per CG.
- **Suggested fix**: pipeline-driver.ts:2058 — `if (!this.#isInternalQueryID(queryID)) this.#goBackend?.removeQuery(...)`.
  Or, equivalently: skip the Go RPC when the pipeline entry was a stub
  (i.e., the query was originally added via internal-query branch).

### HIGH-4: `goHydrateBatchStream` rebuilds internal `tsResultsPerQuery` map for shadow but the result is never observed in Go-primary
- **TS-prod behavior**: N/A — only relevant to shadow.
- **Go-primary wiring**: view-syncer.ts:1944-1977 — the Go-primary
  branch correctly skips the `tsResultsPerQuery` map and the
  `shadowBatchCompare` dispatch (`return;` at line 1977).
- **CHECKED-OK**: this path is wired correctly.

### HIGH-5: `#hydrateUnchangedQueries` always calls `shadowBatchCompare` even in Go-primary
- **TS-prod behavior**: N/A.
- **Go-primary wiring**: view-syncer.ts:1492-1500 — invokes
  `pipelines.shadowBatchCompare(...)` unconditionally after the
  per-query `addQuery` loop. `shadowBatchCompare` (pipeline-driver.ts:1275)
  returns early only on `!this.#goBackend?.initialized` — NOT on
  `!this.#shadowMode`.
- **Divergence**: in Go-primary mode, `shadowBatchCompare` runs a
  full `hydrateManyStream` to Go for the same queries that already
  hydrated through `#goHydrate`. Go executes them twice: once via the
  per-query `addQuery` and once via the batch stream. The `tsResultsPerQuery`
  map passed to compare is Go's own first-pass output (since the
  loop populated it from `addQuery`'s return — which in Go-primary
  IS Go's output). So the comparator compares Go-to-Go (always
  matches) → wasted work.
- **Impact**: doubles Go-side hydrate cost on every reconnect in
  Go-primary. Masks observability for shadow-mode mismatches because
  even when shadow runs over the same path, the comparator is
  redundant.
  Useful side-effect: it DOES create the missing TableSources via
  `#planAstForGo` (mitigates CRITICAL-2's getRow panic). But that's a
  side-effect of a buggy dispatch.
- **Suggested fix**: view-syncer.ts:1492 — gate the call on
  `isGoShadowMode(...)` so it only fires in shadow mode. The
  TableSource side-effect can be made explicit in `#goHydrate`
  (per CRITICAL-2's suggested fix).

### HIGH-6: Protocol violation in `#goPrimaryAdvance` discards collected TS internal-query changes
- **TS-prod behavior**: N/A.
- **Go-primary wiring**: pipeline-driver.ts:2243-2258 — on protocol
  violation, `throw e;`. The catch handler runs AFTER `tsChanges`
  has been fully collected (lines 2292-2299), but `goResults = await
  goPromise` (line 2301) re-throws. The function exits via the
  throw; `tsChanges` is in the local-variable scope and never makes
  it into the merged output.
- **Divergence**: TS's internal-query events for this advance —
  the lmids and mutationResults updates clients need to ack
  mutations — are silently dropped along with the protocol-error
  throw.
- **Impact**: a Go wire-protocol violation kills not just Go's
  user-query advance but also TS's internal-query advance. Mutations
  in flight at that moment may never be ack'd until the next clean
  advance. The view-syncer's outer catch (view-syncer.ts:2319)
  propagates non-`ResetPipelinesSignal` throws to the run loop
  (line 514), which logs and tears down the view-syncer.
- **Suggested fix**: in `#goPrimaryAdvance`'s catch, before
  re-throwing, emit the `tsChanges` to the streamer-equivalent so
  internal-query updates land before the throw. Or restructure so
  protocol-violation throws happen BEFORE awaiting goPromise
  (e.g., synchronously map error class on the first reject and
  delay the throw until after TS's changes flush).

### HIGH-7: `#advanceWithRecovery` discards `r.timings` AND drops them on every recovery branch
- **TS-prod behavior**: per-(table,op) timings always populate the
  histogram in TS native mode.
- **Go-primary wiring**: go-compute-backend.ts:283-345 — every
  early-return branch returns `{changes: [], timings: []}`. The
  successful path at line 263 (`return await call()`) returns the
  raw `GoAdvanceResult` which includes `timings`. BUT
  `#goPrimaryAdvance` (pipeline-driver.ts:2239) consumes
  `r.changes.map(rc => ...)` only — drops the timings before
  awaiting. So even successful Go advances lose their per-table
  timing.
- **Divergence**: see CRITICAL-4 for the histogram impact.
- **Suggested fix**: surface `timings` to `#goPrimaryAdvance`'s
  caller. Restructure `goPromise` to be `Promise<{changes, timings}>`
  not `Promise<RowChange[]>`. After awaiting, fold the timings into
  `#advanceTime.recordMs(...)` per entry.

### HIGH-8: `setHydrationTime` is referenced in comment but does not exist
- **TS-prod behavior**: view-syncer used to call `setHydrationTime` on
  the pipeline driver to refine the initial wall-clock hydration
  estimate down to processing time (see comment at
  pipeline-driver.ts:2028-2030).
- **Go-primary wiring**: grep finds zero implementations of
  `setHydrationTime` anywhere in `/Users/kartik.parsoya/Documents/Go-RS/mono`.
  The only reference is the orphan comment.
- **Divergence**: the comment's promised refinement step was either
  removed or never wired up. `hydrationTimeMs` stays at the
  wall-clock value (or `goResult.timingMs ?? 0` in Go-primary).
- **Impact**: `totalHydrationTimeMs()` (line 842-848) returns the
  sum of these per-query values. That sum drives the advance
  circuit-breaker (`#shouldAdvanceYieldMaybeAbortAdvance` line 2982
  — compares `elapsed` against `totalHydrationTimeMs`). In Go-primary
  the per-query `hydrationTimeMs` is Go's wall-time (server-side),
  so the budget is Go-derived. In TS-native it's TS wall-time. The
  unit is consistent but the SOURCE differs — Go's compute may be
  faster than TS's interpretation, so the advance budget shifts.
- **Suggested fix**: either remove the stale comment or implement
  the `setHydrationTime` refinement consistently across both modes.
  In Go-primary, "processing time" would map to `r.timings.reduce((a, t) => a + t.ms, 0)` for that query — already collected on the
  RPC response but never wired through.

### MEDIUM-1: `#hydrateUnchangedQueries` uses a real timer with `yieldProcess` while the batch path uses noopTimer
- **TS-prod behavior**: `#addQueryImpl` cooperates with a real timer
  to yield to the event loop during long hydrations.
- **Go-primary wiring**:
  - `#hydrateUnchangedQueries` (view-syncer.ts:1448-1486) uses
    `const timer = new TimeSliceTimer(lc)` and calls
    `timer.yieldProcess` on every `'yield'` from `addQuery`. But in
    Go-primary, `addQuery` routes to `#goHydrate` whose generator
    yields `'yield'` every 100 changes (pipeline-driver.ts:1010-1012).
    Each yield triggers a real `await timer.yieldProcess`.
  - `goHydrateBatchStream` (pipeline-driver.ts:1130-1134) uses a
    noopTimer for its internal queries and explicitly DROPS yields
    from its inner generator (lines 1213-1225, 1139-1144).
- **Divergence**: yield-cooperation between the two Go-primary
  hydrate paths is asymmetric. Single-query reconnect hydrate
  yields cooperatively; batch hydrate doesn't. Big batches in
  `generateRowChanges` keep the event loop blocked for entire
  chunk duration on the streaming RPC consume.
- **Impact**: latency hiccups during big initial hydrates in
  Go-primary mode. Other concurrent operations (RPC ack
  handlers, websocket frames) stall during a batch's chunk yield
  block.
- **Suggested fix**: pipeline-driver.ts:1222 — restore cooperative
  yield every N changes in `yieldGoHydration`, but route it through
  the view-syncer's TimeSliceTimer instead of dropping. Requires
  passing a timer into `goHydrateBatchStream` (currently it accepts
  none). The drop comment (lines 1213-1220) cites a fix for a
  separate bug (`not running` assert in TimeSliceTimer) — that's
  the right symptom-handling but a wrong fix; the right fix is to
  ensure the timer IS running when the stream consumer iterates.

### MEDIUM-2: `awaitGoInit` masks the very-first-advance race
- **TS-prod behavior**: `init` is synchronous on the TS side; the
  first advance after init sees a fully populated state.
- **Go-primary wiring**:
  - `awaitGoInit` (pipeline-driver.ts:1246-1263) drains
    `#goBackend.whenInitialized()` AND `#goInitPromise`.
    Called from view-syncer.ts:1937 before `canBatchHydrate` check.
    Good.
  - `advance` (pipeline-driver.ts:2125-2135) checks
    `this.#goInitPromise && this.#goBackend && !this.#goBackend.initialized`.
    If `#goInitPromise` is null but `initialized` is false (e.g.,
    init failed and the promise was nulled at pipeline-driver.ts:656),
    `advance` falls through to TS-only. Good.
  - But `addQuery` (pipeline-driver.ts:916-944) has a TWO-LEVEL
    await: first `#goInitPromise`, then `whenRecovered`. The first
    branch checks `this.#goInitPromise && this.#goBackend && !this.#goBackend.initialized`.
    If `initialized` is true at check time but a restart fires
    between check and dispatch, the dispatch runs on the stale
    `initialized=true` and routes to Go through the recovery gate.
    `whenRecovered` then waits for the restart gate — OK in this
    case.
- **CHECKED-OK**: the dispatch awaits look right under inspection.

### MEDIUM-3: `goHydrateBatchStream` yields entries in COMPLETION order, not input order
- **TS-prod behavior**: `#addQueryImpl` runs queries strictly
  sequentially in caller-controlled order.
- **Go-primary wiring**: pipeline-driver.ts:1090-1094 — explicit
  comment "The returned iterable yields entries in COMPLETION
  order, not input order — callers must not rely on positional
  correspondence with `queries`." The view-syncer's consumer
  (view-syncer.ts:1949) iterates the stream and routes each entry
  via `byID.get(queryID)` — order-agnostic.
- **CHECKED-OK**: dispatch is consistent with the documented
  ordering contract.

### MEDIUM-4: `#advanceContext` / `#hydrateContext` state not reset across mode boundaries
- **TS-prod behavior**: these contexts are set inside `#addQueryImpl`
  / `#advance` and torn down in `finally` blocks. They drive
  `#shouldYield` decisions.
- **Go-primary wiring**:
  - `#goHydrate` / `goHydrateBatch` / `goHydrateBatchStream`: never
    set `#hydrateContext`. So `#shouldYield`'s hydrate branch
    (line 2938) is dead during Go-primary hydrates. Fine — there's
    no TS pipeline to yield from.
  - `#goPrimaryAdvance`: calls `#advance(...)` (line 2293) which
    DOES set `#advanceContext` for TS's internal-query advance.
    `#shouldYield`'s advance branch (line 2951) → `#shouldAdvanceYieldMaybeAbortAdvance`
    fires. With `suppressAbort=true` (HIGH-3), it returns true
    after `yieldThresholdMs`. The TS generator yields cooperatively.
- **CHECKED-OK**: state machine is consistent within Go-primary;
  the contexts cover TS's internal-query advance correctly.

### MEDIUM-5: `destroy()` doesn't await `#goBackend.destroy()` before resolving
- **TS-prod behavior**: `destroy()` (pipeline-driver.ts:826-835)
  shuts down storage and snapshotter synchronously.
- **Go-primary wiring**: line 834 — `this.#goBackend?.destroy().catch(() => {})`.
  Fire-and-forget. The Go-side `destroy` RPC may still be in flight
  when the surrounding teardown finishes.
- **Divergence**: under rapid teardown-then-recreate cycles for the
  same `clientGroupID`, the new `#goBackend` may issue `init` before
  the old `destroy` completes. Both RPCs land in arbitrary order on
  the sidecar's per-CG mutex.
- **Impact**: usually benign — Go's per-CG FIFO serializes them. But
  if the old `destroy` lands AFTER the new `init`, it destroys the
  fresh engine. The next `loadRows` from the new instance trips
  `engine not initialized`, hits `#withReinitRetry`, force-re-inits.
  Adds latency to the first advance after rapid recycle.
- **Suggested fix**: pipeline-driver.ts:826-835 — make `destroy`
  return a `Promise<void>` and `await this.#goBackend?.destroy()`.
  Callers (view-syncer cleanup paths) already pattern-match
  Promise-returning teardown.

### MEDIUM-6: `#advance` reads `tableSource` from `#tables` and silently skips on miss in Go-primary
- **TS-prod behavior**: pipeline-driver.ts:2821-2825 — if no table
  source exists for a changed table, the change is skipped. This is
  correct in TS-only mode: no source means no query reads this table.
- **Go-primary wiring**: same code path. In Go-primary, the TS-side
  `#tables` is populated lazily via `#planAstForGo` side-effects, and
  may not include every user table that Go has — Go loaded ALL user
  tables at init via `#currentTablesForGo`. So Go has data for
  tables that TS-side `#tables` doesn't.
- **Divergence**: in Go-primary, the `#advance` TS generator silently
  skips changes for tables it doesn't have. The Go-side advance gets
  the SAME changes (Go's snapshotChanges is filtered only for
  internal tables, not for table-source-presence on TS). So:
  - Go correctly applies the change to its MemorySource.
  - TS doesn't update any TableSource for it.
  - Next time `getRow` is called for a row in that table whose
    TableSource was created by a LATER query, the TableSource reads
    from SQLite — which reflects the latest commit (since
    TableSource reads live SQLite). So `getRow` is correct.
- **CHECKED-OK**: the silent skip is benign in Go-primary because TS's
  TableSource (when created) reads live SQLite, not a cache. No
  staleness risk.

### MEDIUM-7: Drift-audit pipeline-count probe runs against Go's pipeline registry, not TS-aware
- **TS-prod behavior**: N/A.
- **Go-primary wiring**: pipeline-driver.ts:1400-1422 — fetches
  `goCount = await this.#goBackend.pipelineCount()`, compares to
  `tsExpected = [...this.#pipelines.keys()].filter(qid => !this.#isInternalQueryID(qid)).length`.
  If `goCount < tsExpected`, triggers `#scheduleGoReset`.
- **Divergence**: doesn't filter out pipelines whose
  `transformedAst.table` is an INTERNAL TABLE (some custom queries
  could be rooted at `<appID>.something` though uncommon). The
  filter uses only `#isInternalQueryID` not `#isInternalTable`. So
  `tsExpected` may include user-defined queries that target internal
  tables (rare) → freeze false-positive.
- **Impact**: rare false-positive resets. Spurious
  `ivm.drift-audit-freezes` counter increments and unnecessary
  Go-engine rebuilds.
- **Suggested fix**: pipeline-driver.ts:1407-1409 — symmetrize with
  line 1438-1442's auditableIDs filter: also exclude
  `this.#isInternalTable(entry.transformedAst.table)`.

### MEDIUM-8: Per-CG init `getCurrentTables` callback can be called from non-driver context
- **TS-prod behavior**: N/A.
- **Go-primary wiring**: pipeline-driver.ts:516-517 — the callback
  is `() => this.#currentTablesForGo()` which reads
  `this.#snapshotter.current()` and `this.#tableSpecs`. It's
  invoked from `GoComputeBackend`'s `#onSidecarRestart`, which is
  fired from the SidecarManager's onRestart listeners (potentially
  on a different async context).
- **Divergence**: `this.#snapshotter.current()` is called outside
  the view-syncer lock. Concurrent `#snapshotter.advance` from the
  view-syncer could be modifying snapshotter state mid-read.
- **Impact**: `Snapshotter` is documented as not safe for concurrent
  use; reading `current()` mid-advance could return inconsistent
  state. Could result in Go re-initializing with a corrupted
  snapshot view.
- **Suggested fix**: route the callback through the view-syncer's
  lock. Either by spawning a microtask that waits on the lock, or
  by capturing the snapshot at lock-held callsites (init / reset /
  drift-detected reset). This may already be lock-safe due to
  JavaScript's single-threaded event loop — but ANY await inside
  `#currentTablesForGo` (currently none, but `db.all(...)` is
  synchronous I/O that could in principle block) would open a race.

### MEDIUM-9: `#sqlGroundTruthCompare` only runs in drift-audit, not on every Go-primary advance
- **TS-prod behavior**: TS doesn't need ground-truth checks; it's
  the source of truth.
- **Go-primary wiring**: pipeline-driver.ts:1739 — invoked only from
  `#runDriftAudit`. So the SQL-vs-Go safety net only catches drift
  for ONE random query per audit interval (default 60s).
- **Divergence**: with 10 active queries per CG and a 60s audit, a
  given query is sampled once per 10 minutes. A drift between
  audits can persist for ~10 minutes before any operator-visible
  signal.
- **Impact**: latency to detection of REAL Go drift can be up to
  `(#queries × auditInterval)` per CG. For 100-query CGs that's
  100 minutes per query worst-case.
- **Suggested fix**: outside this audit's scope — but consider
  parameterizing the audit to sample N queries per tick at higher
  load, or weighting samples by query age (rotate through round-robin).

### MEDIUM-10: `#planAstForGo` panics on `must(this.#getSource(tableName))` if `#tableSpecs` misses a table
- **TS-prod behavior**: `#getSource` (pipeline-driver.ts:2914-2936)
  calls `mustGetTableSpec` which throws if the table isn't in
  `#tableSpecs`. Standard TS-side panic on schema mismatch.
- **Go-primary wiring**: pipeline-driver.ts:807-820 — every Go-side
  hydrate path (`goHydrateBatch`, `goHydrateBatchStream`,
  `shadowBatchCompare`, `#runDriftAudit`) calls `#planAstForGo` which
  calls `completeOrdering(ast, tableName => must(this.#getSource(tableName)).tableSchema.primaryKey)`.
  `completeOrdering` recursively walks related[] and CSQ subqueries.
- **Divergence**: if a query AST references a table that exists in
  the schema sent to Go but not in TS's `#tableSpecs` (e.g., a
  schema-change race), `#planAstForGo` throws synchronously. The
  caller's `try/catch` is per-RPC (e.g., `#runDriftAudit` catches
  Go's hydrate failures but `#planAstForGo` runs BEFORE the RPC).
- **Impact**: a schema-change race kills the dispatching code path
  with an unhandled error. View-syncer terminates.
- **Suggested fix**: wrap `#planAstForGo`'s `must` in a try/catch
  that falls back to the unplanned AST + logs a schema-skew warning.
  Or: ensure `#tableSpecs` re-computation happens BEFORE Go's reset
  in `#initAndResetCommon` (it already does at line 707, but
  callers like `#runDriftAudit` may run between snapshotter advance
  and the matching tableSpecs refresh).

### LOW-1: Stub pipeline's `getSchema` returns empty object cast as `never`
- **TS-prod behavior**: real pipelines return a populated `SourceSchema`.
- **Go-primary wiring**: pipeline-driver.ts:994-998, 1050-1054,
  1201-1205 — stub `input` has `getSchema: () => ({} as never)`.
- **Divergence**: if any caller invokes `getSchema()` on a Go-primary
  pipeline entry's `input`, it gets an empty object. Currently no
  caller does (the pipeline is only used for `hydrationTimeMs` /
  `transformedAst` access). But it's a footgun for future maintainers.
- **Impact**: latent — no current crash path.
- **Suggested fix**: throw an explicit error from the stub's
  `getSchema`/`fetch`/`cleanup` so a future misuse fails loudly:
  `getSchema: () => { throw new Error('Go-primary pipeline has no TS schema; use transformedAst'); }`.

### LOW-2: `[shadow]` log prefix on Go-primary reset paths
- **TS-prod behavior**: N/A.
- **Go-primary wiring**: pipeline-driver.ts:2365, 2369, 2373, 2381 —
  all log lines for `#scheduleGoReset` use `[shadow]` prefix.
  `#scheduleGoReset` is invoked from BOTH shadow and Go-primary
  paths (see `go-primary-advance-failure` reason at line 2285,
  `drift-audit-pipeline-count-mismatch` at line 1420).
- **Divergence**: operators see `[shadow]` lines in Go-primary mode.
  Confusing during incident triage when grep'ing for "shadow" to
  find shadow-mode issues.
- **Suggested fix**: drop the `[shadow]` prefix; use
  `[go-reset]` or include the reason in the prefix.

### REGRESSION-1: `ivm.advance-go-rpc-time` histogram is absent
- **TS-prod behavior**: N/A.
- **Go-primary wiring**: grep finds zero references to
  `advance-go-rpc-time` in `/Users/kartik.parsoya/Documents/Go-RS/mono`.
- **Divergence**: REVIEW-final marks MED-CROSS-3 as FIXED with that
  histogram as the deliverable, but it isn't in the code. The only
  advance latency metric is `#transactionAdvanceTime` in view-syncer
  (line 271 / line 2341) which is total view-syncer
  advance-and-process time, not Go's RPC overhead.
- **Impact**: telemetry gap. Cannot distinguish "Go RPC was slow" from
  "TS post-processing was slow".
- **Suggested fix**: add a histogram, instrument it inside
  `#goPrimaryAdvance` around the `await goPromise` call:
  ```ts
  const goStart = performance.now();
  const goResults = await goPromise;
  this.#advanceGoRpcTime.recordMs(performance.now() - goStart);
  ```

---

## Dispatch site matrix

| TS→Go boundary | Plan AST? | Filter internal queries? | Filter internal tables? | Create TableSources side-effect? | Consume all response fields? | Notes |
|---|---|---|---|---|---|---|
| `#goHydrate` (line 985) | **No** | N/A (`#addQueryDispatch` filters) | N/A | **No** | `timingMs` yes, `changes` yes | CRITICAL-2 |
| `goHydrateBatch` (line 1038) | Yes | N/A (only used internally by tests?) | N/A | Yes (via plan) | yes | OK |
| `goHydrateBatchStream` (line 1165) | Yes | Yes (line 1111) | Yes (line 1111) | Yes (via plan) | yes (chunkIndex, final, timingMs) | OK |
| `goHydrateBatchStream` internal-queries split (line 1135) | N/A (TS path) | N/A | N/A | Yes (via `#addQueryImpl`) | N/A | OK |
| `shadowBatchCompare` (line 1303) | Yes | Yes (line 1279) | N/A (no table filter, but only queryID filter) | Yes (via plan) | yes | runs in Go-primary too (HIGH-5) |
| `#runDriftAudit` (line 1523) | Yes | Yes (line 1440) | Yes (line 1441) | Yes (via plan) | yes | OK |
| `#goPrimaryAdvance` Go RPC (line 2238) | N/A (snapshot diff, no AST) | Filters internal tables (line 2195) | Yes | N/A | **changes only — drops timings** | CRITICAL-4 |
| `#shadowAdvance` Go RPC (line 2485) | N/A | Filters internal tables (line 2456) | Yes | N/A | changes yes, **timings ignored** | CRITICAL-4 (shadow side) |
| `getCurrentQueries` (line 521) | **No** | **No** | **No** | N/A | yes | CRITICAL-1 |
| `getCurrentTables` (line 514 / 517) | N/A | N/A | Yes (line 592) | N/A | N/A | OK; but see MEDIUM-8 thread-safety, CRITICAL-5 staleness |
| `removeQuery` (line 2058) | N/A | **No** | N/A | N/A | error swallowed | CRITICAL-6 (low impact) |
| `#scheduleGoReset` → `resetEngine` (line 2366) | N/A | N/A | Yes (table snapshot via `#currentTablesForGo`) | N/A | N/A | CRITICAL-5 (stale snapshot) |
| `#maybeResetGoBackend` (line 665-677) | N/A | N/A | Yes (via `#currentTablesForGo`) | N/A | N/A | OK — called by `reset()` and `advanceWithoutDiff()` only |
| `awaitGoInit` (line 1246) | N/A | N/A | N/A | N/A | N/A | OK — restart-aware via `whenInitialized` |

### Error class handling (per HIGH-CROSS check)

| Error | Where mapped | Action | Notes |
|---|---|---|---|
| `DriftError` | go-compute-backend.ts:276 | reinit + return `partialChanges` | OK; partial output forwarded |
| `StaleInitEpochError` | pipeline-driver.ts:2261 | metric + warn + re-throw | OK |
| `"Sidecar is not running"` | go-compute-backend.ts:310, pipeline-driver.ts:2270 | wait + reinit + drop advance | OK |
| `"Connection closed"` | same | same | OK |
| `"engine not initialized"` | go-compute-backend.ts:336, pipeline-driver.ts:2273 | reinit + drop advance | OK |
| `"chunk order violation"` | pipeline-driver.ts:2243 | re-throw (escalate) | OK; but HIGH-6 (drops TS internal changes) |
| `"finished without a final chunk"` | pipeline-driver.ts:2244 | re-throw (escalate) | OK; same HIGH-6 |
| `"Frame too large"` | pipeline-driver.ts:2245 | re-throw (escalate) | OK |
| `"protocolRev mismatch"` | pipeline-driver.ts:2246 | re-throw (escalate) | OK — though typically caught at `SidecarManager` startup, not advance |
| Other / unclassified | pipeline-driver.ts:2283 | metric + reset | OK; surfaces as `advance-dropped-other` |

---

## Coverage notes

- **Hot paths inspected**: `#addQueryDispatch`, `#advanceDispatch`,
  `#goPrimaryAdvance`, `#shadowAdvance`, `#goHydrate`, `goHydrateBatch`,
  `goHydrateBatchStream`, `shadowBatchCompare`, `#scheduleGoReset`,
  `#planAstForGo`, `#currentTablesForGo`, `#isInternalTable`,
  `#isInternalQueryID`, `#runDriftAudit`, `#sqlGroundTruthCompare`,
  `getRow`, `addQuery`, `advance`, `removeQuery`, `destroy`,
  `advanceWithoutDiff`, `#maybeResetGoBackend`, `#maybeInitGoBackend`.
- **view-syncer paths**: `#hydrateUnchangedQueries`,
  `#syncQueryPipelineSet`, `#addAndRemoveQueries`, `#advancePipelines`,
  `#catchupClients`, `#processChanges`.
- **go-compute-backend paths**: `#advanceWithRecovery`, `#withReinitRetry`,
  `#reinitPerCGAndRegisterQueries`, `#doInit`, `#runInit`,
  `#onSidecarRestart`, `whenInitialized`, `whenRecovered`.
- **Boundary contracts verified**:
  - `protocolRev` mismatch is enforced at `SidecarManager` startup;
    runtime mismatch surfaces via `version` RPC checks.
  - `chunkIndex` monotonicity + `final=true` terminator enforced by
    `createHydrateStreamAccumulator` and `createAdvanceStreamAccumulator`.
  - Per-CG fairness via `MAX_IN_FLIGHT_PER_GROUP = 16` ✓.
  - Restart epoch gating via `#sidecarInitEpoch` on every mutating
    RPC ✓.
  - `whenRecovered` / `whenInitialized` gating in `addQuery` /
    `advance` dispatch ✓.
- **Symmetry check**: `#goPrimaryAdvance` vs `#shadowAdvance`:
  - Both filter internal tables from `snapshotChanges` ✓.
  - Both use `advanceStream` ✓.
  - `#shadowAdvance` recovers from `DriftError.partialChanges`;
    `#goPrimaryAdvance` does not (delegates to `#advanceWithRecovery`
    which DOES extract partials — go-compute-backend.ts:304-307).
    Asymmetric in the catch handler, but `#advanceWithRecovery`
    centralizes it. OK.
  - `#shadowAdvance` filters internal-query events from TS for
    comparison; `#goPrimaryAdvance` doesn't need to (TS's events
    are kept, Go's events are merged). OK.

## Not checked

- **Streaming pipe internals**: `GoIVMClient.#onData`, frame parser,
  back-pressure, slot acquisition. Trusted to the existing
  `go-ivm-client.test.ts` and REVIEW-final RESILIENCE gates.
- **SidecarManager state machine**: `#handleRestartTrigger`,
  `#spawn`, `#waitForReady`, externally-managed mode. Trusted to
  REVIEW-final RESILIENCE-1..7 + the documented in-code state
  machine.
- **`#processChanges` consumer semantics**: trust REVIEW-final.
  Confirmed it iterates the AsyncIterable from
  `#goPrimaryAdvance` correctly.
- **CVR delta correctness**: out of scope; this audit focuses on the
  TS→Go wiring, not CVR maintenance.
- **`#runDriftAudit`'s `#sqlGroundTruthCompare`**: the SQL builder
  (`buildAuditSQL`) has known limitations (no scalar-subquery
  unrolling for value positions, etc.) but those are audit-side
  concerns, not Go-primary wiring.
- **Permissions reload (`reloadPermissionsIfChanged`)**: TS-side;
  not relevant to Go dispatch (Go doesn't enforce permissions).
- **Cost-model planning details (`createSQLiteCostModel`,
  `planQuery`)**: trusted; the question is whether `#planAstForGo`
  is invoked at every Go-bound dispatch, not what `planQuery`
  produces.
- **`#shadowCompare` / `#logShadowDiff` row-comparison logic**:
  REVIEW-shadow-mode covers it.
- **TableSource internal correctness**: trusted; the question is
  WHETHER TableSources exist when expected, not their behavior.
- **Inspector delegate (`addMetric`, `removeQuery`)**: surface
  inspection only; deep semantics out of scope.
