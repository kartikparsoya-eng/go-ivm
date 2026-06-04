# PORT-AUDIT — Pipeline lifecycle (build / hydrate / advance / destroy)

Scope: the per-query pipeline lifecycle on both sides — AST → operator
tree, initial hydration, advance fan-out, destroy. Companion sub-pipeline
lifecycle (built from `whereExists(..., {scalar: true})` rewrite). Scalar
value-change detection during advance.

Files audited:
- TS canonical: `mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts`
  - `#addQueryImpl` (build + hydrate): 1857-2041
  - `#resolveScalarSubqueries` (companion build): 850-899
  - companion `setOutput` w/ value-change check: 1986-2026
  - `removeQuery` (destroy): 2047-2059
  - `reset` (full-driver schema reset): 691-705
  - `destroy` (driver teardown): 826-835
  - `#advance` (push fan-out): 2782-2911
  - `#push` (per-source-change streamer bracket): 3008-3028
  - `#startAccumulating` / `#stopAccumulating`: 3030-3040
  - `Streamer` class: 3043-3171
  - `scalarValuesEqual` helper: 3253-3259
- TS canonical resolver: `mono/packages/zqlite/src/resolve-scalar-subqueries.ts`
- Go: `go-ivm/engine/engine.go`
  - `Engine` struct + `Close`: 141-279
  - `buildAndRegisterLocked` (build + companion wiring): 380-448
  - `hydrateEntry`: 450-471
  - `AddQuery` / `AddQueries` / `AddQueriesStream`: 358-623
  - `removeQueryLocked`: 658-678
  - `Advance` / `AdvanceStream`: 690-953
  - `pipelineOutput.Push`: 1019-1025
  - `engineDelegate.CreateStorage`: 1082-1087
- Go: `go-ivm/engine/streamer.go` (engine-level streamer)
- Go: `go-ivm/ivm/source.go` (`genPush` fan-out, overlay)
- Go: `go-ivm/builder/resolve_scalar.go` (resolver port)
- Go: `go-ivm/sqlite/database_storage.go` (operator storage)
- Existing audit trail: `go-ivm/REVIEW-porting.md`, `go-ivm/REVIEW-final.md`,
  `go-ivm/REVIEW-parallelism.md`, `go-ivm/PORT-AUDIT-FIXES.md`,
  `go-ivm/PORT-AUDIT-streamer.md`.

Severity rubric:
- **CRITICAL**: silent wrong results; leaks state across queries.
- **HIGH**: observably wrong results in specific scenarios.
- **MEDIUM**: edge case bug, sub-optimal.
- **LOW**: cosmetic.
- **CHECKED-OK**: equivalent.
- **REGRESSION**: was FIXED, no longer.

---

## Findings

### CRITICAL-1: Companion push does NOT detect scalar value change (no ResetPipelinesSignal)

- **TS**: `pipeline-driver.ts:1986-2026` — after main hydrate, sets the
  companion's `setOutput` to a callback that derives `newValue` from
  the pushed change (ADD/EDIT: `row[childField]`; REMOVE: `undefined`;
  CHILD: skip). If `!scalarValuesEqual(newValue, resolvedValue)`, it
  throws `ResetPipelinesSignal('scalar-subquery')`. The view-syncer
  layer above catches and re-hydrates from the new snapshot, so the
  main pipeline is rebuilt with the new literal value baked into the
  rewritten WHERE.
- **Go**: `engine/engine.go:439-445` — wires every companion's output to
  the same `pipelineOutput` struct used for main pipelines
  (`engine.go:1019-1025`). The companion's push handler just calls
  `streamer.Accumulate(queryID, schema, [change])`. No `childField`
  value extraction, no `resolvedValue` comparison, no reset signal.
  Comment at `engine.go:114-118` explicitly acknowledges: "this version
  omits the live 'scalar value changed' reset because almost all real
  workloads never change a scalar's child field — they only ADD/REMOVE
  rows that flip existence."
- **Divergence**: TS resets the entire pipeline (forcing re-hydrate with
  the new literal). Go continues to use the STALE resolved literal in
  the main pipeline's rewritten filter (`parentField = <old_value>`)
  while emitting the new companion row as a peer.
- **Impact**: Silent wrong result-set membership. Concrete scenario from
  `pipeline-driver.test.ts:2413-2433`:
  1. `addQuery` with `whereExists(comment id=10, {scalar: true})` on
     correlation `issueID`. Initial: comment 10 has `issueID='1'`.
     Resolver replaces EXISTS with `issues.id = '1'` and ships comment
     10 as companion row.
  2. Advance updates comment 10: `issueID='1' → issueID='2'`.
  3. **TS**: companion push detects `newValue='2'` ≠ `resolvedValue='1'`,
     throws `ResetPipelinesSignal`. View-syncer re-hydrates: rewritten
     filter is now `issues.id = '2'`, returns rows for issue 2.
  4. **Go**: companion push silently emits the new comment 10 row. Main
     pipeline still filters `issues.id = '1'` (the bakedin literal),
     emits an EDIT on the OLD issue or nothing. Client view: issue 1
     stays in the set even though the scalar EXISTS no longer matches
     it; issue 2 never appears.
  Equally bad scenarios from the same test file:
  - `pipeline-driver.test.ts:2475-2495` — delete comment 10 → TS resets;
    Go silently keeps issue 1 in the set even though no comment exists.
  - `pipeline-driver.test.ts:2497-2522` — insert a comment after the
    initial null match → TS resets to load the matching issue; Go's
    filter is permanently `ALWAYS_FALSE` (resolver emitted `1=0` on the
    no-match path; see `resolve_scalar.go:171-175`) so issue stays
    invisible forever.
- **Detection**: invisible to the drift audit in the steady state for
  the no-value-change branches: Go's MemorySource state for the
  companion table tracks correctly; the audit compares re-hydration
  RowChange sets between TS-audit and Go-audit. TS-audit also runs the
  resolver fresh each cycle, picking up the new resolved value, so
  BOTH agree on the audit's hydration… while the LIVE Go pipeline
  carries the stale literal in its main filter. Audit will only catch
  it on freshly-registered queries (which pick up the new value via
  resolver), not on long-lived queries that the user is actually
  watching.
- **Suggested fix**: port the TS check into Go.
  1. Extend `companionEntry` in `engine/engine.go:119-124` to carry
     `resolvedValue` (already passed to it via `value` in the executor)
     and `childField` (already stored). Capture `matched` too — the
     reset condition is `newScalarValue !== resolvedValue` where
     unmatched is `undefined` (TS) / `(value=nil, matched=false)` (Go).
  2. Replace the companion-specific output with a dedicated type
     `companionOutput` (not the shared `pipelineOutput`) whose `Push`:
     - For ADD/EDIT: `newValue = change.Node.Row[ce.childField]`
       (use `nil` when absent, treating absent column as NULL).
     - For REMOVE: `newValue = undefined sentinel`.
     - For CHILD: return without accumulating.
     - Compare against `ce.resolvedValue` using a helper that mirrors
       TS's `scalarValuesEqual` (identity equality:
       `undefined===undefined`, `null===null`, primitive ==).
     - On mismatch, panic with a new `*ResetPipelinesSignal` value
       (or set a flag on the engine and bail out of the advance).
       The sidecar layer must catch it the same way it catches
       `*ivm.DriftError` today and return a reset signal to TS.
  3. Wire-protocol change: extend the advance result's Drift field, or
     add a `ResetReason` field, so the TS GoComputeBackend can route
     the signal to the same handler that processes drift errors. TS's
     pre-existing `ResetPipelinesSignal` path in the view-syncer can
     then run.
  4. Add a Go-side unit test mirroring `pipeline-driver.test.ts:2413`,
     `:2475`, and `:2497` against the engine directly.

  This is the J-row in `PORT-AUDIT-FIXES.md` and the rationale for
  marking it CRITICAL (silent wrong results that the drift audit
  CANNOT catch on already-registered queries).

---

### HIGH-1: companion `setOutput` wired BEFORE hydrate in Go vs AFTER hydrate in TS

- **TS**: `pipeline-driver.ts:1936-1944` wires `input.setOutput(...)` for
  the MAIN pipeline before the `hydrateInternal` loop runs (line 1946).
  Companion `setOutput` is wired AFTER hydrate emission, at lines
  1986-2026, AFTER the companion rows have already been yielded as
  static ADDs at lines 1953-1962. This is deliberate: during hydrate
  the companion's `Fetch` is consumed by `#resolveScalarSubqueries`'s
  executor, but the companion's PUSH side is intentionally inert until
  the main hydrate completes — anything the source might push during
  the hydrate window would land before the companion's reset-detecting
  callback existed.
- **Go**: `engine/engine.go:429-445` wires both main and companion
  `SetOutput` at build time, BEFORE `hydrateEntry` runs.
  `Engine.AddQuery` then calls `hydrateEntry` at line 374, with the
  companion's Push already alive. `Engine.AddQueriesStream` (line 547)
  does the same.
- **Divergence**: A push that lands during the hydrate window would
  reach the companion in Go but not in TS.
- **Impact**: In CURRENT Go this is benign because:
  - `AddQuery` / `AddQueries` / `AddQueriesStream` all hold `e.mu` for
    the duration of build + hydrate (engine.go:363/477/530).
  - `Advance` and `AdvanceStream` also acquire `e.mu` (engine.go:691,
    825).
  So no source push can fire between SetOutput and hydrate. **However,
  if the lock discipline is ever loosened** (e.g., to overlap advance
  with batch hydration for throughput), this becomes a real divergence:
  Go's companion would emit during hydrate (likely accidentally
  duplicating with the static companion ADD at engine.go:464-468), and
  the no-value-check stub from CRITICAL-1 would mean the duplicate
  silently flows downstream.
- **Suggested fix**: when CRITICAL-1 is fixed, also move companion
  `SetOutput` to AFTER `hydrateEntry` in `engine.go:439-445`. Either:
  - In `AddQuery` / `AddQueries` / `AddQueriesStream`, wire companion
    outputs in a separate post-hydrate pass.
  - Or: tag the companion output struct with a "live" flag that
    `hydrateEntry` flips on after emission.

---

### HIGH-2: per-query operator storage rows leak on `removeQuery` (Go-only)

- **TS**: `pipeline-driver.ts:2047-2059`. `removeQuery` calls
  `pipeline.input.destroy()` (which recursively destroys operators and
  their storage handles — TS storage handles are GC'd once the operator
  tree drops them). Per-query storage rows in SQLite are namespaced by
  `(cgID, opID)` where `cgID` is the actual clientGroupID (shared
  across all queries in the driver) and `opID` is per-operator within
  the driver. Rows for removed-query operators do remain in SQLite
  until the whole `ClientGroupStorage.destroy()` runs at driver
  teardown (pipeline-driver.ts:831). This is the TS leak class:
  per-CG, not per-query.
- **Go**: `engine/engine.go:1082-1087`. Each query gets its OWN
  `ClientGroupStorage`, keyed by `cgID = queryID` (note the cgID
  parameter is actually the queryID — see `engineDelegate.queryID` at
  engine.go:1066, passed in from `buildAndRegisterLocked` at line 388).
  `sqlite/database_storage.go:170-182` runs `DELETE FROM storage WHERE
  clientGroupID = ?` ONLY on CreateClientGroupStorage. `removeQueryLocked`
  (engine.go:665-678) destroys the input but never calls `cgs.Destroy()`
  (engine.go:1067 holds the `*sqlite.ClientGroupStorage` in the
  delegate, which is itself dropped when the entry drops, but
  `Destroy()` is never invoked anywhere in the codebase — verified by
  `grep -rn "cgs.Destroy"`).
- **Divergence**: Go's per-query storage rows live in SQLite indefinitely
  after the query is removed. They're cleared only if the SAME queryID
  is later re-added (which triggers `CreateClientGroupStorage`'s DELETE
  at the new build), or when the engine closes (`Engine.Close` at
  engine.go:257-279 closes the whole storage DB).
- **Impact**: slow leak in long-lived client groups with churning
  queries. Magnitude scales with `(distinct queryIDs over CG lifetime)
  × (per-query storage rows)`. Typical IVM queries with Take or Skip
  operators store O(rows-in-window) bookkeeping; a CG that has spun
  through 1000 distinct queries with 100-row windows could accumulate
  ~100k orphan rows. Bounded only by the 30-min idle reaper
  (REVIEW-final HIGH-CROSS-2) which destroys the whole engine.
- **Suggested fix**: in `removeQueryLocked` (engine.go:665-678), after
  destroying the input, locate the entry's `engineDelegate` (which
  holds `cgs`) and call `cgs.Destroy()`. Today the delegate isn't
  retained on the `pipelineEntry`, so either:
  1. Store a reference to the delegate (or its cgs) on `pipelineEntry`.
  2. Or, in `removeQueryLocked`, run `e.storage.CreateClientGroupStorage(queryID).Destroy()`
     which has the same DELETE side-effect (slightly more work since it
     re-DELETEs then constructs an unused object, but simpler).
  Option 1 is cleaner.

---

### HIGH-3: streamer not cleared on non-drift panic in `Engine.Advance` (non-stream)

- **TS**: `pipeline-driver.ts:3008-3028` (`#push`) wraps the per-source
  push in a `try/finally` that always calls `#stopAccumulating()` if
  `#streamer !== null` after the loop. So if a downstream operator
  panics during the push, the next `#startAccumulating()` call sees a
  null streamer and proceeds cleanly.
- **Go**: `engine/engine.go:690-769` (`Engine.Advance`). The recover
  block (lines 699-720) distinguishes Drift (drains streamer + signals
  caller) from non-Drift (re-raises `panic(r)` at line 718 WITHOUT
  draining). The engine-level `e.streamer` (engine.go:145) retains
  whatever entries were accumulated by the panicking source.Push
  before the panic — and possibly by earlier successful pushes in the
  same advance that hadn't yet hit their per-push `e.streamer.Stream()`
  call (engine.go:741). The NEXT successful `Engine.Advance` call's
  first `e.streamer.Stream()` (line 741) would surface those stale
  entries as if they were produced by the new advance.
- **Compare with**: `Engine.AdvanceStream` (engine.go:885-894) does
  drain on non-Drift panic (`_ = e.streamer.Stream()`). Inconsistent
  between the two advance paths.
- **Divergence**: Go's non-stream `Advance` can leak stale streamer
  entries across a panic boundary; TS cannot.
- **Impact**: In production this is partially masked by:
  - Non-drift panics in advance are rare (operator-internal bugs).
  - When they do happen, the sidecar's outer recover converts the
    panic to an RPC error and the TS side schedules a Go reset
    (recreating the engine — `#scheduleGoReset` at
    pipeline-driver.ts:2349). The reset destroys the leaky engine
    before the next advance can fire.
  But "before the next advance can fire" depends on the reset
  successfully running before any concurrent advance attempt sneaks
  through. The `#restartGate` (REVIEW-final invariant 15) covers this
  for the sidecar restart path, but for non-restart "soft" recovery
  (where the engine isn't recreated, just the panic propagated), it's
  not airtight.
- **Suggested fix**: in `Engine.Advance`'s deferred recover
  (engine.go:699-720), before the `panic(r)` re-raise on non-drift,
  drain the streamer with `_ = e.streamer.Stream()`. Match the
  pattern from `AdvanceStream` at engine.go:893. Cosmetic line, real
  correctness benefit.

---

### MEDIUM-1: Go's `hydrateEntry` has no per-row 'yield' tokens

- **TS**: `pipeline-driver.ts:3206-3219` (`hydrateInternal`) yields
  through `streamer.stream()`, which is itself a generator that yields
  every operator's 'yield' tokens (`pipeline-driver.ts:3070-3120`).
  The source's `*#fetch` (`memory-source.ts:256`) emits 'yield' tokens
  via `generateWithOverlay` so a long initial fetch cooperatively
  yields the thread back to the view-syncer, which can then time-slice
  / abort if the deadline is exceeded.
- **Go**: `engine/engine.go:450-471` (`hydrateEntry`) calls
  `entry.pipeline.Input.Fetch(ivm.FetchRequest{})` which returns
  `[]ivm.Node` synchronously (verified at `ivm/source.go:491` —
  `Fetch` returns `[]Node`, no streaming, no yield). The whole fetch
  result is materialized before any row is emitted. `streamNodes` then
  walks the materialized slice synchronously.
- **Divergence**: TS cooperatively yields during long fetches; Go does
  not.
- **Impact**: Under sustained load with large initial-state queries
  (e.g., a `LIMIT 10000` over a large unfiltered table), Go's hydrate
  goroutine doesn't yield to scheduler / cooperatively give up the
  engine lock until the entire fetch completes. Two real consequences:
  1. `Engine.mu` is held throughout, blocking other RPCs on this
     client group (Advance, RemoveQuery, the drift audit's
     `RefreshAllSources` bypass — though the audit uses a COW
     snapshot read that bypasses the lock).
  2. The sidecar's per-CG worker thread is also blocked, so other
     queries in the same CG don't get hydrated until this one finishes.
     This matters most in `AddQueriesStream` where queries DO hydrate
     in parallel goroutines (engine.go:561-620) — each goroutine still
     holds onto whatever CPU the runtime gives it, but the engine lock
     and per-CG worker channel are bottlenecks.
- **Risk under current load**: dashboard-shape workloads with bounded
  result sets (~100s of rows per query) are unaffected. The risk grows
  with limit size and table size; the 30-min unbounded soak
  (REVIEW-final, post-T1-1) showed advance p50 drifting 7→42 ms as
  table grew but didn't surface hydrate timeouts. Worth measuring once
  a workload hits the 10k-row-per-query regime that Take operators
  cap at via `hydrateChunkSize`.
- **Suggested fix**: not gating, but worth tracking. Refactor `Fetch` to
  a streaming variant (channel or iterator) and emit yield-equivalent
  tokens at chunk boundaries. The 10000-row `hydrateChunkSize` already
  acts as a natural chunking unit; `flush(false)` in
  `AddQueriesStream` releases the wire frame, but the calling goroutine
  still holds `e.mu`. Releasing the lock between chunks would require
  the fetch to be resumable mid-iteration, which is a non-trivial
  refactor of `MemorySource.getSortedRows` (engine.go:529-537 — it
  sorts the entire data slice up front; see W in `PORT-AUDIT-FIXES.md`).

---

### MEDIUM-2: Companion `setOutput` overwrites itself on shared `pipelineOutput`

- **TS**: each companion gets a FRESH closure for its `setOutput` (line
  1993-2024). The closure captures `meta`, `companionSchema`,
  `childField`, `resolvedValue` per companion.
- **Go**: each companion in the loop at `engine.go:439-445` gets a
  fresh `&pipelineOutput{...}` literal per iteration — but the struct
  has only `engine`, `queryID`, `schema`. The queryID is the SAME
  parent queryID for every companion (correct per the contract). The
  schema differs per companion (correct). But the struct has no
  `childField` or `resolvedValue` fields — confirming there's nowhere
  for the scalar-value-change detection state to live, which is a
  direct manifestation of CRITICAL-1.
- **Divergence**: encoding of "per-companion state" differs. TS uses
  closure capture; Go uses a struct that's missing the companion-specific
  fields entirely.
- **Impact**: see CRITICAL-1 (this is the structural reason CRITICAL-1
  isn't a one-line fix — the data shape needs to grow).
- **Suggested fix**: introduce a `companionOutput` struct alongside
  `pipelineOutput`, with the extra fields. Wire companion outputs to
  the new struct.

---

### MEDIUM-3: Companion `setOutput` schema mismatch — Go uses subquery schema, TS uses companion-input schema

- **TS**: `pipeline-driver.ts:1991` — `const companionSchema =
  companionInput.getSchema()`. The companion's `setOutput` then
  accumulates with `streamer.accumulate(queryID, companionSchema, ...)`.
  This is the input's schema, which can differ from the raw subquery
  table's schema if any operators were applied (e.g., a Filter).
- **Go**: `engine/engine.go:398` — `subSchema := subPipeline.Input.GetSchema()`
  captured at companion build time, then stored on `ce.schema` and
  used in `engine.go:443`. Same source: the input's schema.
- **Divergence**: none — both use the companion input's actual schema.
  Verified.
- **Status**: **CHECKED-OK**.

---

### MEDIUM-4: hydrate emits companion rows AFTER main rows on both sides — ordering consistent

- **TS**: `pipeline-driver.ts:1946-1962` — `hydrateInternal` first
  (main rows), then a for-loop yields companion ADD rows. Order: all
  main rows, then all companion rows.
- **Go**: `engine/engine.go:453-470` — `for _, node := range nodes`
  (main rows), then `for _, ce := range entry.companions` (companion
  rows). Same order.
- **Streaming variant**: `engine.go:588-611` chunk boundaries can split
  the boundary between main and companion rows, but the relative order
  within and across is preserved.
- **Status**: **CHECKED-OK**.

---

### MEDIUM-5: Companion always alive even when no row matched — both sides agree

- **TS**: `pipeline-driver.ts:882-887` — when `node` is undefined (no
  match), the executor still does `companionInputs.push(input)`. The
  companion input lives so that a future INSERT to the subquery's
  source can be observed (and trigger ResetPipelinesSignal per the
  scenario at `pipeline-driver.test.ts:2497`).
- **Go**: `engine/engine.go:396-413` — the executor ALWAYS appends
  `&companionEntry{pipeline: subPipeline, ...}` to `companions`,
  regardless of whether `nodes` had any rows. Storage of `ce.matchedRow`
  is conditional, but the pipeline itself is kept.
- **removeQuery destroys both**: `engine.go:670-676` destroys the main
  pipeline AND iterates all `entry.companions` (matched or not),
  destroying each. Matches TS at `pipeline-driver.ts:2052-2054`.
- **Status**: **CHECKED-OK**.

---

### MEDIUM-6: scalar value change reset signal absent → `ALWAYS_FALSE` companion path is sticky

- **TS**: when the executor returns `undefined` (no match) or `null`
  (match with NULL value), the resolver collapses the WHERE to
  `ALWAYS_FALSE` (`resolve-scalar-subqueries.ts:171-174`) AND keeps
  the companion alive. A subsequent INSERT that creates a matching
  row triggers the companion's `setOutput`, which raises
  `ResetPipelinesSignal` (newValue is `<inserted value>`, resolvedValue
  is `undefined` → not equal). View-syncer re-hydrates, the resolver
  re-runs, and now the WHERE is `id = <value>` and the issue appears.
- **Go**: `builder/resolve_scalar.go:171-175` also emits `ALWAYS_FALSE`
  on no-match. BUT the companion has no value-change detection
  (CRITICAL-1), so the INSERT case never resets the pipeline. The
  main pipeline's WHERE remains `1 = 0` forever. The issue is
  **permanently invisible** in Go's output.
- **Divergence**: rephrasing of CRITICAL-1 specifically for the
  no-initial-match case. Worth calling out separately because:
  - The drift audit hydrates a fresh query and would observe TS=non-empty,
    Go=empty (since Go's audit query goes through the same broken
    path). Wait — Go's audit re-runs the resolver and gets the new
    value, so Go-audit hydration would now return rows. But the LIVE
    Go pipeline that was hydrated earlier has `ALWAYS_FALSE` baked in
    and continues to emit nothing. So **the audit on this specific
    case WOULD catch the drift (live Go set differs from re-hydrated
    Go set)** — but only on the audit cycle that picks this query,
    which is one query per cycle randomly.
- **Impact**: same severity class as CRITICAL-1 (silent wrong results).
  Detection: partial via drift audit, only for queries the audit
  happens to sample post-INSERT before the user reconnects.
- **Suggested fix**: same as CRITICAL-1.

---

### LOW-1: re-hydrate same queryID flow — `removeQuery` runs first on both sides

- **TS**: `pipeline-driver.ts:1880` — `#addQueryImpl` starts with
  `this.removeQuery(queryID)`. So any prior pipeline (and its
  companions) is destroyed before the new one is built. Storage
  rows for the prior incarnation are NOT explicitly cleared
  (TS storage is per-CG, persistent across query rebuilds —
  intentional, lets Take resume from prior bookkeeping).
- **Go**: `engine/engine.go:367` — `AddQuery` calls
  `e.removeQueryLocked(queryID)`. Also `AddQueries` (line 484) and
  `AddQueriesStream` (line 546) do the same per query.
  CreateClientGroupStorage (called via the delegate's first
  `CreateStorage`) issues `DELETE FROM storage WHERE clientGroupID = ?`
  — so on Go, re-hydrate CLEARS prior storage. This is a behavior
  difference for Take's resumability but doesn't cause divergent
  output because the re-hydrate also re-emits the full initial state.
- **Status**: **CHECKED-OK** functionally; the prior-storage-clear in
  Go is actually a cleaner restart than TS, and at re-hydrate time
  Take rebuilds its bookkeeping from the fresh initial state anyway.

---

### LOW-2: Engine.Close does NOT call `cgs.Destroy` per query before closing storage DB

- **Go**: `engine.go:257-279` — `Close` destroys all pipelines, then
  closes the underlying SQLite DB. Per-query storage rows are still in
  the DB at close time, but the whole DB file is closed. If the
  storage backs a path-on-disk that persists across engine lifetimes,
  these rows would be re-read on the next engine open. In practice
  `sqlite.NewDatabaseStorage(cfg.StoragePath)` uses an in-memory or
  per-spawn path (depends on `EngineConfig.StoragePath`), so this
  doesn't accumulate across process restarts. But for `StoragePath=""`
  or any persistent path, this is a slow leak class identical to
  HIGH-2.
- **Status**: **LOW** — bounded by engine-lifetime, only a real leak
  if `StoragePath` points to a stable persistent file.

---

### LOW-3: pipelineOutput.Push always returns nil — TS returns `[]`

- **TS**: `pipeline-driver.ts:1942` — main pipeline's `push` returns
  `[]` (empty array). Same for companion's at line 2022.
- **Go**: `engine.go:1024` — returns `nil`. The IVM `Output.Push`
  interface signature accepts `[]ivm.Change`. nil slice is treated the
  same as empty slice by all downstream consumers.
- **Status**: **CHECKED-OK** (cosmetic — both treat as "no changes
  pushed back up").

---

### CHECKED-OK-1: Companion rows tagged with PARENT queryID

- **TS**: `pipeline-driver.ts:1957` (`queryID,` in the static-companion
  ADD) and line 2021 (`streamer.accumulate(queryID, ...)`) — the
  ENCLOSING queryID (the parent query's ID), not the subquery's. The
  client uses this to demultiplex per-query streams.
- **Go**: `engine.go:441-444` — `&pipelineOutput{engine: e, queryID:
  queryID, schema: ce.schema}`. The `queryID` parameter is the PARENT
  queryID from `buildAndRegisterLocked`'s signature. `ce.schema` is
  the companion's own schema (so the client knows the table). Same
  contract.
- **Hydrate emit**: `engine.go:457` and `engine.go:468` — both emit
  with `entry.queryID` (the main queryID). Matches.
- **Status**: **CHECKED-OK** — both sides emit companion rows under the
  parent queryID with the companion's own schema.

---

### CHECKED-OK-2: removeQuery destroys companions in same order as TS

- **TS**: `pipeline-driver.ts:2051-2054` — `pipeline.input.destroy()`
  first, then `for (const companion of pipeline.companions)
  companion.input.destroy()`.
- **Go**: `engine.go:670-676` — `entry.pipeline.Input.Destroy()` first,
  then `for _, ce := range entry.companions { ce.pipeline.Input.Destroy() }`.
- **Status**: **CHECKED-OK** — same destroy order. Each `Destroy()`
  call disconnects from the source, removing the Connection from the
  source's connection list, so future pushes don't fan out to a dead
  pipeline.

---

### CHECKED-OK-3: companion Connection removal from source on destroy

- **TS**: TableSource/MemorySource `disconnect` is invoked transitively
  via the operator chain's `destroy`. Source filters its connection
  list.
- **Go**: `ivm/source.go:485-487` (`SourceInput.Destroy`) calls
  `si.source.Disconnect(si)`, which removes the connection from the
  source's `connections` slice under `connsMu` (see
  `parallelism review MEDIUM-1` fix). Companion's Connection (built
  via `Connect` at engine.go:1107-1109) is owned by the operator chain
  built in `BuildPipeline` and destroyed by `Input.Destroy()`.
- **Status**: **CHECKED-OK** — companion's source Connection is cleaned
  up on `removeQueryLocked`.

---

### CHECKED-OK-4: streamer per-source-change drain timing

- **TS**: `pipeline-driver.ts:3008-3028` — `#push` does
  `#startAccumulating()`, iterates `source.genPush`, between yield
  tokens does `#stopAccumulating().stream()` then re-starts. Per
  source-change, fresh streamer.
- **Go**: `engine.go:738-749` — for-loop calls `source.Push(sc)` then
  `e.streamer.Stream()`. Per source-change, streamer is drained
  (Stream nils `accumulated`).
- **Difference**: Go's streamer is engine-level (single instance per
  Engine, shared across all advances); TS allocates a fresh `Streamer`
  per source-change. Functionally equivalent because Go's `Stream()`
  empties the buffer (streamer.go:82). Both serialize correctly under
  the engine.mu / JS event loop.
- **Status**: **CHECKED-OK**.

---

### CHECKED-OK-5: Companion fetch returns no rows but kept alive

- Both TS (pipeline-driver.ts:882-887) and Go (engine.go:407-414) keep
  the companion sub-pipeline alive even when the initial fetch is
  empty. Both `removeQuery` paths destroy them (CHECKED-OK-2).
- **Status**: **CHECKED-OK**.

---

### CHECKED-OK-6: snapshot version advance + `setDB` ordering

- **TS**: `pipeline-driver.ts:2874-2879` — after the iteration loop,
  `setDB(curr.db.db)` on every TableSource, then update
  `#tableSourcesVersion`. Failure recovery in `finally` block at
  2882-2909 also realigns.
- **Go**: `engine.go:760` — `e.signalAdvanceEnd()` calls
  `OnAdvanceEnd()` on every source that implements it (TableSource —
  rotates its snapshot). MemorySource doesn't need it (mutates in
  place).
- **Status**: **CHECKED-OK** for the snapshot-rotation contract; the
  semantics differ (TS rebinds the DB; Go rotates an internal
  snapshot pointer) but both leave the next push at the right frame.

---

### CHECKED-OK-7: Driver `destroy` ordering

- **TS**: `pipeline-driver.ts:826-835` — `destroy` clears the audit
  timer, calls `#storage.destroy()`, `#snapshotter.destroy()`, then
  fire-and-forget `#goBackend?.destroy()`.
- **Go (server-side equivalent in sidecar)**: `Engine.Close` at
  `engine.go:257-279` — destroys all pipelines (including companions),
  nils sources, closes storage DB.
- **Status**: **CHECKED-OK** for the engine-side teardown. The
  pre-Close pipeline destruction in `Engine.Close` is critical
  (per `REVIEW-parallelism HIGH-3`, was added after the audit caught
  this gap).

---

## Lifecycle phase parity matrix

| Phase | TS contract | Go contract | Status |
|---|---|---|---|
| **Build — resolver pass** | `#resolveScalarSubqueries` runs executor per scalar subquery, stores companion meta + input | `builder.ResolveSimpleScalarSubqueries` + per-call executor in `buildAndRegisterLocked` builds sub-pipeline | OK (semantically equivalent) |
| **Build — main pipeline** | `buildPipeline(resolvedQuery, delegate, queryID, costModel)` w/ MeasurePushOperator decorator | `builder.BuildPipeline(result.AST, delegate)` — no MeasurePushOperator analogue | OK functionally (instrumentation gap noted as O in PORT-AUDIT-FIXES.md) |
| **Build — main setOutput** | wired BEFORE hydrate (line 1936) | wired BEFORE hydrate (engine.go:429) | OK |
| **Build — companion setOutput** | wired AFTER hydrate (line 1993) | wired BEFORE hydrate (engine.go:439) | **HIGH-1** (benign under lock discipline; fragile if loosened) |
| **Build — companion holds resolvedValue + childField** | yes (line 2025) | only childField (engine.go:123); no resolvedValue or matched flag | **CRITICAL-1** / **MEDIUM-2** |
| **Hydrate — initial fetch** | input.fetch({}) inside hydrateInternal | entry.pipeline.Input.Fetch(ivm.FetchRequest{}) | OK |
| **Hydrate — per-row yield** | yields 'yield' tokens via operator chain | NO yield tokens (eager slice) | **MEDIUM-1** |
| **Hydrate — companion rows after main** | yes (line 1953) | yes (engine.go:463) | OK |
| **Hydrate — companion rows tagged w/ parent queryID** | yes (line 1957) | yes (engine.go:457 + 468) | OK |
| **Hydrate — unmatched companion stays alive** | yes (line 885) | yes (engine.go:412) | OK |
| **Advance — diff fan-out per table** | per-table source.genPush | per-table source.Push, engine-level | OK |
| **Advance — per-push streamer drain** | per source-change `#startAccumulating`/`#stopAccumulating` | per source-change `e.streamer.Stream()` | OK |
| **Advance — scalar value change detection** | throws `ResetPipelinesSignal` (line 2010) | NONE — companion just emits | **CRITICAL-1** |
| **Advance — drift recovery** | TS throws ResetPipelinesSignal in various op paths | engine recovers `*ivm.DriftError`, drains streamer, signals | OK (different mechanism, equivalent contract) |
| **Advance — streamer cleared on non-drift panic** | yes (finally block at 3023-3027) | NO in Engine.Advance; YES in Engine.AdvanceStream | **HIGH-3** (Advance only) |
| **Advance — snapshot version setDB** | post-loop setDB per TableSource (line 2876) | OnAdvanceEnd per source (engine.go:760) | OK |
| **Destroy — input.destroy()** | yes (line 2051) | yes (engine.go:670) | OK |
| **Destroy — companions iterated** | yes (line 2052) | yes (engine.go:674) | OK |
| **Destroy — source Connection removed** | yes (transitively via operator destroy) | yes (SourceInput.Destroy → source.Disconnect) | OK |
| **Destroy — per-query storage cleared** | NO (TS storage is per-CG, persistent — by design) | NO (Go storage is per-query but Destroy never invoked) | **HIGH-2** (Go-only leak class) |
| **Re-hydrate same queryID** | `removeQuery` then build (line 1880) | `removeQueryLocked` then build (engine.go:367/484/546) | OK |
| **Re-hydrate clears prior storage** | NO (per-CG storage persists) | YES (CreateClientGroupStorage DELETEs first) | OK (Go is stricter; both safe) |
| **Driver destroy / Engine.Close** | `#storage.destroy()` + snapshotter + goBackend.destroy | destroys all pipelines + closes DB | OK |

---

## Coverage notes

- Read in full: `pipeline-driver.ts:680-3041` (the relevant lifecycle
  half of the file), `pipeline-driver.ts:3043-3306` (Streamer + helpers).
- Read in full: `engine.go` (all 1118 lines).
- Read in full: `streamer.go` (179 lines).
- Read in full: `resolve_scalar.go` (259 lines, Go) and
  `resolve-scalar-subqueries.ts` (258 lines, TS) — confirmed the
  resolver itself is line-for-line equivalent. The divergence is in
  the consumer (companion setOutput wiring), not the resolver.
- Cross-checked: `pipeline-driver.test.ts:2155-2523` (companion test
  cases) to validate behavior expectations; confirmed three tests
  (scalar value change, companion row deleted, companion row added)
  all expect `ResetPipelinesSignal` to be thrown.
- Cross-checked: `engine/resolve_scalar_integration_test.go` and
  `engine/scalar_unresolved_emit_test.go` — Go's scalar tests cover
  ADD-and-static-emit but NONE cover the scalar-value-change reset
  scenario. The gap is confirmed not just by code reading but by the
  test coverage gap.
- Spot-checked: `Engine.Close` lock interactions with the parallelism
  review's CRITICAL-1 (worker goroutine leak) — confirmed independently
  closed.
- Spot-checked: `Engine.AddQueriesStream` chunking boundaries
  (engine.go:561-620) — companion rows are appended after the main
  rows to the chunk buffer, with `len(chunk) >= hydrateChunkSize`
  checked after each `append`. Companion rows can split across a
  chunk boundary; that's by design and matches `addQueriesStream`'s
  per-queryID accumulation in the TS client.

## Not checked

- **Cost model planner integration**: the TS path runs `#planAstForGo`
  (completeOrdering + planQuery) before dispatching to Go for hydrate.
  The lifecycle audit didn't verify whether Go's planner pass matches
  TS's — that's a build-time AST equivalence question covered by the
  H18 follow-up (`H18-followup.md`), not pipeline lifecycle per se.
- **Snapshotter advance + DriftError interaction**: when the
  snapshotter advances past a frame where the diff contained a
  drift-inducing row, the engine's recover path drops the partial
  output and signals drift. The audit did not deep-dive into whether
  TS's behavior here is identical (TS's snapshot is rebound and the
  pipelines are reset by view-syncer; Go's engine is re-inited by the
  cross-cutting machinery). Both reach the same end state but via
  different sequences.
- **MeasurePushOperator timing instrumentation**: TS wraps the
  source-input in a `MeasurePushOperator` per query (line 1911-1917)
  for inspector visibility. Go has no analogue. Tracked as O in
  `PORT-AUDIT-FIXES.md`; not gating for correctness.
- **Per-CG worker goroutine lifecycle in the sidecar layer**
  (`cmd/sidecar/main.go`): destroy semantics there are covered by
  `REVIEW-parallelism.md` CRITICAL-1; this audit only covers the
  Engine-level lifecycle.
- **TS-only edge: `auditMode` setOutput** at `pipeline-driver.ts:1932`
  (audit-mode `push: () => []` sink): no Go analogue because Go's
  drift audit re-hydrates via `hydrateMany` and discards the result
  in-process. Doesn't apply to the Engine lifecycle.
- **goHydrate / goHydrateBatch / goHydrateBatchStream stub pipeline
  semantics** at `pipeline-driver.ts:991-1018, 1047-1059, 1198-1210`:
  these write a no-op `Input` stub to `#pipelines` so the queries()
  map and rowSetSignatures bookkeeping survive. Functionally
  side-effect-free relative to Go's engine; out of scope for this
  Go-engine-focused audit.
- **Schema-change `reset(clientSchema)`** at `pipeline-driver.ts:691-705`:
  iterates and destroys all pipelines + companions. Equivalent on Go
  side is `Engine.Close` followed by a fresh `NewEngine`. Verified
  the destroy ordering matches but did not exhaustively trace the
  reset trigger paths in `view-syncer.ts`.
