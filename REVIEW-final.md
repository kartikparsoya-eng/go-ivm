# Final Review — Go IVM Production Readiness

Consolidated audit + fix log across the four review surfaces:

- `REVIEW-porting.md` — Go IVM correctness vs TS reference.
- `REVIEW-parallelism.md` — Go-side concurrency safety.
- `REVIEW-ts-integration.md` — TS client / manager / backend.
- `REVIEW-shadow-mode.md` — Two-path validation harness.

Originally produced by four parallel deep-audit agents (porting, TS
integration, shadow mode, cross-cutting). All CRITICAL / HIGH / MEDIUM /
LOW findings except three feature-level items have now been fixed and
soak-verified.

---

## Bottom line

**READY** for both operating modes:

- `ZERO_GO_SIDECAR_ENABLED=true` + `ZERO_GO_SIDECAR_SHADOW_MODE=true` (shadow, TS source of truth) — production-validated. Safe to roll out today.
- `ZERO_GO_SIDECAR_ENABLED=true` + `ZERO_GO_SIDECAR_SHADOW_MODE=false` (Go-primary) — all gates closed including sidecar-kill resilience (RESILIENCE-1..7) and the Take operator's invariant fix (HIGH-IVM-1). Drift audit catches real drift and doesn't false-positive (validated by drift-injection test + 45-min clean soak). Recommend staged rollout via shadow mode first; cutover is unblocked once shadow-mode observation period completes.

### Performance tuning (post-streaming PR)

For Go-primary deployments, two knobs landed after the initial production-readiness review and meaningfully affect throughput / tail latency:

- **Shared sidecar mode** (`ZERO_GO_SIDECAR_EXTERNALLY_MANAGED=true` + `ZERO_GO_SIDECAR_SOCKET_PATH=…`). Default is one sidecar per zero-cache worker; that fragments client-group state across N sidecars and is the reason "bump `ZERO_NUM_SYNC_WORKERS`" can REGRESS p99 (measured 70ms → 180ms going 2→4 workers under default mode). In shared mode all workers connect to one external sidecar (typically spawned by the container entrypoint), so cg-state colocates and inter-cg parallelism scales with active cgs instead of workers. See `mono/AGENTS.md` § "Shared sidecar mode" for wire-up.

- **Sidecar parallel-push threshold** (`GO_IVM_PARALLEL_THRESHOLD=N`, default **2**). Lowered from the historical 4 because dashboard-shaped workloads typically have 2–3 queries per source per cg and never crossed the old threshold, leaving `genPushAndWriteParallel` as dead code. The new default fans out per-source pushes across pipelines whenever there are ≥2 connections.

Recommended Go-primary pairing: `ZERO_NUM_SYNC_WORKERS=1` + shared sidecar (no replicator duplication, no V8 GC overhead from extra workers, max cg-colocation in the one sidecar).

**T1-1 — typed Row decode (2026-05-21).** Custom `DecodeMsgpack` on `ivm.Row` coerces integer column values to `float64` inline at decode time, replacing the post-decode reflection walk for Row data. Measured **−41% total allocations** on a 60s pprof capture under live load (1289MB → 762MB), of which `reflect.unsafe_New` dropped **−47%**. Wire format unchanged; full `go test ./...` clean; drift audits across two 30-min soaks (unbounded + bounded) showed **0 mismatches**. See `go-ivm/PERF-REVIEW.md` for the full profile-driven analysis trail.

**Pprof support** baked in. The sidecar opens `net/http/pprof` on `GO_IVM_PPROF_ADDR` (env var, off by default) with block + mutex profiling enabled at sample rate 1. ~5% overhead when enabled; leave unset in prod. See `PERF-REVIEW.md` § "How to reproduce the profile" for capture commands.

**Validation evidence:**
- **45-min Go-primary soak** (post all-fixes, audit interval 5s): 9012 queries / 65992 frames, **0 mismatches, 0 panics, 0 client-visible errors, 0 sidecar restarts**. Memory bounded 570–793 MB, zero leak across the run.
- **Drift-injection test**: deliberately drop 1 row from every 10th advance on Go side → audit detected 16 distinct queries / 52 row-level mismatch logs. Reverting the injection → 0 mismatches. Proves the audit can detect real drift, not just non-issues.
- **Sidecar-kill drill** (3 SIGKILLs across one soak): 0 client-visible errors, 0 connections closed, 0 drift-audit mismatches, all sidecars respawned + re-initialized transparently (after RESILIENCE-1..5 fixes; pre-fix this run had 120 errors + 23 connection closures).
- **Shadow soak with streaming** on the hydrate path (20 users, mutate mode, `[batch-stream]` exercises `addQueriesStream`): 787 streaming batches, 0 mismatches, 0 panics.
- **30-min unbounded soak (2026-05-21, post-T1-1)**: 15 clients + continuous PG insert burst (~18k rows over 30 min). 5501 queries / 32847 frames / 0 errors / 0 mismatches / 0 panics / 0 restarts. Throughput perfectly linear (3.07 q/sec across the entire window). Advance peakConc=12-15 — first time we drove real inter-cg parallelism. Latency drifted 7→42ms advance p50 as the `messages` table grew (sort cost scaling with data size — workload physics, isolated by Run B).
- **30-min bounded soak (2026-05-21, post-T1-1)**: same load shape but mutator deletes 50 as it inserts 50 → constant working set. 5564 queries / 33214 frames / 0 errors / 0 mismatches / 0 panics / 0 restarts. **Advance p50 stayed sub-millisecond across the entire 30-min window (47µs → 19µs → 218µs)**. Heap grew only +40MB (vs +470MB in the unbounded run). Conclusively proves no bounded-working-set leak in the Go IVM — the unbounded drift was workload physics, not a sidecar bug.
- `npm --workspace=zero-cache run check-types` / `lint` clean.
- 36/36 PipelineDriver tests pass.
- Full `go test ./...` passes including the 100s testharness suite.

**Performance feature: streaming hydrate (added post-audit)**

The batched hydrate path now streams per-query results from Go to TS as each query's goroutine finishes, instead of buffering the full batch. Wire protocol: new `addQueriesStream` RPC emits one partial frame per query (in completion order, same request id) followed by a `"done"` terminal frame. The view-syncer's batch-hydrate consumer (`view-syncer.ts:1910`) now uses the async iterator `goHydrateBatchStream`, so fast queries' RowChanges reach the WebSocket pokers before slow queries in the same batch finish. Tail-latency win for batches with uneven hydration cost; otherwise no change. The streaming path is also exercised by `shadowBatchCompare` in shadow mode so pre-prod validates it without user impact.

**Performance feature: sub-frame chunking + advanceStream (protocol rev 3)**

Both `addQueriesStream` and the new `advanceStream` RPC chunk their output at `hydrateChunkSize` / `advanceChunkSize` (10,000 RowChanges, matching the view-syncer's `CURSOR_PAGE_SIZE`). Partial frames carry `chunkIndex` (monotonic per call/query) and `final` (terminal-frame marker). For `addQueriesStream`, chunks are scoped per `queryID` so the TS client accumulates per-query before invoking its caller's `onResult` — view-syncer code unchanged. For `advanceStream`, the whole call produces one chunk sequence with cumulative `timings` only on the final frame; `pipeline-driver.ts #goAdvance` reassembles into the same `AdvanceResult` shape `advance` returned, so this is a drop-in upgrade. Eliminates the "one giant msgpack frame" Go-side memory pressure on large hydrations or large snapshot diffs. Protocol rev bumped 2→3; mismatched sidecars refused at handshake.

Validation: 11 streaming-specific Go unit tests (boundary/empty/multi-chunk/ordering/parity-with-non-streaming). 5-min soak at chunkSize=10000 (single-chunk path): 0 mismatches across 1,070 queries / 6,254 frames. 10-min soak at chunkSize=100 (forces multi-chunk on essentially every query): 0 mismatches / 0 chunk-order violations / 0 orphan queries across 2,047 queries / 13,612 frames / 1,008 streaming batches.

---

## Status table

| Layer | Finding | Status | Fix mechanism |
|---|---|---|---|
| **Cross-cutting** | CRITICAL-CROSS-1 — HOL writer | **FIXED** | Per-request writer goroutines + shared `writeMu`; cross-group responses no longer serialize. |
| | CRITICAL-CROSS-2 — Global semaphore starvation | **FIXED** | Per-`clientGroupID` sub-semaphore (`MAX_IN_FLIGHT_PER_GROUP=16`) on top of global `MAX_IN_FLIGHT=1024`. |
| | HIGH-CROSS-1 — No drift detection in Go-primary | **FIXED** | Each `PipelineDriver` runs a periodic sampled-shadow audit in Go-primary mode (`goSidecar.driftAuditIntervalMs`, default 60s). One random active query per tick is re-hydrated on both TS and Go from the current snapshot; results are compared via the existing `#shadowCompare`. Mismatches surface via `ivm.drift-audit-mismatches`; clean runs via `ivm.drift-audit-runs`; busy/skew skips via `ivm.drift-audit-skips`. |
| | HIGH-CROSS-2 — Unbounded memory | **FIXED** | 30-minute idle reaper destroys abandoned groups; bounds aggregate sidecar memory. |
| | HIGH-CROSS-3 — destroy fire-and-forget leaks | **FIXED** | Idle reaper (HIGH-CROSS-2) covers this too. |
| | MED-CROSS-1 — Init stampede | **FIXED** | `SidecarManager.withInitSlot` caps to 4 concurrent inits. |
| | MED-CROSS-2 — Schema-reset rejection kills ViewSyncer | **FIXED** | Reject path nulls `#goInitPromise` so dispatch falls through to TS. |
| | MED-CROSS-3 — Telemetry units mixed | **FIXED** | New `ivm.advance-go-rpc-time` histogram for RPC overhead; `ivm.advance-time` keeps per-table compute. |
| | MED-CROSS-4 — No OTel propagation | **FIXED** | `traceparent` flows through the RPC envelope; Go sidecar inits an OTLP/HTTP exporter from standard OTEL env vars (`OTEL_EXPORTER_OTLP_ENDPOINT` etc.) and emits one server span per RPC handler, linked to the TS-side parent. Tracing is a no-op when no endpoint is set. |
| | MED-CROSS-5 — No version handshake | **FIXED** | `version` RPC; client refuses on `protocolRev` mismatch. |
| | MED-CROSS-6 — Restart epoch race | **FIXED** | Backend's `#restartGate` blocks advance/hydrate until reinit + query re-registration completes. |
| | LOW-CROSS-1 — Docs | **FIXED** | `mono/AGENTS.md` now documents env vars, fallback, wire protocol, ops concerns. |
| | LOW-CROSS-2 — No CI integration test | **DEFERRED (feature)** | Should add `ZERO_USE_GO_SIDECAR=true` variant of `pipeline-driver.test.ts`. |
| | LOW-CROSS-3 — env via `process.env` | **FIXED** | Sidecar feature flags now live under the `goSidecar` group of `zero-config.ts` (`ZERO_GO_SIDECAR_ENABLED` / `_SHADOW_MODE` / `_SHADOW_VERBOSE` / `_BINARY_PATH`). The four old env vars are no longer read; raw `process.env` reads in `go-compute-backend.ts`, `pipeline-driver.ts`, and `syncer.ts` are gone. |
| | RESILIENCE-1 — Sidecar-kill propagates as client Internal error | **FIXED** | `SidecarManager.waitForRunning()` returns a promise that resolves on the next `'running'` transition (rejects on terminal `'failed'`/`'stopped'`). `GoComputeBackend.#withReinitRetry` awaits it instead of throwing "Sidecar is not running" to the caller. Pre-fix: SIGKILL produced 89 "Sidecar is not running" errors per kill + 17 closed WebSocket connections. Post-fix: 0 / 0. |
| | RESILIENCE-2 — `"Connection closed"` not handled as restart-related | **FIXED** | Generalized restart-window detection in `#withReinitRetry` no longer pattern-matches specific error strings; checks `manager.status !== 'running'` OR `"Sidecar is not running"` OR `"Connection closed"`. Any of those triggers `waitForRunning()` + retry instead of surfacing to the caller. |
| | RESILIENCE-3 — Empty-engine race during retry | **FIXED** | After `waitForRunning()` returns, the per-clientGroup engine still isn't initialized until the `onRestart` listener finishes init+loadRows. Retrying immediately would hit a freshly-spawned-but-empty engine and silently return zero rows. `#withReinitRetry` now awaits `whenInitialized()` and force-inits if the listener microtask hasn't fired yet. Drift audit had been catching this (`go_count=0` while TS had data). |
| | RESILIENCE-4 — `advance()` replays stale diff after re-init → `panic: Row not found` | **FIXED** | `advance()` has its own catch (does NOT use `#withReinitRetry`). On restart it waits for re-init, then RETURNS EMPTY `{changes: [], timings: []}`. Go's new state from `init(currentTables)` already reflects the post-advance snapshot; re-applying the diff would double-apply and panic. Clients miss one delta from the IVM stream but Go state stays consistent. |
| | RESILIENCE-5 — Concurrent `#doInit` calls (onRestart listener + retry path) load rows twice | **FIXED** | `#doInit` now deduplicates: if `#currentInitPromise` is already set, return it instead of starting a second init. Before: both callers fired their `init`+`loadRows`+`addQueries` RPCs concurrently, the chunks interleaved at the wire level, and Go ended up with `ts_count=N go_count=2N` (audit caught this). |
| | RESILIENCE-6 — Restart pipeline hangs on mid-handshake spawn failure | **FIXED** | `#spawn()` can reject after status=`'restarting'` if `#waitForReady` times out, `ping` fails, or `version` mismatches. Previously the `.catch` in `#handleRestartTrigger`'s `setTimeout` only logged — status stayed `'restarting'`, `#runningPromise` never resolved, and every `waitForRunning()` caller hung forever. Fix: the catch now advances the state machine — in externally-managed mode it re-triggers `#handleRestartTrigger` (no `proc.on('exit')` exists to fall back on); in spawned mode it SIGKILLs the wedged proc so the existing exit handler re-routes. Both paths eventually trip the failure cap → `'failed'` → `#withReinitRetry` falls back to TS, matching documented behavior. **C10 refinement (D13):** the original fix double-counted failures when the spawned-mode SIGKILL raced with `proc.on('exit')` — both paths called `#handleRestartTrigger` and each bumped the sliding-window counter, so the cap tripped at half its nominal value. `#suppressNextExitCount` (added with C10) is set at SIGKILL and cleared by `proc.on('exit')` so only the explicit re-trigger counts; the exit handler runs as a no-op count-wise. Failure cap now trips at its configured value (default 5/60s). |
| | RESILIENCE-7 — Unhandled `proc.on('error')` event | **FIXED** | Node's `EventEmitter` semantics escalate unhandled `'error'` events to uncaught exceptions on the worker. `'exit'` typically pairs with `'error'` and routes the restart, but the defensive handler ensures spawn-time errors (ENOENT, EACCES, wrong arch) can never crash the worker. |
| **Go-side latent** | HIGH-IVM-1 — `panic: Bound should be set` in Take operator | **FIXED** | Root cause: `take.go:280` (REMOVE handler) wrote `{Size: N-1, Bound: nil}` when neither forward nor reverse fetch found a replacement bound. If N >= 2, this violated the invariant `Size > 0 → Bound != nil`; the next EDIT against that partition tripped the `if takeState.Bound == nil { panic }` check at line 306. Fix: when no replacement bound is findable, force `Size = 0` (unfindable bound ⇔ empty window). `go test ./...` incl. 105s testharness passes. |
| **TS integration** | HIGH-TS-1 — `awaitGoInit` lies post-restart | **FIXED** | `GoComputeBackend.whenInitialized()` is restart-aware; `awaitGoInit()` awaits it. |
| | MED-TS-1 — Drain handler is no-op | **FIXED** | `#drainPromise` gates `#acquireSlot` until socket drains. |
| | MED-TS-2 — pgTypeToGoType bytea/arrays | **FIXED** | Explicit bytea + array handling, one-time warnings per unknown type. |
| | MED-TS-3 — Partial loadRows leaves Go inconsistent | **FIXED** | On failure, `destroy(cgID)` clears Go state before bubbling the error. |
| | MED-TS-4 — `engine not initialized` no retry | **FIXED** | `#withReinitRetry` re-inits + retries once. |
| | MED-TS-5 — `#advanceTime` `type` label asymmetric | **FIXED** | TS `#advance` now assigns `type` ('add'\|'remove'\|'edit') like Go does. |
| | LOW-TS-1 — Quadratic for huge frames | **FIXED** | Pre-allocate buffer to `4 + announcedLen` on first split-frame chunk. |
| | LOW-TS-2 — uint32 ID wrap collision | **FIXED** | `#recentlyTimedOut` set prevents wrong-response routing. |
| **Shadow** | CRITICAL-1 — Nested-content blind compare | **FIXED** | Recursive `stableStringify` + BigInt/NaN safety. |
| | HIGH-1 — Drift unrecoverable on Go advance fail | **FIXED** | `#scheduleGoReset` triggers `resetEngine` with retry + dirty-flag for in-flight failures. |
| | HIGH-2 — Redundant per-query Go hydrate | **FIXED** | `#shadowAddQuery` runs TS only; `shadowBatchCompare` is the sole Go path. |
| | MED-1 — Permissive timer blocked event loop | **FIXED** | `suppressAbort` flag on `#advance` + real timer + `setImmediate` yields. |
| | MED-2 — Sequential TS+Go doubled latency | **FIXED** | Go RPC kicks off before TS drain; `await` at end. |
| | MED-3 — locale-dependent sort | **FIXED** | Direct `<`/`>` compare. |
| | MED-4 — first-mismatch-only logging | **FIXED** | Logs up to 5 plus overflow count. |
| | LOW-1 — speedup math noise | **FIXED** | Dropped. |
| | LOW-2 — AST log spam | **FIXED** | Moved to debug. |
| **Porting** | HIGH-1 — Skip cursor not normalized | **FIXED** | Builder normalizes `ast.Start.Row` via new `Source.NormalizeRow` interface method. |
| | MED-PORT-1 — Test harness uses legacy NormalizeRow | **FIXED** | `testharness` + bench tests use `NewMemorySourceWithConverter(FromSQLiteType)`. |
| | MED-PORT-2 — `sqlite.evalOp` null=null=true | **FIXED** | `=`/`!=` short-circuit on null operands; only `IS`/`IS NOT` accept null. |
| | MEDIUM-3 — Cap operator missing | **OPEN** | Unused by TS builder; truly forward-looking. |
| | LOW-PORT-1 — BulkInsert unsynchronized | **FIXED** | Takes `connsMu.Lock()`; defensive against future caller paths. |
| | LOW-PORT-2 — GetMemorySource raw pointer | **DOCUMENTED** | Invariant "source lifetime == engine lifetime" recorded; no current escape hazard. |
| **Parallelism** | All findings from REVIEW-parallelism | **FIXED** | Recorded in `REVIEW-parallelism.md`. |

---

## Remaining open items (non-gating)

1. **MEDIUM-3 (porting)** — Cap operator. Unused by TS builder; implement when/if TS adopts it.
2. **LOW-CROSS-2** — CI integration test with `ZERO_GO_SIDECAR_ENABLED=true`. The soak is currently the only end-to-end validation. A vitest variant that builds the Go binary and exercises `pipeline-driver.test.ts` would lock in regression coverage.

---

## Rollout-readiness checklist

| step | status |
|---|---|
| 1. Overnight (12h+) shadow soak in staging | not done |
| 2. Drift-injection test | ✅ done — proves audit catches real drift |
| 2a. Audit false-positive root-causing | ✅ done — `#tableSourcesVersion` guard + epoch guard |
| 2b. 45-min clean Go-primary soak | ✅ done — 0 mismatches, 0 leaks |
| 3. Sidecar-kill drill | ✅ done — RESILIENCE-1..5 closed |
| 3a. HIGH-IVM-1 Bound-panic investigation | ✅ done — root-caused at `take.go:280`, fix applied, full test suite passes |
| 4. CI integration test (LOW-CROSS-2) | not done — hygiene |
| 5. Shadow mode → 1 prod node | safe to do today |
| 6. ~1 week observation in shadow mode | depends on 5 |
| 7. Flip 1 node to Go-primary | unblocked — pending steps 5–6 |
| 8. Stage rollout to remaining nodes | depends on 7 |

---

## Confirmed-correct invariants

To preserve when modifying this code:

1. **Length-prefix framing + 64MB cap** symmetric on both sides.
2. **msgpack codec alignment** — `useRecords:false`, `useBigIntExtension:false`, looseInterfaceDecoding, post-decode numeric→float64 normalization.
3. **Per-group FIFO ordering** preserved end-to-end (Node single-thread + PipelineDriver async/await + Go per-group worker channel).
4. **REMOVE row shape parity** — both paths emit `row: undefined`. Inline comment warns against "fixing".
5. **Restart-safe client reference** — `GoComputeBackend.#client()` resolves through `manager.getClient()` on every call.
6. **Init split** — schema-only `init` + chunked `loadRows` (5000 rows/batch) stays under the 64MB cap.
7. **Snapshot-aligned reinit** — `#currentTablesForGo` reads the live snapshot atomically; the next advance's `prev` matches what Go ingested.
8. **`CompareValues` / `ValuesEqual` split** — null-equal-itself for sort, null-never-equal for join. Both required.
9. **Predicate null semantics** distinct from join null semantics in `builder/filter.go`. Do not unify.
10. **Source overlay** — `atomic.Pointer[Overlay]` with `defer Store(nil)`; load-bearing for panic safety.
11. **`Engine.Close`** takes `e.mu` before destroying pipelines; serializes with AddQuery/AddQueries/Advance.
12. **`stableStringify`** is recursive key-sort + deterministic at every depth; BigInt/NaN coercion safe.
13. **TimingMs writes in `AddQueries`** are per-slot, single-statement; happens-before from `wg.Wait` makes them race-free.
14. **`#scheduleGoReset` snapshot timing** — reset uses the snapshot live at scheduling time; per-group FIFO ensures no diff is lost between failed advance and reset.
15. **`#restartGate`** blocks advance/hydrate while reinit + query re-registration are in flight; prevents empty-engine race after sidecar restart.
16. **Per-group sub-semaphore** keys on `clientGroupID`; one bad group can't starve others.
17. **Idle-group reaper** uses `connsMu` snapshot under `s.mu.RLock` then takes `s.mu.Lock` to delete — never holds both at once.
18. **`recentlyTimedOut`** is bounded (4096 entries, FIFO eviction) — late responses for timed-out IDs are dropped, never routed to a new pending call.

---

## Per-doc cross-reference

The four area-specific review docs (`REVIEW-porting.md`, `REVIEW-parallelism.md`, `REVIEW-ts-integration.md`, `REVIEW-shadow-mode.md`) are now historical — they trace the audit trail and each finding's fix. This doc is the current production-readiness summary. When in doubt, prefer this doc's status table; the area docs are kept for traceability.
