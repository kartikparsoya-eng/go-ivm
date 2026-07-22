# PROD-PATH — what executes in production, and what doesn't

Scope map for the Go engine surface (this repo + mono's
`packages/zero-cache/src/services/view-syncer/go-sidecar/`). Purpose: the
TS-faithfulness review covers exactly the PROD column; everything else is
either a marked rollback knob, the shadow/validation harness, or scheduled
for removal. Generated during the RPC-surface cleanup; verify against source
before acting on it — markers below make drift from this map greppable in
deployment logs.

Production configuration = the config DEFAULTS (mono `zero-config.ts`
`goSidecar.*`: enabled, napiLibPath, pullWindow; protocolRev=12) + the
Dockerfile env (`GO_IVM_REPLICA_DB_PATH`/`ZERO_REPLICA_FILE` mandatory).
Table mode is the only mode; advanceDrive is hard-wired ON; drift audit is
gone (drift panics propagate as -32000 unclassified → teardown).

## Log markers

| Marker | Meaning |
|---|---|
| `[GO-IVM][NON-DEFAULT]` | deliberate rollback/experiment knob engaged — deployment is off this map's PROD column |

A production pod's logs must contain ZERO of these (the grep target is
deployments).

## RPC surface (Go `cmd/sidecar` dispatch)

| Method | Status | Notes |
|---|---|---|
| `init` (table-mode) | PROD | replica-backed `tablesource.Source` per table; the only mode |
| `addQueriesStream` (pullMode) | PROD | ABI v3 credit-gated hydrate — the default (`pullHydrate=true`) |
| `addQueriesStream` (push) | PROD (degrade) | non-rowMode / pull-refused degrade path |
| `removeQuery` | PROD | |
| `advanceToHeadStream` | PROD | drive mode; lazy changelog feed; TS-economic abort (budget = per-thread CPU via `internal/procclock`, the TS processing-lap analog — NOT wall) + env budget both map to `rpcCodeAdvanceAborted` → `advancement-timeout` reset |
| `destroy`, `ping`, `version` | PROD | |
| `loadRows` | DELETED | memory-mode seeding removed; table mode reads from replica |
| `advanceToHead` (unary) | DELETED | shadow-only caller set removed; `advanceToHeadStream` is the serving path |
| `advanceStream` (push advance) | DELETED | push advance removed; `advanceToHeadStream` is the only advance |
| `refreshSnapshot` | DELETED | drift audit removed; drift panics propagate as -32000 |
| `pipelineCount` | DELETED | drift audit count probe removed |
| `addQuery`, `addQueries` (unary hydrates) | DELETED | -32601 pinned by `rpc_surface_test.go` |
| socket `main()` entry | STUB | prints "socket transport removed; use NAPI"; napi is the only transport |

## Engine/env knobs (Go)

| Knob | Default | Status |
|---|---|---|
| `GO_IVM_SOURCE_MODE` | DELETED | table mode is the only mode; env knob removed |
| `GO_IVM_LAZY_ADVANCE` | DELETED | The lazy leaf (`fetchDuringPushStream`) is the unconditional advance-time dispatch; the eager escape hatch was removed together with `FetchRequest.Limit` — early termination is propagated pull-stop, exactly TS's lazy-generator semantics |
| `GO_IVM_PARALLEL_ADVANCE` | **false (SERIAL)** | Advance push is SERIAL in prod: `Dockerfile.go-ivm` bakes `ENV GO_IVM_PARALLEL_ADVANCE=false`, and the code default is off (`os.Getenv(...) == "true"`). Per-source-change fanout across pipeline groups all fetch on the ONE prev-tx conn, so PAR>1 only contends on `s.mu` + SQLite's conn mutex — counterproductive for fetch-heavy advances. The parallel `fanOut` path is shadow/experiment only. This matches TS (single-threaded advance). |
| `GO_IVM_LAZY_HYDRATE` | DELETED | Gated only the dead `computeCmax` (never called); the operator tree streams lazily end-to-end and pool sizing always used `ConservativeHydrateCmax` |
| `GO_IVM_HYDRATE_PARALLELISM` / `GO_IVM_ADVANCE_PARALLELISM` | 4 / (moot) | Split parallelism knobs; legacy `GO_IVM_PARALLELISM` is the fallback. Only HYDRATE is live: hydrate uses the reader pool (multiple conns) so lanes scale. `GO_IVM_ADVANCE_PARALLELISM` is a NO-OP while `GO_IVM_PARALLEL_ADVANCE=false` (the fanout master switch gates it) — advance stays serial regardless of the worker count. Code default for the worker count is 1. |
| `GO_IVM_HYDRATE_LANES` / `GO_IVM_HYDRATE_READERS` | 4 / 8 | Hydrate facet overrides; readers default to `2×GO_IVM_HYDRATE_PARALLELISM` |
| `GO_IVM_HYDRATE_CHUNK_SIZE` / `ADVANCE_CHUNK_SIZE` / `CHUNK_SIZE` | 100 (Docker: 10000) | PROD tuning |
| `GO_IVM_WARM_HYDRATE_POOL` | true | PROD |
| `GO_IVM_ADVANCE_BUDGET_MS` | 60000 | PROD belt-and-braces WAL-pin bound (WALL clock — the economics abort is CPU; this backstop covers waiting-not-working advances); typed abort → `advancement-timeout` reset (never teardown) |
| `tablesource.PoolAcquireTimeout` | 5s | PROD bound on reader-pool BUILDS (BEGIN/converge stalls) + init's presence probe (still on the shared read pool). Builds open RAW driver conns (Option B) — they no longer queue on the shared pool, so build-vs-probe starvation is structurally gone; probe exhaustion → fast init rpcError. Sustained saturation logs `replica read pool SATURATED` every 10s |
| `engine.PipelineReaderTripwire` | 120s zero-progress | PROD wedge alarm (Option B; PROGRESS-based): one reader per hydrate pipeline, acquired at start while holding nothing; queueing past pool width K = max(READERS, LANES) is normal admission — and can legitimately outlast any fixed wall bound (the production pull path runs timeoutMs=0, consumer-driven). The deadline RESETS on every pool-wide reader release; it fires only after 120s of ZERO releases — leaked reader / never-released parked producer → loud PANIC → batch rpcError. A fixed deadline panicked whole healthy wide batches. NEVER a fallback |
| `GO_IVM_WEDGE_WATCHDOG_SEC` | 90s | PROD wedge watchdog (wedgewatch.go): any CG handler running past this emits `[GO-IVM][WEDGE]` (cg/method/elapsed/queued/queueWait/reqID) per scan tick + ONE `[GO-IVM][WEDGE-STACKS]` all-goroutine dump per wedge + `[GO-IVM][WEDGE-CLEAR]` (total elapsed, err) when it finally returns. 90s < the TS 120s RPC deadline so the dump lands while the client still waits. Cannot be disabled, only tuned. The ART gate hard-blocks on `[GO-IVM][WEDGE]`. `[SLOW]` is unconditional on traceparent and carries cg= |
| `GO_IVM_DELIVER_TIMEOUT_SEC` | 55s | PROD tripwire on a row-plane PARK against a FULL addon TSFN queue. ABI v5: reduced from 150s to 55s  to stay within the 60s advance budget. Records that find the queue full STAGE (owned copy; the producer keeps producing) and ship as ONE kind-5 batch on recovery; parks survive only at the stage hard bound (256 records / 1MB) and frame delivery, and wake EVENT-DRIVEN via `goivm_queue_drained` (addon low-water signal at queue_max/2). Unparks: drain signal, pull-gate cancel, group teardown; the deadline fires past ALL of those → `[GO-IVM][DELIVER-TIMEOUT]` + stream failure. `[GO-IVM][PERF-NAPI] stalls/timeouts/staged/batchFlushes` per window |
| `GO_IVM_MAX_IDLE_CONNS` | = MAX_OPEN (self-clamped) | PROD keep-warm pools: unset defaults to MAX_OPEN, explicit values clamp ≤ MAX_OPEN — idle-cap churn was a uniform steady-latency tax |
| reader-shell cache | cap 32 / TTL 5m | PROD (replaces the build-slot gate): pool builds provision raw conns from a worker-wide LIFO of idle shells (conn + prepared-stmt cache survive pool GENERATIONS) instead of K fresh SQLite opens per build. `GO_IVM_READER_CACHE_CAP` (0 disables) / `GO_IVM_READER_CACHE_TTL_SEC` (reaper tick closes idle shells). Cached shells hold NO tx (pin nothing; coread TXN_NONE-safe — arm→begin→disarm is atomic in the fork). GOMEMLIMIT note: standing footprint = cap × per-conn stmt-cache C-heap (bounded by `stmtCachePerConnCap`); pager page cache does NOT persist across frame changes (coread forces `*pChanged=1`), so don't budget for it. `[PERF-POOL] reader-shell cache hits/misses` per window |
| `[GO-IVM][POOL-SERIAL]` | failure marker | PROD: serial hydrate is failure-only now — the build-slot skip-to-serial (the last routine-serial path) was deleted with its cause. Marker fires on coread-capture error / pool-build error / frame mismatch (cg= path= reason= err=). Soak bar: count MUST be 0; any nonzero is a failure, same status as `[GO-IVM][WEDGE]`. Config-serial (feature off, K≤1) stays silent |
| `GO_IVM_MAX_DIFF_CHANGES` | DELETED | unary advanceToHead removed |
| `GO_IVM_PULL_IDLE_TIMEOUT_SEC`, `GO_IVM_REAPER_IDLE_SEC` (default 900s/15min), `GO_IVM_REAPER_INTERVAL_SEC` (default 300s/5min), `CONN_MAX_IDLE_SEC`, `COLD_POOL_TTL_SEC`, `TAKE_STATE_CACHE_MAX`, `GOGC`/`GOMEMLIMIT` (+`GO_IVM_` twins), `PPROF_ADDR`, `OTEL_*`, `APP_ID`, `REPLICA_DB_PATH`/`ZERO_REPLICA_FILE` | — | PROD ops/tuning |
| `GO_IVM_BENCH` | — | test-only (bench gates in `_test.go`) |

## TS side (mono go-sidecar)

| Surface | Status |
|---|---|
| `GoIVMClient`: init / addQueriesStream / addQueriesStreamPull / addQueryStream / removeQuery / advanceToHeadStream / destroy / ping / version | PROD |
| `computeBoundTimeoutMs` | PROD — compute-bound RPCs have NO wall-clock timeout in-process |
| `GoIVMClient.loadRows` + backend init rows loop | DELETED | table mode reads from replica; no rows shipped |
| `GoIVMClient.advanceToHead` (unary) | DELETED |
| `GoIVMClient.advanceStream` | DELETED |
| positional decode (`extractChanges`) | PROD — the ONLY streaming decode; legacy `changes` fallback deleted (removed at rev 9) |
| `sidecar-manager` socket branch + spawn-env | DELETED | napi is the only transport |
| pipeline-driver shadow/drift audit | DELETED | shadow block + drift audit + SQL oracle + heal removed (~2000 lines) |
| classifier dispositions | PROD: advance-aborted→`advancement-timeout` reset; scalar-stale→`ResetPipelinesSignal` (-32105); sidecar→reset; protocol/stale-epoch/data-error/unclassified→rethrow (teardown); clean-retryable→in-place retry; drift→teardown (plain panic → -32000 unclassified) |

## Faithfulness-review scoping

Review = PROD rows only, under the declared divergence budget: parallel
compute (hydrate lanes, pull-window pipelining) is
the ONLY intended behavioral divergence from TS; everything else —
including SERIAL advance push (`GO_IVM_PARALLEL_ADVANCE=false`, TS-faithful) —
values, ordering per query, coercions, failure dispositions, abort
economics — is required to be TS-identical and is pinned by parity tests
(realtext/coercion fuzz, overlay contract, abort message byte-shape,
drop-path decision tables) plus the production error paths.
