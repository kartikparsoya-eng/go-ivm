# PROD-PATH — what executes in production, and what doesn't

Scope map for the Go engine surface (this repo + mono's
`packages/zero-cache/src/services/view-syncer/go-sidecar/`). Purpose: the
TS-faithfulness review covers exactly the PROD column; everything else is
either a marked rollback knob, the shadow/validation harness, or scheduled
for removal. Generated during the RPC-surface cleanup; verify against source
before acting on it — markers below make drift from this map greppable in
deployment logs.

Production configuration = the config DEFAULTS (mono `zero-config.ts`
`goSidecar.*`: enabled, napiLibPath, pullWindow; protocolRev=10) + the
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
| `advanceToHeadStream` | PROD | drive mode; lazy changelog feed (D9); TS-economic abort (budget = per-thread CPU via `internal/procclock`, the TS processing-lap analog — NOT wall; 2026-07-06 ART fix) + env budget both map to `rpcCodeAdvanceAborted` → `advancement-timeout` reset |
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
| `GO_IVM_LAZY_ADVANCE` | DELETED (2026-07-07) | The lazy leaf (`fetchDuringPushStream`) is the unconditional advance-time dispatch; the eager escape hatch was removed together with `FetchRequest.Limit` — early termination is propagated pull-stop, exactly TS's lazy-generator semantics |
| `GO_IVM_PARALLEL_ADVANCE` | true | PROD parallel fanout; `false` = serial fallback (marker) |
| `GO_IVM_LAZY_HYDRATE` | DELETED (2026-07-07) | Gated only the dead `computeCmax` (never called); the operator tree streams lazily end-to-end and pool sizing always used `ConservativeHydrateCmax` |
| `GO_IVM_PARALLELISM` / `HYDRATE_LANES` / `HYDRATE_READERS` | 4 / 4 / 8 | PROD tuning |
| `GO_IVM_HYDRATE_CHUNK_SIZE` / `ADVANCE_CHUNK_SIZE` / `CHUNK_SIZE` | 100 (Docker: 10000) | PROD tuning |
| `GO_IVM_WARM_HYDRATE_POOL` | true | PROD |
| `GO_IVM_ADVANCE_BUDGET_MS` | 60000 | PROD belt-and-braces WAL-pin bound (WALL clock — the economics abort is CPU; this backstop covers waiting-not-working advances); typed abort → `advancement-timeout` reset (never teardown) |
| `tablesource.PoolAcquireTimeout` | 5s | PROD bound on reader-pool BUILDS (BEGIN/converge stalls) + init's presence probe (still on the shared read pool). Builds open RAW driver conns (Option B, 2026-07-08) — they no longer queue on the shared pool, so build-vs-probe starvation is structurally gone; probe exhaustion → fast init rpcError. Sustained saturation logs `replica read pool SATURATED` every 10s |
| `engine.PipelineReaderTripwire` | 120s zero-progress | PROD wedge alarm (Option B; PROGRESS-based since F2, 2026-07-10): one reader per hydrate pipeline, acquired at start while holding nothing; queueing past pool width K = max(READERS, LANES) is normal admission — and can legitimately outlast any fixed wall bound (the production pull path runs timeoutMs=0, consumer-driven). The deadline RESETS on every pool-wide reader release; it fires only after 120s of ZERO releases — leaked reader / never-released parked producer → loud PANIC → batch rpcError. Pre-F2 the fixed deadline panicked whole healthy wide batches → client re-hydrated the same width → self-inflicted hydrate storm. NEVER a fallback |
| `GO_IVM_WEDGE_WATCHDOG_SEC` | 90s | PROD wedge watchdog (wedgewatch.go, 7fbeed43 G13 forensics): any CG handler running past this emits `[GO-IVM][WEDGE]` (cg/method/elapsed/queued/queueWait/reqID) per scan tick + ONE `[GO-IVM][WEDGE-STACKS]` all-goroutine dump per incident + `[GO-IVM][WEDGE-CLEAR]` (total elapsed, err) when it finally returns. 90s < the TS 120s RPC deadline so the dump lands while the client still waits. Cannot be disabled, only tuned. The ART gate hard-blocks on `[GO-IVM][WEDGE]`. `[SLOW]` is now unconditional on traceparent (the gate that hid a ~140s silent return) and carries cg= |
| `GO_IVM_DELIVER_TIMEOUT_SEC` | 150s | PROD tripwire on a row-plane PARK against a FULL addon TSFN queue. ABI v5 (2026-07-10, the v4 latency-tax fix — 19,242 sleep-poll parks/20min with p50 regression): records that find the queue full now STAGE (owned copy; the producer keeps producing) and ship as ONE kind-5 batch on recovery; parks survive only at the stage hard bound (256 records / 1MB — memory backstop) and frame delivery, and wake EVENT-DRIVEN via `goivm_queue_drained` (addon low-water signal at queue_max/2) — the 10ms tick bounds cancellation detection only. Unparks: drain signal, pull-gate cancel (≤120s), group teardown; the deadline fires past ALL of those → `[GO-IVM][DELIVER-TIMEOUT]` + stream failure (hydrate: consumer-refusal unwind; advance: panic → -32000 → CG teardown). WAL-pin note: an advance parked here holds its prev-tx pin up to 150s > the 60s advance budget (budget checks are pre-emit only) — accepted, cancellable, documented. `[GO-IVM][PERF-NAPI] stalls/timeouts/staged/batchFlushes` per window: staged tracks JS-loop busyness (benign coalescing), stalls = genuine parks, timeouts = incidents. Pump control frames park deadline-free but drain-woken/escapable |
| `GO_IVM_MAX_IDLE_CONNS` | = MAX_OPEN (self-clamped) | PROD keep-warm pools (2026-07-07): unset defaults to MAX_OPEN, explicit values clamp ≤ MAX_OPEN — idle-cap churn (47k reopens/soak) was a uniform steady-latency tax |
| reader-shell cache | cap 32 / TTL 5m | PROD (2026-07-09, replaces the build-slot gate): pool builds provision raw conns from a worker-wide LIFO of idle shells (conn + prepared-stmt cache survive pool GENERATIONS) instead of K fresh SQLite opens per build. `GO_IVM_READER_CACHE_CAP` (0 disables) / `GO_IVM_READER_CACHE_TTL_SEC` (reaper tick closes idle shells). Cached shells hold NO tx (pin nothing; coread TXN_NONE-safe — arm→begin→disarm is atomic in the fork). GOMEMLIMIT note: standing footprint = cap × per-conn stmt-cache C-heap (bounded by `stmtCachePerConnCap`); pager page cache does NOT persist across frame changes (coread forces `*pChanged=1`), so don't budget for it. `[PERF-POOL] reader-shell cache hits/misses` per window |
| `[GO-IVM][POOL-SERIAL]` | incident marker | PROD (2026-07-09): serial hydrate is FAILURE-ONLY now — the build-slot skip-to-serial (the last routine-serial path) was deleted with its cause. Marker fires on coread-capture error / pool-build error / frame mismatch (cg= path= reason= err=). Soak bar: count MUST be 0; any nonzero is an incident, same status as `[GO-IVM][WEDGE]`. Config-serial (feature off, K≤1) stays silent |
| `GO_IVM_MAX_DIFF_CHANGES` | DELETED | unary advanceToHead removed |
| `GO_IVM_PULL_IDLE_TIMEOUT_SEC`, `REAPER_*`, `CONN_MAX_IDLE_SEC`, `COLD_POOL_TTL_SEC`, `TAKE_STATE_CACHE_MAX`, `GOGC`/`GOMEMLIMIT` (+`GO_IVM_` twins), `PPROF_ADDR`, `OTEL_*`, `APP_ID`, `REPLICA_DB_PATH`/`ZERO_REPLICA_FILE` | — | PROD ops/tuning |
| `GO_IVM_BENCH` | — | test-only (bench gates in `_test.go`) |

## TS side (mono go-sidecar)

| Surface | Status |
|---|---|
| `GoIVMClient`: init / addQueriesStream / addQueriesStreamPull / addQueryStream / removeQuery / advanceToHeadStream / destroy / ping / version | PROD |
| `computeBoundTimeoutMs` | PROD — compute-bound RPCs have NO wall-clock timeout in-process; socket keeps legacy timeouts |
| `GoIVMClient.loadRows` + backend init rows loop | DELETED | table mode reads from replica; no rows shipped |
| `GoIVMClient.advanceToHead` (unary) | DELETED |
| `GoIVMClient.advanceStream` | DELETED |
| positional decode (`extractChanges`) | PROD — the ONLY streaming decode; legacy `changes` fallback deleted (dead at rev 9) |
| `sidecar-manager` socket branch + spawn-env | DELETED | napi is the only transport |
| pipeline-driver shadow/drift audit | DELETED | shadow block + drift audit + SQL oracle + heal removed (~2000 lines) |
| classifier dispositions | PROD: advance-aborted→`advancement-timeout` reset; scalar-stale→`ResetPipelinesSignal` (-32105); sidecar→reset; protocol/stale-epoch/data-error/unclassified→rethrow (teardown); clean-retryable→in-place retry; drift→teardown (plain panic → -32000 unclassified) |

## Faithfulness-review scoping

Review = PROD rows only, under the declared divergence budget: parallel
compute (hydrate lanes, parallel advance fanout, pull-window pipelining) is
the ONLY intended behavioral divergence from TS; everything else —
values, ordering per query, coercions, failure dispositions, abort
economics — is required to be TS-identical and is pinned by parity tests
(realtext/coercion fuzz, overlay contract, abort message byte-shape,
drop-path decision tables) plus the production drift audit.
