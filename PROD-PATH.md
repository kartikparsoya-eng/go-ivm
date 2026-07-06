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
| `advanceToHeadStream` | PROD | drive mode; lazy changelog feed (D9); TS-economic abort + env budget both map to `rpcCodeAdvanceAborted` → `advancement-timeout` reset |
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
| `GO_IVM_LAZY_ADVANCE` | true | PROD lazy cursor feed; `false` = eager fallback (NON-DEFAULT marker at startup) |
| `GO_IVM_PARALLEL_ADVANCE` | true | PROD parallel fanout; `false` = serial fallback (marker) |
| `GO_IVM_LAZY_HYDRATE` | false | Phase-2 experiment (lazy operator streaming, Cmax>1); ON = marker. The PROD hydrate is the eager/compat-shim path with Cmax=1 |
| `GO_IVM_PARALLELISM` / `HYDRATE_LANES` / `HYDRATE_READERS` | 4 / 4 / 8 | PROD tuning |
| `GO_IVM_HYDRATE_CHUNK_SIZE` / `ADVANCE_CHUNK_SIZE` / `CHUNK_SIZE` | 100 (Docker: 10000) | PROD tuning |
| `GO_IVM_WARM_HYDRATE_POOL` | true | PROD |
| `GO_IVM_ADVANCE_BUDGET_MS` | 60000 | PROD belt-and-braces WAL-pin bound; typed abort → `advancement-timeout` reset (never teardown) |
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
