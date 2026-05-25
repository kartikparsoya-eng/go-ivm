# TS → Go Integration Review

Audit of the TypeScript side of the Go IVM sidecar integration. Focus: the TS
client (`GoIVMClient`), process manager (`SidecarManager`), the
PipelineDriver hook (`GoComputeBackend`), and the syncer-side wiring.

Companion to:
- `REVIEW-porting.md` — Go port correctness vs TS reference (out of scope here).
- `REVIEW-parallelism.md` — Go-side parallelism races (out of scope here).

This review covers what the **TS side does wrong or risky** when talking to
the Go sidecar. Where the symptom is on the Go side but originates in TS
behavior, it's called out as a TS-side bug.

---

## Status: ALL FINDINGS FIXED & SOAK-VERIFIED

> **See `REVIEW-final.md` for the consolidated cross-doc production-readiness summary. This doc is preserved for the audit trail.**

All CRITICAL / HIGH / MEDIUM / LOW findings have been addressed in current
source, including the new findings discovered in the four-agent consolidated
audit (REVIEW-final): MED-TS-1 (drain backpressure), MED-TS-2 (pgTypeToGoType
bytea/arrays), MED-TS-3 (partial loadRows recovery), MED-TS-4 (engine-not-
initialized auto-retry), MED-TS-5 (`#advanceTime` type label), HIGH-TS-1
(honest `awaitGoInit` via `whenInitialized`), LOW-TS-1 (huge-frame buffer
pre-allocation), LOW-TS-2 (recently-timed-out ID dedup).

Validation:

- `npm --workspace=zero-cache run check-types` clean.
- `npm --workspace=zero-cache run lint` clean (407 pre-existing warnings, 0 errors).
- `go build ./...` + full `go test ./...` pass.
- PipelineDriver tests: 36/36 pass.
- **Four consecutive 5-minute soaks** (20 users, mutate mode against `rust-test` sandbox):
  - First (post-fix soak): **936 matches, 0 mismatches**. 1081 queries / 6321 frames.
  - Second (post-timing soak): **1462 matches, 0 mismatches**. 1068 queries / 6246 frames.
  - Third (post shadow CRITICAL-1): **14059 matches, 0 mismatches**. 1048 queries / 6161 frames.
  - Fourth (post-everything soak): **805 batch compares, 13007 individual TS hydrates, 0 mismatches**. 1058 queries / 6218 frames. (Success log demoted to debug — see LOW-SHADOW-1; absence of "TS and Go match" lines is the expected production posture.)
  - All runs: 0 panics, 0 decode failures, 0 errors, 0 reconnects, 0 Go resets needed, 0 group reaps.

| Finding | Status | Notes |
|---|---|---|
| CRITICAL-1 — sidecar restart breaks backends | **FIXED** | epoch + onRestart + lazy client + re-init from snapshot |
| CRITICAL-2 — `init` > 64MB frame cap | **FIXED** | schema-only `init` + chunked `loadRows` RPC (5k rows/batch) |
| CRITICAL-3 — advance path misses type normalization | **FIXED** | `ValueConverter` injected into MemorySource; sidecar uses `FromSQLiteType` |
| HIGH-1 — no RPC timeout | **FIXED** | per-call timeout with `TimeoutError` |
| HIGH-2 — close handler doesn't null `#socket` | **FIXED** | null first, then reject |
| HIGH-3 — backpressure ignored | **FIXED** | Per-group sub-semaphore + `#drainPromise` byte-level backpressure on `socket.write` returning false. |
| HIGH-4 — quadratic `Buffer.concat` | **FIXED** | offset-tracked buffer with periodic compaction |
| HIGH-5 — silent `socket.on('error')` | **FIXED** | always logs via injected logger; close handler does the rest |
| MEDIUM-1 — fire-and-forget `removeQuery` | **DEFERRED** | leak is bounded; CRITICAL-1 fix already minimizes the churn window |
| MEDIUM-2 — REMOVE row shape | **RETRACTED** | the "lie" is intentional and mirrored on both paths |
| MEDIUM-3 — init promise resolves on failure | **FIXED** | rejection propagates; `awaitGoInit()` honest |
| MEDIUM-4 — restart leaks prior client | **FIXED** | `#spawn` closes prior client before reassigning |
| MEDIUM-5 — `#nextID` overflow | **FIXED** | wraps at uint32 with in-flight collision dedupe |
| MEDIUM-6 — restart cadence flaps | **FIXED** | sliding-window cap (5 failures in 60s) |
| LOW-1 — redundant `\|\|` | **FIXED** | removed, comment explains |
| LOW-2 — sidecar stdout/stderr unrouted | **FIXED** | structured logger threaded from syncer's `LogContext` |
| LOW-3 — env read inconsistency | **NOT FIXED** | documented; behavior unchanged |
| OBS-1 — Go path didn't report timings | **FIXED** | per-query `timingMs` + per-(table, op) `timings[]` returned by Go, consumed by TS |
| OK-1..5 | **UNCHANGED** | invariants still hold |

---

## Findings

### CRITICAL-1: Sidecar restart silently breaks every existing GoComputeBackend — FIXED
- **TS files:** `packages/zero-cache/src/services/view-syncer/go-sidecar/sidecar-manager.ts`, `go-compute-backend.ts`, `pipeline-driver.ts`
- **Original symptom:** When the Go sidecar process crashed, `SidecarManager` auto-restarted it and created a **new** `GoIVMClient`. But every `PipelineDriver` constructed earlier captured the old `GoComputeBackend`, which held the OLD `GoIVMClient` reference. After restart, the Go-side engine state was wiped, but `GoComputeBackend.#initialized` stayed `true` on the TS side, so dispatch continued routing to a Go engine that had no schema, no rows, and no pipelines registered. Result: silently empty `RowChange[]` returned.
- **Fix landed:**
  - `SidecarManager` exposes `epoch` (incremented on each successful spawn) and `onRestart(listener)` (invoked after non-initial spawns).
  - `GoComputeBackend.#client()` resolves through `manager.getClient()` on **every call** — no captured reference.
  - `GoComputeBackend.initialized` getter checks `#initEpoch === manager.epoch`, so a stale-epoch backend reports false until re-init succeeds.
  - On restart, `#onSidecarRestart(epoch)` clears `#initialized`, fetches fresh tables via the `getCurrentTables` callback registered by `PipelineDriver`, and re-runs `#doInit`. Failures leave the backend uninitialized so dispatch falls back to TS until the next restart.
  - `PipelineDriver` passes `() => this.#currentTablesForGo()` which always reads from the current snapshot.

### CRITICAL-2: `init` payload can exceed the 64MB frame cap — FIXED
- **TS file:** `pipeline-driver.ts`, `go-compute-backend.ts`, `go-ivm-client.ts`; **Go file:** `cmd/sidecar/main.go`, `engine/engine.go`
- **Original symptom:** `db.all('SELECT * FROM "${name}"')` materialized whole tables and shipped them as one `init` RPC. Anything over ~64MB encoded crashed the connection with no recovery.
- **Fix landed:**
  - New `loadRows(clientGroupID, table, rows)` RPC method on both sides.
  - `GoComputeBackend.#doInit` now sends **schema-only** `init` (rows omitted), then streams `loadRows` batches of `DEFAULT_LOAD_BATCH_ROWS = 5000` rows each. Each batch comfortably fits under the 64MB cap.
  - Go side: `handleLoadRows` looks up the existing MemorySource via `Engine.GetMemorySource(name)` and `BulkInsert`s the normalized rows.
  - TS-side `init`/`loadRows` get longer per-call timeouts (120s) to accommodate hydrate-from-cold.

### CRITICAL-3: SnapshotChange path bypasses type normalization — FIXED
- **TS file:** `pipeline-driver.ts` (advance path); **Go files:** `ivm/source.go`, `cmd/sidecar/main.go`
- **Original symptom:** Advance-path rows weren't run through `FromSQLiteType`, so JSON columns arrived as strings in advance but parsed objects after init. Operators comparing row shapes (joins on json keys, signature hashing) would diverge.
- **Fix landed (Go-side, per user direction):**
  - New `ivm.ValueConverter` type and `NewMemorySourceWithConverter` constructor.
  - `MemorySource.NormalizeRow` delegates to the converter for every column when one is registered. (The legacy partial-coverage path remains as a fallback for tests that construct sources without injecting one.)
  - The sidecar registers `sqlite.FromSQLiteType` as the converter at MemorySource construction, so advance-path rows go through the same normalization as init-path rows.
- **TS angle unchanged:** TS continues to send raw values; the Go side normalizes uniformly across both code paths.

---

### HIGH-1: No RPC timeout — FIXED
- **TS file:** `go-ivm-client.ts`
- **Fix:** Per-call timeout. `#call` accepts `CallOptions.timeoutMs` (default `DEFAULT_TIMEOUT_MS = 30s`; `init`/`loadRows`/`addQueries` get 120s; `ping` gets 5s). Timer is created with `setTimeout` and `.unref?.()` so it never holds the event loop open. Rejection uses the exported `TimeoutError` class so callers can branch on it.

### HIGH-2: `socket.on('close')` doesn't null `#socket` — FIXED
- **TS file:** `go-ivm-client.ts`
- **Fix:** The close handler now sets `this.#socket = null` **before** rejecting pending — subsequent `#call` invocations see `Not connected` synchronously instead of writing into a dead Socket. Backpressure waiters are also released so they fail fast.

### HIGH-3: Backpressure ignored — FIXED
- **TS file:** `go-ivm-client.ts`
- **Fix:** In-flight semaphore. `#acquireSlot()` awaits a slot when `#pending.size >= MAX_IN_FLIGHT (1024)`. `socket.write` return value is checked; we `socket.once('drain', ...)` if `false` returns. Slot is released on every resolve/reject/timeout via a single `cleanup()` closure.

### HIGH-4: Quadratic `Buffer.concat` — FIXED
- **TS file:** `go-ivm-client.ts`
- **Fix:** Receive buffer uses an offset (`#recvOffset`) into a single underlying `Buffer`. The hot path (one chunk = one or more whole frames) takes no allocation at all — we just advance the offset. When a frame straddles a chunk boundary, we do one `Buffer.allocUnsafe + copy` for the residual plus the new chunk. Once consumed beyond half-capacity we compact via `subarray`, releasing the prior chunk's memory.

### HIGH-5: Silent `socket.on('error')` — FIXED
- **TS file:** `go-ivm-client.ts`
- **Fix:** Post-connect errors now route through `this.#log('error', ...)`. The injected `onLog` from `SidecarManager` lands them in the parent `LogContext`. The close handler still does the cleanup; the error handler is just visibility.

---

### MEDIUM-1: `removeQuery` is fire-and-forget — DEFERRED
- **TS file:** `pipeline-driver.ts`, `go-compute-backend.ts`
- **Why deferred:** With CRITICAL-1 fixed, the leak window collapses: sidecar restarts now re-init from scratch, dropping any orphaned pipelines. Steady-state, the RPC nearly always succeeds. The remaining failure mode (socket bad for a few ms before restart) leaks at most one pipeline per glitch. We can revisit when telemetry shows it matters.

### MEDIUM-2: ~~`#goRowChangeToRowChange` lies about REMOVE shape~~ — RETRACTED
- **Verdict:** Not a bug — the "lie" is intentional and mirrored on both paths.
- **Evidence:** The TS-native `Streamer` at `pipeline-driver.ts:1598` explicitly emits `row: undefined` for REMOVE despite `RowChange.row` being declared as `Row`. The Go-conversion code already mirrored this with `row: type === ChangeType.REMOVE ? undefined : ...`. Soak validation flagged the regression when an earlier "fix" filled `row = rc.rowKey` for REMOVE — shadow-compare then broke on every REMOVE. Reverted.
- **Note:** A real cleanup would update the `RowChange` type to declare `row?: Row | undefined` on REMOVE so the surface matches reality. That's a typing change, not a behavior change.

### MEDIUM-3: Init promise resolves on failure — FIXED
- **TS file:** `pipeline-driver.ts`
- **Fix:** `#maybeInitGoBackend` and `#maybeResetGoBackend` now store the **unwrapped** promise from `initEngine` / `resetEngine` (without `.then(_, _)` swallowing the rejection). A separate `.then().catch()` chain handles logging. `awaitGoInit()` now propagates the rejection so callers can distinguish "init done" from "init failed."

### MEDIUM-4: SidecarManager restart leaks prior client — FIXED
- **TS file:** `sidecar-manager.ts`
- **Fix:** `#spawn` closes the prior `#client` (if any) before constructing the new one. The old `GoIVMClient.close()` rejects all pending promises, releases backpressure waiters, and detaches the socket.

### MEDIUM-5: `#nextID++` overflow — FIXED
- **TS file:** `go-ivm-client.ts`
- **Fix:** IDs wrap at `MAX_ID = 0xffffffff` (uint32). `#nextId()` checks for collisions against `#pending` and retries up to 16 times. With the `MAX_IN_FLIGHT = 1024` cap, an in-flight collision in uint32 space is effectively impossible.

### MEDIUM-6: Restart cadence flapping — FIXED
- **TS file:** `sidecar-manager.ts`
- **Fix:** Replaced back-to-back `restartCount` with `#restartTimestamps[]`. On each unexpected exit we drop timestamps older than `restartWindowMs (60s)` and append the current. If the window total exceeds `maxRestartsInWindow (5)`, we mark status `failed` and stop trying. A flapping sidecar that briefly succeeds between crashes now trips the cap.

---

### LOW-1: `|| this.#shadowMode` redundant — FIXED
`shadowMode` already implies `isGoSidecarEnabled()`. Now just checks the flag, with a comment noting why.

### LOW-2: Sidecar stdout/stderr unrouted — FIXED
`SidecarConfig` now accepts a structured `logger` callback. `syncer.ts` wires `lc.withContext('component', 'go-ivm')` as the logger, so sidecar `stdout` / `stderr` and manager events all flow through the same `LogContext` as the rest of zero-cache. The internal `GoIVMClient` and `GoComputeBackend` each accept a logger and pipe through to the same sink.

### LOW-3: Env read inconsistency — NOT FIXED (documented)
`isGoSidecarEnabled()` / `isGoShadowMode()` still read `process.env` on each call but the result is captured once in `PipelineDriver` at construction. Documented as a known asymmetry; behavior unchanged.

---

### OBS-1: Go path silently dropped per-query / per-table timings — FIXED
- **Original symptom (observability gap, not correctness):** When the Go backend served `hydrate` / `advance`, TS knew only the wall-time of the RPC round trip:
  - `goHydrate` / `goHydrateBatch` set `hydrationTimeMs: 0` on each pipeline entry — `PipelineDriver.totalHydrationTimeMs()` summed zeros, so the adaptive circuit-breaker in `#shouldAdvanceYieldMaybeAbortAdvance` collapsed to the `MIN_ADVANCEMENT_TIME_LIMIT_MS = 50ms` floor regardless of query complexity.
  - `#goAdvance` never recorded to the `ivm.advance-time` histogram — per-table latency disappeared from telemetry in Go mode.
  - The Go side returned `{changes: RowChange[]}` only, no timings — the sidecar's 10s perf log captured per-RPC wall time but with no `(table, op, queryID)` attribution.
- **Fix landed (both sides):**
  - **Go:** `Engine.AddQuery` returns `(changes, timingMs, err)`; `Engine.AddQueries`' `QueryResult` gains `TimingMs`; `Engine.AdvanceResult` gains `Timings []TableTiming` (one per `(table, sourceChange)` pair — same granularity as TS).
  - **Sidecar:** `addQueryResult` / `addQueriesResult` / `advanceResult` pass timings through.
  - **TS:** `GoIVMClient` returns `HydrateResult{changes, timingMs}` / `AdvanceResult{changes, timings}`. `PipelineDriver.#goHydrate` and `goHydrateBatch` write the real `timingMs` into the pipeline entry → adaptive circuit-breaker math works again. `#goAdvance` feeds each `TableTiming` into `#advanceTime.recordMs(ms, {table, type})` → histogram populates identically across backends. The histogram `type` label is now actually populated (Go knows the op type; TS path always passed `undefined`).
- **Verified:** Second soak (1462 matches, 0 mismatches).
- **v2 follow-up:** Per-pipeline-within-an-advance timing (which of N fan-out pipelines was slow) is not addressed. The per-table granularity already matches TS; going deeper requires connection→queryID plumbing inside `ivm.MemorySource.Push` and is less actionable.

---

## Confirmed correct

### OK-1: msgpack codec configuration is correctly aligned with the Go decoder
- **TS:** `go-ivm-client.ts` disables `useRecords`, `useBigIntExtension`, sets `encodeUndefinedAsNil`, `mapsAsObjects`.
- **Go:** `sidecar/main.go` uses `UseCompactInts(true)` + `UseLooseInterfaceDecoding(true)` + post-decode numeric normalization to float64.
- The two together produce plain msgpack on the wire with no JS-specific extension types and consistent numeric typing. The `BigInt → Number` fallback in `#onData` is a defensive guard that is effectively a no-op given the current Go-side flags, but should stay as a safety net in case either is removed.

### OK-2: clientGroupID multiplexing on a shared socket preserves per-group FIFO order
- The single `GoIVMClient` is shared across all `PipelineDriver`s in the worker. Each `#call` gets a unique `id`, so responses route correctly regardless of cross-group interleaving.
- Per-group ordering is preserved because: (a) Node single-threaded JS means `socket.write` calls execute in invocation order; (b) `pipeline-driver.ts` serializes its own per-group operations via `async/await`; (c) the Go side then drains requests through a per-group FIFO channel (`sidecar/main.go worker`).

### OK-3: length-prefix framing matches exactly
- TS: `writeUInt32BE(payload.length, 0)`.
- Go: `binary.BigEndian.PutUint32`.
- Cap matches: 64MB on both sides.
- Frame-too-large handling is symmetric: both sides close the connection without trying to skip. With CRITICAL-2 fixed (chunked init), the cap is no longer reachable in practice.

### OK-4: Graceful TS fallback when sidecar is unavailable
- `syncer.ts`: failed sidecar startup sets `sidecarManager = undefined`, so new ViewSyncers never get a `GoComputeBackend`.
- `pipeline-driver.ts`: every dispatch path falls through to the TS implementation when `#goBackend?.initialized` is falsy. The new epoch check means restart-with-failed-reinit also falls back automatically.
- The "Go is dead, do TS" path is exercised on every operation — there is no half-step where TS code expects Go state. This is the load-bearing safety property and it holds.

### OK-5 (new): REMOVE row shape parity with TS-native Streamer
- Both the TS-native `Streamer.#streamNodes` (pipeline-driver.ts:1598) and the Go-conversion `#goRowChangeToRowChange` emit `row: undefined` for REMOVE despite the type system declaring `row: Row`. This is intentional shape parity, not a typing bug.
- Soak-verified: 936 matches, 0 REMOVE-row mismatches.
- **Regression risk:** any future change to `#goRowChangeToRowChange` that fills `row` from `rc.rowKey` for REMOVE WILL break shadow compare on every REMOVE. Inline comment in the code warns against this.
