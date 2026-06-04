# RPC wire-protocol + restart/error contract audit

Scope: TS client (`go-ivm-client.ts`, `sidecar-manager.ts`,
`go-compute-backend.ts`) ↔ Go sidecar (`cmd/sidecar/main.go`,
`ivm/drift.go`, `engine/engine.go`).

TS is canonical. Each finding cites file:line on both sides.

Cross-references: `go-ivm/REVIEW-final.md` (RESILIENCE-1..7,
CRITICAL-CROSS-1..2, MED-CROSS-1..6) and
`go-ivm/REVIEW-ts-integration.md`.

---

## Findings

### CHECKED-OK-1: Method-set parity
- **TS**: declares public methods `ping` (792), `version` (801),
  `init` (587), `loadRows` (605), `addQuery` (621), `addQueries` (638),
  `addQueriesStream` (674), `removeQuery` (694), `advance` (700),
  `advanceStream` (728), `destroy` (750), `refreshSnapshot` (762),
  `pipelineCount` (783) in
  `mono/packages/zero-cache/src/services/view-syncer/go-sidecar/go-ivm-client.ts`.
- **Go**: dispatch switch in `handleRequest`
  (`go-ivm/cmd/sidecar/main.go:915-952`) covers every name above and
  rejects unknown methods with `-32601 Method not found`.
- **Status**: every TS-side RPC name is present and handled on Go.
  No method has been added on one side without the other.

### CHECKED-OK-2: Framing — length-prefix + cap match
- **TS**: `MAX_FRAME_SIZE = 64 * 1024 * 1024`
  (`go-ivm-client.ts:358`); writes `frame.writeUInt32BE(payload.length, 0)`
  + `payload.copy(frame, 4)` (`go-ivm-client.ts:958-962`); reads via
  `readUInt32BE` (`go-ivm-client.ts:1004,1023`).
- **Go**: `maxFrameSize = 64 * 1024 * 1024`
  (`go-ivm/cmd/sidecar/main.go:42`); `writeFrame` puts a 4-byte BE prefix
  then payload (`main.go:215-221`); `readFrame` consumes the same
  (`main.go:194-211`).
- **Status**: byte-identical framing on both sides; cap is the same
  constant; both sides reject `len > MAX_FRAME_SIZE`.

### CHECKED-OK-3: Zero-length frame rejection (D7)
- **TS**: `if (len < 1)` destroys the socket and returns
  (`go-ivm-client.ts:1032-1045`).
- **Go**: `minFrameSize = 1`; `readFrame` returns
  `"frame too small"` error which closes the connection at the reader
  loop (`main.go:190-202,1847-1849`).
- **Status**: both sides treat `len=0` as protocol violation and tear
  down. The TS side stays alive on subsequent connections after the
  socket close (manager restart machinery); Go just disconnects the
  client.

### CHECKED-OK-4: Oversized-frame backpressure (H8)
- **TS**: rather than closing on `len > MAX_FRAME_SIZE`, switches to
  skip-mode (`#skipRemaining = len - bufferedBody`); other CGs on the
  same socket keep flowing; the orphan RPC times out via its own timer
  (`go-ivm-client.ts:1046-1073,977-987`).
- **Go**: `readFrame` returns the `"frame too large"` error which kills
  the entire connection at `handleConnection`'s reader loop
  (`main.go:203-205,1846-1849`).
- **Divergence**: asymmetric — TS preserves the connection on Go→TS
  oversize; Go tears down on TS→Go oversize.
- **Impact**: a single misbehaving TS→Go RPC closes the socket for
  every CG on that worker. SidecarManager's exit-handler will respawn,
  but a sustained oversize loop would trip the sliding-window failure
  cap and drop the worker to TS-only for the rest of the process
  lifetime. In practice, TS-side outbound frames are bounded by
  `loadBatchRows` (5_000) and msgpack overhead; an oversize would be a
  bug, not load. Asymmetry is intentional today (defense favors Go
  resilience because Go fails OPEN with skip).
- **Severity classification**: LOW (not actionable without a
  triggering bug; raising would mean a Go-side skip implementation
  that's strictly extra complexity).

### CHECKED-OK-5: RPC envelope — JSON-RPC 2.0 fields
- **TS request**: `{jsonrpc: '2.0', method, params?, id, traceparent?}`
  (`go-ivm-client.ts:338-349,939-944`).
- **Go request**: `RPCRequest{JSONRPC, Method, Params (RawMessage), ID,
  Traceparent}` (`main.go:411-420`).
- **TS response**: `{jsonrpc: '2.0', result?, error?: {code, message},
  id}` (`go-ivm-client.ts:351-356`).
- **Go response**: `RPCResponse{JSONRPC, Result?, Error?{Code, Message,
  Data?}, ID}` (`main.go:422-436`).
- **Status**: shape parity. Go's `Error.Data` is the only field TS
  doesn't reference in the request type, but TS reads `(resp.error as
  {data?: unknown}).data` for drift payload at line 1107 — handled.

### CHECKED-OK-6: BigInt vs Number for response id round-trip
- **Go**: `mpMarshal` configures `UseCompactInts(true)`
  (`main.go:51-60`). For request ids ≤ 2^32 this fits in `uint32` →
  msgpackr returns Number. For ids in 9-byte uint64 form msgpackr
  returns BigInt.
- **TS**: defensively coerces `typeof resp.id === 'bigint' ?
  Number(resp.id) : resp.id` (`go-ivm-client.ts:1093`) before lookup
  in `#pending` (which is keyed by Number).
- **Status**: round-trip is safe even if the Go encoder ever emits
  9-byte uint64 (e.g., if a future change drops UseCompactInts).

### CHECKED-OK-7: ping
- Request: no params.
- Response: `Result: "pong"` (`main.go:917`).
- TS: `ping()` calls `#call('ping', undefined, ...)` then casts result
  to string (`go-ivm-client.ts:792-794`). Default timeout 5_000ms.
- Match.

### CHECKED-OK-8: version
- Request: no params.
- Response: `Result: {version: string, protocolRev: int}`
  (`main.go:919-926`).
- TS: `version()` casts to `{version, protocolRev}` and
  `sidecar-manager.ts:43` enforces `EXPECTED_PROTOCOL_REV = 6`. Mismatch
  refuses to use the sidecar; missing impl (older sidecar) bumps
  `protocolFallbackCounter` and warns (`sidecar-manager.ts:449-473`).
- Match. Both sides at `protocolRev = 6` (`main.go:380`).

### CHECKED-OK-9: init epoch contract
- **Go**: `handleInit` (`main.go:983-1095`) bumps `group.initEpoch`
  under `group.mu` BEFORE creating the new engine
  (`main.go:1007-1010`), and returns `{status: "ok", initEpoch:
  currentEpoch}` (`main.go:1087-1094`).
- **TS**: `init()` asserts `typeof result.initEpoch === 'number'` or
  throws `"protocol mismatch"` (`go-ivm-client.ts:594-597`).
  `GoComputeBackend.#runInit` (`go-compute-backend.ts:539-550`) stashes
  the returned epoch into `#sidecarInitEpoch` BEFORE any loadRows.
- All subsequent mutating RPCs from TS forward `#sidecarInitEpoch` as
  the `initEpoch` field; Go's `checkInitEpoch` (`main.go:1121-1128`)
  rejects mismatches with `rpcCodeStaleInitEpoch = -32101`.
- TS converts that code to `StaleInitEpochError`
  (`go-ivm-client.ts:1125-1133`); PipelineDriver branches on
  `instanceof StaleInitEpochError` to terminate the stale view-syncer
  (`pipeline-driver.ts:2261-2268`).
- All five mutating handlers (`loadRows` 1162, `addQuery` 1230,
  `addQueries` 1303, `addQueriesStream` 1371, `advance` 1498,
  `advanceStream` 1576, `removeQuery` 1433, `refreshSnapshot` 1716)
  invoke `checkInitEpoch`.
- Match: epoch contract is uniformly enforced.

### HIGH-1: `pipelineCount` skips initEpoch check by design — verify nobody else copies the pattern
- **TS**: `pipelineCount()` sends ONLY `{clientGroupID}` — no
  `initEpoch` (`go-ivm-client.ts:783-789`). Documented: "we want this
  probe to succeed even after a stale view-syncer instance was torn
  down".
- **Go**: `pipelineCountParams` has only `ClientGroupID`
  (`main.go:1637-1640`); `handlePipelineCount` does NOT call
  `checkInitEpoch` (`main.go:1647-1671`).
- **Divergence**: this is an intentional epoch carve-out. It is the
  ONLY mutating-or-reading RPC that bypasses staleness — everything
  else (`refreshSnapshot`, `removeQuery`, `loadRows`, all advance/add
  variants) checks epoch.
- **Impact**: low — pipelineCount is a read-only probe used by the
  drift audit. A torn-down instance issuing it would still race with a
  fresh init on the same cgID, but the worst outcome is the audit
  briefly seeing the new instance's pipeline count rather than its own
  expected count — the audit treats mismatch as a freeze signal that
  retries.
- **Status**: documented in TS comments (`go-ivm-client.ts:780-782`)
  and Go comments (`main.go:1641-1646`). The contract holds today. If
  any future RPC inherits the "no-epoch probe" pattern, document
  explicitly there as well.
- **Severity**: HIGH for tracking visibility (NOT a defect — call out
  so the next contributor doesn't accidentally regress by adding an
  unchecked path elsewhere).

### CHECKED-OK-10: refreshSnapshot epoch + cgID + queue bypass
- TS: `refreshSnapshot(cgID, initEpoch, opts?)`
  (`go-ivm-client.ts:762-772`). Sends `{clientGroupID, initEpoch}`.
- Go: `refreshSnapshotParams{ClientGroupID, InitEpoch}`
  (`main.go:1629-1632`). Handler reads `eng + epoch` under brief
  `group.mu` then releases before `RefreshAllSources()` so it doesn't
  serialize with the per-CG worker FIFO (`main.go:1681-1722`).
- Dispatch: `handleConnection` routes `refreshSnapshot` AROUND the
  per-CG FIFO (`main.go:1862-1867`) — handled inline in its own
  goroutine, never enqueued to `g.reqC`.
- Match. The bypass is documented on both sides.

### CHECKED-OK-11: destroy contract
- TS: `destroy(cgID, opts?)` sends `{clientGroupID}` — no initEpoch
  (`go-ivm-client.ts:750-752`).
- Go: `destroyParams{ClientGroupID}` (`main.go:1608-1610`);
  `handleDestroy` calls `s.removeGroup(cgID)` and returns `Result: "ok"`
  (`main.go:1612-1625`). `removeGroup` deletes the group from the map
  and shuts down the worker goroutine (`main.go:855-864`).
- No epoch check is intentional — destroy is idempotent (a no-op for
  an already-removed group is fine).
- Match.

### CHECKED-OK-12: removeQuery contract
- TS: `removeQuery(cgID, queryID, initEpoch, opts?)`
  (`go-ivm-client.ts:694-696`). Sends `{clientGroupID, queryID,
  initEpoch}`.
- Go: `removeQueryParams{ClientGroupID, QueryID, InitEpoch}`
  (`main.go:1406-1410`); handler checks epoch (`main.go:1433`) before
  invoking `eng.RemoveQuery(queryID)` (`main.go:1437`); returns
  `"ok"`.
- Match.

### CHECKED-OK-13: loadRows contract
- TS: `loadRows(cgID, table, rows[], initEpoch, opts?)`; default
  timeout 120_000ms (`go-ivm-client.ts:605-617`).
- Go: `loadRowsParams{ClientGroupID, Table, Rows[]ivm.Row, InitEpoch}`
  (`main.go:1104-1116`). `Rows` typed as `[]ivm.Row` so
  `Row.DecodeMsgpack` runs at decode time (the in-place int→float64
  coercion). Handler returns `Result: "ok"` in ModeMemory
  (`main.go:1185`) or short-circuits success in ModeTable
  (`main.go:1139-1145`). Epoch check at line 1162.
- Match. Both sides expect `[]Record<string, unknown>` over the wire;
  Go's typed decode is a perf optimization that's wire-transparent.

### CHECKED-OK-14: addQuery / addQueries contract
- TS `addQuery`: sends `{clientGroupID, queryID, ast, initEpoch}` →
  receives `{changes: RowChange[], timingMs?: number}`
  (`go-ivm-client.ts:621-634`).
- Go `addQueryParams` (`main.go:1190-1195`), `addQueryResult`
  (`main.go:1197-1200`). Default timeout TS 60_000ms.
- TS `addQueries`: sends `{clientGroupID, queries: [{queryID, ast}],
  initEpoch}` → `{results: [{changes, timingMs}]}`
  (`go-ivm-client.ts:638-653`).
- Go `addQueriesParams` (`main.go:1245-1252`), `addQueriesResult`
  (`main.go:1254-1256`).
- Match.

### CHECKED-OK-15: addQueriesStream contract (chunked hydrate)
- TS sends `{clientGroupID, queries, initEpoch}`
  (`go-ivm-client.ts:674-691`) — same request shape as `addQueries`.
- Go reuses `addQueriesParams` in `handleAddQueriesStream`
  (`main.go:1351`).
- Partial frame shape:
  - Go: `addQueriesStreamPartial{QueryID, Changes, ChunkIndex, Final,
    TimingMs}` (`main.go:1342-1348`).
  - TS reads `{queryID, changes?, chunkIndex?, final?, timingMs?}` in
    `createHydrateStreamAccumulator` (`go-ivm-client.ts:177-184`).
- Per-query chunking contract:
  - Each query may emit 1..N frames, all with the same `queryID`.
  - Exactly one frame per query has `final=true`.
  - `chunkIndex` is per-query monotonic starting at 0.
- Terminal frame: Go writes `Result: "done"` (`main.go:1401`); TS
  detects via `STREAM_DONE_SENTINEL` (`go-ivm-client.ts:390,1163`).
- Defensive invariants enforced TS-side:
  - duplicate-after-final → throw `"duplicate frame after final"`
    (`go-ivm-client.ts:189-194`)
  - chunk-order gap → throw `"chunk order violation"`
    (`go-ivm-client.ts:207-212`)
  - orphan query at finish → throw `"queries never received a final
    chunk"` (`go-ivm-client.ts:233-242`)
- Go-side: per-query chunking is driven by `hydrateChunkSize = 10000`
  (`engine/engine.go:656,591-617`). Engine guarantees per-query
  monotonic chunkIndex by running each query's stream loop in a single
  goroutine; cross-query order is non-deterministic but TS's
  per-queryID accumulator handles that.
- Match.

### CHECKED-OK-16: advance contract
- TS sends `{clientGroupID, changes: SnapshotChange[], initEpoch}` →
  receives `{changes: RowChange[], timings?: TableTiming[]}`
  (`go-ivm-client.ts:700-712`).
- Go `advanceParams` (`main.go:1443-1447`), `advanceResult`
  (`main.go:1449-1452`).
- `SnapshotChange` shape:
  - TS: `{table, prevValues: Record<string,unknown>[], nextValue:
    Record<string,unknown> | null}` (`go-ivm-client.ts:79-83`).
  - Go: `engine.SnapshotChange{Table, PrevValues []ivm.Row, NextValue
    ivm.Row}` (`engine/engine.go:68-72`); `ivm.Row` is
    `map[string]Value`.
- `TableTiming` shape:
  - TS: `{table, type: number, ms: number}` (`go-ivm-client.ts:111-116`).
  - Go: `engine.TableTiming{Table, ChangeT int (json:"type"), Ms float64
    (json:"ms")}` (`engine/engine.go:93-97`).
- Match.

### CHECKED-OK-17: advanceStream contract
- Same request shape as `advance` (`go-ivm-client.ts:728-744`; Go
  reuses `advanceParams` at `main.go:1556`).
- Partial frame shape:
  - Go: `advanceStreamPartial{Changes, ChunkIndex, Final, Timings?,
    Drift?}` (`main.go:1544-1553`).
  - TS reads `{changes?, chunkIndex?, final?, timings?, drift?{table,
    op, pk, hasCount}}` in `createAdvanceStreamAccumulator`
    (`go-ivm-client.ts:266-277`).
- Single-call contract:
  - Per-call (NOT per-query) monotonic `chunkIndex` starting at 0.
  - Exactly one Final frame.
  - `timings` only on Final.
  - `drift` only on Final (and only if MemorySource raised DriftError).
- Terminal: `Result: "done"` (`main.go:1603`).
- Empty-advance contract: Go ALWAYS emits a terminal Final frame, even
  with zero changes and zero timings (engine
  `engine/engine.go:938-942`). TS test confirms
  (`go-ivm-client.test.ts:204-210`).
- Defensive invariants TS-side:
  - chunk-order gap → throw `"chunk order violation"`
    (`go-ivm-client.ts:285-290`)
  - missing Final → throw `"finished without a final chunk"`
    (`go-ivm-client.ts:311-317`)
- Match.

### CHECKED-OK-18: DriftError — non-streaming advance path
- Trigger: MemorySource.Push panics with `*ivm.DriftError` on
  Edit/Remove against a missing row, or duplicate Add
  (`ivm/source.go:240-267`). Panic is raised BEFORE state mutation
  (`ivm/drift.go:9-26`).
- Engine recovery: `engine.Advance` recovers the panic, drains any
  partial output from prior successful pushes in this batch via
  `e.streamer.Stream()`, and returns
  `AdvanceResult{Changes: allRowChanges, Timings: timings, Drift: d}`
  (`engine/engine.go:698-768`).
- Sidecar shipping: `handleAdvance` detects `result.Drift != nil` and
  emits an RPC error with `code: rpcCodeDrift (-32100)`, `message:
  result.Drift.Error()`, `data: driftRPCData{Table, Op, PK, HasCount,
  PartialChanges, PartialTimings}`
  (`main.go:1503-1527`).
- TS decoding: `#onData` matches `resp.error.code === RPC_CODE_DRIFT`,
  parses `error.data` as `{table, op, pk, hasCount, partialChanges,
  partialTimings}`, constructs `DriftError`, sets `drift.partialChanges
  = d?.partialChanges ?? []` and `drift.partialTimings =
  d?.partialTimings`, rejects the pending promise with the drift
  instance (`go-ivm-client.ts:1098-1124`).
- Recovery: `GoComputeBackend.#advanceWithRecovery` catches
  `instanceof DriftError`, calls `#reinitPerCGAndRegisterQueries`,
  trips the drift breaker, and RETURNS the partial output as the
  advance result so clients see the pre-drift RowChanges
  (`go-compute-backend.ts:276-308`).
- Match: drift contract is end-to-end consistent on the non-streaming
  path.

### CHECKED-OK-19: DriftError — streaming advance path
- Trigger: same MemorySource panic.
- Engine recovery: `engine.AdvanceStream` recovers, drains streamer
  into `pending`, sets `drift`. Always flushes a terminal Final frame
  carrying `pending` + cumulative `timings` + `drift`
  (`engine/engine.go:864-952`).
- Sidecar shipping: `handleAdvanceStream` passes the engine's
  `AdvanceStreamPartial` through `streamW` with `Drift: r.Drift` (i.e.,
  the raw `*ivm.DriftError`, NOT the `driftRPCData` shape) on Final
  (`main.go:1580-1596`).
- TS decoding: `createAdvanceStreamAccumulator` parses `v.drift` as
  `{table?, op?, pk?, hasCount?}` ONLY (no partialChanges /
  partialTimings — these aren't part of the drift wire payload on the
  stream path because the accumulator ALREADY has the changes from
  prior partial frames + the Final frame's `changes` field). At
  `finish()` it sets `drift.partialChanges = acc` and
  `drift.partialTimings = timings`
  (`go-ivm-client.ts:294-330`).
- Recovery: same `#advanceWithRecovery` catch path
  (`go-compute-backend.ts:276-308`).
- Match: the two paths achieve the same end state via different wire
  shapes — non-streaming ships partials inside the error envelope;
  streaming ships them as ordinary partial frames preceding the
  drift-on-Final terminal. TS reconciles both into the same
  `DriftError` API surface for the recovery code.

### HIGH-2: Drift.PK type asymmetry between streaming and non-streaming
- **TS DriftError class**: `readonly pk: Record<string, unknown>`
  (`go-ivm-client.ts:429`); both decoder paths construct the class via
  `new DriftError({pk: d?.pk})` (1110-1115) and `new DriftError({pk:
  v.drift.pk})` (300-305) — `pk` is treated as
  `Record<string, unknown>` either way.
- **Go non-streaming**: `driftRPCData.PK map[string]ivm.Value`
  (`main.go:1471`) — wire-encoded as a map.
- **Go streaming**: `advanceStreamPartial.Drift *ivm.DriftError`
  (`main.go:1552`) — `ivm.DriftError.PK map[string]Value`
  (`ivm/drift.go:30`) — same map shape.
- **Divergence**: none in field semantics. BUT note that with
  `UseCompactInts(true)` and PK values that include numeric column
  values > 2^32, msgpackr decodes them as BigInt — and `DriftError.pk`
  type erases that to `unknown`. The drift logging at
  `go-compute-backend.ts:280` calls `JSON.stringify(err.pk)` which
  throws `TypeError: BigInt cannot be serialized` on any BigInt
  member.
- **Impact**: drift on a table whose PK column is a 64-bit integer
  whose value exceeds 2^32 (e.g., a `bigserial` ID past 4 billion)
  would log-crash inside the drift recovery handler, which propagates
  to `#withReinitRetry` and surfaces as `"BigInt cannot be
  serialized"` in the error log. The drift breaker would not count it
  cleanly (the throw happens BEFORE `#recordDriftReinit` if the
  logger is wired to throw — though most loggers swallow).
- **Severity**: HIGH (latent crash on a class of workloads — bigint
  PKs at scale). Likely never seen in current soaks because the test
  fixture uses string PKs.
- **Suggested fix**: TS-side, sanitize PK before logging:
  `go-compute-backend.ts:280` should use a BigInt-safe replacer, e.g.
  `JSON.stringify(err.pk, (_k, v) => typeof v === 'bigint' ?
  v.toString() : v)`. Alternative: enforce Number coercion at decode
  time in `DriftError` constructor.

### CHECKED-OK-20: pipelineCount response coercion
- Go: `Result: count` where `count int` (`main.go:1666-1670`). With
  `UseCompactInts`, msgpack encodes 0..127 as fixint, 128..32767 as
  int16, etc. — msgpackr decodes all as Number.
- TS: `return typeof r === 'number' ? r : 0`
  (`go-ivm-client.ts:787-788`) — defensively coerces non-number to 0
  so the freeze-detection comparison `tsCount !== goCount` never
  throws on a BigInt comparison surprise.
- Match (with defensive fallback).

### CHECKED-OK-21: Init request — clientGroupID handling
- TS: `init(clientGroupID, params, opts)` sends `{clientGroupID,
  ...params}` where params is `{dbPath?, storagePath?, tables}`
  (`go-ivm-client.ts:587-598`). cgID is always provided.
- Go: `initParams{ClientGroupID, DBPath, Storage, Tables}`
  (`main.go:956-961`). `cgID == "" → "default"` fallback for backward
  compat (`main.go:990-993`). `DBPath` is parsed but the comment says
  "deprecated, kept for compat" — handler only uses `Storage`
  (`main.go:1013-1015`).
- TS sends `dbPath?` as an optional field but `GoComputeBackend.#runInit`
  never populates it (`go-compute-backend.ts:539-543` only sends
  `tables`). So `DBPath` is dead in the current TS path; Go's compat
  parsing is harmless.
- Match (with dead optional field on Go side — clean up opportunity).

### MEDIUM-1: TS uniqueKeys may be undefined; Go decodes as nil — verify resolver behavior
- TS: `TableSchema.uniqueKeys?: string[][] | undefined`
  (`go-ivm-client.ts:56-64`). `loadRows` and `init` forward it as-is.
- Go: `tableSchemaParams.UniqueKeys [][]string` with `json:",omitempty"`
  (`main.go:967-970`). msgpack decoder handles absent keys as the zero
  value (`nil` slice). `handleInit` only calls
  `eng.SetTableUniqueKeys` when `len(schema.UniqueKeys) > 0`
  (`main.go:1082-1084`) — so absent and empty are equivalent on Go.
- **Status**: contract holds for the documented "absent = no known
  unique keys" semantics. No risk of misclassifying an empty
  uniqueKeys[][] as "scalar resolver active for table".
- **Severity**: CHECKED-OK (called out only because the type carries
  three indistinguishable states on the wire — undefined / [] / [[]]
  — and they all collapse to "no resolver"; future contributors who
  rely on `[]` vs `undefined` having different meaning will be
  surprised).

### CHECKED-OK-22: Stream-terminal sentinel collision defense (D6)
- TS: `STREAM_DONE_SENTINEL = 'done'` (`go-ivm-client.ts:390`). At
  frame-routing time, if `pending.onPartial` is set AND result is not
  the literal `"done"`, the result MUST be an object — otherwise
  `pending.reject` with `"Streaming RPC received non-object partial:
  ..."` (`go-ivm-client.ts:1163-1175`).
- Go: streaming handlers always emit `streamW(req.ID, addQueriesStreamPartial{...})`
  or `streamW(req.ID, advanceStreamPartial{...})` for partials — both
  are msgpack-encoded as maps. The literal `"done"` Result only ever
  appears on the terminal RPCResponse (`main.go:1401,1603`).
- Match. The sentinel-collision defense is real coverage if Go ever
  regressed to emit `Result: "done"` as a partial.

### CHECKED-OK-23: Error code allocation
- Standard JSON-RPC: `-32700` parse error
  (`go-ivm-client.ts:N/A`; Go uses on parse fail at `main.go:1853`),
  `-32601` method not found (`main.go:948`), `-32602` invalid params
  (Go uses across handlers, e.g., 986, 1133, 1212, etc.), `-32603`
  internal error (`main.go:1782`).
- Custom: `-32000` generic engine error (used throughout Go for "engine
  not initialized", "client group destroyed", etc.); `-32100`
  `rpcCodeDrift` (`main.go:1460`); `-32101` `rpcCodeStaleInitEpoch`
  (`main.go:388`).
- TS dispatch: by code, NOT by message string for drift and stale
  epoch (`go-ivm-client.ts:1098,1125`); everything else falls into
  generic `new Error(\`RPC error ${code}: ${message}\`)` at line 1135.
- Match. The custom codes are stable; standard codes follow JSON-RPC
  spec.

### MEDIUM-2: `#withReinitRetry` string-matches Go error messages for restart-related errors
- **TS**: `#withReinitRetry` and `#advanceWithRecovery` both pattern
  match `err.message` for `"Sidecar is not running"`, `"Connection
  closed"`, `"engine not initialized"`
  (`go-compute-backend.ts:310-313,336,373-377,406-407`).
- **Go**: emits `"engine not initialized (call init first)"` from
  several handlers (`main.go:1154,1160,1222,1228,1295,1301,1363,1369,
  1425,1431,1490,1496,1568,1574,1692,1714`); the other strings
  ("Sidecar is not running", "Connection closed") originate
  TS-internally (sidecar-manager.ts:315 and the socket close handler at
  go-ivm-client.ts:532).
- **Divergence**: this works because Go emits exactly one
  canonical phrase, and TS just substring-matches it. If a future Go
  contributor renames the message to "engine missing" or "no engine
  for cg", TS would silently fall through to the generic error path
  and clients would see Internal errors instead of recovery.
- **Impact**: silent regression on message-text change.
  Functionality breaks; no test asserts the literal string.
- **Severity**: MEDIUM (works today; sharp edge for future changes).
- **Suggested fix**: allocate a stable error code for
  "engine not initialized" (e.g., `-32102 rpcCodeEngineNotInitialized`),
  return it from every "engine not initialized" path on Go side, and
  switch TS to `if (resp.error.code === RPC_CODE_ENGINE_NOT_INIT)`
  in `#onData` (parallel to the existing drift / stale-epoch handling).
  Then `#withReinitRetry` can check `err instanceof EngineNotInitError`
  instead of `msg.includes('engine not initialized')`.
  Files: `main.go:1154` etc.; `go-ivm-client.ts:1135`;
  `go-compute-backend.ts:336,407`.

### MEDIUM-3: TS string-matches `"Sidecar is not running"` from its OWN code — but the literal lives on the manager
- **TS**: `sidecar-manager.ts:315`:
  `throw new Error('Sidecar is not running (status: ${this.#status})');`
  Then `#withReinitRetry` at `go-compute-backend.ts:312,376` matches
  `msg.includes('Sidecar is not running')`.
- **Divergence**: this is an intra-TS contract — manager throws a
  string that backend pattern-matches. Coupling is invisible (no
  exported constant, no helper).
- **Impact**: if the manager ever wraps the throw or changes the
  phrasing (e.g., new logger), recovery silently breaks.
- **Severity**: MEDIUM.
- **Suggested fix**: export a sentinel from `sidecar-manager.ts` —
  e.g., `export class SidecarNotRunningError extends Error` — and have
  `getClient()` throw it. `#withReinitRetry` then uses `instanceof`.
  Files: `sidecar-manager.ts:313-316`,
  `go-compute-backend.ts:312-313,376-377`.

### CHECKED-OK-24: Sidecar restart — spawned mode
- `proc.on('exit')` fires when the child dies (SIGKILL, OOM, panic,
  or unexpected exit). Handler routes through `#handleRestartTrigger`
  with `isRetrigger = #suppressNextExitCount`
  (`sidecar-manager.ts:400-415`).
- `#handleRestartTrigger` runs sliding-window failure cap
  (default 5/60s) and either schedules `#spawn()` via `setTimeout` or
  marks status `'failed'` (`sidecar-manager.ts:545-629`).
- `#spawn()` clears the previous client, spawns the binary, waits for
  socket readiness, opens a fresh `GoIVMClient`, ping-checks, version-
  checks, transitions to `'running'`, bumps `#epoch`, and fires
  `onRestart` listeners on the SECOND through Nth start
  (skipping the first via `#firstStartComplete`) at
  `sidecar-manager.ts:369-524`.
- Match.

### CHECKED-OK-25: Sidecar restart — externallyManaged mode
- No spawn; manager just connects to a pre-existing socket. Sets up a
  2-second health-check ticker that polls `#client.isConnected()` and
  routes disconnect through `#handleRestartTrigger`
  (`sidecar-manager.ts:489-508`).
- On reconnect, the same `#spawn()` path runs (ping/version/onRestart),
  but no process spawn — just a fresh `GoIVMClient.connect()`.
- `stop()` in externallyManaged mode tears down the client and the
  ticker only — does NOT kill the (foreign) process or unlink the
  socket (`sidecar-manager.ts:325-364`).
- Match. Documented as required by the shared-sidecar deployment
  recipe in `mono/AGENTS.md` § "Shared sidecar mode".

### CHECKED-OK-26: Sliding-window failure cap — sole-failure-counts-once invariant (RESILIENCE-6 D13)
- `#suppressNextExitCount` is set TRUE immediately before
  `this.#proc.kill('SIGKILL')` in the post-spawn-failure cleanup
  branch (`sidecar-manager.ts:596-597`).
- `proc.on('exit')` reads it, passes `isRetrigger = true` to
  `#handleRestartTrigger`, then resets to false
  (`sidecar-manager.ts:412-414`).
- `#handleRestartTrigger` does `if (!isRetrigger) push(now)` so the
  follow-up SIGKILL-driven exit does NOT consume a second slot
  (`sidecar-manager.ts:551-553`).
- Match. Cap trips at the configured value, not half.

### CHECKED-OK-27: `waitForRunning()` queue across restart
- During `'restarting'`, `#runningPromise` is replaced with a fresh
  pending promise (`sidecar-manager.ts:559-563`). New
  `waitForRunning()` callers await it; the previous resolve/reject
  pair is overwritten so the new spawn's resolve fires the right
  promise.
- Terminal state `'failed'` rejects the promise
  (`sidecar-manager.ts:623`).
- Terminal state `'stopped'` rejects from `stop()`
  (`sidecar-manager.ts:329`).
- Match — every queued caller eventually settles.

### CHECKED-OK-28: `#advanceWithRecovery` "miss exactly one delta" contract (RESILIENCE-4)
- On sidecar-unavailable / engine-not-initialized branches,
  `#advanceWithRecovery` waits for `waitForRunning() + whenInitialized()`,
  triggers `#reinitPerCGAndRegisterQueries` if needed, then returns
  `{changes: [], timings: []}` — does NOT retry the diff
  (`go-compute-backend.ts:310-345`).
- Documented rationale: Go's new state from init+loadRows already
  reflects the post-advance snapshot; re-applying the diff would
  double-apply and the next genPush would trip "Row not found".
  Client misses one IVM delta but state stays consistent.
- Match.

### CHECKED-OK-29: `#withReinitRetry` retries hydrate after re-init (asymmetric vs advance)
- Hydrate uses `#withReinitRetry` (`go-compute-backend.ts:363-421`) —
  which DOES retry after re-init (`return await fn()` at lines 404,
  419).
- Advance uses `#advanceWithRecovery` (`go-compute-backend.ts:258-348`)
  — which does NOT retry; returns empty result.
- Asymmetry is intentional: hydrate is idempotent against fresh
  engine state (just re-fetches), while advance is a destructive
  operation that already had its diff applied to Go.
- Match.

### CHECKED-OK-30: Per-CG drift breaker
- 5 drift-driven reinits within 60s → breaker open for 300s → backend
  reports `initialized=false` until cooldown elapses; PipelineDriver
  dispatch falls through to TS for that CG
  (`go-compute-backend.ts:49-51,121-133,604-615`).
- Documented in TS — no Go-side analog needed (Go just keeps emitting
  drift signals; breaker is a TS-side rate limiter).
- Match.

### CHECKED-OK-31: Init concurrency cap (MED-CROSS-1)
- TS: `SidecarManager.withInitSlot` caps to 4 concurrent inits across
  all backends sharing the manager (`sidecar-manager.ts:52,207-219`).
- `GoComputeBackend.#doInit` wraps the init call in `withInitSlot`
  (`go-compute-backend.ts:511`).
- Go: no analog cap — relies on TS to throttle. The per-cg FIFO on Go
  serializes within a cg but not across; cold-start stampede
  protection is purely TS-side.
- Match (TS-side enforcement is the contract).

### CHECKED-OK-32: Per-clientGroupID semaphore (CRITICAL-CROSS-2)
- TS: `MAX_IN_FLIGHT_PER_GROUP = 16` cap on top of `MAX_IN_FLIGHT =
  1024` global (`go-ivm-client.ts:360-362,815-836`).
- Go: no analog cap — relies on TS to fairness-shape per cg.
- Match (TS-side enforcement).

### CHECKED-OK-33: Per-call timeout, recently-timed-out ID dedupe
- TS: `DEFAULT_TIMEOUT_MS = 30_000` (`go-ivm-client.ts:359`);
  per-method overrides documented per method. On timeout the ID is
  recorded in `#recentlyTimedOut` (`go-ivm-client.ts:855-863`) and
  `#nextId` skips any IDs in that set
  (`go-ivm-client.ts:866-880`) — prevents late responses routing to a
  fresh same-ID call (REVIEW-final LOW-TS-2).
- Go: no per-call timeout — relies on socket-level liveness + TS
  timeout. Per-cg FIFO has no internal deadline; long handlers just
  block the cg's queue.
- Match (TS owns deadline enforcement).

### CHECKED-OK-34: Byte-level backpressure
- TS: `socket.write` returning false sets `#drainPromise`; subsequent
  `#acquireSlot` awaits it before issuing more writes
  (`go-ivm-client.ts:496,818-820,962-973`). Drain handler clears the
  promise.
- Go: writer goroutine cap `maxWriterGoroutines = 4096`
  (`main.go:1776-1777`); reader applies TCP backpressure when `outC`
  fills (cap 1024 at `main.go:1795`).
- Match — both sides bound in-flight; saturation propagates back to
  socket flow control.

### CHECKED-OK-35: Connection close → fail-fast all pending
- TS: `socket.on('close')` nullifies `#socket`, rejects every pending
  with `new Error('Connection closed')`, releases all backpressure
  waiters (`go-ivm-client.ts:528-547`).
- Go: connection reader exits on `readFrame` error; `defer close(outC)`
  + `defer writerWg.Wait()` drain writers, but pending per-CG worker
  responses on `respCh` get nowhere — the writer goroutine's
  `<-respCh` blocks forever if the worker is still computing.
  However, `shutdownGroup` does NOT fire on connection drop (only on
  removeGroup / closeAll); the worker keeps running and its response
  is discarded by the goroutine reading respCh, which never fires
  because the connection closed.
- **Subtle**: in the per-connection path, when the socket dies, the
  defer at `main.go:1823-1827` drains `outC` and waits for writers
  — but the writer goroutines waiting on respCh will block until the
  workers complete and send. Workers don't know the connection is
  gone; they finish naturally and the writer's `writeFrameLocked`
  becomes a no-op against a closed conn. So no goroutine leak — but
  the per-CG worker is still busy doing the work that no one will
  read.
- Match — orderly drain. Documented behavior.

### CHECKED-OK-36: Streaming-frame atomicity under concurrent writers
- Go: `streamW` calls `writeFrameLocked` which takes `writeMu` for
  the duration of the write (`main.go:1837-1843,1779-1787`). Multiple
  streaming handlers + the per-request-writer goroutines all race for
  the same mutex, so individual frames never interleave.
- TS: receiver doesn't depend on frame ordering across requests — the
  ID-based router fans out per-request, so out-of-order responses are
  fine. Per-request frame order (`chunkIndex` monotonic) is enforced
  by Go's single-sender invariant inside one handler.
- Match.

### CHECKED-OK-37: msgpack codec — int / float / BigInt normalization
- TS encoder (`Packr`): `useRecords: false, encodeUndefinedAsNil: true,
  mapsAsObjects: true, useBigIntExtension: false`
  (`go-ivm-client.ts:30-42`). Plain TS numbers encode to msgpack
  positive/negative integer types or float64 depending on value.
- TS decoder (`Unpackr`): `useRecords: false, mapsAsObjects: true,
  useBigIntExtension: false`. Decodes msgpack integers → JS Number
  where safe, BigInt where not.
- Go encoder: `UseCompactInts(true)` (`main.go:55`). Picks the
  smallest int type that fits.
- Go decoder: `UseLooseInterfaceDecoding(true)` (`main.go:81`).
  Promotes any msgpack int to int64 or uint64 in `interface{}`
  positions. Then `normalizeNumericInterfaces` walks the decoded
  struct and converts numerics in interface{} positions to float64
  (`main.go:78-179`).
- Match — both sides converge numeric types so TS's single-Number
  model and Go's row-comparison paths see the same values.

### CHECKED-OK-38: `traceparent` propagation
- TS: `getActiveTraceparent()` reads the active OTel span and formats
  the W3C traceparent header; attaches to every RPCRequest
  (`go-ivm-client.ts:14-26,943-944`).
- Go: `RPCRequest.Traceparent` parsed and threaded through
  `extractTraceparent(ctx, req.req.Traceparent) + startHandlerSpan`
  per request (`main.go:770-771,419`). Slow handlers also log the
  traceparent (`main.go:798-803`).
- Match. Cross-process trace correlation works.

### CHECKED-OK-39: `onRestart` listener fires after epoch++ (RESILIENCE-1..5)
- Manager: `#spawn()` transitions to `'running'`, bumps `#epoch`,
  resolves the running promise, THEN fires `onRestart` listeners
  (skipping the first start) (`sidecar-manager.ts:475-523`).
- Backend: subscribes in constructor
  (`go-compute-backend.ts:118`); listener calls
  `#reinitPerCGAndRegisterQueries('sidecar-restart-epoch-N', ...)`
  (`go-compute-backend.ts:617-629`).
- `#reinitPerCGAndRegisterQueries` holds `#restartGate` for the
  duration; advance/hydrate calls `await this.#restartGate` before
  issuing RPCs (`go-compute-backend.ts:202,209,221,261,656-725`).
- Match — REVIEW-final HIGH-TS-1 + MED-CROSS-6 contract holds.

### CHECKED-OK-40: `#doInit` dedupe (RESILIENCE-5)
- Concurrent `#doInit` calls reuse the in-flight `#currentInitPromise`
  rather than spawning a second concurrent init+loadRows
  (`go-compute-backend.ts:494-520`).
- Critical because the onRestart listener AND any retry path can both
  call `#doInit` after a restart; pre-fix Go ended up with `2N` rows.
- Match.

### LOW-1: `#runInit` snapshots manager epoch BEFORE init; uses it for `#initEpoch` tracking
- TS: `epoch = this.#manager.epoch` captured at the start of
  `#runInit` (`go-compute-backend.ts:526`); after init success,
  `#initEpoch = epoch`. The `initialized` getter checks `this.#initEpoch
  === this.#manager.epoch` (`go-compute-backend.ts:132`).
- If the sidecar restarts BETWEEN epoch-capture and the init-RPC
  completing on the wire, `#initEpoch` records the OLD epoch and the
  `initialized` getter returns false — the onRestart listener will
  re-init at the new epoch.
- **Concern**: what if the sidecar restarts and re-spawns BETWEEN
  capture-epoch and the RPC actually arriving at the new sidecar?
  The init lands on the NEW sidecar (cgID is fresh, initEpoch=1 from
  Go side). The wire returns `initEpoch: 1` which TS stashes into
  `#sidecarInitEpoch = 1`. Manager epoch is now 2 (post-restart).
  `this.#initEpoch = 1` (pre-restart capture) vs
  `this.#manager.epoch = 2` → `initialized === false`. Good — backend
  treats itself as needing re-init.
- onRestart listener then fires for epoch 2, re-runs init, and gets a
  fresh `initEpoch: 2` from the (already-newer) sidecar (Go bumps on
  every handleInit). All consistent.
- **Match** — covered. Calling out as LOW only because the epoch
  bookkeeping is dual (manager-epoch vs sidecar-initEpoch) and the
  semantics aren't obvious from a quick read.

### CHECKED-OK-41: Connection-loss vs sidecar-died — TS handles both as restart
- TS: `#withReinitRetry` and `#advanceWithRecovery` use the same
  `sidecarUnavailable` predicate: `manager.status !== 'running' ||
  msg.includes('Sidecar is not running') || msg.includes('Connection
  closed')` (`go-compute-backend.ts:310-313,373-377`).
- This handles both: (a) RPC issued while manager was already
  restarting → throws "Sidecar is not running" from `getClient()`;
  (b) RPC in flight when socket dropped → `socket.on('close')` rejects
  pending with "Connection closed".
- Match. RESILIENCE-2 contract.

### CHECKED-OK-42: `recordHydrateChunks` / `recordAdvanceChunks` only fire on Final
- Go: `recordHydrateChunks(r.ChunkIndex + 1)` is only invoked when
  `r.Final == true` (`main.go:1391-1393,1593-1595`). Provides
  observability of "is streaming actually chunking?" via the
  `[GO-IVM][PERF-CHUNKS]` log line at 10s intervals.
- No TS counterpart — TS's `ivm.advance-time` and friends record per
  call, not per chunk.
- Match (purely Go-side metric; no wire impact).

### MEDIUM-4: `#runInit` partial-loadRows failure destroy is best-effort but post-init `#initialized` remains false
- TS: if any `loadRows` chunk fails mid-`#runInit`, the catch block
  calls `client.destroy(cgID)` best-effort then re-throws
  (`go-compute-backend.ts:570-580`).
- `#initialized` stays `false`; subsequent `whenInitialized()` returns
  the rejected promise via `#currentInitPromise` (with catch-swallow
  at `go-compute-backend.ts:172-174`).
- **Subtle**: between `client.destroy` and the next caller's `#doInit`
  retry, the Go side has destroyed the group entirely
  (`s.removeGroup` deletes the map entry). So the retry's `init` runs
  with `initEpoch=1` on a fresh group — the previous `initEpoch`
  number is gone with the group.
- TS's previous `#sidecarInitEpoch` would be whatever the previous
  init returned (likely 1 if it completed enough to return). Retry's
  init returns `initEpoch=1` again. The "stale" check on the Go side
  would NEVER trigger here because the cgID was wiped clean — and
  even if it survived, the epoch numbers match by accident at this
  point. Drift-loop guard (breaker) is the actual safety net for
  pathological retries.
- **Severity**: MEDIUM — the cleanup path is correct but relies on the
  Go-side group-wipe semantic to make epoch bookkeeping irrelevant
  during recovery. If Go ever switched to "destroy preserves epoch"
  semantics, this path would silently break.
- **Suggested fix**: TS-side `#sidecarInitEpoch = -1` reset in the
  catch block at `go-compute-backend.ts:571` would make the contract
  defensive rather than rely on Go's group-wipe behavior.

### CHECKED-OK-43: `#nextId` collision protection
- TS: 32-attempt loop skips IDs in `#pending` and
  `#recentlyTimedOut`; throws on inability to allocate
  (`go-ivm-client.ts:866-880`). With `MAX_IN_FLIGHT = 1024` and uint32
  space, collisions are virtually impossible.
- Match.

### CHECKED-OK-44: `client.close()` rejects pending + releases waiters
- TS: `close()` nulls `#socket`, rejects every pending with `"Client
  closed"`, releases all backpressure waiters
  (`go-ivm-client.ts:552-575`).
- Match.

### CHECKED-OK-45: Idle group reaper (HIGH-CROSS-2/3)
- Go: 5-minute ticker; reaps groups whose `lastUsedNs` is older than
  30 min (`main.go:2005-2014,675,725-755,838-851`).
- `lastUsedNs` stamped on every request arrival (NOT dequeue) so a
  long handler doesn't let queued reqs make the group reap-eligible.
- TS has no analog — it's a Go-side memory bound only.
- Match (Go-side enforcement).

### CHECKED-OK-46: `groupIdleTimeout` and TS `destroy` race
- If TS sends `destroy` for a cgID that Go has already reaped, the
  handler's `s.removeGroup` is a no-op (already deleted) and returns
  `"ok"` (`main.go:856-864,1612-1625`). No error to TS.
- If TS sends a mutating RPC on a reaped cgID, Go returns `-32000
  "engine not initialized (call init first)"` → TS's
  `#withReinitRetry` re-inits and continues.
- Match — graceful.

### CHECKED-OK-47: `#whenInitialized` swallows rejection
- TS: `whenInitialized()` returns `this.#currentInitPromise.catch(()
  => undefined)` so callers don't need a try/catch; they self-check
  `this.initialized` after await (`go-compute-backend.ts:168-176`).
- Match — documented contract.

### CHECKED-OK-48: Defensive `#advanceWithRecovery` engine-not-initialized branch
- If `msg.includes('engine not initialized')` after `await call()`,
  re-init via `#reinitPerCGAndRegisterQueries`, return empty
  result if reinit succeeded; throw original otherwise
  (`go-compute-backend.ts:336-345`).
- Match.

### MEDIUM-5: `loadRows` `Result: "ok"` is a plain string, not a map
- Go: `handleLoadRows` returns `Result: "ok"` in success path
  (`main.go:1185`) — a plain string, NOT `{status: "ok"}`.
- TS: `loadRows` just awaits and discards (`go-ivm-client.ts:611-617`).
- **Concern**: inconsistency with `handleInit` which returns
  `{status: "ok", initEpoch: N}` map. Other plain-string Result values
  on the wire could trigger the "Streaming RPC received non-object
  partial" sentinel-collision guard — BUT only if the same call had
  `onPartial` set. `loadRows` never has `onPartial`, so no issue
  today.
- If `loadRows` ever becomes streaming (chunked progress reporting), the
  plain-string success Result would collide with `STREAM_DONE_SENTINEL`
  on the off chance it returned literally `"done"` — extremely
  unlikely but the pattern is fragile.
- **Severity**: MEDIUM — pure code-smell today, fragile invariant.
- **Suggested fix**: standardize all non-streaming success Results as
  maps (`{status: "ok"}`) so streaming-vs-non distinction is type-
  enforced. Apply to `main.go:1185` (loadRows), `1438` (removeQuery),
  `1624` (destroy), `1722` (refreshSnapshot).

### CHECKED-OK-49: Streaming methods reject non-object partials (D6 collision defense)
- TS rejects streaming partial that's not an object with explicit
  error message (`go-ivm-client.ts:1163-1175`). Reserved sentinel
  `"done"` is the only legal plain-string Result for a streaming call.
- Match.

### CHECKED-OK-50: `addQueriesStream` empty-queries case
- Go: empty `built` slice → `wg.Wait()` returns immediately, no per-
  query frames emitted, then `Result: "done"` terminal
  (`engine/engine.go:559-622`; `main.go:1383-1401`).
- TS: `createHydrateStreamAccumulator.finish()` only throws if `acc.size
  > 0` — empty acc passes (`go-ivm-client.ts:236-242`).
- Match. (No frames is fine; finish() doesn't require any per-query
  frames to exist.)

### CHECKED-OK-51: `advanceStream` empty-advances case
- Go: AdvanceStream loop with empty changes; `flush(true)` always
  fires at end so terminal Final frame is emitted with empty
  pending+timings (`engine/engine.go:938-942`). Test:
  `engine_streaming_test.go` and TS-side test
  `go-ivm-client.test.ts:204-210`.
- Match.

### CHECKED-OK-52: `onPartial` throw propagation
- TS: try/catch around `pending.onPartial(resp.result)` in `#onData`;
  on throw, the pending RPC is rejected with the thrown error and the
  socket connection stays up (`go-ivm-client.ts:1177-1188`).
- Documented rationale: pre-fix a sync throw from `#onData` propagated
  via Node's EventEmitter to `'error'` → `uncaughtException` →
  worker crash on any protocol bug.
- Match.

---

## RPC method contract matrix

| Method | TS request fields | Go param struct | Response shape | Error codes | Streaming? | Epoch checked? | Default timeout |
|---|---|---|---|---|---|---|---|
| `ping` | (none) | (none) | `Result: "pong"` | `-32601` not found | no | no | 5s |
| `version` | (none) | (none) | `Result: {version:string, protocolRev:int}` | `-32601` (older sidecars) | no | no | 5s |
| `init` | `{clientGroupID, dbPath?, storagePath?, tables:{[name]:{columns, primaryKey, uniqueKeys?, rows}}}` | `initParams{ClientGroupID, DBPath, Storage, Tables}` | `Result: {status:"ok", initEpoch:uint64}` | `-32602` decode; `-32000` engine/replica fail | no | bumps epoch | 120s |
| `loadRows` | `{clientGroupID, table, rows[], initEpoch}` | `loadRowsParams{ClientGroupID, Table, Rows []ivm.Row, InitEpoch}` | `Result: "ok"` (string) | `-32602` decode; `-32000` "engine not initialized"/"no MemorySource"; `-32101` stale | no | yes | 120s |
| `addQuery` | `{clientGroupID, queryID, ast, initEpoch}` | `addQueryParams{ClientGroupID, QueryID, AST, InitEpoch}` | `Result: {changes:[RowChange], timingMs?}` | `-32602` decode; `-32000` engine; `-32101` stale | no | yes | 60s |
| `addQueries` | `{clientGroupID, queries:[{queryID,ast}], initEpoch}` | `addQueriesParams` | `Result: {results:[{changes, timingMs?}]}` | `-32602`; `-32000`; `-32101` stale | no | yes | 120s |
| `addQueriesStream` | same as `addQueries` | reuses `addQueriesParams` | partial: `{queryID, changes[], chunkIndex, final, timingMs}` per-query; terminal `Result: "done"` | `-32602`; `-32000`; `-32101` stale | yes (per-query) | yes | 120s |
| `removeQuery` | `{clientGroupID, queryID, initEpoch}` | `removeQueryParams` | `Result: "ok"` (string) | `-32602`; `-32000`; `-32101` stale | no | yes | DEFAULT (30s) |
| `advance` | `{clientGroupID, changes:[SnapshotChange], initEpoch}` | `advanceParams` | `Result: {changes:[RowChange], timings?:[TableTiming]}` | `-32602`; `-32000`; `-32101` stale; **`-32100` DRIFT with `data: driftRPCData`** | no | yes | DEFAULT (30s) |
| `advanceStream` | same as `advance` | reuses `advanceParams` | partial: `{changes[], chunkIndex, final, timings?, drift?{table,op,pk,hasCount}}`; terminal `Result: "done"` | `-32602`; `-32000`; `-32101` stale; drift surfaces on Final frame, NOT as RPC error | yes (per-call) | yes | 120s |
| `destroy` | `{clientGroupID}` | `destroyParams{ClientGroupID}` | `Result: "ok"` (string) | `-32602` | no | NO (idempotent) | DEFAULT (30s) |
| `refreshSnapshot` | `{clientGroupID, initEpoch}` | `refreshSnapshotParams{ClientGroupID, InitEpoch}` | `Result: "ok"` (string) | `-32602`; `-32000` engine; `-32101` stale | no | yes | 120s (per call site) |
| `pipelineCount` | `{clientGroupID}` | `pipelineCountParams{ClientGroupID}` | `Result: int` (count) | `-32602` only | no | NO (intentional probe) | DEFAULT (30s) |

Notes on the matrix:
- "DEFAULT (30s)" = TS `DEFAULT_TIMEOUT_MS = 30_000`
  (`go-ivm-client.ts:359`).
- All numeric ints over the wire decode as JS Number (msgpackr +
  Go's UseCompactInts cooperation). Init epoch in particular fits in
  uint32 in practice — but TS does NOT defensively coerce BigInt
  here (`go-ivm-client.ts:594`); if epoch ever exceeded 2^53 the type
  assertion would fail. See CHECKED-OK-9 + HIGH-2 for related
  numeric concerns.
- `Result: "ok"` plain-string responses are a code-smell — see
  MEDIUM-5. They work today only because no method that returns plain
  string is also a streaming method.

---

## Restart machinery state diagram

```
                       ┌──────────────┐
                       │   stopped    │  (initial; or after stop())
                       └──────┬───────┘
                              │ start()
                              ▼
                       ┌──────────────┐
                       │   starting   │  (#runningPromise pending)
                       └──────┬───────┘
                              │ #spawn() success
                              │   (waitForReady + ping + version)
                              ▼
                       ┌──────────────┐
                       │   running    │  (epoch++; onRestart fires on 2nd+)
                       └──┬────────┬──┘
                          │        │
              proc.exit() │        │ stop()
              OR          │        │
              health-tick │        │
              disconnect  │        │
                          ▼        ▼
              ┌─────────────────┐  ┌──────────────┐
              │  #handleRestart │  │   stopped    │
              │     Trigger     │  └──────────────┘
              └────┬────────────┘
                   │ window check
              ┌────┴────────────┐
              │ within cap      │ over cap
              ▼                 ▼
       ┌──────────────┐   ┌──────────────┐
       │  restarting  │   │    failed    │  (terminal; runningPromise rejects)
       └──────┬───────┘   └──────────────┘
              │ setTimeout(restartDelayMs)
              │   then #spawn()
              ▼
        (loop back to "running" or "#handleRestartTrigger")
```

Both sides agree on the transitions visible to dependents:
- Go has no state of this kind — every restart on the manager side
  appears to Go as a fresh socket connection (and in spawned mode, a
  brand-new process). Go's per-cg state is wiped because the new
  process starts with an empty `Server.groups` map.
- TS's onRestart listener does the per-cg reinit + query
  re-registration AFTER manager transitions to `'running'` and BEFORE
  any backend RPCs flow (gated by `#restartGate` in each backend).

State-transition coverage gaps:
- Mid-handshake failure (RESILIENCE-6): `#spawn` rejection routes back
  to `#handleRestartTrigger(isRetrigger=true)` so the window counter
  doesn't double-count. In spawned mode, the wedged process is
  SIGKILLed first; the `'exit'` handler reads `#suppressNextExitCount`
  to also skip the double-count.
- All terminal-state callers (`waitForRunning`, `getClient`) reject
  explicitly so dependents fall through to TS-only.

---

## Coverage notes

- Verified by code-trace: every TS method has a matching Go handler;
  every Go handler decodes the same field set TS sends.
- Numeric-coercion paths: TS msgpackr emits `Number` for everything;
  Go's `mpUnmarshal` post-processes via `normalizeNumericInterfaces`
  to ensure interface{} positions hold float64 — sufficient for IVM
  comparison code.
- Streaming invariants: confirmed via `go-ivm-client.test.ts` and
  the Go-side `engine/engine_streaming_test.go` + `engine/advance_streaming_test.go`
  patterns.
- Restart machinery: cross-checked against
  `go-ivm/REVIEW-final.md` RESILIENCE-1..7 closure rows.
- DriftError contract: traced through both paths (RPC error for
  non-streaming, Final-frame field for streaming) and confirmed TS
  reconstructs the same `DriftError` API surface for the recovery
  handler.
- Init epoch contract: all eight mutating handlers on Go invoke
  `checkInitEpoch`; pipelineCount is the documented carve-out.
- Backpressure: TS-side per-call + per-group + global + byte-level all
  present; Go-side writer-goroutine cap + outC cap apply TCP
  backpressure inward.

---

## Not checked

- **Wire-level shadow comparator**: this audit covers the protocol
  contract, not whether Go's IVM output for a given input set
  semantically matches TS's IVM output. That belongs to the shadow-
  comparator + drift-audit code, which is out of scope here.
- **AST/builder protocol**: `addQuery.AST` is `builder.AST` on Go and
  `unknown` on TS. AST shape parity (column types, operators,
  predicate kinds) is the builder's concern — REVIEW-porting covers
  it. We confirmed the AST round-trips as a black box, not its
  decoding fidelity.
- **Permissions/system schema filtering**: Go's `streamChanges` skips
  `schema.System == "permissions"` (`engine/streamer.go:91-93`); we
  did not verify the TS-side equivalence.
- **OTel span attribute parity**: traceparent propagates; whether the
  span attributes match the TS-side parent's recorded attributes is a
  separate trace audit.
- **`addQuery` decode-failure diagnostics**: Go logs first-64-byte
  hex dumps on decode failure for `addQuery` / `addQueries`
  (`main.go:1206-1212,1262-1284`). The TS encoder doesn't have a
  corresponding "show me my last encoded bytes" hook on the failed-
  decode path. Not load-bearing — diagnostics only.
- **Connection-drop cleanup race**: confirmed worker goroutines do NOT
  leak (writers drain), but per-CG workers continue computing even
  after their reply target is gone. Out of scope for protocol audit,
  but worth a separate concurrency audit if engine cost matters.
- **`Replica DB` cold-open path** (`main.go:545-631`): the
  singleflight retry/probe semantics are Go-internal; TS only sees the
  first init either succeed within 60s or error out as
  `"replica not ready: ..."` which TS's `#runInit` will surface to
  `start()` rejection or to the next `whenInitialized()` poll.
- **`sourceMode = ModeTable`-specific paths**: loadRows is a no-op,
  init builds tablesource.Source, etc. The wire contract holds the
  same shape; the difference is purely Go-internal source-of-truth.
  Verified the wire shape; did not verify ModeTable semantics here.
- **PORT-AUDIT-FIXES.md drift**: did not cross-check whether each
  finding in PORT-AUDIT-FIXES has its counterpart in the current
  files. This audit is independent.
