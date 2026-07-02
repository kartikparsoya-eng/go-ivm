# Review: feat/napi-transport (go-ivm + mono), all fronts

Date: 2026-07-02. Branches: go-ivm `feat/napi-transport` (6 commits over
streaming-hydrate), mono `feat/napi-transport` (8 commits). Reviewed:
architecture, process model/lifecycle, ABI memory+threading, boundary
encoding, parallelization, defaults, tests. go-ivm suite run on-branch:
**1 failing test** (F1). Mono napi tests not executed (vitest not run).

## What the branch is

- **In-process transport**: `addon.c` dlopens `libgoivm.so` (Go c-shared,
  `napilib` tag). Requests: JS → `goivm_send` (copies, unbounded Go-side
  queue, never blocks JS). Responses: Go → C callback → **one bounded TSFN
  queue (8192, `napi_tsfn_blocking`)** → JS. The Go host wires the existing
  `handleConnection` to a `net.Pipe` — the production dispatch/FIFO/flusher
  path runs byte-identical; no protocol fork. Backpressure preserved on both
  planes (slow JS blocks the TSFN call → blocks the deliverer, same chain as
  a slow socket).
- **Row plane (`napiRowMode`, default ON)**: streaming RPCs opt into per-row
  delivery — `groupDef` (kind 2) + flat LE binary row records (kind 3)
  decoded JS-side with a DataView; msgpack only for control frames +
  fallback rows. Engine gains `AdvanceStreamChunked`/`AddQueriesStreamChunked`
  with chunkSize=1 for this. External Buffers with
  `napi_adjust_external_memory` accounting both directions (the ART RSS
  fix, 37d1ef65f/82af358).
- **Parallel advance fan-out** (68e35bc): `tablesource.Source` push fanout now
  runs per-QUERY-group goroutines (group = engine queryID tagged at Connect;
  companions share the parent's group — verified). Per-goroutine recover
  with drift/non-drift split mirrors ivm/parallel.go. Serial fallback knob
  `GO_IVM_PARALLEL_ADVANCE=false`.
- **Lazy advance leaf fetch** (7569e38, default ON, `GO_IVM_LAZY_ADVANCE=false`
  reverts): advance-time fetches stream off a live cursor; stmt cache moved
  to CHECKOUT semantics (a shared live sqlite3_stmt re-Query silently resets
  the open cursor — verified experimentally per comments; checkout prevents).
- **Defaults flipped** (524f13d): chunk size 10000→100 (`GO_IVM_CHUNK_SIZE`),
  one parallelism knob (`GO_IVM_PARALLELISM`, lanes=P, readers floor 2P),
  streaming + parallel + warm pool always on.
- **Native-leak fixes**: pool `SetConnMaxIdleTime` (90s default — SQLite page
  cache is C heap, invisible to GOMEMLIMIT) + idle-group reaper now also run
  by the NAPI host (`abi.go` runs `runReaper`; socket path had it in main()).

## Findings

### F1 (HIGH, branch is RED): dead kill switch `GO_IVM_WARM_HYDRATE_POOL`
`server.warmHydratePoolEnabled` is parsed from env (abi.go:106, main.go) but
**consumed nowhere** — 524f13d removed the gate from
`buildWarmReaderPoolLocked` without removing the field/env/test.
`TestBuildWarmReaderPool_Guards/feature_off` fails on-branch: with the flag
off, the build proceeds and a capture failure increments the warm-SERIAL
metric (mislabeled guard). Decide: (a) restore the one-line gate — keeps the
documented escape hatch for a fresh prod default (recommended for first
rollout), or (b) delete field+env+test and log "flag deprecated/ignored".
Either way the branch must not ship with a silently-ignored operator knob.

### F2 (HIGH): row-mode within-partial reordering on mixed record/fallback
`rowPlane.emitChanges` delivers encodable rows as records IMMEDIATELY and
ships fallback rows in a frame AFTER the loop. If ONE partial carries ≥2
changes for the SAME rowKey with mixed encodability (e.g. remove-first group:
removes encode, add/edits fall back forever since `g.cols` never fixes), an
original `[add(fallback), remove(record)]` hits the wire as
`[remove, add]` → TS applies the add last → phantom row → drift. chunkSize=1
makes partials single-change on the main path, BUT multi-change partials are
still reachable: the residual drain appends a whole source-change's
slice-mode output (companion emissions) to `pending` before the ≥chunkSize
check — one flush can carry several changes. Fix (cheap): if ANY change in a
partial is unencodable, ship the WHOLE partial as a frame (order trivially
preserved); or flush the pending fallback frame before emitting the next
record. Add a regression test: remove-first group + same-key add/remove in
one partial.

### F3 (HIGH): napi failure story vs Go-primary/lean-primary
On any post-start transport failure the manager goes terminal `failed` and
"dispatch falls back to TS". Under `leanPrimary` TS holds STUB user
pipelines — there is no TS fallback to fall back to; under plain
goPrimaryTrigger the fallback works but silently degrades the fleet member
forever (only a worker restart restores Go). Config guards
externallyManaged+napi but NOT leanPrimary+napi. Recommend: in napi mode,
a terminal transport failure should crash the worker (supervisor restarts it
into a working Go path) — or at minimum forbid `leanPrimary` with
`transport=napi` at config validation.

### F4 (MED): multi-worker memory/CPU sizing in-process
`tuneRuntime` defaults GOMEMLIMIT to **40% of the container cgroup** —
correct for ONE shared sidecar process, but in napi mode every syncer worker
embeds its own Go runtime: N workers × 40% = overcommit → OOM of the whole
pod. GOMAXPROCS similarly unbounded per worker. Recommendation: napi mode
should divide the default by worker count (env visible?) or the config
should require/recommend `ZERO_NUM_SYNC_WORKERS=1` (same guidance as shared
sidecar); document explicitly.

### F5 (MED): late-row error-log storm after RPC timeout
After `cleanup()` clears the row-group registry for a timed-out RPC, every
still-streaming kind-3 record throws "unknown group" → caught → per-row
`error` log (go-ivm-client.ts #handleDelivery catch). A big abandoned advance
= thousands of error logs on the JS thread during exactly the overload that
caused the timeout (positive feedback). Fix: consult the `#recordTimeout`
set before decoding kind-2/3 and drop silently (same policy as late frames).

### F6 (MED): TSFN headroom under row mode
Queue 8192 × blocking. Row mode multiplies deliveries per advance by row
count; one 10k-row advance = >10k queue entries. With a busy JS thread the
Go worker blocks mid-advance holding e.mu (accepted, same as slow socket) —
but the threshold is now hit by a single large advance rather than sustained
overload. Consider making TSFN_MAX_QUEUE an addon option and/or capturing a
queue-depth metric before prod.

### Notes (LOW)
- `addon.c` uses `assert(status == napi_ok)` — compiled out under NDEBUG;
  release builds silently ignore napi failures. Convert load-bearing ones to
  real checks.
- `deliver_from_go`'s unlocked `g_bridge.tsfn == NULL` check is safe ONLY
  because `goivm_shutdown()` (Go host fully drained, closeAll waits on
  group.mu) completes before the addon releases the TSFN. Document as an
  invariant; a future reorder breaks it as a use-after-free.
- `rowValI64 → BigInt` decode path: effectively unreachable (FromSQLiteType
  guards >2^53; engine numerics are f64), but a BigInt row value would throw
  in downstream JSON serialization. Consider encoding i64 outside ±2^53 as
  blob instead.
- Remove-first groups permanently fall back add/edits to frames (cols never
  fixed) — correct-over-fast; remove-heavy tables lose most of the row-plane
  win. A supplementary-def record kind would fix it later.
- Boot log "hydrate config: streaming=%v" prints `advanceDriveEnabled` as
  the streaming value — misleading when eyeballing logs.
- `napi.send` return-code path rejects the RPC but does NOT trigger the
  restart machinery (rc!=0 = host closed) — callers keep failing per-call
  until something else notices. Consider routing rc!=0 into
  `#handleRestartTrigger` (which in napi mode goes terminal → F3 story).

## Verified sound (checked, no action)
- Frame pump reuses `handleConnection` verbatim; ordering invariant for the
  row plane (records direct from handler goroutine; done frame necessarily
  after via pipe) holds given single TSFN queue.
- cgo pointer rules respected both directions (copy-on-send, copy-in-callback,
  duration-of-call payload contract).
- ABI version gate on dlopen; one-host-per-process guard both sides;
  Go-runtime-cannot-restart documented and enforced (`started` stays true).
- Parallel fanout: group = queryID incl. companions (delegate-threaded);
  overlay read-only during fanout; per-goroutine recover with deterministic
  re-raise priority; serial knob; group-major output order; -race parity
  tests on both engine and tablesource levels.
- Checkout stmt cache correctly prevents live-cursor reset corruption; lazy
  advance parity-tested vs eager.
- Idle-conn deadline (90s) only reaps pool-idle conns — prevConns/snapshot
  conns/reader pools untouched.
- Row-record encode/decode locked by mirrored tests both repos; group
  registry cleaned on RPC settle (resolve/reject/timeout).
- External-buffer memory accounting registered on create and reversed in the
  finalizer; copy fallback when external buffers are refused.

## Pass 2 — porting / streaming / parallelization / transport deep-dive

### F7 (MED-HIGH): `fetchDuringPushStream` can wedge `s.mu` forever
The lazy advance fetch's locked section uses MANUAL Lock/Unlock (unlike
`fetchForConn`'s `defer s.mu.Unlock()`), and `overlaySplicePlan` + the
effective comparator run INSIDE that window (source.go ~1378-1386). Any
panic there — `CompareValues` raises `*ivm.DataError` on a non-scalar or
mismatched-type value in the OVERLAY row (JSON column in a sort key, poison
value) — unwinds past the manual Unlock and leaves `s.mu` held forever:
every later Push/Fetch on that source blocks, the CG wedges silently (no
crash, no error frame, no restart trigger — strictly worse than the panic).
The explicit-unlock error paths (ensurePrevTx, checkout) are handled; the
splice-plan window is not. Fix: restructure the locked section as
`func() { s.mu.Lock(); defer s.mu.Unlock(); ... }()` capturing the splice
plan, or move the splice-plan computation out of the lock (it only needs the
overlay pointer + epoch snapshot). Add a poison-overlay regression test.

### Verified sound in pass 2 (each chased to ground, no action)
- **Pool bound during advance is impossible** (the frame-visibility hazard):
  warm co-read pools are ephemeral — built and `defer`-torn-down inside the
  same `group.mu`-held hydrate call (main.go:1765/1848); the cold pool is
  torn down at the first advance; advance handlers hold `group.mu`. A
  pool-routed fetch can therefore never shadow the prev-tx+overlay read
  during a push.
- **Split-edit phase ordering under parallel fanout**: the remove-phase
  `fanOut` joins (wg.Wait) before the add-phase starts and before each
  `writeChange` — TS's remove→write→add→write sequence is preserved.
- **Source-level drift checks never fire inside fanout goroutines**
  (`driftCheckLocked` runs before the overlay is set, on the caller);
  operator-level drift (Take stale-bound) inside a group goroutine is
  recovered per-slot and re-raised deterministically.
- **Per-query wire order in row mode**: one group goroutine → sequential
  operator emissions → chunk-mode Accumulate (per-call local buffer) →
  `sendFrame` under flushMu → rowPlane under rp.mu → single TSFN. A query's
  rows keep push order end-to-end; cross-query interleave is unordered by
  design and demuxed by queryID (CVR merge is per-(query,row) — commutative).
- **Drift + row mode**: pre-drift rows already delivered as records
  accumulate in `acc`; the Drift rides the terminal frame; `finish()`
  attaches partials to the DriftError — identical contract to frame mode.
- **Lazy-cursor cleanup**: `defer rows.Close()` + `defer returnSelectStmt`
  fire on both panic and early yield-stop (LIFO order correct) — but only
  AFTER the locked window, which is exactly why F7 exists.
- **Hydrate row-mode accumulator**: per-query routing via groupDef queryID,
  duplicate-final guards, non-decreasing chunkIndex relaxation — correct.
- **Empty advance / reset paths in row mode**: reset aborts before the
  engine apply and ships via streamW with no records in flight — ordering
  trivially holds.

### Pass-2 notes (LOW)
- `chunked` + `rowMode` hydrate: onRow deliveries use a SYNTHETIC per-query
  `rowChunkIndex` starting at 0 while fallback/final frames carry the
  ENGINE's chunkIndex — the two sequences can collide numerically. Current
  consumers are invocation-order-driven so it's cosmetic, but it's a trap
  for any future consumer that keys on (queryID, chunkIndex).
- `putValue` int-width asymmetry: `int8/int16/uint*` fall to the msgpack-blob
  branch rather than `rowValI64` (unreachable given f64 normalization, but
  asymmetric with `toFloat64`'s completed matrix — cheap to align).
- `putShortStr` silently TRUNCATES identifiers >64KB (queryID/table/column
  names). Never legitimate — prefer fail-loud (fallback to frame path) over
  silent truncation.

## Pass 3 — the other angles: operations, observability, limits, CI

### O1 (MED-HIGH, observability): napi mode is BLIND — no pprof, no PERF line
`GO_IVM_PPROF_ADDR` handling and the 10-second `[GO-IVM][PERF]` metrics
reporter both live in `main()` (main.go:2538 / :2601) — the NAPI host runs
`handleConnection` but never `main()`, so neither exists in-process. abi.go
replicated the idle reaper for exactly this reason but missed these two.
These are THE tools every perf investigation on this project used (pprof
pinned the EXISTS N+1, the pin race, the GC ceiling; the PERF line is what
every soak greps). Fix: start both from `startABIHost`. pprof needs care:
with workers>1 a fixed port collides — derive from PID or require explicit
per-worker config, and keep main()'s S3 bind-address guard (pprof is an
RCE-grade surface).

### O2 (MED, blast radius): chunk default 10000→100 hits the SOCKET path too
`defaultChunkSize = envChunkSize("GO_IVM_CHUNK_SIZE", 100)` is engine-level
and transport-agnostic. Any SOCKET deployment upgrading to this binary
silently multiplies frame count ~100× per large advance/hydrate (per-frame
syscalls + length prefixes + TS-side decode dispatch — the exact overhead
napi exists to avoid, imposed on the transport that still pays it). If
intended (streaming everywhere), it needs a release note + a socket-path
soak at chunk=100; otherwise gate the new default on the in-process
transport and keep 10000 for socket.

### R1 (MED, limits): the row plane has no oversize guard
`capFrameBytes` (64MB cap → loud error frame) protects the frame plane and
the row plane's FALLBACK frames — but kind-3 records themselves are
unbounded (value strings/blobs carry u32 lengths, up to 4GB). One fat JSON
value = one giant Go buffer → malloc+memcpy in the addon → external Buffer,
no cap, no error path. Fix: size-check in `encodeRow`/`putValue` and fall
back to the frame path (which caps) above a threshold.

### C1 (LOW-MED, crash forensics): Go fatal inside the worker
GOTRACEBACK is unset; a Go runtime fatal (not panic — those are recovered)
now dumps its goroutine trace into the WORKER's stderr and kills the whole
worker. Ensure the log pipeline keeps multi-hundred-KB single writes, set
GOTRACEBACK explicitly, and alert on `fatal error:` in worker logs — a
K8s restart otherwise makes these invisible (they were process-scoped and
obvious when the sidecar was its own pod process).

### T1 (MED, CI): napilib code + e2e path have no CI enforcement
`napi_lib.go` compiles only under `-tags napilib` — `go test ./...` never
builds it (verified: `go build -tags napilib` passes today, but only the
Docker c-shared stage would catch a regression). The mono napi e2e suites
`describe.skipIf(!available)` on `/tmp/libgoivm.*` — permanently skipped in
CI unless a job builds the artifacts. Add: (a) a `go build -tags
"napilib libsqlite3"` CI step in go-ivm; (b) a mono CI job that builds the
.so + addon and runs napi-transport/napi-records e2e (otherwise the ONLY
e2e coverage of the addon is manual).

### Notes
- Deliberate and correct: `stop()` does NOT call `addon.shutdown()` (worker
  teardown order vs pending TSFN); TSFN unref'd so an idle worker can exit;
  process-per-worker makes exit cleanup kernel-side.
- Per-row addon cost: 2 mallocs + 1 TSFN entry per row — allocator churn at
  high row rates; a freelist is a later perf lever, not correctness.
- fd budget: each worker's in-process pools default to 256+32 conns; with
  workers>1 multiply accordingly (same F4 sizing conversation).
- dlopen trust: `napiLibPath` is operator config (same trust domain as
  binaryPath); bare-name default resolves via LD_LIBRARY_PATH — docs already
  say use absolute paths in containers.
- TS RecordReader: `subarray` clamps on truncated records before the
  trailing-bytes check fires — acceptable for an in-process trusted producer;
  keep the trailing-byte asserts.

## Pass 4 — the PROD path end-to-end (Go-primary trigger, drive,
## advanceToHeadStream(rowMode), per-chunk hydrate)

Traced: version-ready → pipeline-driver `advanceToHeadStream(rowMode)` →
group.mu → `tearDownReaderPool` → `snap.Advance` (leapfrog) → bind sources
to diff.Prev() → maxDiffChanges guard → `diff.Collect` → parallel-fanout +
lazy-fetch apply via `AdvanceStreamChunked(1)` → rowPlane → TSFN →
accumulator → watermark reconcile → CVR. Plus the hydrate path (cold pool /
warm co-read / per-chunk delivery).

### P1 (MED): `rebindCurr` is skipped when the apply panics (both drive handlers)
Sources are explicitly bound to `diff.Prev().Conn()` for the apply
(:485/:675); the rebind to `diff.Curr()` is a plain call after the apply
returns (:538, :748, :779). `AdvanceStream` deliberately RE-RAISES non-drift
panics after its terminal flush — that unwind skips every `rebindCurr()`,
leaving all sources bound to the conn that the next leapfrog turns into the
rolled-back/re-pinned PREV. Window consequences: any hydrate / drift-audit
read until the next advance reads ONE FRAME BEHIND head, so a query
registered in that window misses the N-1→N delta permanently (the silent
staleness class). Mitigation that caps severity: a -32000 advance failure
drives TS's `#scheduleGoReset` → `resetEngine()` = destroy+re-init (fresh
bindings, fresh hydrates) — but that reset is async + best-effort
(3 retries), and DataError-class panics (-32102) tear down anyway; the
exposed case is the reset-retry-exhaustion / race window. Fix is one line
per handler: make the curr-rebind a `defer` (double-bind is idempotent —
the error paths that already call rebindCurr stay correct).

### P2 (LOW-MED): `recordAdvanceChunks` counts ROWS in row mode
With chunkSize=1 the Final's ChunkIndex+1 == row count, so the
advance-chunks histogram silently changes meaning between socket (chunks of
100) and napi rowMode (rows). Any dashboard comparing the two transports —
exactly what the A/B rollout will do — reads apples vs oranges. Emit rows
and chunks as separate metrics, or normalize.

### Verified sound on the prod path (each traced, no action)
- **Hydrate-frame ↔ diff-prev coordination**: cold hydrate pins/refreshes
  curr, first advance's leapfrog makes that exact frame the diff's PREV —
  no gap, no double-apply; warm hydrates co-read curr's existing frame and
  the next diff starts there; all under group.mu.
- **Reader-pool rotation safety**: `tearDownReaderPool` before the leapfrog
  (:471/:663); warm pools ephemeral within their hydrate call.
- **Reset path (truncate/schema/permissions)**: `rebindCurr()` runs BEFORE
  the reset Final frame is emitted, so the re-hydrate TS performs at
  `version` reads exactly that frame.
- **Eager `diff.Collect` is bounded**: GO_IVM_MAX_DIFF_CHANGES (50k default)
  refuses oversized catch-up diffs with a loud error → TS resets and
  re-hydrates with bounded memory (the old "advanceToHead unstreamed"
  finding is now input-bounded + output-streamed).
- **Leapfrog error atomicity**: `advanceWithoutDiffLocked` swaps prev/curr
  only after `resetToHead` succeeds; an error leaves the snapshotter state
  unchanged and the next Advance retries.
- **Drift on the drive path**: recovered in-engine (no panic escapes), Final
  carries Drift, rebindCurr runs, TS re-inits via the drift pipeline.
- **rowMode watermark semantics**: rows accumulate client-side and commit
  transactionally at finish() — per-row delivery changes decode cost, not
  CVR atomicity; version/numChanges ride the Final only.
- **TS recovery for unclassified advance errors** = destroy + re-init
  (`resetEngine`), with in-flight-reset dedup, snapshotter-destroyed guard,
  and dirty-retry — the strongest of the recovery ladders.

## Pass 5 — packaging, topology, retry semantics, hydrate consumer

### Found in flight (commit it)
mono has UNCOMMITTED addon.c changes implementing two review findings —
the NDEBUG-proof `GOIVM_FATAL_IF` macro (LOW-1) and a tunable TSFN queue
(`GOIVM_NAPI_QUEUE_MAX`, F6). Don't lose them.

### P3 (LOW-MED): hydrate retry can double-yield a query into the CVR consumer
`goHydrateBatchStream`'s `buffered` array and `byQueryID` map are closures
SHARED across `#withReinitRetry` attempts, and `byQueryID` is never pruned
on a query's final chunk. A restart mid-`hydrateManyStream` (socket mode
only — napi is terminal, no retry) makes the retry re-deliver every query;
already-yielded queries pass the `byQueryID` check and are yielded AGAIN —
duplicate hydration rows into the view-syncer's CVR updater + double
final-gated metrics. Pre-existing (not napi-specific), narrow (restart must
land mid-hydrate), CVR merge is likely idempotent for identical rows — but
prune `byQueryID` on final (one line) to close it.

### Verified sound in pass 5
- **Client topology**: exactly ONE GoIVMClient in napi mode (created by the
  manager's `#startNapi`, shared via `getClient()` by every
  GoComputeBackend) — so the global + per-CG in-flight slot caps apply
  across all CGs of the worker, matching the socket shape.
- **Retry × accumulator**: each retry attempt constructs a FRESH stream
  accumulator inside the client call — no cross-attempt double-count at the
  accumulator level (the P3 duplication is one layer up).
- **Packaging**: `build/` is gitignored (no committed binaries);
  `Dockerfile.go-ivm` builds the addon at image build (`node-gyp rebuild`
  + `test -f build/Release/goivm_napi.node` gate) and bundles libgoivm.so
  from the go-ivm image; the entrypoint runs zero-cache FROM SOURCE via
  `npx tsx`, so index.ts's import.meta.url-relative addon path resolves.
  Fragility note: bundling zero-cache to dist would silently break the
  addon path — the loader's descriptive throw covers it, but remember this
  if the runtime packaging ever changes.
- **Per-chunk hydrate consumer**: sub-batched (`GO_HYDRATE_SUB_BATCH`)
  bounding heap to one sub-batch's results; mid-stream RPC errors propagate
  as a throw out of the async generator (→ the hydrate error path); stub
  pipeline registered on first chunk and refreshed with real timing on the
  final; yield-token dropping for internal queries documented and safe.
- **F4 drift breaker** wired identically for advanceToHeadStream (record +
  maybe-trip on DriftError before re-throw) — the trigger-mode breaker that
  was dead pre-branch stays alive on the streaming variant.

## Recommended before merge/deploy
1. Fix F1 (branch red) + decide the flag story.
2. Fix F2 with the whole-partial fallback rule + regression test.
3. Guard F3 (leanPrimary×napi) in zero-config validation; decide crash-vs-
   degrade for terminal napi failure.
4. Size F4 explicitly (workers=1 guidance or divided GOMEMLIMIT).
5. Run the mono napi vitest suites + a kill/chaos soak (review item #7 from
   the edge audit) against the napi build — the RESILIENCE gates were
   validated on the socket transport only.
