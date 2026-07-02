# Edge-Case / Error-Handling Audit — Go-primary prod path

Date: 2026-07-02. Branch: `feat/streaming-hydrate` (streaming hydrate + advance,
parallel hydrate lanes + reader pool, stream-by-default).

Scope: what stands between this branch and prod. Method: walk the TS
zero-cache error ladder (view-syncer / pipeline-driver / go-ivm-client),
walk the Go side (engine / ivm / tablesource / sidecar RPC), find the seams,
test the unhappy paths.

## 1. The recovery ladder (TS ↔ Go contract)

TS zero-cache degrades in three tiers (view-syncer.ts):

| Tier | Trigger | Effect |
|---|---|---|
| 1. Pipeline reset | `ResetPipelinesSignal` returned by advance | re-hydrate CG in place; clients keep their connection |
| 2. CG teardown | reset circuit breaker (`RESET_CIRCUIT_LIMIT` resets / window), or any error escaping `run()` | clients reconnect into a fresh CG |
| 3. Process supervision | worker crash | K8s restart |

The Go sidecar maps its failures onto that ladder:

- `*ivm.DriftError` (source state diverged: edit/remove of a missing row,
  dup add, Take stale bound, Join key change) → **recovered in-band**:
  `AdvanceStream` puts `Drift` on the terminal Final frame → TS
  `classifyGoAdvanceFailure` → DROP → `ResetPipelinesSignal` → Tier 1.
- `*ivm.DataError` (poison input: unsupported json column, unknown table,
  int > MAX_SAFE_INTEGER) → panic → handler recover → RPC error **-32102**
  → TS tears the CG down (Tier 2 directly — a reset would re-read the same
  bad row and loop; this is the pre-prod reset-storm lesson).
- Any other panic (programmer bug, wire hiccup) → handler recover → RPC
  error **-32000** → TS 'unclassified' → Tier 1 reset.
- Sidecar process death → SidecarManager respawn/reconnect + re-init;
  restart cap 5/60s → fall back to TS path (Go-primary: reset).

Panic-recovery coverage points (each is load-bearing — a panic on a plain
goroutine kills the whole multi-CG process):

| Site | Covers |
|---|---|
| `ivm/parallel.go:105` per-goroutine recover | parallel push fan-out (MemorySource/shadow only) |
| `engine.go` hydrate-lane recovers (AddQueries / AddQueriesStream) | per-query hydrate panics → returned error |
| `engine.Advance` / `AdvanceStream` recover | drift in-band, non-drift re-raise after clean Final frame |
| `cmd/sidecar handleRequest` / `handleStreamWithRecover` | last line of defense → RPC error, classified |

Prod-topology note: table mode (`internal/tablesource.Source.Push`) fans out
to connections **serially** on the advance goroutine — parallel push +
its recover matrix applies only to MemorySource (shadow / tests). Prod
parallelism = hydrate lanes + frame-pinned reader pool + cross-CG.

## 2. Findings this pass (fixed + pinned by new tests)

### F1 — `AdvanceStream.sendFrame` deadlock on panicking sink (FIXED)
`sendFrame` held `flushMu` across the caller-supplied `onResult` without a
deferred unlock. A panicking sink left `flushMu` locked; the recover captured
the panic and the *guaranteed* terminal `flush(true)` then deadlocked on
`flushMu.Lock()` — wedging the engine **with `e.mu` held**, i.e. every CG
sharing the engine, forever (no panic, no error, no restart trigger).
Fix: deferred unlock; the sink panic now escapes to the handler recover →
RPC error → reset. Test: `TestAdvanceStream_PanickingSink_NoDeadlockAndEngineReusable`
(fails by 10s timeout pre-fix).

### F2 — batch build panic leaked registered-but-unhydrated pipelines (FIXED)
`AddQueries`/`AddQueriesStream` Phase 1 builds sequentially; a build panic on
query k (e.g. unknown table → `*ivm.DataError`, builder.go:101) left queries
0..k-1 registered with wired outputs but never hydrated. The batch RPC
rejects, so TS doesn't know they exist — yet they emit RowChanges on every
subsequent advance, and un-hydrated operators (Take before its bound is set)
panic on push → one bad query in a batch becomes an advance-time reset loop.
Fix: `buildBatchLocked` unwinds every entry registered by the failing call,
then re-raises (preserving the -32102 classification). Residual: the failing
query's own partially-built source connections (nil output — skipped by push)
are a bounded memory leak until CG destroy; documented, not unwound.
Tests: `TestAddQueries_BuildPanicMidBatch_UnwindsRegistered`,
`TestAddQueriesStream_BuildPanicMidBatch_UnwindsRegistered`.

### F3 — panic→RPC classification seam had zero tests (PINNED)
`panicErrorCode` / `handleStreamWithRecover` / `handleRequest` decide
Tier 1 (reset, -32000) vs Tier 2 (teardown, -32102). Misclassifying DataError
as -32000 recreates the reset storm; misclassifying transient panics as
-32102 tears CGs down needlessly. Now pinned:
`cmd/sidecar/panic_classification_test.go` (DataError → -32102, string/error/
DriftError → -32000, response carries the request ID, unknown method → -32601).

### F4 — drift after flushed chunks (PINNED)
Existing drift test only covered drift on the FIRST change (no frames on the
wire yet). Streaming means drift now regularly arrives after partial frames.
Pinned: contiguous chunkIndex through the drift, Drift only on the single
terminal Final frame, pre-drift output emitted (TS partial-emit parity),
engine clean on the next advance.
Test: `TestAdvanceStream_DriftAfterFlushedChunks`.

### F5 — byte-based chunk flush had zero coverage (PINNED)
Every streaming test crossed the ROW threshold; `softChunkBytes` — the lever
that actually protects prod against the fat-frame freeze (frame >
`maxFrameSize` → TS reader skips → 60s orphan timeout) — was untested on all
three surfaces. Tests: `TestAddQueriesStream_FatRows_ByteFlush`,
`TestAdvanceStream_FatRows_ByteFlush`, `TestAdvanceStream_OperatorChunkSink_ByteFlush`.

## 3. Already-covered edges (verified, no action)

- **Hydrate panic → error, not crash** (nil-PK row): `hydrate_panic_recover_test.go`.
- **Parallel-push panic split** (DriftError → recoverable / other → re-raise,
  non-drift priority): `parallel_panic_recovery_test.go`.
- **Chunk-sink concurrency** (flushMu monotonic chunkIndex under parallel
  fan-out, -race): `advance_operator_chunking_test.go`.
- **Closed-engine streaming calls** → `ErrEngineClosed` (not silent empty
  success): `engine_streaming_test.go`.
- **Non-drift panic mid-advance** → empty Final frame BEFORE the re-raise so
  TS never sees "finished without a final chunk" (wire stays clean): built
  into `AdvanceStream`; exercised via F1's test.
- **Streamer residue across an aborted advance** (HIGH-10): drained in both
  recover paths; pinned by F1/F4 follow-up-advance assertions.
- **Oversized wire frame** → substituted error frame instead of orphaning the
  RPC: `frame_cap_test.go`.
- **Reader-pool bind failure** → serial fallback (non-wal2):
  `coread_fallback_test.go`; pin race reworked in 6454980, validated live.
- **Group teardown / orphan groups / destroy epoch**:
  `client_group_shutdown_test.go`, `orphan_group_test.go`, `destroy_epoch_test.go`.
- **advanceToHeadStream truncate/schema-change → reset frame**:
  `advance_to_head_stream_test.go`.
- **Intra-batch dup remove / SET+DEL** (the pre-prod reset storm):
  `duplicate_remove_test.go` + tablesource BUG 1/1b/1c handling.
- **Connection teardown ordering**: reader exit → in-flight handlers complete
  (writerWg → respCh) → only then flushCh closes. streamW during the window
  writes to the dead socket; error swallowed; no send-on-closed-channel.

## 4. Known gaps — accepted / deferred (with reasons)

1. **Backpressure wedge (the big one, design-level)**: a slow/stalled TS
   reader fills flushCh (256) → outC (1024) → engine blocks mid-advance with
   `e.mu` held. Same triple coupling documented in
   DESIGN-streaming-hydrate.md (writeMu-blocking-write / hydrateReaders /
   group.mu). Mitigation plan exists (Phase 0 single-flusher shipped; CORE =
   spill-on-backpressure). Not a quick test; tracked as the streaming-hydrate
   plan's next phase.
2. **No time-slicing during Go hydrate/advance**: TS yields to the event loop
   (`view-syncer.yield-during-*.pg.test.ts`); Go holds `e.mu` for the batch.
   Cross-CG concurrency comes from per-CG engines instead. Accepted design
   difference (documented in DESIGN docs).
3. **`tablesource` writeChange/ensurePrevTx failure** panics with a plain
   string (→ -32000 → reset). Correct behavior; not separately unit-tested
   (requires corrupting the replica DB mid-tx). Accepted.
4. **AddQuery replacing an existing healthy query, then build panics**: the
   old query is removed before the new build — TS believes it still exists.
   TS-side recovery (reset → full re-register) heals it. Low priority.
5. **Partial-build source-connection leak** (F2 residual): nil-output
   connections from the panicking query's own build survive until CG destroy.
   Memory-only; bounded by reset/teardown.
6. **Doc inconsistency**: `AdvanceStreamPartial.Drift` comment says TS "must
   discard ALL partial frames" on drift, while the drift path deliberately
   emits pre-drift partial output for TS parity. Behavior is safe either way
   (drift → full re-hydrate supersedes), but the comment and the TS client
   should agree on one story.

## 5. Test inventory added this pass

```
engine/advance_stream_sink_panic_test.go        F1 deadlock + engine reuse
engine/addqueries_build_unwind_test.go          F2 unwind (AddQueries + Stream)
engine/advance_stream_drift_after_chunks_test.go F4 mid-stream drift
engine/stream_byte_flush_test.go                F5 byte lever ×3 surfaces
cmd/sidecar/panic_classification_test.go        F3 RPC classification
```
