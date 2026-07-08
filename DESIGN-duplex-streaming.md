# DESIGN: Duplex streaming — push-based advances, pull-based hydration, end-to-end

Status: proposal v1 · Created 2026-07-06
Unifies and supersedes-as-contract: DESIGN-streaming-hydrate.md (SHIPPED),
DESIGN-streaming-advance.md (SHIPPED, incl. mid-flatten chunk sink),
DESIGN-abi-v3-pull.md (APPROVED direction, absorbed + refined here).
Grounding: every file:line verified against go-ivm @ c11491a and
mono @ HEAD in session of 2026-07-06.

---

## 1. The TS reference model (ground truth)

TS runs **two asymmetric streaming disciplines** over the same operator
graph. The asymmetry is the point — it is not an accident of JS:

### 1a. Hydration = PULL (demand-driven, consumer→producer)

```
WebSocket poker demand
  → view-syncer for..of / yield*        (view-syncer.ts:1608, :2139-2166)
    → pipeline-driver function* hydrate (pipeline-driver.ts:6299)
      → input.fetch({})                 (lazy Stream<Node>, operator.ts:43)
        → operator generator chain
          → SQLite cursor
```

- `function* hydrate` calls `input.fetch({})` and hands the **lazy**
  iterable to a `Streamer` whose `.stream()` is itself a generator
  (`yield* streamer.stream()`, pipeline-driver.ts:6310). Nothing executes
  until the consumer calls `next()`.
- The view-syncer pulls one element at a time and forwards to the pokers
  "AS SOON as available" (view-syncer.ts:2139), awaiting downstream
  (`yieldProcess`, :2131, :1616) between pulls. **Consumer demand is the
  clock.** A suspended generator suspends the whole chain, cursor included.
- `return()` on the iterator (early client close) unwinds every generator
  frame via their `finally` blocks — cursors close, nothing else runs.

### 1b. Advance = PUSH (source-driven, producer→consumer)

```
replication log → snapshotter.advance() diff   (pipeline-driver.ts:3864)
  → per-change source.push(change)
    → operator output.push chain (depth-first, synchronous;
      join.ts:132-140 / :231-244; fan-out accumulation push-accumulated.ts)
      → Streamer flattens INSIDE the push (#streamNodes generator)
        → view-syncer drains the advance iterable TO COMPLETION
```

- Changes propagate by **calls**, not demand: each operator invokes
  `output.push(...)` on its downstream the moment it derives a change.
- The advance result iterable (pipeline-driver.ts:3932) is consumed
  synchronously and completely by the view-syncer — an advance that is
  half-applied is drift by definition. There is **no demand gating inside
  an advance**; backpressure exists only at the poke/socket layer after
  the diff is fully derived.

### 1c. Why the asymmetry is semantic, not stylistic

- Hydration output is **re-derivable at any prefix**: stopping after N
  rows loses nothing (the query can re-run). So laziness + cancellation
  are safe and cheap — pull is correct.
- An advance is a **state transition**: the engine's operator state
  (join #storage, take bounds, exists counts) mutates as the push runs.
  Consumers cannot decline the tail of a transition. So push-to-completion
  is the only correct discipline — gating it on a slow consumer would
  wedge the engine's ability to advance at all.

Any faithful port must reproduce **both** disciplines **at every layer**:
operators, engine loop, and the Go↔JS boundary.

---

## 2. Current Go state — layer-by-layer fidelity audit

| Layer | Direction | Current mechanism | Faithful? |
|---|---|---|---|
| Operators | fetch | `Fetch(req) iter.Seq[Node]` (operator.go:141) — lazy, break-unwinds via defers | **YES** (shipped, DESIGN-streaming-hydrate) |
| Operators | push | `Output.Push(change) []Change` call chain; FanIn accumulates (fan_in.go:56) per push-accumulated.ts | **YES** (see §2a) |
| Terminal sink | push | `Streamer.Accumulate` flattens **during** push (streamer.go:122), mid-flatten chunk sink (streamer.go:110, :154-162) | **YES** (shipped, DESIGN-streaming-advance + chunk sink) |
| Engine hydrate | pull | producer ranges lazily: `for node := range entry.pipeline.Input.Fetch(...)` (engine.go:966), chunk-flushes via `onResult` | **producer-side yes** — but demand never reaches it |
| Engine advance | push | `AdvanceStream` pushes per source-change, chunkSink flushes mid-flatten (engine.go:1329-1335) | **YES** |
| NAPI boundary | hydrate | TSFN queue, fixed 8192 window; **ABI v5 (2026-07-10):** nonblocking enqueue + congestion STAGING (records coalesce into kind-5 batches while the queue is full — the producer keeps producing) + event-driven drain wakeup (`goivm_queue_drained`) | pull credits close G1; staging makes queue occupancy demand-shaped (batches, not rows) — hydrate rows are all pre-credited so demand remains the clock |
| NAPI boundary | advance | same TSFN queue; v5 staging bounded by the stage hard bound (256 recs / 1MB) whose park is the backpressure, event-woken, with the `GO_IVM_DELIVER_TIMEOUT` tripwire | **YES** (TS drains advance synchronously; a bounded, event-woken buffer is the faithful equivalent — and the v4 sleep-poll's dead-air tax is gone) |
| Cancellation | hydrate | none — `onResult` cannot abort; client close ≠ `.return()` | **NO — gap G2** |
| Advance-diff derivation | push | changelog materialized up to `GO_IVM_MAX_DIFF_CHANGES` (50k) before pushing | **NO — gap G4** (TS's changelog cursor feeds `#advance` lazily) |

### 2a. Why the operator layer needs NO further work — pinned

This design deliberately **freezes** the operator contracts. Both are
already the faithful translations:

- **`Fetch(req) iter.Seq[Node]`** is `Stream<Node>` minus the `'yield'`
  scheduling token (Go's scheduler is preemptive; operator.go:5-9).
  Crucially, `iter.Seq` composition is *synchronous on the consumer's
  goroutine*: when the terminal consumer stops consuming (parks), every
  operator frame and the SQLite cursor suspend mid-iteration — **exactly**
  TS generator suspension. Pull demand therefore propagates through the
  operator graph for free; no operator changes are needed to make
  hydration demand-driven. Early stop (`break` = yield-returns-false)
  unwinds the chain via defers → cursor/pool-reader release
  (operator.go:134-137).
- **`Output.Push(change, pusher) []Change`** is the push chain. The
  `[]Change` return is plumbing for the fan-out/fan-in accumulation
  barrier (fan_out.go:68-77 → fan_in.go:62 → push_accumulated.go:18),
  a line-by-line port of TS push-accumulated.ts; the terminal sink
  returns nil. TS's `push(): Stream<'yield'>` carries only scheduling
  tokens, never rows — dropping it is the same `'yield'` erasure as in
  fetch. Flatten-in-push (streamer.go:118-169) already runs the flatten
  while the overlay + `join.inprogressChildChange` are live
  (DESIGN-streaming-advance §3 — the load-bearing correctness invariant),
  and the chunk sink already bounds a single source-change's fan-out to
  ~one chunk.

**The entire remaining gap is at and above the NAPI boundary.** The user
-visible symptom of that gap: JS consumer demand and JS-side cancellation
do not cross into Go — Go hydration runs open-loop into a standing 8192
window regardless of what the client does with the rows.

---

## 3. Gaps this design closes

- **G1 — hydration demand doesn't cross the boundary.** Go produces
  hydrate rows at its own pace; the TSFN window is backpressure (blocks
  the producer when JS is 8192 items behind), not demand (JS `next()`
  does not cause production). TS semantics: one `next()` ⇒ one row.
- **G2 — cancellation doesn't cross.** TS `.return()/.throw()` unwinds
  the generator chain and closes cursors. Go's `onResult` has no abort
  signal; a client that stops caring still pays for the full hydrate
  (and holds its warm-pool reader until exhaustion).
- **G3 — `e.mu` held for the whole hydrate** (engine.go:875-876). Today
  this is merely redundant serialization (group.mu already serializes,
  main.go:1172 comment, :2108); under pull it becomes a hazard — a
  producer parked on client demand while holding `e.mu` would freeze the
  CG's advances for client-think-time. Must be restructured **before**
  parking is possible.
- **G4 — advance-diff derivation is eager.** The changelog scan
  materializes up to 50k changes before the push loop starts. TS feeds
  `#advance` from a lazy changelog cursor. (Push *delivery* is already
  streaming; this is the push lane's input side.)

Non-gaps, pinned so they aren't "fixed" into divergence:
- Advance delivery being push over a bounded blocking queue is faithful
  (§1b/§2). **Never credit-gate the advance lane** — gating a state
  transition on consumer demand is the one way to make this design
  *less* correct than today (D1 below).
- The `[]Change` return on `Output.Push` and FanIn accumulation stay.

---

## 4. Design

### 4.0 Shape: two lanes, one ordered queue

```
                 Go side                                 JS side
  ┌───────────────────────────────────┐      ┌────────────────────────────┐
  │ PULL LANE (hydrate)               │      │                            │
  │  producer goroutine per query     │      │  per-query AsyncIterator   │
  │  parks on streamGate.acquire(1) ──┼──────┼── next(): buffer < W/2 →   │
  │  range Input.Fetch → operators    │credit│  streamCredit(reqID, n)    │
  │  → SQLite cursor (suspended when  │(dlsym│  return()/throw() →        │
  │    parked — whole chain lazy)     │ call)│  streamCancel(reqID)       │
  ├───────────────────────────────────┤      ├────────────────────────────┤
  │ PUSH LANE (advance)               │      │                            │
  │  source.Push → operator chain →   │      │  advance accumulator       │
  │  flatten-in-push → chunkSink →    │      │  (drains synchronously,    │
  │  rowPlane records — NO gate       │      │   as today)                │
  └───────────────┬───────────────────┘      └──────────▲─────────────────┘
                  │        ONE ordered TSFN queue        │
                  └──────── abiDeliver (kind 1/2/3/4) ───┘
```

Both lanes exit through the **same** ordered TSFN queue (`abiDeliver`) —
the rowplane.go:7-22 ordering invariant is untouched. The pull gate sits
**before** enqueue on the hydrate lane only: a parked hydrate enqueues
nothing, so it cannot occupy or reorder the shared queue.

### 4.1 D1 — scope: pull applies to hydrate streams ONLY

`addQueriesStream` / batch row-mode deliveries are credit-gated.
`advanceStream` / `advanceToHeadStream` stay push (§1b, §3 non-gaps).
`GO_IVM_ADVANCE_BUDGET_MS` + a1 reset-classification remain the advance
lane's bounds.

### 4.2 D2 — demand gate (`cmd/sidecar/streamgate.go`, new)

Registry `map[uint64]*streamGate` keyed by numeric reqID, owned by the
Server, own mutex (never `s.mu` — the registry is touched from JS-thread
direct calls and must not couple to server-wide state).

```go
type streamGate struct {
    mu        sync.Mutex
    cond      *sync.Cond    // on mu
    credit    int64
    cancelled bool
    lastGrant time.Time     // idle-timeout + reaper liveness
}
// acquire(1): park while credit==0 && !cancelled; false on cancelled.
// grant(n):   credit += n; lastGrant = now; Broadcast.
// cancel():   cancelled = true; Broadcast. Idempotent.
```

- Created in `newRowPlane` when the request carries `pullMode:true`;
  removed on handler return (defer). Unknown-reqID grant/cancel = no-op
  (stream already settled — benign race, same as late TSFN frames today).
- One gate per **RPC**, not per query: a batch's lanes all draw from the
  same gate, so client demand bounds the batch's aggregate row rate and
  the JS iterator's buffer, regardless of how many queries interleave.
  (JS demultiplexes by queryID via RowGroupRegistry — unchanged.)

### 4.3 D3 — gate placement: per row-BEARING delivery; control bypasses

The rowPlane sink acquires 1 credit before each row-bearing delivery:
kind-3 row records, and kind-1 fallback frames that carry rows (the B2
all-or-nothing partial counts as ONE acquire — the gate wraps whole
deliveries and never splits a partial, so B2 is untouched).

**Free (never gated):** group defs (kind 2 — metadata; deferring them
breaks the "def before first referencing record" ordering contract,
rowplane.go:86-93), the terminal Final frame, and error frames. Gating
any of these deadlocks: the client is waiting for exactly that frame to
decide whether to grant more credit.

With rowMode's chunkSize=1 (engine.go:854-858), one gated delivery ==
one row == literal TS `next()` semantics at window 1.

**Window (refines ABI-v3 D3):** `GO_IVM_PULL_WINDOW`, **default 64**,
low-water refill at W/2 from the JS iterator. W=1 remains the lockstep
test mode ("exact TS ditto" pin, §6). Rationale for not defaulting to 1:
the discipline TS actually exhibits is *bounded lookahead driven by
consumer demand* — a JS generator's consumer also isn't woken per-row by
the event loop for free. W=64 gives identical semantics (produced ≤
consumed + W, cancellation within W rows) while amortizing park/wake and
JS→Go grant calls 32× on the hot path. The efficiency of the shipped
per-row fast path (record aliases encoder scratch, zero copies,
rowplane.go:98-119) is preserved — the gate adds one mutex op per row
uncontended, one park per W rows contended.

### 4.4 D4 — cancel = `.return()`: bool-returning onResult

Engine change (the only one): `onResult func(QueryResult) bool` — false
means "consumer gone: stop producing". The producer loop becomes

```go
for node := range entry.pipeline.Input.Fetch(ivm.FetchRequest{}) {
    ...
    if len(chunk) >= chunkSize || chunkBytes >= softChunkBytes {
        if !flush(false) { return errStreamCancelled }   // breaks the range
    }
}
```

Breaking the range **is** `.return()` propagation: `iter.Seq` defers
unwind the operator chain → cursor close → pool-reader release. This is
the precise Go dual of TS generator `finally` unwind. The sidecar's
rowPlane returns false from onResult when `gate.acquire` reports
cancelled. `errStreamCancelled` maps to a terminal error frame for the
reqID (client already left; the frame is for bookkeeping symmetry and
rides free per D3). Non-pull callers (socket transport, tests) pass an
always-true onResult — zero behavior change.

Cancellation cleanup on the JS side: `return()/throw()` →
`streamCancel(reqID)` + RowGroupRegistry.clearRequest (drop any buffered
records for the reqID — they are prefix rows of a hydrate the CVR never
committed, so dropping is safe; the query re-hydrates on retry).

**All-or-nothing at the RPC level is preserved:** a cancelled or errored
pull-hydrate rejects the whole `addQueriesStream` on the TS side exactly
like a hydrate panic does today (engine.go:903-906) — the CVR transaction
never commits a partial hydrate. Pull adds *earlier abort*, not partial
commit.

### 4.5 D5 — engine lock restructure (G3): shrink `e.mu` to the build phase

`addQueriesStreamChunked` today holds `e.mu` for build **and** drain
(engine.go:875-876). Split:

1. **Build phase** (mutates `e.pipelines`, sources registry, scalar
   resolution, companion wiring — buildBatchLocked): under `e.mu`,
   unchanged.
2. **Drain phase** (the fetch loops): **outside `e.mu`**.
3. **Post phase** (`wireCompanionOutputsLocked`, engine.go:1009-1011):
   re-acquire `e.mu`.

Correctness of the drain-outside-lock rests on: **every source-mutating
engine entry point is serialized against the drain by `group.mu`**, which
the sidecar already holds across the whole hydrate RPC (main.go:1172,
:2108) and across every advance. That makes "sources are read-only during
the hydration window" a *group-level* invariant instead of an
engine-level one — which is exactly TS's model: the view-syncer
serializes hydrate/advance per CG (suspended hydrate generator + syncer
lock); the engine itself has no second lock.

**Implementation gate — the entry-point audit.** Before shipping, table
EVERY exported Engine method that can mutate source state or the
pipeline map, and prove each is (a) called only under `group.mu`, or
(b) made safe explicitly. Known audit list from this session:
`Advance/AdvanceStream/AdvanceToHead*` (group.mu ✓), `AddQuery/AddQueries*`
(group.mu ✓), `RemoveQuery` (verify the removal path that runs on
transform-response), `RefreshAllSources` (engine.go:437 — verify caller),
`OnAdvanceEnd`/`signalAdvanceEnd` (engine.go:460), `SetMinRowVersions`,
`Close` (must cancel gates first, D7), drift-audit timers if any run on
their own goroutine. Any entry found outside `group.mu` gets moved under
it (preferred) or the drain keeps a narrower read lock for that state.

`e.closed` check moves under the build phase; `Close` interlocks via D7.

### 4.6 D6 — parallelization hazards → resolutions

| Hazard | Resolution |
|---|---|
| Parked producer holds `e.mu` → CG frozen | D5: drain runs outside `e.mu`. |
| Parked producer holds `group.mu` → CG's advances wait | **Keep — TS-faithful** (TS serializes hydrate/advance per CG; a slow client delays its own CG in TS too). Go-specific extra cost is the pinned WAL frame → bounded by D7 idle timeout, which TS doesn't need (better-sqlite3 snapshot lives in the same process). |
| Other CGs / other engines | Untouched: gates are per-request, engines per-group, and a parked stream enqueues nothing on the shared TSFN queue — the queue never blocks on a parked stream. |
| Hydrate lane pool starvation (engine.go:919-925): a parked pull query would occupy a lane forever | Pull-mode queries run on their **own goroutine outside the lane pool** — their concurrency is client-bounded by credits; lane bounding is redundant for them. Reader demand (Option B, 2026-07-08): each RUNNING pipeline holds exactly ONE pool reader acquired at start while holding nothing; batches wider than the pool's K queue at admission (`AcquireForPipeline`) — the K = P × Cmax per-fetch model and its hold-and-wait deadlock class are gone. |
| Warm-pool reader pinned while parked; `tearDownReaderPool` (main.go:2133) blocks on in-flight fetches | Teardown FIRST cancels every gate for the group (broadcast) → producers unpark, see cancelled, break, release readers → teardown proceeds. Same order in `Engine.Close`. |
| Reaper vs parked-but-live CG | Credit grants touch `group.lastActive`; a client actively pulling is live. A parked-forever one is D7's job, not the reaper's. |
| Advance lane regression | None: advance path untouched — per-goroutine local chunk buffers + single mutex append (streamer.go:140-168), parallel fan-out push unchanged, TSFN blocking backpressure identical to today. |
| Two lanes interleaving on the TSFN queue | Already the shipped reality (hydrate lanes + advances share abiDeliver). The gate only *removes* hydrate items from the queue while parked; kinds/ordering per RPC unchanged. |

### 4.7 D7 — idle timeout (bounds the park)

`GO_IVM_PULL_IDLE_TIMEOUT` (default 60s): a gate parked at zero credit
longer auto-cancels — same unwind as client cancel; client receives a
terminal error frame → re-hydrates. This bounds the WAL-frame pin and the
`group.mu` hold exactly like `GO_IVM_ADVANCE_BUDGET_MS` bounds advances.
One sweeper goroutine over the registry (O(gates), simplest); it also
covers the crashed-client-no-cancel case together with group teardown.

### 4.8 D8 — ABI v3 boundary surface

- `goivmABIVersion` 2 → 3 (napi_lib.go:68). New exports
  `goivm_stream_credit(reqID uint64, n int32)` and
  `goivm_stream_cancel(reqID uint64)`: **JS-thread direct calls** (dlsym,
  no TSFN) — registry lookup + grant/cancel, O(1), no allocation, safe
  because they never touch N-API and never block (Broadcast under a
  leaf mutex).
- addon.c dlsyms both, exposes `streamCredit/streamCancel` on the binding.
  Version mismatch refuses at start (existing check) → JS never sets
  `pullMode` → Go behaves exactly as ABI v2 (rollout-safe: pull is a
  per-request opt-in, old-JS/new-Go and new-JS/old-Go both degrade to
  today's push behavior).
- JS side: row-mode hydrate becomes a per-query AsyncIterator over the
  RPC's record stream — internal buffer of decoded RowChanges; `next()`
  serves from buffer, grants W/2 when buffer < W/2; `return()/throw()`
  cancels. The accumulator path stays for advance streams. Each
  pipeline-driver consumption site (`#goHydrate`,
  `goHydrateBatchStream`) migrates individually from
  for-await-over-arrays to the iterator.

### 4.9 D9 — advance-diff lazy derivation (push lane input, G4)

The snapshotter changelog cursor feeds `Advance`/`AdvanceStream` as
`iter.Seq[SnapshotChange]` instead of a materialized slice capped at
`GO_IVM_MAX_DIFF_CHANGES` (50k). Memory O(chunk); the a3 time budget
remains the bound; the count cap and its reset failure mode are deleted.
Independent of the pull gate (different lane) — sequenced LAST.

---

## 5. Correctness invariants (pinned; each maps to a test in §6)

- **I1 — single ordered queue.** All of one RPC's output rides abiDeliver
  in production order (rowplane.go:7-22). The gate delays enqueue; it
  never reorders or splits.
- **I2 — B2 all-or-nothing per partial.** Gate wraps whole deliveries;
  a fallback partial = one acquire.
- **I3 — hydrate all-or-nothing per RPC.** Cancel/timeout/panic ⇒ error
  frame ⇒ TS rejects the whole addQueriesStream; CVR never commits a
  prefix. Pull adds earlier abort, never partial commit.
- **I4 — flatten-in-push state liveness.** Unchanged: Accumulate runs
  while `Output.Push` is on the stack (overlay + join in-progress state
  live — DESIGN-streaming-advance §3). The pull gate is strictly
  downstream of the flatten and cannot move it.
- **I5 — advances are never demand-gated** and always drain to
  completion or classified failure (D1).
- **I6 — lockstep.** With W=1: produced ≤ consumed + 1 at every step
  (the "exact TS ditto" pin); with W=64: produced ≤ consumed + W.
- **I7 — unwind releases resources.** Mid-stream cancel returns the
  pool reader to the warm pool and leaks no goroutine.
- **I8 — park isolates.** A parked pull-hydrate blocks nothing outside
  its own client group.
- **I9 — drift semantics unchanged.** Error-ladder classification
  (-32102 teardown vs -32000 reset, EDGE-AUDIT.md) is untouched;
  errStreamCancelled is a NEW terminal class (client-initiated, never
  a reset signal — it must not join the a1 reset ladder or a storm of
  tab-closes becomes a reset storm).

---

## 6. Sequencing (each step: verify → implement → fail-pre-fix test → -race → napilib build → E2E)

1. **streamgate.go** + unit tests (park/grant/cancel/idle/concurrent, -race).
2. **Engine bool-returning onResult** + errStreamCancelled + test proving
   cursor/pool-reader release on mid-stream cancel (fails pre-change).
   All existing callers pass always-true.
3. **D5 e.mu restructure** + the entry-point audit table committed into
   the PR description + a group-serialization pin test (concurrent
   advance vs draining hydrate on one group: serialized by group.mu;
   on two groups: parallel).
4. **ABI v3 exports** + version bump + addon.c credit/cancel + mismatch
   test (old JS on new Go behaves exactly as ABI v2).
5. **JS AsyncIterator** + E2E lockstep pin (W=1: Go produces row N+1
   only after N `next()` calls — instrumented producer counter).
6. **Idle timeout + teardown-cancels-gates + reaper liveness.**
7. **D9 advance-diff streaming.**

Validation gates (all must hold):
- I6 lockstep pin (W=1 and W=64 variants).
- I7 cancel pin: pool stats show reader returned; goleak clean.
- I8 parallel pin: one parked pull-hydrate; a concurrent advance for
  ANOTHER CG and a hydrate for another query complete unblocked;
  same-CG advance blocks until cancel/timeout (documented, TS-faithful).
- I2/I3: fallback-partial + cancel interleave test (multi-change
  residual partial cancelled mid-batch → zero records delivered, error
  frame terminal).
- Full -race suite; napilib build under BOTH tag sets (plain `napilib`
  AND `libsqlite3 sqlite_omit_load_extension osusergo netgo napilib` —
  the coread.go lesson); go-sidecar E2E 81/81; no-pg suite; divergence
  harness green (shadow soak) since D5 moves lock scopes.

---

## 7. Risks / failure modes

- **Client crashes without cancel** → gate parked forever → D7 idle
  timeout (60s) + group teardown broadcast. Bounded WAL pin.
- **Reset-storm regression via cancel** → I9: errStreamCancelled is
  explicitly NOT reset-classified.
- **D5 misses an entry point** → a source mutates mid-drain → wrong
  hydrate rows. Mitigated by the audit gate in §6 step 3 + the shadow
  divergence harness (SQL oracle catches wrong rows) before rollout.
- **Throughput regression on eager consumers** → W=64 default keeps the
  producer ahead; the per-row fast path is unchanged; A/B the sandbox
  cold-hydrate benchmark (real-RPC max 2.44s baseline) before/after.
- **Rollout** → pull is per-request opt-in behind the ABI handshake;
  first deploy ships with pullMode off, flipped by env on the TS side
  (`ZERO_GOIVM_PULL_HYDRATE=true`), same playbook as the warm-pool flag.

## 8. What this deliberately does NOT do

- No operator signature changes (§2a — they are already faithful).
- No advance-lane gating, ever (D1/I5).
- No change to shadow-compare, drift classification, B2, chunk sizes,
  group.mu semantics, or the ordering invariant.
- No SHM/second queue — serialization, not transport, is the boundary
  cost (project_wire_format_serialization), and one queue is what makes
  I1 trivially true.
