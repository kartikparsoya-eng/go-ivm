# DESIGN: ABI v3 — demand-driven pull streaming at the NAPI boundary

Status: APPROVED direction (user), implementation not started.
Grounding: every file:line below verified against source in session ending
2026-07-04 (go-ivm @ 3aa1d70, mono @ bc7f2d030).

## Goal

True pull-based streaming for query hydration across the Go↔JS boundary —
"exact streaming like TS ditto": one client `next()` ⇒ one row produced.
The JS consumer's demand drives the Go producer; `return()/throw()` cancels
and unwinds the operator chain. Must not hamper correctness (ordering,
B2 all-or-nothing, drift) or parallelization (other CGs/queries/advances).

## Pinned current state

- All producer→JS output rides ONE ordered TSFN queue: everything through
  `abiDeliver` (`cmd/sidecar/rowplane.go:9-22` ordering invariant;
  `abi.go:222` sets `server.abiDeliver` once; payload valid only for the
  call duration).
- `napi_tsfn_blocking` at `napi/addon.c:187` = fixed 8192 standing window
  (push + backpressure, NOT pull).
- ABI exports today: `abi_version/start/send/shutdown` only
  (`cmd/sidecar/napi_lib.go:78-157`); `goivmABIVersion = 2` (:68).
- Producer loop: `for node := range entry.pipeline.Input.Fetch(...)`
  (`engine/engine.go:966`). Breaking this range IS `.return()`
  propagation: `iter.Seq` defers unwind the operator chain → cursor /
  pool-reader release. `onResult` has no abort signal today.
- Hazards (verified): `e.mu` held for hydrate duration (`engine.go:875`);
  `group.mu` held across the whole hydrate (`main.go:2108-2109`); hydrate
  lanes share a job pool (`engine.go:919-925`); warm-pool reader pinned per
  fetch, teardown at `main.go:2133` blocks until fetches finish.
- Row plane kinds: 1=frame 2=groupDef 3=row 4=hostDeath
  (`rowrecord.go`); rowMode uses chunkSize=1; B2 all-or-nothing fallback
  per partial (`rowplane.go` header comment).

## Design decisions (rethought for correctness + parallelization)

### D1. Scope: HYDRATE streams only

Pull gating applies to `addQueriesStream` (and its batch variant) row-mode
deliveries. Advance streams (`advanceStream`, `advanceToHeadStream`) stay
push: an advance diff MUST be fully consumed for engine consistency, and
TS itself drains its advance synchronously — gating it would diverge from
TS. (`GO_IVM_ADVANCE_BUDGET_MS` + a1 reset-classification already bound
the advance failure mode.)

### D2. Demand gate (new `cmd/sidecar/streamgate.go`)

Registry `map[uint64]*streamGate` keyed by numeric reqID, owned by the
Server, guarded by its own mutex.

    type streamGate struct {
        mu        sync.Mutex
        cond      *sync.Cond // on mu
        credit    int64
        cancelled bool
        lastGrant time.Time  // for idle timeout + reaper liveness
    }

- `acquire(1)`: blocks while `credit == 0 && !cancelled`; on cancelled
  returns false; else decrements and returns true.
- `grant(n)`: adds credit, updates lastGrant, Broadcast.
- `cancel()`: sets cancelled, Broadcast. Idempotent.
- Created in `newRowPlane` when params carry `pullMode:true`; removed on
  handler return (defer). Unknown-reqID grant/cancel = no-op (stream
  already settled — benign race).

### D3. Gate placement: per row-bearing delivery, final/error bypass

The rowPlane sink acquires 1 credit before each row-BEARING delivery
(kind-3 record, or a kind-1 fallback frame that carries rows). groupDefs,
the terminal Final frame, and error frames ride FREE — gating them
deadlocks (client waits for final; Go waits for credit the client will
never grant). With chunkSize=1, one gated delivery == one row == literal
TS `next()` semantics at window=1. Window configurable via
`GO_IVM_PULL_WINDOW` (default 1). B2 all-or-nothing is untouched: the
gate wraps whole deliveries, never splits a partial.

### D4. Cancel = `.return()` — bool-returning onResult

Signature change (engine):
`AddQueriesStreamChunked(specs, chunkSize, onResult func(QueryResult) bool)`
— false ⇒ stop draining THIS query, unwind via breaking the range at
engine.go:966 (defers release cursors/pool readers), skip remaining
output, return a distinguished `ErrStreamCancelled`. The sidecar handler's
onResult returns `!gate.isCancelled()`. Handler maps ErrStreamCancelled to
a terminal "cancelled" frame (client already discarded the RPC — frame is
for queue hygiene only). No sentinel panics.

### D5. New exports — ABI v3

`napi_lib.go`: `goivm_stream_credit(double reqID, int32 n)` and
`goivm_stream_cancel(double reqID)`. Bump `goivmABIVersion = 3`; the addon
refuses mismatch (existing check). These are JS→Go direct calls on the JS
thread — no TSFN involvement, just registry lookup + grant/cancel.
addon.c: dlsym both, expose `streamCredit(reqID, n)` / `streamCancel(reqID)`
on the binding object.

### D6. Parallelization hazards — resolutions

- **e.mu (engine.go:875):** a parked pull-hydrate MUST NOT hold e.mu (it
  would freeze advances + all other queries). Restructure: register the
  pipeline under e.mu, RELEASE it, drain outside. Verify at implementation
  exactly what the drain reads that e.mu protected (pipeline map/source
  registry) — per-pipeline goroutine confinement already holds.
- **group.mu (main.go:2108):** held-while-parked is TS-FAITHFUL (TS
  serializes hydrate/advance per CG — suspended generator + syncer lock).
  Keep it. Bound the Go-specific WAL-frame pin via D7.
- **Lanes (engine.go:919-925):** pull-mode queries run on their OWN
  goroutine outside the lane pool — credits client-bound their
  concurrency; lane bounding is redundant and starvation-prone for them.
- **Reader-pool teardown (main.go:2133):** `tearDownReaderPool` must first
  cancel every pull gate for the group (broadcast), so teardown never
  blocks on a parked producer.
- **Reaper:** credit grants touch `group.lastActive` — an actively-pulled
  CG is live, a parked-forever one is handled by D7, not the reaper.

### D7. Idle timeout

`GO_IVM_PULL_IDLE_TIMEOUT` (default 60s): parked at zero credit longer
⇒ auto-cancel (same unwind as client cancel; client gets an error frame →
re-hydrate). Bounds the WAL-frame pin exactly like GO_IVM_ADVANCE_BUDGET_MS
bounds advances. One timer goroutine sweeping the registry, or per-gate
timer armed on park — decide at implementation (sweep is simpler, O(gates)).

### D8. JS side (`go-ivm-client.ts` + `napi-records.ts` untouched)

Row-mode hydrate becomes a per-query AsyncIterator: internal buffer of
decoded RowChanges; `next()` serves from buffer and grants 1 credit when
buffer < low-water (W/2); `return()/throw()` → `streamCancel(reqID)` +
RowGroupRegistry.clearRequest. The accumulator path stays for advance
streams (D1). pipeline-driver's consumption loops move from
"for await over accumulated arrays" to the iterator — verify each call
site (`#goHydrate`, `goHydrateBatchStream`) individually.

### D9. Same milestone: streaming advance-diff derivation

Kill the 50k `GO_IVM_MAX_DIFF_CHANGES` materialization: snapshotter
changelog cursor feeds Collect/apply as `iter.Seq[Change]`, memory
O(chunk). The a3 time budget remains the bound; the count cap and its
reset failure mode go away. Independent of the pull gate — do LAST.

## Sequencing (each step: verify → implement → fail-pre-fix test → -race → napilib build → go-sidecar E2E)

1. `streamgate.go` + unit tests (pure Go: park/grant/cancel/idle, -race).
2. Engine bool-returning onResult + ErrStreamCancelled + a test proving
   cursor/pool-reader RELEASE on mid-stream cancel (fails pre-change).
3. e.mu drain-outside-lock restructure + lane bypass for pull queries.
4. Exports + version bump + addon.c credit/cancel + ABI mismatch test.
5. Client AsyncIterator + E2E (window=1 lockstep test: Go produces row
   N+W only after N `next()` calls — the "exact TS ditto" pin).
6. Idle timeout + teardown cancel + reaper liveness.
7. Advance-diff streaming (D9).

## Validation gates

- Lockstep pin: with W=1, instrument the Go producer (test hook counting
  produced rows) and assert produced ≤ consumed+1 at every step.
- Cancel pin: mid-hydrate `return()` → pool reader returns to warm pool
  (assert via pool stats), no goroutine leak (goleak or runtime count).
- Parallelization pin: one parked pull-hydrate; a concurrent advance for
  ANOTHER CG and a hydrate for another query complete unblocked.
- Full -race suite, napilib build (BOTH tag sets: plain napilib AND
  `libsqlite3 sqlite_omit_load_extension osusergo netgo napilib` — the
  coread.go lesson), go-sidecar E2E 81/81, no-pg suite.
