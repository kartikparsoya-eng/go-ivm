# TS→Go Port Audit Fixes

Tracker for divergences found in the 2026-06-03 audit. Rule: **follow TS exactly. No shortcuts.**

Symptom this list is rooted in: Go's Take operator emits a different displaced row than TS during advance, when source has rows tying at the same sort key with id-asc tiebreaker. Example: TS=REMOVE ticket-11 / Go=REMOVE ticket-19; TS=ADD ticket-13+REMOVE ticket-11 / Go=ADD ticket-13+REMOVE ticket-14.

## Status legend
- `[ ]` not started
- `[~]` in progress
- `[x]` fixed + tested
- `[!]` fixed but failing tests
- `[?]` re-examined, not a real divergence

---

## Session log — 2026-06-03 (prev-tx refactor)

- **Prev-tx architecture** ✅ — replaced `sqlite3_snapshot_get` + per-Fetch
  `snapshot_open` + in-memory `batchDelta` with a per-Source dedicated
  `*sql.Conn` holding a `BEGIN CONCURRENT` (plain `BEGIN` fallback in
  mattn builds). `writeChange` (INSERT/UPDATE/DELETE) runs against the prev
  tx after fanout; `OnAdvanceEnd` does ROLLBACK + lazy re-BEGIN. Direct port
  of TS Snapshotter prev/curr + TableSource.#writeChange. Commit `9ad33ad`.
- **Overlay gate inversion** ✅ — `fetchForConn` applied the in-flight
  overlay on the WRONG connections (`lastPushedEpoch < epoch`). TS applies
  it for the connection CURRENTLY being pushed (`>= epoch`,
  `memory-source.ts:634`). The inversion hid the in-flight row from the
  EXISTS/Join child re-fetch (which runs on the pushed connection) →
  advance emitted far fewer changes than TS ("TS produced 21, Go produced
  2"). Fixed to `>=`. Old batchDelta arch masked it by applying every batch
  push to every fetch. Un-skipped + un-external-INSERT'd the 4 engine tests
  that this fixed (EXISTS/NotExists advance, Take bulk).

### Post-prev-tx soak (4u × ~130s active, gate fix in):
- ✅ in-flight-visibility count divergence ("TS 21 / Go 2") — GONE.
- 🔴 **X** (new): `writeChange` UNIQUE-constraint raw panic — see below.
- 🟠 **B/C-residual**: Take tied-sort row-pick divergence STILL present
  (the original symptom) — prev-tx did NOT fix it; it's in Take's bound
  selection, not the source snapshot timing. See header symptom.

---

## NEW — prev-tx-introduced (P0, stability)

- [ ] **X.** `writeChange` ADD of an already-present row → raw panic
  - `internal/tablesource/source.go` `writeChangeLocked`: `INSERT` fails with
    `UNIQUE constraint failed` when a batch ADD targets a row already in the
    prev tx (dup messageId from insert+delete+insert / coalesced diff).
  - Raw `panic`, NOT `*ivm.DriftError` → `engine.Advance` recover re-raises
    → risks crashing the cg/sidecar. Drives the `Go produced 0` re-hydrates.
  - TS: `genPush` runs `assert(!exists(row))` BEFORE fanout → throws →
    view-syncer re-hydrates (`memory-source.ts:531`). Symmetric for
    REMOVE/EDIT of a missing row.
  - Fix: exists-check (or catch UNIQUE) and raise `DriftError` so the engine
    recovers cleanly + re-hydrates, matching TS. ✅ DONE (commit 3c70e2d,
    driftCheckLocked).

## Session results — 2026-06-03 (cont.)

- [x] **Reverse-aware overlay comparator + start filter** ✅ (commit 4e95bed)
  - fetchForConn applied the overlay with the FORWARD comparator; TS uses
    makeComparator(sort, req.reverse) + overlaysForStartAt. Fixed → shadow
    soak row-pick ("at index") mismatches **27 → 0**. This was the real
    Take-bound correctness divergence (the original symptom).
- [x] **Eager prev-tx re-pin in OnAdvanceEnd** ✅ (commit 4e95bed)
  - Lazy re-pin pinned the prev tx after the next batch was already
    committed → false drift. Eager re-pin (matches resetToHead timing)
    reduces but does not fully eliminate frame-timing drift (residual
    replicator-race; see note below).
- [x] **I.** Internal-query filter in Go reset re-registration ✅
  (mono commit — pipeline-driver.ts)
  - resetEngine re-registered internal (clients/permissions/mutations)
    queries; Go has no Source for those → AddQueries panic "no source for
    table ...clients" → resetEngine threw → drift recovery cascaded into
    client-connection closures. Added the !#isInternalQueryID &&
    !#isInternalTable filter the other dispatch sites already use. Shadow
    soak: resetFail + no-source panics **→ 0**; drift now self-heals.

### Residual: frame-timing false drift (fundamental) — DEFERRED, see deep-dive below
The Go TableSource reads the shared replica file; the replicator commits a
batch BEFORE sending its advance RPC, and can race further ahead during
advance processing. The single prev-tx can't pin at exactly TS's
prev.version frame, so some ADDs see their own freshly-committed row →
DriftError. This is PRE-EXISTING (the old snapshot_get arch had it too)
and now SELF-HEALS via resetEngine. Full analysis + the decision to defer
the real fix is in the deep-dive section at the bottom of this file
("Deep-dive: frame-timing drift — root cause, options, decision").

---

## Direct causes of Take-bound mismatch (FIX FIRST)

- [x] **A.** tablesource Push order: filter-transition runs BEFORE split-edit, per-conn instead of global ✅
  - Fixed: source.go Push now (1) decides shouldSplitEdit globally across ALL connections, (2) calls genPushAndAppendDelta once-or-twice, (3) each fan-out goes through filterPush + maybeSplitAndPushEditChange ports.
  - Tests: all pass.

- [x] **B.** Take.Push REMOVE empty-partition silently zeroes Size ✅
  - Fixed: take.go now writes `{Size: takeState.Size - 1, Bound: newBound?.node.Row}` unconditionally matching take.ts:413-418.

- [x] **C.** Take.pushEditChange (oldCmp>0, newCmp<0) Go-only fallback bumps Size+1 silently ✅
  - Fixed: take.go now panics when `newBoundNode == nil`, matching TS's `assert(newBoundNode !== undefined)` at take.ts:597-600. Engine recovers via DriftError.

- [?] **D.** tablesource.applyOverlay REMOVE doesn't respect filter — **re-examined: not a real divergence**
  - Audit claim: TS strips Remove overlay if filter rejects row; Go calls removeByPK regardless.
  - Reality: Go's SQL fetch doesn't push filter to WHERE (closure-based predicate), so applyFilter runs post-scan. Rows that fail filter are NOT in `out` when applyOverlay runs. removeByPK is a no-op when the row isn't there. Final filter re-pass also re-runs filter on overlay-added rows. Functionally equivalent to TS even though the mechanism differs.

- [?] **E.** Skip uses CompareWithPartialBound vs TS's full comparator — **re-examined: NOT a divergence, audit recommendation would REINTRODUCE drift**
  - Verified TS's `compareValues` (data.ts:32) does `a = a ?? null` and returns
    **-1 when `a === null`** — IDENTICAL to Go's `CompareValues(nil,x) = -1`. So a
    literal port of TS's `Skip.#shouldBePresent` (full comparator) would INCLUDE
    the partial-bound row too. TS excludes it at the **SQL source** (three-valued
    logic), not at the Skip operator. Go's `CompareWithPartialBound` (skip.go:57-90)
    replicates that SQL boundary at the Skip operator to match TS end-to-end, and is
    soak-validated (it fixed the `channelConversationsPaginatedV3` drift).
  - Reverting to the full comparator (the audit's "fix") would reintroduce that
    drift. The real divergence is *structural* (where the boundary is enforced:
    SQL source in TS vs Skip operator in Go) — not a bug. KEEP current code.

- [x] **F.** Per-query Go hydrate (`#goHydrate`) skips `#planAstForGo` ✅
  - Fixed: `pipeline-driver.ts:985` now calls `#planAstForGo(query)` before `#goBackend.hydrate`, matching the batch path. Stored `transformedAst` is the planned variant. TS check-types green.

## Engine-level (high impact)

- [?] **G.** Engine.Advance doesn't bracket pushes with beginFilter/endFilter — **re-examined: NOT a divergence, already implemented (audit misframed)**
  - The brackets are NOT an advance-loop concern. In TS, `FilterStart.fetch`
    (filter-operators.ts:86-103) wraps EACH **fetch** with `beginFilter`/`endFilter`;
    the only stateful use is `Exists.endFilter()` clearing its per-fetch memoization
    cache (exists.ts:75-78).
  - Go already does exactly this: `FilterStart.Fetch` (filter_operators.go:74-76)
    has `BeginFilter()` + `defer EndFilter()`, and `Exists.EndFilter` clears its cache
    (exists.go:82-86), with the `inPush` guard so the cache isn't used during push.
    No change needed.

- [?] **H.** snapshotToSourceChanges PK-match is last-wins, not stable — **re-examined: NOT a divergence, matches TS exactly**
  - Go's `snapshotToSourceChanges` (engine.go:965-995) is line-for-line equivalent to
    TS (pipeline-driver.ts:2851-2886): both overwrite `editOldRow` in the loop
    (last-wins), and push removes-in-order then the edit/add. The "last-wins, not
    stable" concern applies equally to TS, so it is not a port divergence. In practice
    only one prevValue can match the full PK anyway. KEEP.

## Lifecycle / restart (CRITICAL but not the Take symptom)

- [ ] **I.** getCurrentQueries doesn't filter internal queries on restart-recovery
  - TS: `pipeline-driver.ts:521-525` post-restart callback iterates ALL `#pipelines` without filter
  - Other dispatch sites filter correctly (959, 1111, 1279, 1438-1442)
  - Fix: add `!#isInternalQueryID && !#isInternalTable` filter to re-register callback.

- [x] **J.** Scalar-subquery value change during advance: TS resets, Go doesn't ✅ **FIXED (2026-06-03)**
  - TS: `pipeline-driver.ts:2017-2047` throws `ResetPipelinesSignal('scalar-subquery')`
    via `scalarValuesEqual` when a companion push moves the resolved scalar's child
    field to a new value.
  - Fix: added `companionOutput` (engine.go) wrapping `pipelineOutput`. On advance it
    computes the new child value (ADD/EDIT → `node.row[childField]`; REMOVE → always
    changed; CHILD → no-op) and, if `!scalarValuesEqual(new, resolvedValue)`, panics a
    `*ivm.DriftError{Op:"ScalarSubquery"}` that rides the engine's existing
    recover→re-hydrate path (TS re-registers the query → re-resolves the scalar with
    the NEW value). `scalarValuesEqual` ports TS's strict `===` (Go `a == b`, nil==null,
    recover-guarded for non-comparable). `companionEntry` now stores `resolvedValue`.
  - Tests: rewrote `TestAddQuery_CompanionLiveTracking` (child-field edit now asserts
    the reset, not an emitted EDIT) + added `TestAddQuery_CompanionEditNonScalarFieldNoReset`
    (non-child edit still emits a companion EDIT, no over-reset). Full suite green.
  - Note: piggybacks on the DriftError telemetry/breaker (vs TS's separate
    ResetPipelinesSignal). Acceptable given scalar-value changes are rare; a dedicated
    reset-signal type + protocol field would separate the metrics if it ever matters.

## Go-primary readiness sweep — K–W disposition (2026-06-03)

Full pass for Go-primary readiness. Outcome: 3 real fixes (K, L, U) + 2
diagnostics (R, S); the rest are non-divergences, already-covered, MemorySource-
only (not in the ModeTable Go-primary path), or telemetry that's a non-bug /
not portable across the RPC boundary.

- [x] **K.** minRowVersion bump ✅ **FIXED** — ported TS streamNodes
  (pipeline-driver.ts:3172-3178). TS now forwards `minRowVersion` per table
  (`#currentTablesForGo` → `TableData.minRowVersion` → init payload). Go stores it
  (`Engine.SetMinRowVersions` from handleInit) and `bumpRowVersions` rewrites an
  emitted non-REMOVE row's `_0_version` up to minRowVersion when below, applied at
  every RowChange output point (Advance, AdvanceStream, AddQuery/AddQueries/
  AddQueriesStream). Copy-on-bump (never mutates source rows); no-op when no
  minRowVersions set (steady state). Test: `TestBumpRowVersions`. Go + TS green.
- [x] **L.** Go advance timings ✅ **FIXED** — `#goPrimaryAdvance` now records Go's
  `r.timings` (per-(table,op) TableTiming) into the `#advanceTime` histogram
  instead of discarding them. (mono)
- [x] **R.** REMOVE-row fallback ✅ **FIXED** — `#goRowChangeToRowChange` logs at
  error level when an ADD/EDIT arrives without a row (Go wire bug) before the
  rowKey fallback, instead of silently shipping a PK-only row. (mono)
- [x] **S.** Stale `setHydrationTime()` ✅ **FIXED** — removed the dead
  comment reference (no such method); documented hydrationTimeMs's real basis. (mono)
- [x] **U.** Relationship emission order ✅ **FIXED** (committed earlier) —
  streamNodes sorts relationship names for determinism (Go map range was randomized).
- [?] **T.** Phantom TableSources — **already fixed.** `#addQueryDispatch` gates on
  `whenRecovered()` (pipeline-driver.ts:952, go-compute-backend.ts:455); the comment
  documents the phantom as a past bug now resolved. No change.
- [?] **P.** Advancement circuit-breaker — **hang-safety already covered** by the 120s
  per-call RPC timeout (go-ivm-client.ts:592-647). TS's `advancement-timeout` is an
  early-abort latency heuristic (re-hydrate-is-cheaper) that doesn't run in Go-primary
  and is marginal for the fast Go path. No change.
- [?] **M.** hydrationTime basis — **non-bug.** Go-primary's `goResult.timingMs` is
  Go's actual end-to-end hydrate time (what the client waits on) — the legitimate
  measure. No change.
- [?] **N.** Slow-hydrate warning — **debug-only + not portable.** TS warning is gated
  on `trackRowCountsVended` and uses TS-side `debugDelegate` vended counts that don't
  exist in Go-primary. Disproportionate to replicate. No change.
- [?] **O.** MeasurePushOperator — **not portable.** Per-operator timing can't cross
  the RPC boundary (operators live in the sidecar). Go reports per-(table,op), which
  L now wires — the achievable parity. No change.
- [?] **Q.** Drift-audit freeze count basis — **defense-in-depth; safe failure mode.**
  Root cause (C2) already fixed; a count-basis mismatch at worst triggers a spurious
  (safe) resetEngine. Not worth the churn. No change.
- [?] **V.** MemorySource re-sort / **W.** MemorySource applyStart — **N/A to
  Go-primary.** Both are MemorySource-only (ModeMemory). Go-primary runs ModeTable
  (main.go:1065-1081) where the leaf is `tablesource.Source` and SQLite handles
  ordering/indexing — no Go-slice re-sort, no `applyStart`. (W also re-examined as
  not-a-correctness-divergence: Go materializes+filters the full sorted slice, so it
  can't miss rows the way a mis-positioned btree scan could — same equivalence class
  as item D.)

---

## Fix order

Done: A, D, F, prev-tx, overlay-gate (✅).

Remaining, by priority:
1. **X** — `writeChange` → DriftError on dup-ADD / missing-REMOVE (P0 crash risk).
2. **Take tied-sort row-pick** — the original symptom; line-by-line `pushEditChange`/`pushAddChange` bound logic vs `take.ts`.
3. **G** — engine push-loop beginFilter/endFilter brackets.
4. **E** — skip full-comparator.
5. **H, I, J** — lifecycle/restart.
6. **K–W** — observability (won't cause runtime mismatch).

After each fix: rebuild amd64 → swap mounted binary → soak (4 users × 4 min shadow + mutate, per Mac limit) and record observed delta. See `memory/reference_soak_run_procedure.md` for the header-size gotchas.

---

## Deep-dive: frame-timing drift — root cause, options, decision (2026-06-03)

### Root cause (confirmed in both codebases)
TS's `Snapshotter` (snapshotter.ts:183-196 `advanceWithoutDiff` → `resetToHead`)
opens its `prev` connection's `BEGIN CONCURRENT` at the instant head crossed
version `P`, and holds it frozen by MVCC. So `prev.version` is *defined by when
TS opened it*, and TS derives the shipped diff to match that exact frame. The
diff is shipped to Go as `SnapshotChange[]` (pipeline-driver.ts:2216-2225,
`advanceStream(snapshotChanges)`) — **carrying no version**.

Go's `OnAdvanceEnd` (source.go:318) re-pins its single conn at *current head* —
a different instant than when TS pinned. The replicator commits ahead
independently (sole writer, doesn't wait on readers), so Go's snapshot lands at
a version ≥ TS's `prev.version`. TS then ships a diff keyed to its `P`; Go
applies `ADD(row)` where `row` was already committed in `(P, head_go]` →
`driftCheckLocked` fires. **Two independent processes reading "head" at
different instants cannot agree on a version unless one tells the other.** No
Go-side re-pin *timing* closes this — it is structural.

### Is it a correctness issue, or only a shadow-mode artifact?
It IS a (minor, self-limiting) **Go-primary correctness issue**, not just a
shadow artifact:
- Shadow mode: TS serves clients, Go output only compared → client unaffected.
- Go-primary: Go serves user queries. On drift, `#advanceWithRecovery`
  (go-compute-backend.ts:276-308) re-inits Go's engine from current snapshot but
  **discards the re-hydrate output** ("we only need Go's internal pipeline state
  rebuilt; TS already owns the client view" — go-compute-backend.ts:130-131) and
  returns only `partialChanges`. It does NOT throw `ResetPipelinesSignal`, so the
  view-syncer's full CVR reconciliation (view-syncer.ts:2320) never runs. Net:
  the client's CVR advances to curr.version but is **missing the drifted delta**,
  not auto-corrected until those rows change again or the query re-hydrates
  (reconnect/TTL). Repeated drift trips the breaker → Go disabled → TS fallback.
  This is strictly weaker than TS's own drift path, which re-hydrates + reconciles
  the client.

### Options considered
1. **Ship TS's snapshot handle to Go** — INFEASIBLE. The handle is a
   process-local C pointer (`*C.sqlite3_snapshot` into TS's address space,
   snapshot.go:68). Cannot be serialized/shipped over the Unix socket.
2. **Private accumulating copy** (Go keeps its own SQLite copy, mutated ONLY by
   TS's shipped diffs, committed not rolled back, never re-reads the racing
   replica). Zero drift by construction. BUT = reverting to the MemorySource
   memory profile: init loads `SELECT * FROM <table>` for every syncable table
   per CG (pipeline-driver.ts:618, no per-CG filter) → memory =
   (active CGs) × (full syncable dataset). Measured baseline: Go heap ~12MB,
   dataset ~13MB, ~4 CGs in soak → fine here; but at high CG-fanout (Zero's
   scaling story) → GBs. This is exactly the cost ModeTable was built to escape.
   The row TRANSFER is already paid today (TS does `SELECT *` and ships rows;
   ModeTable's `handleLoadRows` is a no-op that discards them — main.go:1172), so
   the private copy adds ~zero new transfer, only memory.
3. **Snapshotter-in-Go** (port TS's leapfrog Snapshotter into Go: two
   `BEGIN CONCURRENT` conns + read `_zero.changeLog2` + derive own diff). Keeps
   the shared-replica memory model (no per-CG copy) AND gets zero drift, because
   Go becomes self-consistent like TS. BUT moves version authority for user
   queries into Go → ripples into the wire protocol (advance becomes a trigger;
   Go reports its own version), CVR version bookkeeping (view-syncer must stamp
   user-query CVR at Go's version), and shadow-mode validation (per-advance
   compare breaks under coalescing races → must rely on the full-state drift
   audit). Biggest change, heaviest part lands in mono, but the only option that
   is both zero-drift AND keeps the shared-memory profile.
4. **Make Go-primary drift recovery client-correct** — route Go drift through the
   same `ResetPipelinesSignal` full re-hydrate TS uses, instead of discarding the
   re-hydrate. Smallest change (mono only). Drift still happens, but stops being a
   correctness issue (client always reconciles). Cost: a full re-hydrate per
   drift; shadow still logs the per-advance mismatch.

### Decision (2026-06-03)
**DEFERRED.** Snapshotter-in-Go (option 3) is the right long-term answer (zero
drift + shared-memory), but it is a major cross-repo change (protocol + CVR
version authority). Not doing it now. When revisited, the smaller stop-gap for
Go-primary *correctness* (not shadow cleanliness) is option 4 — make the drift
recovery trigger a real client re-hydrate. Proceeding with the remaining
audit-fix items (G, E, H, J) in the meantime.
