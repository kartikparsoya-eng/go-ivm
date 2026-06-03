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
    recovers cleanly + re-hydrates, matching TS.

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

- [ ] **E.** Skip uses CompareWithPartialBound vs TS's full comparator
  - Go: `ivm/skip.go:113` (and `getStart`)
  - TS: `mono/packages/zql/src/ivm/skip.ts:68-73` uses full comparator
  - Fix: use full comparator; missing column comparison should treat nil as less-than-any-value.

- [x] **F.** Per-query Go hydrate (`#goHydrate`) skips `#planAstForGo` ✅
  - Fixed: `pipeline-driver.ts:985` now calls `#planAstForGo(query)` before `#goBackend.hydrate`, matching the batch path. Stored `transformedAst` is the planned variant. TS check-types green.

## Engine-level (high impact)

- [ ] **G.** Engine.Advance doesn't bracket pushes with beginFilter/endFilter
  - Go: `engine/engine.go:738-749` no bracketing
  - TS: view-syncer advance loop calls beginFilter/endFilter on each ArrayView; clears Exists cache
  - Fix: wire begin/endFilter brackets matching TS dispatch.

- [ ] **H.** snapshotToSourceChanges PK-match is last-wins, not stable
  - Go: `engine/engine.go:961-998` keeps LAST PK-matching prev as `editOldRow`
  - Risk: low in prod; replicator rarely emits dup PKs

## Lifecycle / restart (CRITICAL but not the Take symptom)

- [ ] **I.** getCurrentQueries doesn't filter internal queries on restart-recovery
  - TS: `pipeline-driver.ts:521-525` post-restart callback iterates ALL `#pipelines` without filter
  - Other dispatch sites filter correctly (959, 1111, 1279, 1438-1442)
  - Fix: add `!#isInternalQueryID && !#isInternalTable` filter to re-register callback.

- [ ] **J.** Scalar-subquery value change during advance: TS resets, Go doesn't
  - TS: `pipeline-driver.ts:2009-2015` throws `ResetPipelinesSignal` via `scalarValuesEqual`
  - Go: `engine/engine.go:580-596` companion path just emits, no value-comparison
  - Fix: port `scalarValuesEqual` check + emit a drift/reset signal.

## Observability (won't cause runtime mismatch)

- [ ] **K.** Streamer's `minRowVersion` bump is TS-only (`pipeline-driver.ts:3147-3155` vs `streamer.go:127-178`)
- [ ] **L.** Go-side per-table advance timings dropped (`pipeline-driver.ts:2239` discards `r.timings`)
- [ ] **M.** hydrationTime histogram measures emit+flush wall-clock in Go-primary vs hydrate-only in TS-prod
- [ ] **N.** Slow-query warning fires on wrong elapsed in Go-primary
- [ ] **O.** MeasurePushOperator instrumentation absent in Go-primary
- [ ] **P.** No advancement circuit-breaker in Go-primary
- [ ] **Q.** Drift-audit freeze detection uses inconsistent count basis
- [ ] **R.** REMOVE-row fallback hides Go-side wire bugs (`pipeline-driver.ts:2336`)
- [ ] **S.** Stale `setHydrationTime()` reference (`pipeline-driver.ts:2030`)
- [ ] **T.** Phantom TableSources from `#planAstForGo`
- [ ] **U.** Relationship-emission order non-deterministic on Go (`streamer.go:165-175` iterates Go map)
- [ ] **V.** MemorySource re-sorts on every Fetch (perf)
- [ ] **W.** MemorySource applyStart vs TS scanStart — direction-aware partial cursor handling

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
