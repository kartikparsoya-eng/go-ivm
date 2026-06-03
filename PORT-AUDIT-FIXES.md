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

1. A + D (single PR, both in `tablesource/source.go` + `tablesource/overlay.go`)
2. B + C (`ivm/take.go` — revert Go-specific safety nets)
3. F (one-line `pipeline-driver.ts:985`)
4. G (engine push-loop brackets)
5. E (skip full-comparator)
6. Remaining in priority order, working down severity

After each fix: rebuild + soak (20 users × 30 min shadow + mutate) and record observed delta.
