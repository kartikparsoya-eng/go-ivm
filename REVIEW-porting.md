# Porting Correctness Review

This audit compares the Go IVM port against its TypeScript reference (mono/packages/zql/src/ivm).
Findings are scored by likelihood of producing different output than TS for some valid input.

> **See `REVIEW-final.md` for the consolidated cross-doc production-readiness summary. This doc preserves the audit trail.**

## Status: ALL FINDINGS FIXED (1 OPEN as forward-looking)

| Finding | Status | Fix |
|---|---|---|
| CRITICAL-1, CRITICAL-2, CRITICAL-3 | **FIXED** | See per-finding fix notes below. |
| HIGH-1 (Skip cursor) | **FIXED** | Builder now normalizes `ast.Start.Row` via the new `Source.NormalizeRow` interface method; closes the residual JSON/blob risk. |
| HIGH-2, HIGH-3, HIGH-4 | **FIXED** | See below. |
| MEDIUM-1, MEDIUM-2, MEDIUM-4, MEDIUM-5, MEDIUM-6 | **FIXED** | MEDIUM-2 now covers test paths too via `NewMemorySourceWithConverter` + `sqlite.FromSQLiteType` in testharness + bench. |
| MEDIUM-3 (Cap operator) | **OPEN, forward-looking** | Unused by TS builder; implement when/if TS adopts it. |
| MED-PORT-1 (test harness legacy converter), MED-PORT-2 (sqlite `evalOp` null=null) | **FIXED** | New findings from the consolidated audit; both addressed. |
| LOW-PORT-1 (BulkInsert unsynchronized), LOW-PORT-2 (GetMemorySource raw pointer) | **FIXED / DOCUMENTED** | BulkInsert takes `connsMu`; GetMemorySource lifetime invariant documented. |
| LOW-1, LOW-2, LOW-3, LOW-4 | **OPEN / CONFIRMED CORRECT** | Style nits; behavior unchanged. |

## Summary
- 3 CRITICAL findings (would cause divergence with TS for some inputs)
- 4 HIGH findings (likely divergence in edge cases)
- 6 MEDIUM findings (suspicious, needs verification)
- 4 LOW findings (style / documentation)
- 8 CONFIRMED CORRECT (intentional 1:1 match)

## Findings

### CRITICAL-1: ValuesEqual(nil, nil) returns true but TS returns false
- File: `ivm/data.go:112-120`
- TS reference: `mono/packages/zql/src/ivm/data.ts:112-118`
- Issue: TS `valuesEqual(null, null) === false` (TS specifically chose this for join semantics — see TS comment "treats null as unequal to itself"). Go returns true.
- TS behavior:
  ```ts
  if (a == null || b == null) { return false; }  // either null → false
  return a === b;
  ```
- Go behavior:
  ```go
  if a == nil && b == nil { return true }   // <-- diverges
  if a == nil || b == nil { return false }
  return a == b
  ```
- Repro: any join with null FK on parent matching null FK on child. TS: no match. Go: matches.
- Impact propagates to: `join_utils.go:24 IsJoinMatch`, `source.go:152 splitEdit comparison`, `flipped_join.go:414 constraintsAreCompatible` would also use this. Also `union_fan_in.go` would use it indirectly via predicate construction.
- Fix: Remove the `if a == nil && b == nil { return true }` block. The original analysis (#19) introduced this divergence; the prior version was actually correct per TS.

### CRITICAL-2: ConstraintMatchesRow uses Go interface `!=` not ValuesEqual
- File: `ivm/source.go:593-603`
- TS reference: `mono/packages/zql/src/ivm/constraint.ts:16-26`
- Issue: TS uses `valuesEqual(row[key], constraint[key])`. Go uses `row[k] != v` (interface equality), which:
  1. Differs from valuesEqual semantics for null handling (Go interface: `nil == nil` true; TS valuesEqual: returns false)
  2. Fails across types: `int64(5) != float64(5)` evaluates true in Go even though the values represent the same number, while TS's `===` would fail too — BUT after NormalizeRow, row values are float64 yet Constraint values (built from rows or AST literals) may be other numeric types depending on the source path.
- Specifically: when `BuildJoinConstraint` (join_utils.go:34) builds a constraint from a row, it copies the value as-is. If the source row has a numeric column that wasn't normalized (e.g., a column not present in the column map), constraint contains the raw type, and matching against another row's normalized value fails.
- Fix: Use `ivm.ValuesEqual(row[k], v)` (and ensure ValuesEqual itself matches TS — see CRITICAL-1).

### CRITICAL-3: Filter predicate literal type mismatch with normalized row values
- File: `builder/filter.go:67-90`, `sqlite/table_source.go:689-724`
- TS reference: `mono/packages/zql/src/builder/filter.ts:78-94, 108-150`
- Issue: AST literals arrive via msgpack with `UseLooseInterfaceDecoding(true)`, so a JSON `5` becomes Go `int64(5)`. But row values for "number" columns are normalized to `float64` (source.go:289, table_source.go:113). When the predicate evaluates `row["count"] > 5`:
  - Go: `CompareValues(float64(N), int64(5))` → falls through to **panic** at `data.go:106`
  - TS: `lhs > rhs` works because JS has one number type
- The same applies to `=`/`!=` via `ValuesEqual` (returns false because interface type differs, even if value is same).
- Predicate path is exercised when: a query has EXISTS conditions (in-memory FilterPredicate is built — `builder.go:158`).
- Fix: When constructing predicates, normalize literal values against the column schema's type. Either:
  1. Pass column schema into BuildPredicate and convert literals at predicate-build time, OR
  2. Convert literals once at AST ingestion using `FromSQLiteType` keyed by column type from the ValuePos.ColType field (this field exists in `sqlite/query_builder.go:32` but isn't populated for in-memory predicates).

### HIGH-1: Skip cursor Row is not normalized vs row values
- File: `builder/builder.go:96-103`, `ivm/skip.go:54-57`
- TS reference: `mono/packages/zql/src/ivm/skip.ts:83-86`
- Issue: `ast.Start.Row` (from msgpack-decoded AST) is passed directly to `NewSkip` without normalization. `Skip.shouldBePresent` uses `s.comparator(s.bound.Row, row)` which calls `CompareValues`. If `bound.Row["created_at"]` is `int64` (from msgpack) and `row["created_at"]` is `float64` (post-NormalizeRow), `CompareValues` panics.
- Fix: At builder time, normalize `ast.Start.Row` against the table's column schema. Either:
  1. `source.NormalizeRow(ast.Start.Row)` before constructing the `ivm.Bound`, OR
  2. Convert per-column using `FromSQLiteType(value, columns[k].Type)`.

### HIGH-2: Filter literals not normalized for ValuesEqual (= / !=)
- File: `builder/filter.go:75-90`, `sqlite/table_source.go:697-705`
- TS reference: `mono/packages/zql/src/builder/filter.ts:108-150`
- Issue: Even for `=` and `!=` (which use `ValuesEqual`, not `CompareValues`), the type-tagged interface comparison `a == b` returns false for `(int64(5), float64(5))`. So predicate `comment_count = 5` would always return false when comment_count is normalized to float64 and literal is int64.
- This is the same root cause as CRITICAL-3 but specifically for equality ops. Splitting because the failure mode is silent-wrong (false negative) rather than panic.
- Same fix as CRITICAL-3.

### HIGH-3: Take pushEditChange oldCmp==0/newCmp<0 limit>1: Go falls back where TS panics
- File: `ivm/take.go:343-347`
- TS reference: `mono/packages/zql/src/ivm/take.ts:502-505`
- Issue: When oldCmp==0 (the edited row IS the bound), newCmp<0 (new value sorts before bound), and limit>1, TS asserts `beforeBoundNode !== undefined`. If the fetched beforeBoundNode is undefined, TS panics. Go silently falls back to `replaceBoundAndForwardChange()` (line 346), treating the new value as the new bound.
- Risk: If this state is reachable in some input that TS would reject as invalid, Go would produce a different result than TS. Conversely, if the state is unreachable, the Go code is dead — harmless but unverified.
- Repro: window with size=1, limit>1 (somehow), edit that row to a smaller value. With proper invariants (size==limit when bound is set?), this might be unreachable.
- Fix: Either match TS by panicking (`panic("beforeBoundNode must be found")`), or document why the Go path is reachable when TS's is not.

### HIGH-4: Source overlay not cleared on panic
- File: `ivm/source.go:199-215`, `ivm/parallel.go:51-96`
- TS reference: `mono/packages/zql/src/ivm/memory-source.ts:552-578` (TS doesn't have a try/finally either, but generators clean up on throw)
- Issue: If `conn.Output.Push` panics mid-loop, `ms.overlay = nil` (line 214) never runs. The next push would see a stale overlay. In `genPushAndWriteParallel` (parallel.go), if a goroutine panics, the panic propagates but `ms.overlay = nil` (line 95) may not run depending on goroutine semantics.
- TS uses generators; an exception thrown during iteration unwinds, and the source's `setOverlay(undefined)` after the loop may still not run. So this is roughly parity with TS — but Go has defer available and could be more robust.
- Fix: Wrap the overlay clearing with `defer func() { ms.overlay = nil }()` at function entry.

### MEDIUM-1: AST literals not normalized when used in IN/NOT IN
- File: `builder/filter.go:174-197` (`valueIn`)
- TS reference: `mono/packages/zql/src/builder/filter.ts:133-145`
- Issue: `valueIn` iterates `right` (a slice) and calls `ValuesEqual(left, v)`. If left (a row value) is normalized to float64 but the slice elements are int64 (from msgpack-decoded AST array of literals), no matches. Same root cause as CRITICAL-3.
- Fix: Same as CRITICAL-3.

### MEDIUM-2: MemorySource.NormalizeRow doesn't cover all column types
- File: `ivm/source.go:289-335`
- TS reference: `mono/packages/zqlite/src/table-source.ts:588-643` (`fromSQLiteTypes` → `fromSQLiteType`)
- Issue: MemorySource.NormalizeRow only handles "number" and "boolean" column types. Compare to TableSource.NormalizeRow (table_source.go:113-125) which calls FromSQLiteType for every column — covering string, json, etc.
- Practical risk: MemorySource is rarely used in production paths (most data goes through TableSource and SQLite). But MemorySource is exposed and used in tests; mismatched type coverage between sources could mask test failures or surface bugs in alternate code paths.
- Fix: Refactor MemorySource.NormalizeRow to delegate to the same FromSQLiteType (or a shared utility), matching TS's table-source approach.

### MEDIUM-3: Cap operator missing from Go port
- File: (missing) — TS: `mono/packages/zql/src/ivm/cap.ts`
- Issue: The Cap operator (count-limit without ordering, used for EXISTS subqueries) is implemented in TS but not in Go. Currently appears unreferenced outside its test file (no production usage in TS builder), so this is forward-looking. If TS adopts Cap (e.g., to optimize EXISTS queries), Go would need to implement it.
- Fix: Track as future work; not currently a divergence.

### MEDIUM-4: PushAccumulated zero-value Change on unexpected absent type
- File: `ivm/push_accumulated.go:78-100`
- TS reference: `mono/packages/zql/src/ivm/push-accumulated.ts:215-218` (uses `must(addChange ?? removeChange)`)
- Issue: In `case ChangeTypeEdit` line 78-81: if neither hasEdit, hasAdd, nor hasRemove is true, the function falls through to `output.Push(addEmptyRelationships(removeChange), pusher)` with `removeChange` being a zero-value Change (Type==0==ChangeTypeAdd, empty Node). TS would crash via `must()`.
- The prior analysis (#42) said this was FIXED but only for ADD/REMOVE handler paths, not EDIT and CHILD. EDIT branch still has this hole. Reachable only if accumulated is non-empty but contains exclusively types other than ADD/REMOVE/EDIT — which shouldn't happen per the comment but isn't enforced.
- Fix: Add a panic before the final `return output.Push(addEmptyRelationships(removeChange), pusher)` if neither hasAdd nor hasRemove.

### MEDIUM-5: Source overlay reads from non-synchronized map field
- File: `ivm/parallel.go:51-79`, `ivm/source.go:208`
- TS reference: not applicable (TS is single-threaded)
- Issue: In parallel push, `ms.overlay` is set (line 51), then goroutines call `conn.Output.Push(...)` which may invoke `Fetch` on this source from a different goroutine. The pointer assignment is atomic on 64-bit, but Go's memory model does NOT guarantee visibility between goroutines without synchronization. A goroutine may observe a stale `nil` overlay.
- The doc claims `pointer read is atomic on 64-bit` but atomicity is not the same as memory ordering. A Push-triggered downstream fetch within the same goroutine sees the overlay (because of program order). But cross-goroutine fetches (e.g., a Join's child fetch hitting the same source) may not.
- Realistic risk: If pipelines fan out across goroutines AND any downstream operator re-fetches the same source, the overlay may be invisible. Whether this happens depends on the pipeline topology.
- Fix: Use atomic.Pointer[Overlay] for `overlay` field, or document that parallel mode requires pipelines to be source-disjoint.

### MEDIUM-6: Constraint comparison in constraintsAreCompatible uses CompareValues (nil-equal) instead of valuesEqual (nil-unequal)
- File: `ivm/flipped_join.go:414-423`
- TS reference: `mono/packages/zql/src/ivm/constraint.ts:33-43`
- Issue: TS `constraintsAreCompatible` uses `valuesEqual` (which treats null !== null). Go uses `CompareValues(v, bv) != 0` (which treats nil === nil). For null FK values on both sides, TS returns false (incompatible), Go returns true (compatible).
- Combined with CRITICAL-1, both implementations are wrong relative to each other but in different ways: TS's valuesEqual returns false for both-null; Go's ValuesEqual currently returns true for both-null; Go's CompareValues treats nil correctly per Go semantics but doesn't match TS valuesEqual.
- Fix: Use the corrected ValuesEqual (after fixing CRITICAL-1) here. Also update the call site to use ValuesEqual.

### LOW-1: Duplicate maybeSplitAndPushEditChange implementation
- File: `ivm/source.go:605-622` and `ivm/filter.go:90-102`
- Issue: Two near-identical implementations of the same function (one exported, one unexported). Functionally equivalent. Style cleanup.

### LOW-2: applyConstraint scans all when TS uses break (perf only, prior #22)
- File: `ivm/source.go:564-572`
- Already documented in PORT_CORRECTNESS_ANALYSIS.md as performance-only. Acknowledged.

### LOW-3: FlippedJoin overlay filter uses pointer comparison that's effectively constant
- File: `ivm/flipped_join.go:229-233`
- Already documented in PORT_CORRECTNESS_ANALYSIS.md as benign with unique PKs. Acknowledged.

### LOW-4: Streamer EDIT does not recurse into relationships
- File: `engine/streamer.go:92-104`
- TS reference: `mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:1523-1527`
- Issue: Both Go and TS use empty-relationships for EDIT in streamNodes. Go matches TS. Listed here only because the TS path uses `() => [{row, relationships: {}}]` (explicitly empty) — confirming intentional design.

## Confirmed Correct (1:1 match)

1. **Skip getStart three-way return** (skip.go:109) — three-value semantics match TS (Bug 4 fix confirmed).
2. **FlippedJoin localExists latch across parent loop** (flipped_join.go:284-316) — `exists` is a function parameter mutated across loop iterations, matching TS closure capture (Bug 3 fix confirmed).
3. **FanIn identity merge keeps existing (first occurrence)** (fan_in.go:86-88) — matches TS's `identity` from sentinels.
4. **PushAccumulated EDIT → ADD+REMOVE → reconstituted EDIT** (push_accumulated.go:72-74) — matches TS line 202-213.
5. **Source overlay set/clear lifecycle** (source.go:199-215) — matches TS per-conn pattern.
6. **Source applyStart for reverse fetch** (source.go:523-561) — filter-then-reverse approach is semantically equivalent to TS's reversed indexComparator.
7. **Join.processParentNode inprogress state propagation** (join.go:216-248) — overlay state correctly visible during child stream lazy evaluation.
8. **MergeFetches dedup keeps first occurrence** (union_fan_in.go:190-227) — matches TS's reduce-with-strict-less-than.

## Files audited

- `ivm/source.go` (vs `memory-source.ts`) — 3 findings (CRITICAL-2, HIGH-4, MEDIUM-2)
- `ivm/take.go` (vs `take.ts`) — 1 finding (HIGH-3)
- `ivm/flipped_join.go` (vs `flipped-join.ts`) — 1 finding (MEDIUM-6); benign LOW-3 acknowledged from prior
- `ivm/exists.go` (vs `exists.ts`) — no new findings; confirmed correct
- `ivm/join.go` (vs `join.ts`) — no new findings; confirmed correct
- `ivm/join_utils.go` (vs `join-utils.ts`) — uses ValuesEqual; affected by CRITICAL-1
- `ivm/push_accumulated.go` (vs `push-accumulated.ts`) — 1 finding (MEDIUM-4)
- `ivm/union_fan_in.go` (vs `union-fan-in.ts`) — no new findings; confirmed correct
- `ivm/union_fan_out.go` (vs `union-fan-out.ts`) — no new findings; confirmed correct
- `ivm/fan_in.go` (vs `fan-in.ts`) — no new findings; confirmed correct
- `ivm/fan_out.go` (vs `fan-out.ts`) — no new findings; confirmed correct
- `ivm/skip.go` (vs `skip.ts`) — affected by HIGH-1 (cursor Row not normalized)
- `ivm/filter.go` (vs `filter.ts`, `filter-push.ts`, `maybe-split-and-push-edit-change.ts`) — 1 finding (LOW-1 duplicate)
- `ivm/data.go` (vs `data.ts`) — 1 finding (CRITICAL-1)
- `ivm/parallel.go` (no TS analog) — 1 finding (MEDIUM-5)
- `ivm/operator.go` (vs `operator.ts`) — confirmed correct
- `ivm/change.go` (vs `change.ts`) — confirmed correct
- `ivm/filter_operators.go` (vs `filter-operators.ts`) — confirmed correct
- `builder/builder.go` (vs `builder/builder.ts`) — affected by HIGH-1
- `builder/filter.go` (vs `builder/filter.ts`) — 2 findings (CRITICAL-3, HIGH-2, MEDIUM-1)
- `sqlite/query_builder.go` (vs `zqlite/query-builder.ts` and `table-source.ts`) — FromSQLiteType matches TS sufficiently for uint64 extension
- `sqlite/table_source.go` (vs `zqlite/table-source.ts`) — affected by CRITICAL-3, HIGH-2
- `engine/engine.go` (vs `pipeline-driver.ts`) — confirmed correct
- `engine/streamer.go` (vs `pipeline-driver.ts` Streamer) — confirmed correct (LOW-4 design)
- TS files with no Go counterpart: `cap.ts` (MEDIUM-3); also `array-view.ts`, `view.ts`, `view-apply-change.ts`, `snitch.ts`, `catch.ts`, `default-format.ts`, `stopable-iterator.ts`, `skip-yields.ts`, `memory-storage.ts` (all client/test/helper-side and intentionally not ported).

## Notes on Methodology

- The audit was deep-reading-based: each Go file's function bodies compared line-by-line with the corresponding TS function bodies.
- The PORT_CORRECTNESS_ANALYSIS.md was consulted to avoid re-flagging triaged items.
- Findings marked CRITICAL/HIGH/MEDIUM each name a specific, reproducible input class that would diverge.
- The interaction between msgpack's `UseLooseInterfaceDecoding(true)` (which produces int64/uint64 for AST literals) and `NormalizeRow` (which produces float64 for "number" columns) is the dominant source of new CRITICAL/HIGH findings. The shadow-mode validation passed because the zbugs workload exercises mostly string PKs and `id`-based filters, missing the numeric-filter/numeric-cursor paths.

## Suggested Verification

For CRITICAL-1: a simple test against MemorySource verifies behavior:
```go
ms.Push(MakeSourceChangeAdd(Row{"id": "1", "name": nil}))
ms.Push(MakeSourceChangeAdd(Row{"id": "2", "name": nil}))
// Build a Join where parent.name = child.name. TS: 0 matches (null != null). Go: 4 matches.
```

For CRITICAL-3/HIGH-1/HIGH-2: any query with a numeric filter literal (e.g., `comment_count > 5`) over data from msgpack-encoded changes. Either panics on `>` or silently returns false on `=`.

For HIGH-3: requires a specific Take state that's hard to reach in normal workflows. A targeted unit test setting size=1, limit=2, then edit-down would surface this.
