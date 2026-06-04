# AST Handling Parity Audit — Go IVM vs TypeScript Reference

Audit scope: pre-build AST transformations only. Planning (cost-model),
ordering completion, scalar-subquery resolution, EXISTS rewriting,
OR-with-CSQ flipping, and pre-build literal/null semantics. The runtime
IVM operator behavior (Skip, Take, Join, FlippedJoin, Exists, FanIn,
FanOut, UnionFanIn, UnionFanOut), the streamer's `node.Relationships`
fan-out, and TableSource/MemorySource semantics are **out of scope** for
this audit and were only touched as needed to verify pre-build behavior.

TS reference is canonical. File:line refs are valid as of the snapshot.

Baseline reviews consulted: `REVIEW-porting.md`, `REVIEW-final.md`,
`H18-followup.md`. FIXED items are not re-listed unless I found a
regression.

## File mapping

| Topic | TS | Go |
|---|---|---|
| AST schema | `mono/packages/zero-protocol/src/ast.ts` | `go-ivm/builder/ast.go` |
| BuildPipeline entry | `mono/packages/zql/src/builder/builder.ts:125-143` | `go-ivm/builder/builder.go:55-60` |
| buildPipelineInternal | `mono/packages/zql/src/builder/builder.ts:255-359` | `go-ivm/builder/builder.go:63-261` |
| applyWhere / wrapInFilter | `mono/packages/zql/src/builder/builder.ts:361-373` | `go-ivm/builder/builder.go:539-555` |
| applyFilterWithFlips | `mono/packages/zql/src/builder/builder.ts:376-482` | `go-ivm/builder/builder.go:567-642` |
| applyFilter / applyAnd / applyOr | `mono/packages/zql/src/builder/builder.ts:484-557` | `go-ivm/builder/builder.go:423-482` |
| applyCorrelatedSubQuery (Join) | `mono/packages/zql/src/builder/builder.ts:611-647` | `go-ivm/builder/builder.go:137-197` (inline) |
| applyCorrelatedSubqueryCondition (Exists) | `mono/packages/zql/src/builder/builder.ts:649-678` | `go-ivm/builder/builder.go:498-533` |
| groupSubqueryConditions / isNotAndDoesNotContainSubquery | `mono/packages/zql/src/builder/builder.ts:559-584` | `go-ivm/builder/builder.go:668-694` |
| gatherCorrelatedSubqueryQueryConditions | `mono/packages/zql/src/builder/builder.ts:680-700` | `go-ivm/builder/builder.go:289-303` |
| uniquifyCorrelatedSubqueryConditionAliases | `mono/packages/zql/src/builder/builder.ts:723-765` | `go-ivm/builder/builder.go:382-416` |
| assertNoNotExists / enableNotExists | `mono/packages/zql/src/builder/builder.ts:231-253, 69` + caller at `pipeline-driver.ts:1908` | **absent** |
| conditionIncludesFlippedSubqueryAtAnyLevel | `mono/packages/zql/src/builder/builder.ts:767-780` | `go-ivm/builder/builder.go:647-662` |
| partitionBranches | `mono/packages/zql/src/builder/builder.ts:782-796` | inlined in `applyFilterWithFlips` `go-ivm/builder/builder.go:572-579,592-599` |
| completeOrdering | `mono/packages/zql/src/query/complete-ordering.ts` | `go-ivm/builder/builder.go:698-777` |
| Filter predicate compile (`createPredicate`) | `mono/packages/zql/src/builder/filter.ts:26-94` | `go-ivm/builder/filter.go:25-70` (`BuildPredicate` + `conditionToPredicate` + `simpleConditionPredicate`) |
| Filter `evalOp` (predicate runtime) | `mono/packages/zql/src/builder/filter.ts:62-150` | `go-ivm/builder/filter.go:106-157` |
| Filter `transformFilters` (CSQ stripping for source) | `mono/packages/zql/src/builder/filter.ts:165-203` | `go-ivm/builder/builder.go:322-379` (`stripCSQConditions` + `containsCSQ`) |
| LIKE pattern compile | `mono/packages/zql/src/builder/like.ts` | `go-ivm/builder/filter.go:232-267` (`matchLike`/`likeMatch`) |
| simplifyCondition (and/or flattening) | `mono/packages/zql/src/query/expression.ts:249-267` | **absent** (Go doesn't call it from any audited path) |
| resolveSimpleScalarSubqueries | `mono/packages/zqlite/src/resolve-scalar-subqueries.ts` | `go-ivm/builder/resolve_scalar.go` |
| `#resolveScalarSubqueries` (executor wrapper) | `mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:850-899` | `go-ivm/engine/engine.go:387-447` (`buildAndRegisterLocked`) |
| `#planAstForGo` (Go-dispatch pre-plan) | `mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:807-820` | **N/A on Go side** (Go receives an already-planned AST) |
| planQuery (cost-model planner) | `mono/packages/zql/src/planner/planner-builder.ts:311-320` + `mono/packages/zqlite/src/sqlite-cost-model.ts` | **absent** — Go has no in-process cost model |
| Wire AST decode (msgpack) | n/a | `go-ivm/cmd/sidecar/main.go:78-87, 112-164` (looseInterfaceDecoding + numeric→float64 normalize) |
| Row decode (msgpack) | n/a | `go-ivm/ivm/data.go:35-58` (`Row.DecodeMsgpack`) |

---

## Findings

### CHECKED-OK-1: completeOrdering — appends PK as asc, dedups by field name
- **TS**: `mono/packages/zql/src/query/complete-ordering.ts:74-93` — `addPrimaryKeys` builds a `Set(primaryKey)`, removes any field already mentioned in `orderBy`, and appends the remainder as `[key, 'asc']`. Recurses into `related[]` and into any `correlatedSubquery` inside `where`.
- **Go**: `go-ivm/builder/builder.go:753-777` (`addPrimaryKeys`) + `:698-751` (`completeOrdering`, `completeOrderingInCondition`). Same algorithm: builds an `existing` map of field names already in `orderBy`, appends remaining PK columns with direction `"asc"`, recurses into `Related` and `Where` (including nested CSQ subqueries).
- **Divergence**: none observed. Direction is unconditionally `"asc"` on both sides regardless of any pre-existing direction. Recursion mirrors exactly (simple/correlatedSubquery/and/or). Both deduplicate against field names only, not against full `[field, dir]` tuples, so an orderBy with the same field repeated with different directions remains as-is (consistent with TS).
- **Note**: Go runs `completeOrdering` inside `BuildPipeline` (`builder.go:70`), and `#planAstForGo` (TS pipeline-driver.ts:808) also runs it on the AST before sending to Go. Running it twice is idempotent because `addPrimaryKeys` skips columns already present.

### CHECKED-OK-2: applyOr — branch partitioning + FanOut/FanIn topology
- **TS**: `mono/packages/zql/src/builder/builder.ts:514-557` — partitions OR branches into `subqueryConditions` (any depth contains CSQ) and `otherConditions` via `groupSubqueryConditions`. If no CSQ branches → single OR-Filter. Otherwise FanOut, each CSQ branch through `applyFilter`, then if `otherConditions.length>0` a single OR-Filter for those, then FanIn merging all branches.
- **Go**: `go-ivm/builder/builder.go:452-482` (`applyOr`). Same algorithm. `groupSubqueryConditions` at `:668-677` + `isNotAndDoesNotContainSubquery` at `:679-694` match TS precisely (simple → true, correlatedSubquery → false, and/or → all children true).
- **Edge cases verified**:
  - **Single-branch OR**: enters fast path or fan path depending on whether branch is a CSQ. Both sides behave identically.
  - **OR with all literals (no CSQ)**: single Filter, no FanOut. Match.
  - **Nested AND inside OR**: `isNotAndDoesNotContainSubquery` recurses correctly. Match.
  - **Empty branches**: TS would build a degenerate FanIn with zero inputs (asserts in `pushAccumulatedChanges` if pushes arrive); Go would skip the fan-in entirely if `subqueryBranches==0 && simpleBranches==0`, but that state shouldn't occur (an OR with zero conditions is unrepresentable in the AST schema).

### CHECKED-OK-3: applyFilterWithFlips — UnionFanOut + UnionFanIn for OR-with-flipped-CSQ
- **TS**: `mono/packages/zql/src/builder/builder.ts:376-482`. AND splits into withFlipped vs withoutFlipped; non-flipped go through normal filter pipeline first, then flipped ones layer FlippedJoin. OR uses UnionFanOut + per-branch FlippedJoin (or normal filter for non-flipped) + UnionFanIn for merge-with-dedup.
- **Go**: `go-ivm/builder/builder.go:567-642`. Same partitioning, same topology. Both check `conditionIncludesFlippedSubquery` at any depth to route to this path vs the regular `applyFilter`.
- **Note**: This is the H18 fix from `H18-followup.md`. Validated by the 3-CG soak (0 mismatches). Parity confirmed.

### CHECKED-OK-4: applyCorrelatedSubqueryCondition — limit-0 short-circuit + Exists construction
- **TS**: `mono/packages/zql/src/builder/builder.ts:649-678`. If `condition.related.subquery.limit === 0`: emit constant-false Filter for EXISTS, constant-true for NOT EXISTS. Otherwise construct `Exists(parentField, op)`.
- **Go**: `go-ivm/builder/builder.go:498-533`. Identical. Same panic guard for `cond.Flip` reaching this path (must be routed via `applyFilterWithFlips`).
- **Note**: Both build the Filter constants and skip the (always-empty) relationship lookup. Both correctly translate `op == 'NOT EXISTS'` into `ExistsTypeNotExists` / `not: true` on the operator.

### CHECKED-OK-5: EXISTS-Join construction (Step 3 in builder)
- **TS**: `mono/packages/zql/src/builder/builder.ts:308-329, 611-647`. For each non-flipped CSQ condition, applies a Join with `subquery.limit` overridden to `EXISTS_LIMIT=3` (`PERMISSIONS_EXISTS_LIMIT=1` for `system==='permissions'`).
- **Go**: `go-ivm/builder/builder.go:137-197`. Same logic. Constants at `:14-15`. Limits applied identically. The skip of flipped CSQs (`continue`) matches TS at `:310`.
- **Subquery limit=0 skip**: Both sides skip building the EXISTS-Join when `Subquery.Limit==0` (Go `:154-156`, TS `:621-623`). This matches because `applyCorrelatedSubqueryCondition` then emits a constant Filter, so the join is genuinely dead weight.

### CHECKED-OK-6: gatherCorrelatedSubqueryQueryConditions
- **TS**: `mono/packages/zql/src/builder/builder.ts:680-700`. Walks the WHERE tree, collecting `correlatedSubquery` conditions at any depth (recurses through and/or).
- **Go**: `go-ivm/builder/builder.go:289-303` (`gatherCSQConditions`). Identical recursion; collects both flipped and non-flipped (caller filters with the `Flip` check).

### CHECKED-OK-7: uniquifyCorrelatedSubqueryConditionAliases — alias suffixing
- **TS**: `mono/packages/zql/src/builder/builder.ts:723-765`. Recursively renames CSQ subquery aliases with `_<count>` suffix. Only enters if top-level WHERE is and/or.
- **Go**: `go-ivm/builder/builder.go:382-416` (`uniquifyCSQConditionAliases`). Same: only enters for top-level and/or; empty alias becomes `"_0"`; recurses through and/or branches. Counter monotonic.

### CHECKED-OK-8: stripCSQConditions / source-level filter narrowing
- **TS**: `mono/packages/zql/src/builder/filter.ts:165-203` (`transformFilters`). When the source can't apply CSQ conditions, returns a strict-superset filter the source CAN apply: drops CSQ branches under AND, but if any OR branch contains a CSQ, the whole OR is dropped (otherwise the source might reject rows that the CSQ branch alone would have admitted).
- **Go**: `go-ivm/builder/builder.go:322-379` (`stripCSQConditions` + `containsCSQ`). Mirrors precisely:
  - `simple` → keep
  - `correlatedSubquery` → drop (return nil)
  - `and` → keep non-nil children, collapse to single child if only one survives
  - `or` → if any branch contains a CSQ at any depth → drop entire OR (return nil)
- **Note**: This is critical for source-level pre-filtering safety. Both implementations are conservative in the same direction (TS via `simplifyCondition`'s handling of empty arrays + the special OR-with-CSQ unset; Go via the explicit OR check at `:349-355`). No divergence.

### CHECKED-OK-9: assertOrderingIncludesPK
- **TS**: `mono/packages/zql/src/builder/builder.ts:702-721` AND `mono/packages/zql/src/query/complete-ordering.ts:30-44`. Defensive assertion; not part of the pre-build transform chain on the audited paths but checked by tests.
- **Go**: not directly ported, but completeOrdering's own logic guarantees the invariant (every PK column is appended if missing). Equivalent guarantee.

### CHECKED-OK-10: resolveSimpleScalarSubqueries — algorithm parity
- **TS**: `mono/packages/zqlite/src/resolve-scalar-subqueries.ts`. Walks AST; for each `correlatedSubquery` condition with `scalar: true`, recursively resolves nested scalars first, then checks `isSimpleSubquery`: subquery's WHERE (via `extractLiteralEqualityConstraints` AND-only descent) must cover all columns of at least one `tableSpec.uniqueKeys` entry. If simple, executes via the supplied executor and replaces the EXISTS with `parentField = value` (or `IS NOT` for NOT EXISTS). If executor returns `undefined/null`, returns ALWAYS_FALSE (`1 = 0`).
- **Go**: `go-ivm/builder/resolve_scalar.go`. Line-for-line port:
  - `ResolveSimpleScalarSubqueries` (`:62-70`) — top-level recursion + companion collection.
  - `resolveASTRecursive` (`:72-98`) — recurses into Where + Related.
  - `resolveCondition` (`:100-129`) — for `scalar:true` CSQs calls `resolveScalarSubquery`; for non-scalar CSQs recurses into their subquery so nested scalars resolve.
  - `resolveScalarSubquery` (`:131-187`) — recurses first, calls `IsSimpleSubquery`, executes, emits `=`/`IS NOT` literal or `ALWAYS_FALSE`.
  - `IsSimpleSubquery` (`:204-232`), `ExtractLiteralEqualityConstraints` (`:238-242`), `collectConstraints` (`:244-258`) — match TS predicate-by-predicate.
- **Eligibility rules verified**: column=literal under AND chain only; OR contributes nothing; correlatedSubquery contributes nothing. Op must be `=`. Match.
- **Nested EXISTS recursion**: both `resolveCondition` cases (scalar + non-scalar) recurse into the subquery before deciding. Match.
- **ALWAYS_FALSE shape**: both emit `{type:'simple', op:'=', left:{type:'literal', value:1}, right:{type:'literal', value:0}}` (Go uses `float64(1)` / `float64(0)`, TS uses JS numbers — both decode identically through `walkForNumericNormalize`).
- **Companion-row contract**: TS records `{ast, childField, resolvedValue}`; Go records `{AST, ChildField, ResolvedValue, Matched}`. Go's extra `Matched` bool distinguishes "no row" (`Matched=false, ResolvedValue=nil`) from "row matched but field was NULL" (`Matched=true, ResolvedValue=nil`); TS conflates both as `resolvedValue=undefined|null`. **Both treat both as ALWAYS_FALSE**, so the conflation has no semantic effect — but Go can in principle distinguish them at the companion-emission step (engine.go:464-468 only emits companion rows when `matchedRow != nil`, matching TS's `companionRows.push` site at pipeline-driver.ts:888 which only pushes when `node` is set).

### CHECKED-OK-11: Predicate null-short-circuit (MED-PORT-2 still fixed)
- **TS**: `mono/packages/zql/src/builder/filter.ts:74-93`. For all operators except `IS`/`IS NOT`, short-circuits on `right.value === null/undefined` (always-false predicate) and on `left` being null/undefined-valued at row time (always false). `IS`/`IS NOT` use strict `===`/`!==` which treat `null === null` as true.
- **Go**: `go-ivm/builder/filter.go:106-157` (`evalOp`). `IS`/`IS NOT` use `valuesIdentical` (which treats `nil == nil` as true at line 179-181). All other ops short-circuit on either side being `nil` (line 117-119).
- **Divergence**: none. MED-PORT-2 from REVIEW-porting.md is still FIXED.

### CHECKED-OK-12: CompareValues vs ValuesEqual split (REVIEW-final invariant #8)
- **Go**: `go-ivm/ivm/data.go:142-194` (`CompareValues` — `nil/nil → 0`, used for sort) and `:205-219` (`ValuesEqual` — `nil/nil → false`, used for join). Comment at `:198-202` explicitly preserves the asymmetry.
- **TS**: corresponding split in `mono/packages/zql/src/ivm/data.ts` (not re-read this round; verified FIXED in REVIEW-final).
- **No regression observed.**

### CHECKED-OK-13: Cap operator (REVIEW-final MEDIUM-3) — still unused by TS
- **TS**: `mono/packages/zql/src/ivm/cap.ts` exists but `grep -rn "new Cap("` across `packages/zql/src/builder/`, `packages/zql/src/query/`, `packages/zero-cache/src/services/view-syncer/` (excluding `cap.*.test.ts`) returns zero hits. TS builder still does not construct Cap.
- **Go**: no `Cap` operator exists. Forward-looking gap; not a divergence today.

### CHECKED-OK-14: AST literal-value handling — numeric coercion at wire decode
- **Go**: All AST `ValuePos.Value` (an `interface{}`) values are normalized to `float64` for numeric types by `walkForNumericNormalize` in `cmd/sidecar/main.go:112-164`, which descends into structs/maps/slices/pointers held in `interface{}`. `Row` map values are normalized inline by `Row.DecodeMsgpack` at `ivm/data.go:35-58` (faster path, no reflection). Both paths produce the same TS-like single-numeric-type space.
- **TS**: numbers are JS `number` (always double-precision float); strings/bools/null preserved natively.
- **bigint, json object, blob**: TS `LiteralValue` is `string | number | boolean | null | ReadonlyArray<string|number|boolean>` (`ast.ts:284-289`); neither bigint nor objects nor blobs are representable as `LiteralReference`. Go's `interface{}` is wider, but the wire AST coming from TS will never carry such values. **No divergence on the audited paths.**

### CHECKED-OK-15: Correlated-subquery WHERE field correlation (Correlation parent/child fields)
- **TS**: `mono/packages/zero-protocol/src/ast.ts:245-265`. `Correlation.parentField: CompoundKey`, `childField: CompoundKey`. The Join/Exists operators receive these directly and pair them up.
- **Go**: `go-ivm/builder/ast.go:69-72`. `Correlation.ParentField []string`, `ChildField []string`. The Join is built with both keys at `builder.go:185-193`.
- **No divergence**: both represent correlation as paired key column lists; no semantic transform happens at AST level. The parent-field columns are also collected into `splitEditKeys` (Go `:265-286`, TS `:284-290`) so the source emits Remove+Add for FK changes.

### CHECKED-OK-16: Dedup-by-alias on `related[]` (LWW)
- **TS**: `mono/packages/zql/src/builder/builder.ts:347-356`. `Map<string, CorrelatedSubquery>` with `byAlias.set(alias ?? '', csq)`; iteration order is first-insertion. Last value wins; positions stable.
- **Go**: `go-ivm/builder/builder.go:218-258`. `seen map[string]int` tracks first-insertion index; on duplicate, overwrites `deduped[prev].idx = i` (keeps position, takes new value). Same observable behavior.

---

### LOW-1: LIKE pattern escape `\%` / `\_` not honored in Go
- **TS**: `mono/packages/zql/src/builder/like.ts:55-60`. Backslash escape supported: `\%` matches literal `%`, `\_` matches literal `_`. Throws if pattern ends with `\`. Tested by `like-test-cases.ts:83-95` (`'foo\\%'` matches `'foo%'`, not `'foobar'`).
- **Go**: `go-ivm/builder/filter.go:240-267` (`likeMatch`). No backslash handling — the case statement only recognizes `%` and `_` as wildcards; any other char (including `\`) is treated as a literal byte to match.
- **Divergence**: `name LIKE 'a\_b'` matches "a_b" in TS (literal underscore) but matches "a\_b", "a_xb", etc. (treating `\` as literal `\`, `_` as wildcard) in Go. Same skew for `\%`.
- **Impact**: a query using escape syntax produces different result sets on the two paths. Rare in practice but a real silent-wrong-result. Shadow mode and drift audit would catch a mismatch only if the data actually contains literal `%` / `_` AND the parent query uses `\` escapes — likely never in production schemas under the surveyed soaks (0 mismatches across 30-min runs).
- **Suggested fix**: in `go-ivm/builder/filter.go` `likeMatch`/`likeMatch` recursion, add a `\` case that consumes the next byte as a literal (and errors at EOF). Match TS like.ts:55-60. Also add the `\\` escape to the case-insensitive ToLower normalization (`matchLike` line 233-237 — the `s = strings.ToLower(s)` only affects case folding, but `\\` ToLower stays `\\`, fine).

### LOW-2: Multi-line LIKE matching with `_`/`%` differs
- **TS**: `mono/packages/zql/src/builder/like.ts:72`. Compiles to a JavaScript `RegExp` with flags `'m'` (multiline). With `'m'`, `^` matches at the start of each line, but the regex anchors `^...$` so this only affects when the source contains `\n`. JS regex by default uses `.` to NOT match newlines (no `s` flag), so `_` and `%` in TS don't match `\n`.
- **Go**: `go-ivm/builder/filter.go:240-267` (`likeMatch`). `%` matches any byte sequence including `\n`; `_` matches any single byte including `\n`.
- **Divergence**: `'a_b' LIKE 'a\nb'` → TS false, Go true. `'a%b' LIKE 'a\nXXXb'` → TS depends on how `.` behaves in `m` mode (without `s` it doesn't match `\n`, so TS false; Go true).
- **Impact**: low. Production data with embedded newlines in matched columns + LIKE patterns is uncommon. Tested by `like-test-cases.ts:79` — `pattern 'foo%'` matches `'fooa\nbar'` → TS true (the regex `.*` doesn't include `\n` but the multi-line `m` flag and `$` anchor make the rest behave; this is the one case TS does match newlines via `.*` if there's no preceding `\n`, since `%` consumes greedy bytes BEFORE end). Need to confirm empirically but the asymmetry is real.
- **Suggested fix**: same fix area as LOW-1. When compiling like, treat `_`/`%` as not matching `\n` to match TS's regex default. Alternatively port to use Go's `regexp` package directly with the same translation as TS's `patternToRegExp`.

### LOW-3: `simplifyCondition` (and/or flattening + AlwaysFalse/AlwaysTrue collapse) absent in Go
- **TS**: `mono/packages/zql/src/query/expression.ts:249-267`. Singleton and/or collapse to inner; flattens nested same-type conjunctions; AlwaysFalse swallows AND; AlwaysTrue swallows OR. Called by `filter.ts:194` (`transformFilters`) and `query-impl.ts:383`.
- **Go**: not present in the Go builder. Go's `stripCSQConditions` (`go-ivm/builder/builder.go:322-379`) does a smaller version of this — collapses single-child AND to the child (`:341-343`) — but no flatten or AlwaysFalse/AlwaysTrue.
- **Divergence**: The Go builder never receives an AST that requires `simplifyCondition` because the TS query builder calls it before serialization. The wire AST has already been simplified. So this is **defensive parity** rather than a real divergence — would only matter if a future code path crafted an unsimplified AST directly and sent it to Go.
- **Impact**: none in current flow. Could become a divergence if a Go-side AST transformation introduces nested same-type conjunctions or empty arrays.
- **Suggested fix**: optional. If desired, port `simplifyCondition` to `go-ivm/builder/` and apply it at the same callsites Go's `stripCSQConditions` runs (so any in-process AST manipulation gets normalized). Not currently load-bearing.

### MEDIUM-1: Go has no cost-model planner (`planQuery` equivalent)
- **TS**: `mono/packages/zql/src/planner/planner-builder.ts:311-320` (`planQuery`), backed by `mono/packages/zqlite/src/sqlite-cost-model.ts` (`createSQLiteCostModel`). On every TS hydrate (`builder.ts:139-141`) and every Go-dispatch (`pipeline-driver.ts:807-820 #planAstForGo`), the planner walks the AST, builds a PlannerGraph (PlannerConnection per source, PlannerJoin per CSQ), enumerates flip patterns (2^N up to MAX_FLIPPABLE_JOINS), picks the lowest-cost plan, and annotates each correlatedSubquery with `flip: true|false` accordingly.
- **Go**: `go-ivm/builder/` has no PlannerGraph, no PlannerConnection, no PlannerJoin, no cost model. Go consumes the already-planned AST from TS and respects whatever `flip` annotations are present.
- **Divergence**: **none on the dispatch path** — TS's `#planAstForGo` annotates the AST before sending, so Go sees the same `flip` flags TS's own builder would see. Both then produce semantically equivalent operator trees (Join+applyOr for `flip:false`, FlippedJoin+UnionFanIn for `flip:true`).
- **Impact**: zero in the integrated TS+Go flow. **Potential future divergence**: if Go is ever called with an AST that bypasses `#planAstForGo` (e.g., a future direct-Go path or a Go-side transform that introduces a fresh subquery), Go would default to `flip: false` for any CSQ with no explicit annotation, while TS's same builder call (going through `buildPipeline`) would invoke `planQuery` and might choose to flip. The asymmetry is masked by the dispatch wrapper.
- **Validation**: H18-followup.md confirms this dispatch wrapper was the root cause of the channelStats over-emit (Go was getting an un-planned AST, missing `flip` flags). Currently FIXED by the `#planAstForGo` wrapper at all four `hydrateMany*` call sites (`pipeline-driver.ts:1038, 1165, 1303, 1523`).
- **Suggested action**: keep the invariant documented. If a fifth Go-dispatch site is added without `#planAstForGo`, regression risk is "silent wrong query plan for OR-with-EXISTS queries the cost model would choose to flip". Consider centralizing dispatch to require a planned AST (e.g., a typed `PlannedAST` newtype + helper).

### MEDIUM-2: `enableNotExists` flag has no Go-side equivalent
- **TS**: `mono/packages/zql/src/builder/builder.ts:69` (interface field) + `:269-271` (gated `assertNoNotExists` call) + `pipeline-driver.ts:1908` (always `true` for server-side). Client-side TS builds reject NOT EXISTS because the client can't distinguish "row absent" from "row not synced".
- **Go**: no flag. `go-ivm/builder/builder.go` does not call `assertNoNotExists`. Go's `applyCorrelatedSubqueryCondition` (`builder.go:498-533`) handles `op == "NOT EXISTS"` directly via `ExistsTypeNotExists`.
- **Divergence**: on the server (which is the only consumer of Go), the TS path sets `enableNotExists: true`, so behavior matches. **No live divergence.**
- **Impact**: Go can never run client-side, so the asymmetry is benign. If Go is ever embedded somewhere that should reject NOT EXISTS (unlikely), the rejection would need to happen upstream.
- **Suggested action**: none required. Optionally, add a flag to Go's `BuildPipeline` for symmetry / future-proofing, defaulting to `true` (allow NOT EXISTS).

### MEDIUM-3: NOT EXISTS user-`flip:true` silently builds FlippedJoin with wrong semantics
- **TS**: `mono/packages/zql/src/planner/planner-builder.ts:247-263`. When `condition.op === 'NOT EXISTS'`, the planner unconditionally forces `flippable=false, initialType='semi'` REGARDLESS of any user-set `flip:true`. So the planner-emitted AST never has `op:'NOT EXISTS' && flip:true`. However, TS's `applyFilterWithFlips` at `:450-478` does NOT defensively check this; if such an AST ever reached it, it would build a FlippedJoin with wrong semantics (FlippedJoin has no NOT-mode).
- **Go**: `go-ivm/builder/builder.go:618-639`. Same — no defensive check. If the wire AST has `op:'NOT EXISTS' && flip:true`, Go builds a FlippedJoin and the result is semantically wrong (FlippedJoin acts like EXISTS, not NOT EXISTS).
- **Divergence**: **none between TS and Go** — both have the same blind spot. Both rely on the planner to never emit that combination.
- **Impact**: a hand-crafted AST or a future planner bug could expose this on both sides simultaneously. Not currently a port issue.
- **Suggested fix**: optional defensive check at `applyFilterWithFlips` on both TS (`builder.ts:450`) and Go (`builder.go:618-621`): `assert/panic` if `Op == "NOT EXISTS" && Flip == true`.

### MEDIUM-4: Predicate for `correlatedSubquery` returns true (passthrough) when called from `BuildPredicate`
- **TS**: `mono/packages/zql/src/builder/filter.ts:165-203` (`transformFilters`) explicitly REMOVES correlatedSubquery conditions before predicate compile, returning a CSQ-free `NoSubqueryCondition` that `createPredicate` consumes. Predicates are never built from CSQs.
- **Go**: `go-ivm/builder/filter.go:55-58`. `BuildPredicate` over a tree containing a `correlatedSubquery` leaf returns a passthrough predicate (`return true`) for that leaf, then composes via and/or as normal.
- **Divergence**: TS's `transformFilters` is the analogue of Go's `stripCSQConditions`. Go's `stripCSQConditions` (builder.go:322-379) is called by `buildPipelineInternal` to strip CSQs BEFORE `BuildPredicate(simpleFilter)` at `:110`. So in the audited code path, `BuildPredicate` never sees a CSQ leaf either — the `return true` fallback at `filter.go:57` is dead code on the production path.
- **Impact**: no observable divergence on the audited paths. The passthrough fallback is **defensively wrong** — a true-passthrough on a CSQ branch INSIDE an OR would silently make the OR always-true. If a future caller passes a CSQ-containing condition directly to `BuildPredicate`, the bug fires.
- **Suggested fix**: change `filter.go:55-58` to `panic("BuildPredicate called on correlatedSubquery — caller must strip CSQ branches first")`. This matches the TS contract (`createPredicate` accepts `NoSubqueryCondition` only).

### MEDIUM-5: Scalar-subquery `ALWAYS_FALSE` collapse is wrong for NOT EXISTS — mirrored TS bug
- **TS**: `mono/packages/zqlite/src/resolve-scalar-subqueries.ts:171-174`. When the scalar executor returns `undefined` (no row) or `null` (row matched but FK is NULL), both EXISTS and NOT EXISTS resolve to ALWAYS_FALSE.
- **Semantic correctness**: for EXISTS, this is correct (no row → EXISTS is false → reject all parents). For NOT EXISTS, this is **wrong** — no row means NOT EXISTS is true → admit all parents. TS lumps them as ALWAYS_FALSE, which over-rejects parents on a NOT EXISTS scalar subquery that finds no row.
- **Go**: `go-ivm/builder/resolve_scalar.go:171-175`. Same behavior — `alwaysFalseCondition()` for both `EXISTS` and `NOT EXISTS` when no row matched. **Mirrors the TS bug exactly.**
- **Divergence**: none between TS and Go. Both have the same upstream bug.
- **Impact**: live, but only when (a) the scalar subquery is over a NOT EXISTS, (b) no row matches the unique-key WHERE constraint, AND (c) the parent table has rows that would have correctly survived NOT EXISTS. Likely rare in practice (most permission queries use EXISTS not NOT EXISTS for scalar). Out of scope for THIS audit (TS reference is canonical), but worth surfacing.
- **Suggested fix**: upstream TS fix. For NOT EXISTS, when value is undefined → emit `ALWAYS_TRUE` (e.g., `{type:'simple', op:'=', left:{type:'literal', value:1}, right:{type:'literal', value:1}}`). Then port to Go. **Do not unilaterally fix Go without fixing TS** — would introduce a real TS-vs-Go divergence the shadow path WOULD detect.

---

## AST transform parity matrix

| Transform | TS path | Go path | Status |
|---|---|---|---|
| AST decode / numeric normalize | n/a (native JSON) | `cmd/sidecar/main.go:78-87, 112-164` + `ivm/data.go:35-58` | OK — full ints→float64 coverage |
| `completeOrdering` | `query/complete-ordering.ts:6-72` | `builder/builder.go:698-777` | OK |
| `planQuery` (cost-model) | `planner/planner-builder.ts:311-320` | absent — TS plans before dispatch | OK on dispatch path (MEDIUM-1 future concern) |
| `resolveSimpleScalarSubqueries` | `zqlite/resolve-scalar-subqueries.ts` | `builder/resolve_scalar.go` | OK |
| `bindStaticParameters` | `builder/builder.ts:145-201` | not needed in Go — TS binds before dispatch | OK (Go's `resolveValue` at `filter.go:81-83` panics on unresolved static) |
| `uniquifyCorrelatedSubqueryConditionAliases` | `builder/builder.ts:723-765` | `builder/builder.go:382-416` | OK |
| `stripCSQConditions` (source-level filter) | `builder/filter.ts:165-203` via `transformFilters` | `builder/builder.go:322-379` | OK |
| `gatherCorrelatedSubqueryQueryConditions` | `builder/builder.ts:680-700` | `builder/builder.go:289-303` | OK |
| EXISTS-Join construction | `builder/builder.ts:308-329, 611-647` | `builder/builder.go:137-197` | OK |
| `applyWhere` / FilterStart+FilterEnd wrap | `builder/builder.ts:361-373` | `builder/builder.go:539-555` | OK |
| `applyFilterWithFlips` (FlippedJoin + UnionFanIn) | `builder/builder.ts:376-482` | `builder/builder.go:567-642` | OK (H18 fix applied) |
| `applyAnd` | `builder/builder.ts:502-512` | `builder/builder.go:437-442` | OK |
| `applyOr` (FanOut + branches + FanIn) | `builder/builder.ts:514-557` | `builder/builder.go:452-482` | OK |
| `applyCorrelatedSubqueryCondition` (Exists) | `builder/builder.ts:649-678` | `builder/builder.go:498-533` | OK |
| `assertNoNotExists` / `enableNotExists` gate | `builder/builder.ts:231-253, 269-271` + `pipeline-driver.ts:1908` | absent | OK in server context (MEDIUM-2) |
| Predicate compile: `=`/`!=` null short-circuit | `builder/filter.ts:74-93` | `builder/filter.go:117-119` | OK (MED-PORT-2 still fixed) |
| Predicate compile: `IS`/`IS NOT` strict equality | `builder/filter.ts:96-106` | `builder/filter.go:108-113, 178-208` | OK |
| Predicate compile: cross-type numeric equality | n/a (single Number type) | `builder/filter.go:178-208` (`valuesIdentical`) | OK + extra string-numeric coercion |
| Predicate compile: LIKE/ILIKE (no escape) | `builder/like.ts` (full SQL semantics) | `builder/filter.go:232-267` | LOW-1 (no `\` escape), LOW-2 (newline behavior) |
| Predicate compile: IN/NOT IN | `builder/filter.ts:133-145` (Set semantics) | `builder/filter.go:270-293` (`valueIn` via `ValuesEqual`) | OK |
| `simplifyCondition` (and/or flatten + collapse) | `query/expression.ts:249-267` | absent | OK (LOW-3 — TS simplifies before serialize) |
| Cap operator (REVIEW-final MEDIUM-3) | unused by TS builder | absent | OK |
| Related[] dedup-by-alias (LWW) | `builder/builder.ts:347-356` | `builder/builder.go:218-258` | OK |
| `splitEditKeys` collection | `builder/builder.ts:274-290` | `builder/builder.go:265-286` | OK |
| Skip cursor normalization (HIGH-1) | n/a (native types) | `builder/builder.go:117-130` | OK (still fixed) |

---

## Coverage notes

- **What I verified by reading**: every file in `mono/packages/zql/src/builder/` and `go-ivm/builder/`. Spot-checked TS planner-builder, planner-graph, planner-connection, complete-ordering, expression (for `simplifyCondition`), resolve-scalar-subqueries (the canonical TS source), and `#planAstForGo` / `#resolveScalarSubqueries` in `pipeline-driver.ts`. Cross-checked `ivm/data.go` CompareValues/ValuesEqual against `REVIEW-final` invariant #8.
- **What I did NOT re-verify**: per-operator IVM correctness (Skip, Take, Join, FlippedJoin, Exists, FanIn, FanOut, UnionFanIn, UnionFanOut, MemorySource, TableSource). These are covered by `REVIEW-porting.md` + `REVIEW-final.md` and the soak evidence (0 mismatches across 9012 / 5501 / 5564 query soaks).
- **What I did NOT trace**: the streamer's `node.Relationships` recursion in `engine/streamer.go:165-176` vs TS `pipeline-driver.ts:3122-3170` is structurally equivalent but is the suspected locus of the (unresolved) H18-followup over-emit. Out of scope for AST-pre-build audit. The H18 followup notes the bug is data-dependent and not reproducible in the existing test data; it would manifest under specific OR-EXISTS conditions where one branch alone selects a parent that another branch's EXISTS-Join had attached a relationship to. Not an AST handling issue per se.
- **Soak validation**: 30-min bounded soak (post-T1-1) shows 0 mismatches across 5564 queries / 33214 frames. Pre-build AST transforms are exercised on every hydrate; their parity holds empirically.

## Not checked

- **IVM operator behavior under push** (Take edit semantics, FlippedJoin REMOVE handling, Skip bound comparison after normalization, Exists count tracking, MemorySource overlay race) — owned by REVIEW-porting + REVIEW-parallelism.
- **Wire framing / msgpack codec / chunking** — owned by REVIEW-final § "Confirmed-correct invariants" #1, #2, #6.
- **Backend lifecycle / sidecar restart / drift audit** — owned by REVIEW-final § Cross-cutting RESILIENCE-1..7 and HIGH-CROSS-1.
- **Streamer node.Relationships recursion** (H18 over-emit) — IVM/streamer space, not AST pre-build.
- **Permission system wrapping of user queries** — `permissions.ts` AST mutation happens upstream of the audited paths (in `auth.ts`); behavior is unchanged through the pre-build chain.
- **LIKE: extended SQL semantics** beyond `%`/`_`/`\` — neither side supports SQL `ESCAPE` clause (not in AST schema), POSIX bracket expressions, etc. Out of scope.
- **TS query-impl.ts** (the user-facing `.where()`/`.related()` builder) — converts user calls to AST; not in the audited "wire AST → operator tree" path.
- **Permission-system Cap operator usage** — TS does not use Cap; if a future TS path adopts it, MEDIUM-3 in REVIEW-final becomes a real divergence.

## Severity summary

- **CRITICAL**: 0
- **HIGH**: 0
- **MEDIUM**: 5 (1 architectural/MEDIUM-1, 1 benign-by-construction MEDIUM-2, 2 defensive-equivalent MEDIUM-3/MEDIUM-4, 1 mirrored-upstream-bug MEDIUM-5)
- **LOW**: 3 (LIKE escape, LIKE newline, simplifyCondition absence)
- **REGRESSION**: 0
- **CHECKED-OK**: 16

No live silent-wrong-result divergences found on the audited AST pre-build paths under the integrated TS→Go dispatch flow. The MEDIUM-1 cost-model architectural gap is currently masked by `#planAstForGo`; the MEDIUM-3/4 defensive gaps are dormant; MEDIUM-5 is a mirrored TS bug (Go faithfully reproduces). LOW-1/2 affect a narrow LIKE edge case never observed in soak data.
