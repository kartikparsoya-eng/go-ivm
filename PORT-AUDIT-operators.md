# Port Audit — IVM Operators (Go vs TS)

Comparison of `go-ivm/ivm/` + `go-ivm/builder/` against `mono/packages/zql/src/ivm/` + `mono/packages/zql/src/builder/`. TS is canonical. Source/MemorySource intentionally out of scope (separate agent).

Prior baseline: `REVIEW-porting.md` + `REVIEW-final.md`. All findings from those docs were re-verified; FIXED ones were confirmed still in place except where noted as REGRESSION.

## Findings

### CHECKED-OK-1: ValuesEqual nil/nil semantics (CRITICAL-1 regression check)
- **TS**: `mono/packages/zql/src/ivm/data.ts:112-118` — both null → false.
- **Go**: `go-ivm/ivm/data.go:205-219` — both nil → false. Doc comment line 198-201 explicitly warns against "fixing" by returning true.
- **Status**: Prior CRITICAL-1 fix still in place. CompareValues (data.go:142-194) intentionally keeps nil-equals-nil for sort ordering — both contracts maintained per REVIEW-final invariant #8.

### CHECKED-OK-2: ConstraintMatchesRow uses ValuesEqual (CRITICAL-2 regression check)
- **TS**: `mono/packages/zql/src/ivm/constraint.ts:16-26` — uses `valuesEqual`.
- **Go**: `go-ivm/ivm/source.go:702-719` — uses `ValuesEqual`, comment explicitly explains both the null-handling and cross-type numeric reasoning.
- **Status**: Prior CRITICAL-2 fix still in place.

### CHECKED-OK-3: Skip cursor row normalization (HIGH-1 regression check)
- **TS**: AST start.row normalized indirectly via SQL coercion.
- **Go**: `go-ivm/builder/builder.go:117-130` — explicit `source.NormalizeRow(startRow)` on a copy before `NewSkip`.
- **Status**: Prior HIGH-1 fix still in place.

### HIGH-1: Skip.shouldBePresent uses CompareWithPartialBound, TS uses full comparator
- **TS**: `mono/packages/zql/src/ivm/skip.ts:83-86` — `this.#comparator(this.#bound.row, row)`; `row[key]` for missing-from-bound columns goes through `compareValues(undefined, value)` which normalizes undefined→null, then null < non-null returns -1.
- **Go**: `go-ivm/ivm/skip.go:57-90` — `CompareWithPartialBound` stops comparing at the first sort column the bound row doesn't specify (returns 0).
- **Divergence**: For a partial bound `{createdAt: X}` against row `{createdAt: X, conversationId: "abc"}` with sort `[createdAt asc, conversationId asc]`:
  - TS: cmp = `compareValues(X, X) → 0`, then `compareValues(undefined, "abc") = compareValues(null, "abc") = -1`. cmp = -1, row IS shouldBePresent regardless of `exclusive`.
  - Go: cmp = `compareValues(X, X) → 0`, second column missing from bound → return 0. cmp = 0, row excluded if `exclusive=true`.
- **Impact**: Production-correctness: Go matches the SQL-backed TS pipeline output (which clients see), TS in-memory IVM diverges from itself. Documented in `skip.go:47-56` ("Drift seen in channelConversationsPaginatedV3 ... Go included the row at exactly the start's createdAt"). This is an intentional Go correction for client-observed behavior, but it IS a divergence from the TS reference operator. Tests in `skip.test.ts` only use full bounds, so the TS suite doesn't catch its own bug.
- **Suggested fix**: Document as intentional. Optionally upstream the fix to TS so the reference matches production behavior.

### HIGH-2: `valuesIdentical` cross-type numeric/string coercion in builder/filter.go diverges from TS createPredicate
- **TS**: `mono/packages/zql/src/builder/filter.ts:113-114` — `case '=': return lhs => lhs === rhs;` — strict equality, no coercion. `5 === '5'` is false in JS.
- **Go**: `go-ivm/builder/filter.go:178-208` — `valuesIdentical` falls through to string-parsed-as-number coercion (lines 192-205). `5 == '5'` is true after coercion.
- **Divergence**: For `participantCount = '5'` where `participantCount` is a normalized float64 row column and `'5'` is an AST string literal:
  - TS createPredicate (in-memory): no match.
  - TS via SQL TableSource: matches (SQLite implicit cast).
  - Go: matches.
- **Impact**: Go matches production TS (which always uses TableSource for filters) but diverges from TS in-memory predicate. Comment at `filter.go:174-177` documents this was deliberate: "Discovered via gap-cross-type-num-eq-str in the soak (2026-05-25): `participantCount = '5'` produced TS=1/Go=0 mismatches because Go stopped at rule 3 and rejected the string literal." Note the TS=1 reflects SQL-coerced TS, not the IVM-level predicate.
- **Suggested fix**: Document as intentional. Same as HIGH-1 — Go is matching the client-observable contract.

### MEDIUM-1: Take EDIT oldCmp>0 newCmp<0 — Go panics on newBoundNode==nil; TS would too, but reachability differs
- **TS**: `mono/packages/zql/src/ivm/take.ts:592-600` — asserts both `oldBoundNode` and `newBoundNode` non-undefined; assertion throws on violation.
- **Go**: `go-ivm/ivm/take.go:455-460` — matching panics.
- **Status**: Match. (Earlier audit feared a silent Size+1 fallback; the inline comment at lines 452-454 confirms that was removed: "Pre-fix audit C had a Go-only fallback that silently bumped Size+1 while keeping the stale Bound. That diverged the partition's state from TS, causing subsequent pushes to operate on a wrong baseline.") MEDIUM here flags only that Go has a `DriftError`-based recovery path for the oldCmp/newCmp==0 cases (lines 424, 474) that TS lacks — Go converts what TS would surface as an assertion crash into an engine-level reinit. Behavioral output to clients should differ: TS = sidecar crash, Go = silent drop of one advance + re-init from SQLite truth. Better for ops, divergent from TS at the operator boundary.
- **Suggested fix**: None; intentional resilience improvement. Document in REVIEW-final if not already.

### MEDIUM-2: Take REMOVE with no replacement bound — Go forces Size=0 instead of TS's Size-1+Bound=undefined
- **TS**: `mono/packages/zql/src/ivm/take.ts:402-419` — when neither forward nor reverse fetch finds a `newBound.push` row, `setTakeState(key, size-1, newBound?.node.row /* undefined */, ...)`. Next EDIT on this partition would hit `assert(takeState.bound, 'Bound should be set')` at line 448 → TS throws.
- **Go**: `go-ivm/ivm/take.go:317-325` — explicitly forces `Size=0` when `newBound==nil`, treating "unfindable bound" as "empty window". Avoids the downstream EDIT-time panic TS would experience.
- **Divergence**: Output differs: TS partition state = `{Size: N-1, Bound: undefined}` (broken invariant, latent crash); Go = `{Size: 0, Bound: nil}` (clean empty state).
- **Impact**: Go's behavior is correct for an empty window; TS's is a known latent bug. Per `REVIEW-final` HIGH-IVM-1 ("`panic: Bound should be set`"), this is the documented fix.
- **Suggested fix**: None; intentional Go improvement. The TS bug exists upstream.

### MEDIUM-3: UnionFanIn skips schema cross-validation that TS asserts
- **TS**: `mono/packages/zql/src/ivm/union-fan-in.ts:59-71` — asserts `schema.primaryKey === inputSchema.primaryKey` (reference equality), `schema.compareRows === inputSchema.compareRows`, `schema.sort === inputSchema.sort` for every input.
- **Go**: `go-ivm/ivm/union_fan_in.go:29-50` — only `TableName` match (line 31), `len(PrimaryKey)` match (line 34), `System` match (line 37). Does NOT verify primaryKey columns match per index, comparators match, or sort matches.
- **Divergence**: If two branch inputs have schemas built independently (different primaryKey columns of the same length, or different sort orders), TS rejects at build time, Go silently accepts. Downstream merging via `MergeFetches(..., ufi.schema.CompareRows)` would then sort branches with different orderings using one comparator, producing wrong output.
- **Impact**: In current builder paths the branches share an upstream and so share schemas; the unsafe configuration is unreachable from the production builder. But the missing guard means a future builder change wouldn't get caught.
- **Suggested fix**: Add column-by-column primaryKey check and (best-effort) compareRows/sort identity check at `union_fan_in.go:29-50`.

### MEDIUM-4: PushAccumulatedChanges drops type-count asserts that TS enforces
- **TS**: `mono/packages/zql/src/ivm/push-accumulated.ts:108-113, 139-152, 159-167, 222-234, 246-249` — multiple `assert` statements verifying type constraints: REMOVE expects all removes, ADD expects all adds, CHILD expects ≤2 types, etc.
- **Go**: `go-ivm/ivm/push_accumulated.go:36-108` — only checks `if !ok` for the expected type; does not verify that ONLY that type was accumulated. CHILD branch DOES check "not both add and remove" at line 96-98.
- **Divergence**: A buggy upstream that accumulates mismatched-type changes would crash TS at the assert, while Go would proceed past the type-cardinality check and use one of the values (the one matching `fanOutChangeType`). Downstream behavior depends on which change wins the map-lookup race.
- **Impact**: Defensive — would only fire on an upstream that violates the push-routing invariants. Hard to construct from production inputs.
- **Suggested fix**: Add equivalent type-cardinality asserts in `push_accumulated.go` REMOVE/ADD branches and CHILD's `types.length <= 2` check.

### MEDIUM-5: Skip.Fetch reverse path uses `break` not "stop on first non-present"; semantically OK but differs from TS in error handling
- **TS**: `mono/packages/zql/src/ivm/skip.ts:62-72` — `return` (exits generator) on first non-present node when reversing.
- **Go**: `go-ivm/ivm/skip.go:111-117` — `break` (exits loop, returns accumulated). Semantically equivalent for the happy path.
- **Status**: CHECKED-OK behaviorally. Both stop iteration on first node that wouldn't pass shouldBePresent. The break-vs-return distinction doesn't matter because Go's slice approach doesn't have a separate cleanup path.

### MEDIUM-6: Take initialFetch reverse not asserted in Go
- **TS**: `mono/packages/zql/src/ivm/take.ts:160` — `assert(!req.reverse, 'Reverse should be false');` in `#initialFetch`.
- **Go**: `go-ivm/ivm/take.go:149-170` — no such assert. If a caller passes `reverse=true` to `Take.Fetch` when the partition's takeState doesn't yet exist, Go silently builds takeState from rows in reverse order (saving the LAST row as bound, not the first).
- **Impact**: In production, Take.Fetch never sees reverse=true (no operator above sends one). But defensive: a future regression could silently corrupt take state.
- **Suggested fix**: Add a panic at `take.go:149` matching TS.

### MEDIUM-7: Take initialFetch `start` not asserted in Go
- **TS**: `mono/packages/zql/src/ivm/take.ts:159` — `assert(req.start === undefined, 'Start should be undefined');`.
- **Go**: `go-ivm/ivm/take.go:149-170` — no assert. A caller passing start to a non-existent takeState would silently apply the start (since `input.Fetch(req)` honors it).
- **Impact**: Same as MEDIUM-6.
- **Suggested fix**: Add panic matching TS at `take.go:149`.

### MEDIUM-8: CompareValues for relational ops in builder/filter.go panics on cross-type; TS coerces via JS semantics
- **TS**: `mono/packages/zql/src/builder/filter.ts:117-124` — JS `lhs > rhs` for strings/numbers/mixed: coercion + NaN semantics (returns false on incomparable).
- **Go**: `go-ivm/builder/filter.go:126-133` — `ivm.CompareValues(left, right)`, which panics on type mismatch (data.go:193).
- **Divergence**: `participantCount > '5'` where left is float64 and right is string:
  - TS: parses string to number, compares, returns boolean.
  - Go: panics.
- **Impact**: Sidecar crash on the offending advance. Recovered by engine reinit, but client misses one delta. The cross-type `=`/`!=` already gained coercion via `valuesIdentical` (HIGH-2); the relational ops did not. Asymmetric handling.
- **Suggested fix**: Extend `valuesIdentical`'s cross-type promotion to the `<`/`>`/`<=`/`>=` branches of `evalOp`. Or replace `CompareValues` with a coercion-aware variant for predicate use.

### MEDIUM-9: FanIn assert dropped — different-schema branches accepted silently
- **TS**: `mono/packages/zql/src/ivm/fan-in.ts:41` — `assert(this.#schema === input.getSchema(), 'Schema mismatch in fan-in');` per branch.
- **Go**: `go-ivm/ivm/fan_in.go:14-24` — no schema validation at all. Branches with different schemas accepted.
- **Impact**: Same class as MEDIUM-3 (UnionFanIn). Current builder ensures branches share schema, but a future bug would not be caught.
- **Suggested fix**: Add per-input schema comparison in `NewFanIn`.

### LOW-1: FlippedJoin overlay REMOVE filter — Go's pointer compare is unreachable but the CompareRows fallback works
- **TS**: `mono/packages/zql/src/ivm/flipped-join.ts:271-273` — `filter(n => n !== this.#inprogressChildChange?.[ChangeIndex.NODE])`; JS reference equality.
- **Go**: `go-ivm/ivm/flipped_join.go:222-228` — `if &n != &fj.inprogressChildChange.Node && CompareRows != 0`; the address-compare always evaluates true (because `&n` is the loop-variable address), but the CompareRows check catches the row-equality case.
- **Status**: CHECKED-OK behaviorally; the dead pointer compare is harmless. Matches `REVIEW-porting.md` LOW-3.

### LOW-2: UnionFanOut drops branch push return values; relies on accumulation
- **TS**: `mono/packages/zql/src/ivm/union-fan-out.ts:29-32` — yields branch pushes (which return undefined), then yields fanIn-done result.
- **Go**: `go-ivm/ivm/union_fan_out.go:36-40` — calls `output.Push(change, ufo)` for each output but DISCARDS the returned `[]Change` (line 37). Returns only `unionFanIn.FanOutDonePushing` result.
- **Status**: CHECKED-OK behaviorally because the branch outputs feed UnionFanIn which accumulates and returns nil (`union_fan_in.go:97-103`). The final result comes through `FanOutDonePushing`. Behavior matches TS but only because of the accumulation invariant; a future change that put a non-accumulating output downstream of UFO would silently lose changes.
- **Suggested fix**: Either append the per-output results (defensive — they should always be empty) or document the contract.

### LOW-3: Take.pushEdit oldCmp>0/newCmp==0 and oldCmp<0/newCmp==0 — Go converts to DriftError, TS asserts
- **TS**: `take.ts:563`, `take.ts:620` — `assert(newCmp !== 0, 'Invalid state. Row has duplicate primary key')`.
- **Go**: `go-ivm/ivm/take.go:418-430, 469-474` — panics with `*DriftError` so the engine catches and re-inits.
- **Status**: Intentional resilience improvement (engine recovery). Behavior to client differs: TS = sidecar crash, Go = silent drop of advance + re-init. Same class as MEDIUM-1. Listed separately because the trigger conditions differ.

### LOW-4: Exists cache mutex / inPush atomic — Go adds concurrency safety TS doesn't need
- **TS**: `mono/packages/zql/src/ivm/exists.ts:39, 110-115` — `#inPush` is a boolean; TS is single-threaded so no synchronization.
- **Go**: `go-ivm/ivm/exists.go:43-44, 137-141` — `inPush atomic.Bool`, `pushMu sync.Mutex` serializing Push.
- **Status**: Intentional. Doc comment at exists.go:23-33 explains: parallel push fan-out can re-enter the same Exists from multiple goroutines via downstream Fetch. Go serializes; the original TS re-entrancy assertion would fire on Go without this. Behavioral output preserved (still at-most-one Push in flight per Exists).

### LOW-5: Source overlay clear-on-panic
- **TS**: `mono/packages/zql/src/ivm/memory-source.ts:552-578` — no try/finally; relies on generator unwinding.
- **Go**: `go-ivm/ivm/source.go:199-215` — overlay clear via `defer` (per REVIEW-porting HIGH-4 fix).
- **Status**: CHECKED-OK — fix in place. Out of scope per task but flagged here because it impacts operator-level fetches that touch the source overlay (Join/FlippedJoin overlay paths).

### CHECKED-OK-4: Filter operator parity
- **TS**: `filter.ts:1-57`. Stateless predicate operator with `beginFilter`/`endFilter`/`filter`/`push`. Push delegates to `filterPush` which handles ADD/REMOVE/CHILD via predicate, EDIT via `maybeSplitAndPushEditChange`.
- **Go**: `filter.go:1-83`. 1:1 port. filterPush switches identically.
- **Status**: Equivalent. The `LOW-1` from REVIEW-porting (duplicate maybeSplitAndPush implementation in source.go vs filter.go) was consolidated — filter.go:81-83 now aliases the exported version from source.go.

### CHECKED-OK-5: FilterStart / FilterEnd bridge parity
- **TS**: `filter-operators.ts:61-146`.
- **Go**: `filter_operators.go:42-130`.
- **Status**: Equivalent. Both bridge Input↔FilterInput and FilterOutput↔Output. Fetch through FilterStart calls `BeginFilter`/`EndFilter` + per-node `Filter`. FilterEnd's `Filter` always returns true (terminal). Go's `FilterEnd.Push` correctly forwards to `fe.output.Push(change, fe)` matching TS.

### CHECKED-OK-6: Exists cache reuse / push CHILD ADD+REMOVE semantics
- **TS**: `exists.ts:80-99` (cache hit/miss in filter), `138-198` (CHILD ADD/REMOVE size==1/0 handling).
- **Go**: `exists.go:92-119` (filter), `155-200` (CHILD).
- **Status**: Equivalent including the special-case rewrites: CHILD-ADD with size==1 in EXISTS → push ADD of parent; in NOT EXISTS → push REMOVE of parent with relationship overridden to empty. CHILD-REMOVE with size==0 → corresponding flip. Cache key uses `JSON.stringify(values)` (TS) / `json.Marshal(values)` (Go) — same serialization for equivalent values.

### CHECKED-OK-7: Join in-progress overlay for child Fetch during pushChild
- **TS**: `join.ts:252-302` — `processParentNode` sets up `childStream` closure that consults `#inprogressChildChange` + position and applies overlay.
- **Go**: `join.go:215-247` — equivalent.
- **Status**: REVIEW-final invariant #7 (Join.processParentNode inprogress state propagation) verified.

### CHECKED-OK-8: FlippedJoin pushChild — `exists` parameter mutated across loop iterations
- **TS**: `flipped-join.ts:346-425` — `exists?: boolean` parameter, reassigned inside the `for (const parentNode ...)` loop at line 384.
- **Go**: `flipped_join.go:278-342` — `exists bool` parameter, reassigned at line 306.
- **Status**: Bug 3 fix confirmed.

### CHECKED-OK-9: FlippedJoin constraintsAreCompatible uses ValuesEqual (not CompareValues)
- **TS**: `mono/packages/zql/src/ivm/constraint.ts:33-43` — `valuesEqual`.
- **Go**: `go-ivm/ivm/flipped_join.go:411-420` — `ValuesEqual`.
- **Status**: REVIEW-porting MEDIUM-6 fix verified — uses ValuesEqual so null-FK constraints are correctly incompatible, matching TS.

### CHECKED-OK-10: MergeFetches dedup keeps first-occurrence
- **TS**: `union-fan-in.ts:241-272` — `comparator(lastNodeYielded, minNode) === 0 ? continue : yield`.
- **Go**: `union_fan_in.go:181-218` — `comparator(lastNode.Row, minNode.Row) == 0 ? continue : append`.
- **Status**: Bug-equivalent. REVIEW-porting "Confirmed Correct #8" verified.

### CHECKED-OK-11: FanIn identity merge keeps first occurrence
- **TS**: `fan-in.ts:90-92` — uses `identity` from sentinels: `identity(existing, incoming) → existing`.
- **Go**: `fan_in.go:80-87` — `identity(existing, incoming) → existing`.
- **Status**: REVIEW-porting "Confirmed Correct #3" verified.

### CHECKED-OK-12: PushAccumulated EDIT → ADD+REMOVE → reconstituted EDIT
- **TS**: `push-accumulated.ts:202-213`.
- **Go**: `push_accumulated.go:68-71`.
- **Status**: REVIEW-porting "Confirmed Correct #4" verified.

### CHECKED-OK-13: Take getStateAndConstraint partition handling
- **TS**: `take.ts:218-245`.
- **Go**: `take.go:614-631`.
- **Status**: Equivalent. Constraint built from row using partitionKey columns; nil if no partition key.

### CHECKED-OK-14: Take.ConstraintMatchesPartitionKey handles duplicate columns
- **TS**: `take.ts:727-743` — checks length equality then iterates partitionKey.
- **Go**: `take.go:660-680` — uses DISTINCT column set (comment at line 646-659 explains the duplicate-column fix for `parentField:["id","id"]`).
- **Status**: Go improvement; the TS-equivalent length-only check caused the channels MISMATCH soak issue (2026-05-27). Go's fix matches what `BuildJoinConstraint` produces. TS may have the same latent issue with composite-key tables but isn't documented to have hit it.

### CHECKED-OK-15: Skip getStart three-way return
- **TS**: `skip.ts:107-167` — returns `Start | undefined | 'empty'`.
- **Go**: `skip.go:139-179` — returns `(*Start, bool)` with empty=true for 'empty' case.
- **Status**: Equivalent. REVIEW-porting "Confirmed Correct #1" verified.

### CHECKED-OK-16: ScalarSubquery resolution (resolve_scalar.go) parity
- **TS**: `mono/packages/zqlite/src/resolve-scalar-subqueries.ts`.
- **Go**: `go-ivm/builder/resolve_scalar.go`.
- **Status**: Equivalent. Walks AST, finds scalar EXISTS with simple subqueries (all unique-key columns equality-constrained by literals), runs executor, replaces with `parentField = value` or ALWAYS_FALSE. Companion subquery rows tracked for client sync. Go's `ALWAYS_FALSE` uses `float64(1)/float64(0)`; TS uses JS numbers — both equivalent after numeric normalization.

### CHECKED-OK-17: Builder applyOr / applyFilterWithFlips routing
- **TS**: `mono/packages/zql/src/builder/builder.ts:386-557`.
- **Go**: `go-ivm/builder/builder.go:452-642`.
- **Status**: Equivalent partitioning of subquery-containing vs subquery-free branches. FanOut/FanIn for OR-with-subquery, UnionFanOut/UnionFanIn for OR-with-flipped-CSQ. Single OR-predicate filter when no CSQ in any branch.

### CHECKED-OK-18: Builder MeasurePushOperator missing — not in operator scope
- **TS**: `mono/packages/zql/src/query/measure-push-operator.ts` — query-package operator wrapping push with metrics.
- **Go**: Not ported. Sidecar uses Go-native metrics surfacing via OTel.
- **Status**: Out of operator scope; metrics handled at sidecar/engine layer, not at operator layer. Per REVIEW-final, this is intentional.

### CHECKED-OK-19: Cap operator missing — TS doesn't use it either
- **TS**: `mono/packages/zql/src/ivm/cap.ts` — implemented but unreferenced outside its test file.
- **Go**: Not ported.
- **Status**: REVIEW-porting MEDIUM-3 / REVIEW-final OPEN. No production TS path constructs Cap, so divergence is forward-only. If TS adopts Cap for an EXISTS optimization, Go would need to port.

### CHECKED-OK-20: UnionFanIn reverse-Fetch merging — TS and Go both use forward comparator
- **TS**: `union-fan-in.ts:103-108` — `mergeFetches(iterables, (l, r) => this.#schema.compareRows(l.row, r.row))`. The forward compareRows is used regardless of `req.reverse`.
- **Go**: `union_fan_in.go:83-89` — same.
- **Status**: Both produce wrong order if `req.reverse=true`. CHECKED-OK because match. Production has not hit this — no operator above UnionFanIn currently passes reverse. Latent bug shared by both.

### CHECKED-OK-21: Filter / Filter-push / maybe-split-and-push-edit-change
- **TS**: `filter.ts` + `filter-push.ts` + `maybe-split-and-push-edit-change.ts`.
- **Go**: All consolidated into `filter.go` + `source.go:MaybeSplitAndPushEditChange`.
- **Status**: REVIEW-porting LOW-1 fix verified (duplicate impl removed; `filter.go:81-83` aliases to source.go's version).

### CHECKED-OK-22: pushAccumulatedChanges accumulator clearing
- **TS**: `push-accumulated.ts:124` — `accumulatedPushes.length = 0` mutates input array.
- **Go**: `push_accumulated.go` does NOT clear the input slice. Callers handle: `fan_in.go:75` clears after, `union_fan_in.go:159-160` clears before.
- **Status**: Behaviorally equivalent. Different mechanism, same outcome.

### CHECKED-OK-23: ALWAYS_FALSE shape parity
- **TS**: `resolve-scalar-subqueries.ts:185-189` — `{type:'simple', op:'=', left:{type:'literal', value:1}, right:{type:'literal', value:0}}`.
- **Go**: `resolve_scalar.go:192-199` — same shape, `float64(1)/float64(0)`.
- **Status**: Equivalent after numeric normalization (CRITICAL-3 / HIGH-2 fixes ensure cross-type numeric equality).

### CHECKED-OK-24: Strings compared via UTF-8 byte ordering
- **TS**: `data.ts:40-49` — `compareUTF8` from external package; encodes JS strings to UTF-8 bytes and compares.
- **Go**: `data.go:160-166` — `strings.Compare` on Go strings (which are already UTF-8 bytes).
- **Status**: Equivalent UTF-8 byte ordering.

### CHECKED-OK-25: Push split-edit handling in source
- **TS**: `memory-source.ts:452-506` — per-connection splitEditKeys, split EDIT into REMOVE+ADD when any split key changes.
- **Go**: `source.go:195-226` — equivalent.
- **Status**: Out of operator scope but flagged because operators (Take, FlippedJoin) rely on Join-correlation parent-fields being included in splitEditKeys. Builder includes them: `builder.go:89-95`.

## Operator parity matrix

| TS operator | Go operator | Status | Notes |
|---|---|---|---|
| `Filter` (filter.ts) | `Filter` (filter.go) | OK | 1:1 port |
| `FilterStart` | `FilterStart` | OK | 1:1 port |
| `FilterEnd` | `FilterEnd` | OK | 1:1 port |
| `Exists` (exists.ts) | `Exists` (exists.go) | OK + concurrency hardening | LOW-4 |
| `Join` (join.ts) | `Join` (join.go) | OK | inprogress overlay verified |
| `FlippedJoin` (flipped-join.ts) | `FlippedJoin` (flipped_join.go) | OK | LOW-1 (benign pointer compare) |
| `Skip` (skip.ts) | `Skip` (skip.go) | DIVERGENT (intentional) | HIGH-1: partial-bound fix |
| `Take` (take.ts) | `Take` (take.go) | DIVERGENT (intentional) | MEDIUM-1/-2 + DriftError recovery |
| `FanIn` (fan-in.ts) | `FanIn` (fan_in.go) | OK | MEDIUM-9 (missing schema assert) |
| `FanOut` (fan-out.ts) | `FanOut` (fan_out.go) | OK | 1:1 port |
| `UnionFanIn` (union-fan-in.ts) | `UnionFanIn` (union_fan_in.go) | OK | MEDIUM-3 (missing schema asserts) |
| `UnionFanOut` (union-fan-out.ts) | `UnionFanOut` (union_fan_out.go) | OK | LOW-2 (drops branch returns, OK by invariant) |
| `pushAccumulatedChanges` | `PushAccumulatedChanges` | OK | MEDIUM-4 (missing type-count asserts) |
| `Cap` (cap.ts) | Absent | OPEN (forward-looking) | Not used by TS builder |
| `MeasurePushOperator` | Absent | OK (not operator scope) | Metrics handled at sidecar layer |
| `filterPush` (filter-push.ts) | `filterPush` (filter.go) | OK | Inlined |
| `maybeSplitAndPushEditChange` | `MaybeSplitAndPushEditChange` (source.go) | OK | Consolidated per LOW-1 fix |
| `mergeFetches` | `MergeFetches` (union_fan_in.go) | OK | Forward-only merge — CHECKED-OK-20 |
| `buildJoinConstraint` | `BuildJoinConstraint` | OK | null-FK → undefined/nil |
| `generateWithOverlay`/`...Unordered` | `GenerateWithOverlay`/`...Unordered` | OK | All four overlay types covered |
| `constraintsAreCompatible` | `constraintsAreCompatible` | OK | Uses ValuesEqual per MEDIUM-6 fix |
| `ConstraintMatchesRow` | `ConstraintMatchesRow` (source.go) | OK | CRITICAL-2 fix verified |
| Builder `buildPipeline` | `BuildPipeline` | OK | CHECKED-OK-17 |
| Builder `applyOr` | `applyOr` | OK | CHECKED-OK-17 |
| Builder `applyFilterWithFlips` | `applyFilterWithFlips` | OK | CHECKED-OK-17 |
| Builder `resolveSimpleScalarSubqueries` | `ResolveSimpleScalarSubqueries` | OK | CHECKED-OK-16 |
| Builder `createPredicate` | `BuildPredicate` (builder/filter.go) | DIVERGENT (intentional) | HIGH-2 (`=`/`!=` cross-type coerce); MEDIUM-8 (`<>` does not) |

## Coverage notes

Systematically checked for each operator pair:

- **Push contract per ChangeType (ADD/REMOVE/EDIT/CHILD)**: branch-by-branch comparison of pushParent/pushChild/push.
- **Fetch contract**: initial fetch, reverse, start, constraint; what nodes are returned and in what order.
- **Schema construction**: relationship merging, primaryKey/sort/system propagation in Join/FlippedJoin/UnionFanIn.
- **Null handling**: ValuesEqual vs CompareValues distinction (REVIEW-final invariant #8) re-verified at every call site.
- **Sort order**: ASC/DESC + reverse + tie-breaking via PK columns (added by completeOrdering).
- **Equality semantics**: ValuesEqual nil/nil semantics in IsJoinMatch, BuildJoinConstraint, constraintsAreCompatible, ConstraintMatchesRow.
- **Edge cases**: empty input (Take initialFetch with no rows), limit=0 (Take returns nil), skip past end (getStart 'empty'), take with no replacement bound (MEDIUM-2).
- **State storage**: Take.TakeState format, partition-key serialization (`GetTakeStateKey` JSON output). Verified `GetTakeStateKey` produces identical JSON for identical inputs.
- **Companion sub-pipeline behavior**: ScalarExecutor + CompanionSubquery tracking (CHECKED-OK-16).
- **In-progress child-change overlay**: Join and FlippedJoin overlay-during-push paths verified.
- **Cache semantics**: Exists.cache with parentJoinKey key + JSON serialization; reuse skipped during `inPush`.
- **Filter pipeline bridging**: FilterStart/FilterEnd Push/Fetch routing.
- **Test file existence**: filter_test.go, parallel_panic_recovery_test.go, source_parallel_test.go, take_*_test.go all present; spot-checked take_displacement_test for bug 26 coverage.

## Not checked

- **Builder edge cases for nested ALIAS uniquification**: `uniquifyCorrelatedSubqueryConditionAliases` in TS uses a closure counter, Go uses an outer-scope counter via assignment in `uniquify`. Both increment globally per AST. Counter behavior under recursion not verified beyond obvious cases.
- **`planner` package** (TS `planQuery`): Go does not have a planner; queries arrive pre-planned from TS. This means any planner-applied AST rewrite that TS does before reaching the operator builder won't be applied by Go if a Go-only path emerges. Out of operator scope.
- **`completeOrdering` semantics** for queries with `orderBy.length > pk.length` and `pk` not a strict prefix: visually verified port at `builder.go:698-722` matches TS at `mono/packages/zql/src/query/complete-ordering.ts`, but tie-breaking with mixed asc/desc was not exhaustively traced.
- **`isSimpleSubquery` constraint extraction**: Go `ExtractLiteralEqualityConstraints` only collects from AND-nested simple `=` conditions. TS does the same. Behavior with deeply-nested mixed AND/OR not fuzzed.
- **`Take.pushEditChange` partition-key change detection**: TS asserts via `partitionKeyComparator`, Go matches. The branch where partition key actually changes is unreachable per the source's split-edit handling. Trace assumes the source upstream is correctly populated with splitEditKeys.
- **`Distinct` operator**: Searched both TS and Go; no such operator exists in either. Task mentioned it but there's no TS reference to compare. Marking as N/A — Distinct may live in query/expression layer (e.g., `distinct(...)` becomes a different AST shape handled by other operators).
- **`Or` (applyOr) routing under deeply nested OR with mixed CSQ/non-CSQ branches**: spot-checked top-level + one nesting level; not exhaustively recursed.
- **MeasurePushOperator behavior**: confirmed absent from Go operator layer; not compared in depth because it's a TS query-layer wrapper.
- **Async/Yield contract**: TS uses generators with `'yield'` sentinel for cooperative scheduling. Go drops this entirely (slices). Per `operator.go:3-4`: "The 'yield' scheduling signal is dropped entirely." This means Go cannot exhibit TS's mid-stream-yield behavior; whether any operator depends on it for correctness (vs just responsiveness) was assumed FALSE per the existing port design but not exhaustively verified.
- **AST `flip` field semantics with NESTED flipped CSQs**: Builder verified for single-level flip, not for nested flipped CSQ in WHERE of another flipped CSQ's subquery.
