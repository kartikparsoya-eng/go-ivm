# Master Port Audit — TS reference → Go IVM port

Consolidated 8-surface audit (2026-06-03). TS is canonical. Findings rank
by user-visible impact; CHECKED-OK items document what was systematically
ruled out — read the negative space too.

| Surface | Doc | CRIT | HIGH | MED | LOW | OK | NEW |
|---|---|---|---|---|---|---|---|
| 1. Operators | [PORT-AUDIT-operators.md](PORT-AUDIT-operators.md) | 0 | 2† | 9 | 5 | 25 | — |
| 2. Source | [PORT-AUDIT-source.md](PORT-AUDIT-source.md) | 0 | 1‡ | 4 | 5 | 14 | HIGH-1 |
| 3. Types | [PORT-AUDIT-types.md](PORT-AUDIT-types.md) | 2 | 1 | 9 | 4 | 12 | CRIT-1, CRIT-2 |
| 4. Streamer | [PORT-AUDIT-streamer.md](PORT-AUDIT-streamer.md) | 1 | 2 | 4 | 5 | 5 | CRIT-1, HIGH-1 |
| 5. AST | [PORT-AUDIT-ast.md](PORT-AUDIT-ast.md) | 0 | 0 | 5 | 3 | 16 | — |
| 6. Lifecycle | [PORT-AUDIT-lifecycle.md](PORT-AUDIT-lifecycle.md) | 1 | 3 | 6 | 3 | 7 | HIGH-2, HIGH-3 |
| 7. Dispatch | [PORT-AUDIT-dispatch.md](PORT-AUDIT-dispatch.md) | 6 | 8 | 10 | 2 | many | CRIT-5/6, HIGH-5/6/7/8 |
| 8. RPC | [PORT-AUDIT-rpc.md](PORT-AUDIT-rpc.md) | 0 | 1§ | 5 | 1 | 52 | HIGH-2 |
| **Total (deduped)** | — | **6** | **11** | **~30** | **~25** | **~135** | — |

† intentional Go corrections, documented as parity divergences.
‡ latent, not currently triggerable.
§ pipelineCount epoch-bypass call-out; not a defect.

---

## Executive summary

**Six real CRITICAL gaps to TS parity, eleven HIGH.** Two CRITICALs are
silent-wrong-result class (scalar-subquery reset, minRowVersion bump);
two are restart/recovery failures that pin Go-primary into permanent
TS-fallback after first sidecar restart; two are observability voids
that lie to operators about Go-primary performance. The HIGH tier is
dominated by leaks (storage rows, event-loop blocking), silent edge-case
divergences (null-PK handling, LIKE semantics, BigInt PK logging), and
broken or never-implemented telemetry that was claimed FIXED in
`REVIEW-final.md`.

**No CRITICAL gaps in:** Operators (every fix from REVIEW-porting still
holds), Source semantics, AST handling, RPC wire protocol.

**Soak-evidence caveat:** The 0-mismatch shadow soaks (REVIEW-final) and
the 45-min Go-primary soak don't exercise: any backfilled tables with
`minRowVersion`, scalar subqueries whose child-field changes, BigInt-PK
columns, non-ASCII LIKE patterns, paginated queries through NULL on a
nullable order column, any sidecar restart that lands on a CG with
`lmids`/`mutationResults` already in `#pipelines`. Several CRITICALs are
**latent in the soak**.

---

## CRITICAL findings (6)

Severity rule: silent wrong client data, panic, or permanent stuck state.

### CRIT-1: Scalar-subquery value change during advance — TS resets, Go doesn't
**Where:** `mono/.../pipeline-driver.ts:2009-2015` (TS) vs
`go-ivm/engine/engine.go:439-445` + `1019-1025` (Go).

TS's companion `setOutput` checks `scalarValuesEqual(newValue, resolvedValue)`
and throws `ResetPipelinesSignal` when they differ. View-syncer catches
and re-hydrates with a fresh resolved literal. Go's `pipelineOutput.Push`
just accumulates the change. Main pipeline's WHERE keeps the stale
literal forever.

**Impact:** silent wrong result-set membership for any query with a
scalar-resolved EXISTS whose child value changes (test cases at
`pipeline-driver.test.ts:2413/2475/2497` — all expect the reset that Go
omits). Drift audit can detect it ONLY on the cycle that happens to
sample the affected query (one random query per ~60s). For a 100-query
CG, expected detection latency is ~100 minutes per drifting query.

**Fix:** port `scalarValuesEqual` check into a new `companionOutput`
struct on Go side that carries `(childField, resolvedValue, matched)`.
Signal back to TS via existing `DriftError` channel (reuse the recovery
path; reset semantically equivalent). See lifecycle audit CRITICAL-1
for the four-step plan and required test cases.

Cross-ref: lifecycle CRIT-1, lifecycle MED-6 (no-initial-match
permanently-invisible variant), `PORT-AUDIT-FIXES.md` item J,
dispatch audit CRIT-2 (related — the per-query path that exposes this).

### CRIT-2: `getCurrentQueries` doesn't filter internal queries → restart pins Go to TS fallback
**Where:** `mono/.../pipeline-driver.ts:521-525`.

```ts
() =>
  Array.from(this.#pipelines.entries(), ([queryID, p]) => ({
    queryID,
    ast: p.transformedAst,
  })),
```

No filter. `#pipelines` always contains `lmids` and `mutationResults`
(rooted at `<appID>.clients` / `<appID>_<shard>.permissions`) which were
deliberately excluded from Go's `init` (pipeline-driver.ts:592-594). On
any restart/reset, `#reinitPerCGAndRegisterQueries` ships these ASTs to
Go; `builder.BuildPipeline` panics at `go-ivm/builder/builder.go:100` —
recovered into an RPC error → recovery returns false → `#initialized`
stays false → TS fallback for the rest of the process.

**Impact:** every Go-primary CG goes permanently to TS-fallback on
first sidecar restart, drift recovery, or `resetEngine`. Documented
"transparent restart" in REVIEW-final is misleading.

**Fix:** symmetrize with every other dispatch site (lines 959, 1111,
1279, 1440, 2195, 2456). Also plan the AST via `#planAstForGo` while
you're there:
```ts
() =>
  Array.from(this.#pipelines.entries())
    .filter(([qid, p]) =>
      !this.#isInternalQueryID(qid) &&
      !this.#isInternalTable(p.transformedAst.table),
    )
    .map(([queryID, p]) => ({queryID, ast: this.#planAstForGo(p.transformedAst)})),
```

Cross-ref: dispatch CRIT-1, `PORT-AUDIT-FIXES.md` item I.

### CRIT-3: `#goHydrate` sends raw AST + skips TableSource creation
**Where:** `mono/.../pipeline-driver.ts:985`.

```ts
const goResult = await this.#goBackend!.hydrate(queryID, query);
```

Two simultaneous gaps vs the canonical TS path:
- **Unplanned AST:** missing `#planAstForGo` → Go misses cost-model
  `flip:true` decorations → OR-with-CSQ queries route to plain
  Join+applyOr → over-emit (H18 class). Every other Go-bound dispatch
  (`goHydrateBatch`, `goHydrateBatchStream`, `shadowBatchCompare`,
  drift audit) calls `#planAstForGo` first.
- **No TableSource creation:** `#planAstForGo` has a load-bearing side
  effect — it calls `#getSource(tableName)` for every walked table,
  creating TableSources in `#tables`. `getRow` (line 2106) then
  `must()`-panics on missing TableSource. The per-query path is
  exercised on every reconnect via `view-syncer.ts:1463` →
  `#hydrateUnchangedQueries` → `pipelines.addQuery` → `#goHydrate`.

**Currently partially masked** because `view-syncer.ts:1492-1500` calls
`shadowBatchCompare` unconditionally after the loop (HIGH-5 below);
that path DOES plan the AST and creates TableSources as a side-effect.
But fixing HIGH-5 without fixing CRIT-3 exposes the latent panic.

**Fix:** `pipeline-driver.ts:979-985`:
```ts
async #goHydrate(transformationHash, queryID, query) {
  this.removeQuery(queryID);
  const planned = this.#planAstForGo(query);   // ← add this
  const goResult = await this.#goBackend!.hydrate(queryID, planned);
  // ... store `planned` as transformedAst in the pipeline entry
```

Cross-ref: dispatch CRIT-2, `PORT-AUDIT-FIXES.md` item F.

### CRIT-4: Streamer's `minRowVersion` bump is TS-only — Go emits stale `_0_version`
**Where:** `mono/.../pipeline-driver.ts:3147-3155` (TS) vs
`go-ivm/engine/streamer.go:147-163` (Go absent).

TS bumps `_0_version` to `spec.minRowVersion` on every non-REMOVE row
before emission. Go's `streamNodes` emits `node.Row` verbatim. Repo-wide
grep for `_0_version` in `go-ivm/**/*.go` returns zero. The mirror copy
at `go-ivm/client/pipeline-driver-go.ts:1267-1275` proves the logic was
once known on the Go side and was dropped.

**Impact:** when a table has `minRowVersion` set in
`_zero.tableMetadata` (set by replication after backfill), Go emits
rows with stale versions. View-syncer CVR bookkeeping compares against
recorded versions → deltas misclassified as "client already has it" or
shipped redundantly. **Silent client-view corruption**. Latent in the
soak because no test table currently has a non-null `minRowVersion` —
the moment a schema-evolution `tableMetadata` upsert lands, shadow will
start logging mismatches and Go-primary will start corrupting.

**Fix (lower risk, TS-side):** apply bump in `#goRowChangeToRowChange`
at `pipeline-driver.ts:2336`. `this.#tableSpecs` is in scope; 3-line
change. Go side stays raw.

**Fix (full parity, Go-side):** add `MinRowVersion` to `ivm.SourceSchema`
populated from `init.TableData`, apply in `streamer.go:streamNodes`
between rowKey construction (line 152) and RowChange assembly (line 154).

Cross-ref: streamer CRIT-1, `PORT-AUDIT-FIXES.md` item K (which had it
listed as LOW observability — re-elevated to CRITICAL by the streamer
audit's analysis of CVR-version interaction).

### CRIT-5: `#scheduleGoReset` captures `#currentTablesForGo()` BEFORE await — drift-loop amplifier
**Where:** `mono/.../pipeline-driver.ts:2364`.

```ts
const tables = this.#currentTablesForGo();  // ← captured pre-await
this.#goInitPromise = this.#goBackend.resetEngine(tables);
```

Network round-trip for `init` + chunked `loadRows` happens after the
snapshot is captured. Mutations land on the replica in between. Go
re-inits from a stale snapshot; the next `#snapshotter.advance()` diff
applies against state that doesn't match the just-loaded data. Result:
likely panic with `Row not found` → drift signal → another
`#scheduleGoReset` → loop.

**Impact:** drift-loop circuit breaker (5 trips / 60s → 5-min cooldown
on TS-fallback) catches this, but every reset has measurable chance of
follow-up reset; alerts become noisy; Go-primary spends substantial
windows in TS fallback.

**Fix:** capture `tables` inside `resetEngine` (or pass a thunk), not
in `#scheduleGoReset`. Even better: synchronize the snapshot to the
boundary the next advance will start from.

Cross-ref: dispatch CRIT-5 (NEW finding).

### CRIT-6: Type-coercion init/advance shape parity depends on Go-side completeness as single source — no startup check
**Where:** `mono/.../pipeline-driver.ts:602` (init: raw SQLite) vs
`mono/.../snapshotter.ts:532-537` (advance: `fromSQLiteTypes`-processed).

TS init sends raw SQLite values (booleans as 0/1 int, JSON as text
string). TS advance sends pre-coerced values (JS bool, JS object). Both
land in Go's MemorySource, and the two paths only converge because Go's
`sqlite.FromSQLiteType` happens to accept BOTH input shapes for every
listed colType. Any future PG type added to `pgTypeToGoType` without a
matching `case` in Go's `FromSQLiteType` would silently leave init and
advance with mismatched per-column shapes.

**Impact:** today functions correctly — but a single missed coercion
case in a future type addition turns into silent client data
corruption. The contract is invisible and untested.

**Fix:** add a sidecar startup self-check that walks every column's
declared colType through `FromSQLiteType` with a representative value
of each shape (int64, float64, string, []byte, bool, nil) and asserts
non-default coercion exists. Also add `case "null"` to
`FromSQLiteType` mirroring TS's `'number'|'string'|'null'` folded case
(types MED-4).

Cross-ref: types CRIT-1.

---

## HIGH findings (11)

### HIGH-1: Cursor pagination on nullable order columns returns 0 rows on Go-primary
**Where:** `mono/.../pipeline-driver.ts:596-599` (TS doesn't send
`optional`) vs `go-ivm/sqlite/query_builder.go:270-286`
(`nullableAwareEquality` / `nullableAwareRangeComparison` always emit
`= ?` / `field > ?`).

TS column schema sent over the wire never sets `Optional`. Go's
`ColumnSchema.Optional` defaults to `false`. The nullable-aware SQL
branches require `Optional=true` to emit `(? IS NULL OR field > ?)` /
`(field IS NULL OR field > ?)`. So when a paginated query has
`start.Row[col] === null` on a nullable order column: TS yields N rows;
Go yields 0.

**Fix:** TS one-line at `pipeline-driver.ts:598`:
```ts
columns[col] = {type: pgTypeToGoType(colSpec.dataType, warn), optional: colSpec.notNull !== true};
```

Cross-ref: types CRIT-2.

### HIGH-2: `#goPrimaryAdvance`/`#shadowAdvance` drop `r.timings` — `ivm.advance-time` histogram populated only by TS internal queries
**Where:** `mono/.../pipeline-driver.ts:2239` and `:2504`.

```ts
.then(r => r.changes.map(rc => this.#goRowChangeToRowChange(rc)))
```

Only `changes` consumed. Go's advanceStream returns per-(table, op)
`timings` array — silently dropped. The only TS code that calls
`#advanceTime.recordMs(elapsed, {table, type})` (line 2868) is inside
the TS-native `#advance` generator. In Go-primary mode that generator
processes only internal-query pushes (lmids/mutationResults), so the
histogram contains only those tiny samples and is meaningless for
user-table work.

**REGRESSION of REVIEW-final OBS-1** — claimed FIXED in `#goAdvance`;
the method was renamed/refactored to `#goPrimaryAdvance` and the
`recordMs` call wasn't carried over. Also, `ivm.advance-go-rpc-time`
histogram claimed FIXED in REVIEW-final MED-CROSS-3 — grep finds zero
references anywhere in TS tree (dispatch REGRESSION-1).

**Fix:** restructure `goPromise` to be `Promise<{changes, timings}>`;
after `await`, feed each timing into `#advanceTime.recordMs(ms,
{table, type})`. Add the missing `#advanceGoRpcTime` histogram around
the `await goPromise` call.

Cross-ref: dispatch CRIT-4 + REGRESSION-1, dispatch HIGH-7,
`PORT-AUDIT-FIXES.md` item L.

### HIGH-3: `MeasurePushOperator` decoration absent from Go-primary stub pipelines
**Where:** TS `pipeline-driver.ts:1911-1917` vs Go-primary stub
`pipeline-driver.ts:991-1003`.

TS wraps every `SourceInput` with `MeasurePushOperator` that reports
per-query push timings to `inspectorDelegate` under
`query-update-server` category. Go-primary user-query pipeline entries
are stubs — no MeasurePushOperator. The `query-update-server` per-query
push metric is permanently zero for every Go-primary CG.

**Fix:** surface Go's `TableTiming[]` (after HIGH-2 fix) and feed
`inspectorDelegate.addMetric('query-update-server', ms, queryID)` in
`#goPrimaryAdvance`. Requires mapping `TableTiming.table` → queryID
(same table may be in multiple queries; pick slowest or sum).

Cross-ref: dispatch CRIT-3, `PORT-AUDIT-FIXES.md` item O.

### HIGH-4: Per-query operator storage rows leak — `cgs.Destroy` never called
**Where:** `go-ivm/engine/engine.go:665-678` (`removeQueryLocked`).

`engineDelegate.CreateStorage` keys storage by `cgID = queryID`
(engine.go:1066, 1082-1087). `CreateClientGroupStorage` issues
`DELETE FROM storage WHERE clientGroupID = ?` only on construction;
`Destroy()` is never invoked. Repo-wide `grep -rn cgs.Destroy` is empty.

**Impact:** slow leak per (distinct queryID over CG lifetime) × (per-query
storage rows). For a 1000-distinct-query CG with 100-row Take operator
windows: ~100k orphan rows. Bounded only by the 30-min idle reaper
(HIGH-CROSS-2) which destroys the whole engine.

**Fix:** store the delegate's `cgs` on `pipelineEntry`; call
`entry.cgs.Destroy()` in `removeQueryLocked`. Or `e.storage.CreateClientGroupStorage(queryID).Destroy()`
which has the same DELETE side-effect (slightly wasteful but simpler).

Cross-ref: lifecycle HIGH-2.

### HIGH-5: `#hydrateUnchangedQueries` calls `shadowBatchCompare` in Go-primary — Go-vs-Go double hydrate (and accidentally masks CRIT-3)
**Where:** `mono/.../view-syncer.ts:1492-1500`.

`shadowBatchCompare` returns early on `!#goBackend?.initialized` but
NOT on `!#shadowMode`. So in Go-primary, after the per-query hydrate
loop, a full `hydrateManyStream` runs the same queries against Go a
second time. The comparator compares Go-output-from-loop vs
Go-output-from-batch (always matches) → wasted work.

**Useful side-effect that's masking the bug:** `shadowBatchCompare`
calls `#planAstForGo` (line 1303) which creates the TableSources
missing from CRIT-3's `#goHydrate` path. **Fix HIGH-5 alone and you
expose the CRIT-3 `getRow` panic.** Must be fixed alongside CRIT-3.

**Fix:** gate the call on `isGoShadowMode(...)`. Fix CRIT-3 in the
same PR.

Cross-ref: dispatch HIGH-5 (NEW finding, very subtle interaction).

### HIGH-6: `getRowKey` null-PK — Go writes nil silently, TS throws loud
**Where:** TS `pipeline-driver.ts:3184` uses `must(row[col])` → throws.
Go `engine/streamer.go:149-152` just writes `node.Row[pk]` → nil if
absent.

**Impact:** a row reaching the streamer without its PK (corrupted
replica, join `processParentNode` mis-constructing) — TS panics loudly,
the ViewSyncer's unhandledRejectionHandler tears down the CG and the
operator sees it. Go silently ships a wire message with `rowKey: {pk:
nil}` which the CVR records; no future advance ever matches that
nil-keyed row → permanent client-view soft leak.

**Fix:** Go `streamer.go:151` — panic on nil PK to match TS's
loud-fail-fast contract.

Cross-ref: streamer HIGH-1.

### HIGH-7: Relationship-emission order non-deterministic on Go (map iteration), masked by shadow's sort
**Where:** TS `pipeline-driver.ts:3165` `Object.entries(relationships)` —
insertion order. Go `engine/streamer.go:165-175` `range
node.Relationships` — Go runtime hash randomization. `materializeNode`
at engine.go:1042-1060 rebuilds the relationship map fresh per call,
erasing any upstream order.

**Impact:** for a node with relationships `photos` + `comments`, TS
always emits `photos→comments` (or whatever AST traversal order
produced), Go emits one of the two orderings non-deterministically
per call. Client-side code depending on RowChange arrival order
(per-row effect callbacks, intermediate-state UI) behaves
non-deterministically. Shadow comparator and drift audit both sort by
`(queryID, table, rowKey, type)` BEFORE diffing — so the soaks
**cannot** detect this divergence.

**Fix:** alphabetical sort (deterministic-but-not-TS-equivalent, easy)
or carry an ordered relationship-list alongside the map for full TS
parity. Streamer audit suggests the latter for correctness.

Cross-ref: streamer HIGH-2, `PORT-AUDIT-FIXES.md` item U.

### HIGH-8: LIKE semantics diverge — Unicode/escape/newline
**Where:** TS `mono/packages/zql/src/builder/like.ts` (regex,
Unicode-aware, escape-aware, multiline) vs Go
`go-ivm/builder/filter.go:240-267` (byte-by-byte, no `\` escape,
matches `\n` freely).

Three observable cases:
- Multi-byte UTF-8: `_` on `'café'` — TS matches `é` as one code point;
  Go matches one byte (the leading 0xC3) and leaves 0xA9 dangling.
- Escape: pattern `10\\%` — TS matches literal `%`; Go has no escape
  handling.
- Embedded newline: pattern `%foo%` on `bar\nfoo` — TS regex `.` won't
  cross `\n`; Go's `%` matches `\n` freely.

**Impact:** any LIKE/ILIKE query against non-ASCII columns or escaped
patterns silently produces different row sets between TS and
Go-primary. Soaks don't exercise it.

**Fix:** port to Go's `regexp` package using TS's `patternToRegExp`
translation rule-for-rule. `regexp` is Unicode-aware.

Cross-ref: types HIGH-1, AST LOW-1+LOW-2.

### HIGH-9: BigInt PK silently truncates / crashes drift-log
**Where:** Two related issues with int64 PKs above 2^53:

- `go-ivm/sqlite/query_builder.go:347-374` does `float64(int64)` in
  `FromSQLiteType("number", ...)` — silent precision loss; PKs above
  2^53 alias to adjacent integer values; joins on such keys silently
  match wrong rows. TS throws `UnsupportedValueError` on the same
  input (types MED-2).
- `go-compute-backend.ts:280` calls `JSON.stringify(err.pk)` on drift
  recovery path — throws on BigInt (msgpackr returns BigInt for >2^32
  ints when `UseCompactInts` doesn't apply). Drift recovery itself
  crashes with the BigInt-stringify error rather than recovering.

**Impact:** any bigserial / int8 PK > 2^53 enters the
silent-wrong-results regime; if drift then triggers, recovery itself
crashes loudly with a misleading error.

**Fix:** Go `FromSQLiteType("number", ...)` — bounds-check int64
against ±MAX_SAFE_INT, panic on overflow to match TS throw. Add a
BigInt-safe replacer to the `JSON.stringify` call at
`go-compute-backend.ts:280`.

Cross-ref: types MED-2, RPC HIGH-2.

### HIGH-10: Streamer not drained on non-drift panic in `Engine.Advance` (inconsistent with `AdvanceStream`)
**Where:** `go-ivm/engine/engine.go:699-720` (Advance) re-raises
`panic(r)` on non-drift without draining `e.streamer`. The streaming
variant at `engine.go:885-894` does drain (`_ = e.streamer.Stream()`).

**Impact:** in Go's non-streaming Advance, a non-drift panic leaves
accumulated streamer entries. The next successful Advance's first
`e.streamer.Stream()` call surfaces those stale entries as if produced
by the new advance — wrong RowChanges shipped to clients. Partially
masked by the sidecar's outer recover + `#scheduleGoReset` (which
recreates the engine), but isn't airtight if a soft recovery happens.

**Fix:** drain the streamer in the recover block before re-raising:
match the pattern from `AdvanceStream`.

Cross-ref: lifecycle HIGH-3.

### HIGH-11: Companion `SetOutput` wired before hydrate in Go (vs after in TS)
**Where:** TS `pipeline-driver.ts:1936-1944, 1986-2026` wires main
before hydrate, companion AFTER. Go `engine.go:429-445` wires both
BEFORE hydrate. Benign today because `e.mu` serializes build+hydrate
in Go's `AddQuery*` paths — but fragile.

**Impact:** if Go ever loosens locking to overlap advance with batch
hydration for throughput, companion outputs would emit during the
hydrate window (likely double-emitting against the static companion
ADDs at engine.go:464-468). With CRIT-1 unfixed, no value-check
catches the duplicates.

**Fix:** when CRIT-1 is fixed, also move companion `SetOutput` to
AFTER `hydrateEntry` in `AddQuery` / `AddQueries` / `AddQueriesStream`.

Cross-ref: lifecycle HIGH-1.

---

## MEDIUM findings (~30)

Grouped by area. Each one is documented in its area's detail doc.

**Source layer (4):** MED-1 missing `generateWithOverlayUnordered` (latent),
MED-2 `getSortedRows` re-sorts every Fetch (perf), MED-3 `BulkInsert`
takes wrong lock (documentation-only today), MED-4 no `getRow` on
MemorySource (by design — TS-side TableSource owns row catchup).

**Types (9):** MED-1 string-typed booleans diverge (`!!"false"` vs Go's
literal-list check), MED-3 `Optional` flag wire-shape asymmetry,
MED-4 missing `case "null"` in `FromSQLiteType` (test-only),
MED-5 TIME/TIMETZ missing from `pgTypeToGoType` numeric list,
MED-6 bare `INT` not recognized (canonical is `integer`),
MED-7 SERIAL family + bare FLOAT missing (PG rewrites SERIAL → INTEGER),
MED-8 IN/NOT IN SQL-path JSON-marshals vs predicate-path ValuesEqual,
MED-9 `pgTypeToGoType` doesn't strip `(N)` from `varchar(N)` (cosmetic
warning).

**Operators (9):** MED-1 Take EDIT recovery via DriftError (Go) vs assert
(TS) — intentional resilience; MED-2 Take REMOVE no-replacement-bound
forces Size=0 (Go fixes a latent TS bug); MED-3 UnionFanIn missing
per-input schema asserts; MED-4 PushAccumulatedChanges drops type-count
asserts; MED-5 Skip reverse `break` vs `return` (CHECKED-OK behaviorally);
MED-6 Take initialFetch missing reverse assert; MED-7 Take initialFetch
missing start-undefined assert; MED-8 cross-type panic on `<`/`>` (HIGH-2
fixed `=`/`!=`, asymmetric); MED-9 FanIn missing schema-mismatch assert.

**AST (5):** MED-1 no cost-model planner on Go (masked by `#planAstForGo`
wrapper — fragile if a fifth dispatch site forgets); MED-2 `enableNotExists`
flag absent (benign — server-side always true); MED-3 user-`flip:true` +
NOT EXISTS edge case (mirrored blindspot on both sides); MED-4
`BuildPredicate` passthrough on CSQ leaf (defensive bug, dead code today);
MED-5 NOT EXISTS no-match collapses to ALWAYS_FALSE (should be ALWAYS_TRUE
— mirrored upstream TS bug, do NOT fix Go unilaterally).

**Lifecycle (6):** MED-1 no per-row yield in hydrateEntry (event-loop
blocking at scale); MED-2 companion struct missing scalar-state fields
(structural manifestation of CRIT-1); MED-3 companion schema source
(CHECKED-OK); MED-4 hydrate companion-row ordering (CHECKED-OK);
MED-5 companion alive on no-match (CHECKED-OK); MED-6 no-initial-match
permanently invisible (rephrasing of CRIT-1).

**Dispatch (10):** MED-1 asymmetric cooperative yielding between
`#goHydrate` (yields) and `goHydrateBatchStream` (drops); MED-2
`awaitGoInit` race (CHECKED-OK on inspection); MED-3 completion-order
yield contract (CHECKED-OK); MED-4 context state across mode boundaries
(CHECKED-OK); MED-5 `destroy()` doesn't await `#goBackend.destroy()`
(rapid-recycle race); MED-6 `#advance` silent skip in Go-primary
(CHECKED-OK); MED-7 drift-audit `tsExpected` count filter inconsistency;
MED-8 `getCurrentTables` callback thread-safety (snapshotter read
outside lock); MED-9 `#sqlGroundTruthCompare` sampling latency-to-detect;
MED-10 `#planAstForGo` panics on schema skew without try/catch.

**RPC (5):** MED-1 `uniqueKeys` may be undefined → Go decodes as nil
(verify resolver behavior); MED-2/MED-3 string-match coupling on
error messages (`"engine not initialized"`, `"Sidecar is not running"`);
MED-4 `#runInit` partial-loadRows failure leaves `#initialized` false;
MED-5 `Result: "ok"` plain string code smell.

**Streamer (4):** MED-1 dead-code `rc.row ?? rc.rowKey` fallback in TS
conversion (defense net); MED-2 permissions guard split cosmetic
mismatch; MED-3 `IsScalar` guard is Go-only (documented asymmetry);
MED-4 cooperative yields per 100 changes vs operator-chain yields.

---

## LOW findings (~25)

Cosmetic, defensive, or intentional. See individual area docs.
Highlights:

- Streamer LOW-2: `hydrateEntry` skips outer permissions guard (defensive,
  latent — main pipeline schema never `system=permissions` today).
- Streamer LOW-4: Edit path bypasses `streamNodes` → missing IsScalar
  guard for Edit-on-IsScalar-child (latent over-emission).
- Lifecycle LOW-2: `Engine.Close` doesn't call `cgs.Destroy` per query
  before closing storage DB (bounded by engine-lifetime).
- Source LOW-3/4/5: missing PK uniqueness assert, missing
  `assertOrderingIncludesPK` at Connect, no `fork()` — all defensive.
- Dispatch LOW-1: stub pipeline's `getSchema` returns `{} as never`
  (footgun for future maintainers).
- Dispatch LOW-2: `[shadow]` log prefix appears in Go-primary reset
  paths — confusing during incident triage.
- Pipeline-driver.ts:2030 — stale comment referencing nonexistent
  `setHydrationTime()` method (dispatch HIGH-8 has more detail).

---

## What's verified clean — positive coverage

The audits collectively CHECKED-OK ~135 invariants. Highlights of what
you can stop worrying about:

**Operators (25 OK):** every operator port has correct ValuesEqual /
CompareValues null semantics (REVIEW-final invariant #8), correct
in-progress child-change overlay in Join/FlippedJoin, correct CHILD
cache reuse + ADD+REMOVE size handling in Exists, correct
PushAccumulated EDIT split/reconstitute, correct Take partition-key
duplicate handling, correct Skip three-way return, correct
ScalarSubquery resolution + companion tracking, correct applyOr /
applyFilterWithFlips partitioning, correct UTF-8 byte ordering for
strings, correct REMOVE row shape parity (OK-5 invariant).

**Source (14 OK):** Push validate-before-fanout-then-writeChange
ordering, split-edit global scan in both sequential and parallel paths,
Add/Remove/Edit pre-mutation validation, NormalizeRow via ValueConverter,
deterministic connection iteration order (sequential AND parallel),
overlay set/clear via `atomic.Pointer` with defer-clear (REVIEW-final
invariant #10), ConstraintMatchesRow ValuesEqual, reverse-aware overlay
comparator, applyStart for reverse fetch with partial cursor support,
generateWithOverlayInner ordering with trailing yield, DriftError typed
panic recovery contract preserved in both sequential and parallel paths,
ValuesEqual nil-nil = false (REVIEW-porting CRITICAL-1).

**Types (12 OK):** every common PG type — bool, int2/4/8, real,
double, numeric, text, varchar, uuid, json, jsonb, date, timestamp,
timestamptz, enums (via TEXT_ENUM), arrays (opaque JSON), NULL
handling, ValueConverter wiring at sidecar startup, Row.DecodeMsgpack
typed decoder, walkForNumericNormalize for non-Row payloads.

**Streamer (5 OK):** ADD emits row + recurses, REMOVE emits
row=undefined + recurses, CHILD recurses without top-level emit, rowKey
column order matches via PrimaryKey slice iteration + stableStringify
comparison resilient to map vs object ordering, ChangeType enum mapping
(ADD=0, REMOVE=1, EDIT=2).

**AST (16 OK):** completeOrdering, applyOr branch partitioning,
applyFilterWithFlips H18 fix, applyCorrelatedSubqueryCondition limit-0
short-circuit, EXISTS-Join construction with PERMISSIONS_EXISTS_LIMIT,
gatherCorrelatedSubqueryQueryConditions, alias uniquification,
stripCSQConditions / source-level filter narrowing,
resolveSimpleScalarSubqueries algorithm parity, predicate null
short-circuit, AST literal numeric coercion at wire decode, Related
dedup-by-alias LWW, splitEditKeys composition.

**Lifecycle (7 OK):** companion-row queryID tagging, removeQuery
companion destroy ordering, companion Connection removal from source
on destroy, per-source-change streamer drain timing, companion alive
on no-initial-match, snapshot setDB / OnAdvanceEnd ordering, driver
destroy ordering.

**RPC (52 OK):** method-set parity, length-prefix framing with
matching 64MB cap, zero-length frame rejection (D7), oversized-frame
backpressure (H8), JSON-RPC envelope shape, BigInt/Number id
round-trip safety, every RPC method's contract individually (ping,
version, init, loadRows, addQuery, addQueries, addQueriesStream,
removeQuery, advance, advanceStream, destroy, refreshSnapshot,
pipelineCount), init-epoch enforcement on every mutating handler,
streaming invariants (chunkIndex monotonic, exactly one Final=true,
"done" terminator), DriftError contract on both streaming and
non-streaming, sidecar restart in spawned + externallyManaged modes,
sliding-window failure cap (RESILIENCE-6 + D13 fix), waitForRunning
queue across restart, advance "miss exactly one delta" contract,
withReinitRetry retry-after-reinit, per-CG drift breaker, init
concurrency cap, per-clientGroupID semaphore, per-call timeout with
recently-timed-out dedupe, byte-level backpressure, connection-close
fail-fast, streaming-frame atomicity under concurrent writers, msgpack
codec normalization, traceparent propagation, onRestart firing
post-epoch-bump, doInit dedup (RESILIENCE-5), idle group reaper.

---

## Coverage gaps — what was NOT checked

Be honest about the negative space:

- **Streamer:** msgpack encoding fidelity of `RowChange.Row` (trusted
  to T1-1 / PORT_CORRECTNESS_ANALYSIS), drift-audit SQL-ground-truth
  comparator's `_0_version` sensitivity (recommend follow-up: confirm
  CRIT-4 isn't masked by SQL comparator ignoring version).
- **Operators:** deeply nested OR-with-mixed-CSQ recursion beyond one
  level; AST `flip` field with NESTED flipped CSQs; per-operator yield
  contract (Go drops yields entirely — assumed FALSE-dependence for
  correctness but not exhaustively proven).
- **Types:** hstore, interval, PostGIS, network types (inet/cidr/macaddr),
  range types, tsvector/tsquery, xml, money/pg_lsn, composite types,
  DOMAIN types, empty arrays vs NULL array equality semantics, NaN /
  Infinity in numerics, embedded NUL bytes in strings.
- **AST:** TS user-facing `.where()/.related()` builder semantics
  (audit only covers wire-AST → operator tree), permission-system AST
  mutation, planner Cap operator adoption (Go would need port if TS
  starts using).
- **Lifecycle:** Cost-model planner integration (covered by H18-followup),
  Snapshotter advance + DriftError interaction at sequence level,
  per-CG worker goroutine lifecycle in sidecar layer
  (REVIEW-parallelism CRITICAL-1).
- **Dispatch:** `GoIVMClient.#onData` frame-parser internals (trusted to
  RESILIENCE gates), `SidecarManager` state machine internals (trusted
  to RESILIENCE-1..7), CVR delta correctness, `#sqlGroundTruthCompare`'s
  `buildAuditSQL` known limitations, permissions reload.
- **RPC:** internal `GoIVMClient` accumulator code paths covered by
  unit tests but not deep-audited per-line.
- **Source:** Debug delegate parity (TS threads it through Connect; Go
  doesn't expose query introspection), splitEditKeys behavior when a
  split key is missing from row/oldRow, `internal/tablesource` package
  parity (separate audit surface — the "true port" of TS TableSource
  with snapshot rotation; brief check OK).

---

## Recommended fix order

Ordered by user-visible impact, prerequisite ordering, and effort.

**Round 1 (must-fix before Go-primary GA — silent correctness):**
1. CRIT-2 (`getCurrentQueries` filter) — one-line, unblocks restart recovery
2. CRIT-3 (`#goHydrate` plan AST + create TableSources) — one-line + verify
3. CRIT-5 (`#scheduleGoReset` snapshot capture timing) — small refactor
4. HIGH-1 / CRIT-2 of types (`Optional` flag wire) — one-line TS

**Round 2 (silent wrong-data, harder fixes):**
5. CRIT-1 (scalar value-change reset) — Go-side struct work + new
   ResetSignal protocol field + 3 new test cases. Most invasive.
6. CRIT-4 (minRowVersion bump) — pick TS-side 3-line OR Go-side
   schema-extension. Recommend TS-side until shadow soak runs against
   a backfilled table to validate.
7. HIGH-8 (LIKE Unicode/escape) — Go port to `regexp`. ~30 LOC.
8. HIGH-9 (BigInt PK bounds + drift-log) — two small changes.

**Round 3 (latent leaks, observability lies):**
9. HIGH-4 (storage leak — `cgs.Destroy` in `removeQueryLocked`)
10. HIGH-5 (gate `shadowBatchCompare` on shadow mode) — MUST sequence
    AFTER CRIT-3 fix; otherwise exposes the panic
11. HIGH-2 + HIGH-3 (plumb `r.timings` → histogram + inspector metrics)
12. HIGH-6 (Go panic on null PK)
13. HIGH-10 (drain streamer on non-drift panic in Advance)

**Round 4 (defensive parity, intentional divergences to document):**
14. CRIT-6 + types MED-4 (FromSQLiteType `case "null"` + startup assertion)
15. HIGH-7 (relationship-emission order — alphabetical sort is the
    pragmatic interim; full AST-order is the rigorous fix)
16. HIGH-11 (companion SetOutput-after-hydrate — pair with CRIT-1 fix)
17. RPC MED-2/MED-3 (error-string coupling → sentinel classes / codes)
18. Source HIGH-1 (`GenPushParallel` epoch bumping — fix or add
    SAFE-2 cross-conn-fetch invariant assertion)

**Soak gating per round:**
After each round, run:
- 30-min shadow soak (20 users × mutate against rust-test sandbox)
- Drift-injection test from REVIEW-final
- Sidecar-kill drill (3 SIGKILLs across one soak)

Round 1 + 2 must show 0 mismatches and clean restart recovery before
Go-primary is safe for production rollout.

---

## Per-area detail docs

Read for fix details, full context, and CHECKED-OK lists:

1. [`PORT-AUDIT-operators.md`](PORT-AUDIT-operators.md) — 299 lines
2. [`PORT-AUDIT-source.md`](PORT-AUDIT-source.md) — 563 lines
3. [`PORT-AUDIT-types.md`](PORT-AUDIT-types.md) — 623 lines
4. [`PORT-AUDIT-streamer.md`](PORT-AUDIT-streamer.md) — 605 lines
5. [`PORT-AUDIT-ast.md`](PORT-AUDIT-ast.md) — 265 lines
6. [`PORT-AUDIT-lifecycle.md`](PORT-AUDIT-lifecycle.md) — 679 lines
7. [`PORT-AUDIT-dispatch.md`](PORT-AUDIT-dispatch.md) — 755 lines
8. [`PORT-AUDIT-rpc.md`](PORT-AUDIT-rpc.md) — 1031 lines

Tracker: [`PORT-AUDIT-FIXES.md`](PORT-AUDIT-FIXES.md) — open vs in-progress
items pre-audit. Items E-W in that tracker correspond to findings
re-confirmed and re-prioritized here.

---

## Methodology + caveat

Each surface was audited by a dedicated subagent reading
TS-side + Go-side end-to-end with explicit property checklists. Findings
cite file:line on both sides. Severities are this auditor's own
re-ranking after consolidation (some area docs rated items LOW/MEDIUM
that consolidate to HIGH/CRITICAL once cross-area interactions are
visible — e.g., minRowVersion was LOW in streamer audit alone but
CRITICAL once you trace into CVR version bookkeeping).

**Audit caveat:** code audits find code-visible divergences. Behavioral
edge cases (timing-sensitive races, type-conversion at specific value
boundaries, complex query interactions) need runtime validation too.
The shadow-mode soak machinery + drift audit are the runtime
backstops; this audit identifies several places where those backstops
**cannot** detect existing divergences (CRIT-1 partial detection,
CRIT-4 latent until backfill, HIGH-7 fully masked by sort).
