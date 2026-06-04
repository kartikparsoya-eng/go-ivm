# PORT-AUDIT — Streamer (row-change emission layer)

Scope: how operator-chain `Change`s become `RowChange`s that leave the system,
on both the TS-native and Go-sidecar paths. Covers every `ChangeType`
(ADD / REMOVE / EDIT / CHILD), relationship recursion, per-row transforms
(`_0_version` bump, permissions filter, `IsScalar` skip), `rowKey`
computation, and hydration emission.

Files audited:
- TS canonical: `mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts`
  - `Streamer` class: lines 3043-3171
  - `toAdds`: line 3173
  - `getRowKey`: line 3183
  - `hydrate` / `hydrateInternal`: lines 3192-3219
  - `#goRowChangeToRowChange` (Go-result conversion): lines 2318-2338
- Go: `go-ivm/engine/streamer.go` (entire file)
- Go: `go-ivm/engine/engine.go`
  - `pipelineOutput` + `materializeChange` + `materializeNode`: lines 1010-1060
  - `hydrateEntry`: lines 450-471
  - `AddQueriesStream` per-node chunking: lines 561-622
- Wire type: `mono/packages/zero-cache/src/services/view-syncer/go-sidecar/go-ivm-client.ts:85-91`
- Mirror copy (Go-mode TS streamer): `go-ivm/client/pipeline-driver-go.ts:1196-1290`
- Baseline: `go-ivm/REVIEW-porting.md`, `go-ivm/REVIEW-final.md`
  (specifically the OK-5 / REMOVE-row-shape invariant — see REVIEW-final § "bottom line" item 4)

Severity rubric: **CRITICAL** = silent corruption of client view state;
**HIGH** = observable wrong rows/order to clients; **MEDIUM** = edge case,
sub-optimal emission ordering; **LOW** = cosmetic / missing instrumentation;
**CHECKED-OK** = equivalent; **REGRESSION** = was fixed, no longer.

---

## Findings

### CRITICAL-1: Go side never bumps `_0_version` to `spec.minRowVersion`
- **TS**: `pipeline-driver.ts:3147-3155` — inside `#streamNodes`, before
  yielding a non-REMOVE row, it reads `row[ZERO_VERSION_COLUMN_NAME]` and
  if it's a string strictly less than `spec.minRowVersion ?? '00'`, it
  rebuilds the row with the bumped column:
  ```ts
  if (op !== ChangeType.REMOVE) {
    const rowVersion = row[ZERO_VERSION_COLUMN_NAME];
    if (typeof rowVersion === 'string' &&
        rowVersion < (spec.minRowVersion ?? '00')) {
      row = {...row, [ZERO_VERSION_COLUMN_NAME]: spec.minRowVersion};
    }
  }
  ```
  Mirror in `go-ivm/client/pipeline-driver-go.ts:1267-1275` (same logic).
- **Go**: `engine/streamer.go:147-163` — **absent**. The row is yielded as
  `node.Row` verbatim. A repo-wide grep for `_0_version`/`ZERO_VERSION` in
  `go-ivm/**/*.go` returns zero hits. The Go-side `RowChange.Row` carries
  whatever `_0_version` was loaded into `MemorySource` during `loadRows`.
- **Divergence**: when a table has a `minRowVersion` set in
  `_zero.tableMetadata` (TS `LiteAndZqlSpec.tableSpec.minRowVersion`,
  defined in `mono/packages/zero-cache/src/db/specs.ts:98` and populated
  from the SQLite metadata table per `db/lite-tables.ts:62-72`), TS
  guarantees every emitted row carries `_0_version >= minRowVersion`. Go
  carries through whatever stale per-row version was on disk at the time
  the sidecar's `MemorySource` was loaded.
- **Impact**: The view-syncer uses `_0_version` to decide whether the
  client already holds the row at-version-or-newer (CVR row-version
  bookkeeping). If Go emits a smaller `_0_version` than TS would for the
  same logical row:
  - In **Go-primary mode** the CVR records that stale version; subsequent
    deltas may be classified as "client already has it" and dropped, OR
    the row is re-shipped on poke because the recorded version doesn't
    match what later writes assume; this is **silent client-view
    corruption** — the client sees an inconsistent row vs the same query
    on TS. The exact failure mode depends on how the view-syncer compares
    versions, but the contract that "every emitted row has version
    `>= spec.minRowVersion`" is broken.
  - In **shadow mode** the `_0_version` is part of `stableStringify(c.row)`
    inside `#shadowCompare` (pipeline-driver.ts:2611), so any divergence
    fires a row-content mismatch. Soaks did not surface this only because
    no production table has currently exercised a non-null
    `minRowVersion` while running shadow — but the moment a `tableMetadata`
    row is upserted (e.g., after a schema-evolution operation), shadow
    will start logging mismatches and Go-primary will start corrupting.
- **Why the `goRowChangeToRowChange` path doesn't save us**: at
  `pipeline-driver.ts:2336` the row is taken verbatim from the wire
  (`rc.row`). There is NO version-bump call on the conversion path —
  not in `#goRowChangeToRowChange`, not in `#goHydrate`, not in
  `goHydrateBatchStream`. The Go-side streamer is the only place this
  transform could be applied for Go-emitted rows, and it isn't.
- **Suggested fix**: port the version-bump into `streamer.go` between
  rowKey construction (line 152) and RowChange assembly (line 154). The
  spec needs to ride alongside the schema; either:
  (a) add `MinRowVersion *string` to `ivm.SourceSchema` and populate it
      from `init`'s `TableData.MinRowVersion`, OR
  (b) thread `map[tableName]minRowVersion` through `Engine` and look it
      up by `schema.TableName` in `streamNodes`.
  Option (a) keeps everything on the schema object (one lookup, no extra
  parameter); option (b) avoids touching `SourceSchema` (used by every
  operator). The protocol already carries per-table metadata in `init`,
  so wiring it is mechanical.
- **Alternative (TS-side) fix**: apply the bump in
  `#goRowChangeToRowChange` (pipeline-driver.ts:2318). The
  `LiteAndZqlSpec` map is already in scope (`this.#tableSpecs`), so this
  is a 3-line change that keeps the canonical version-bump logic on the
  TS side and treats Go's RowChange as raw row-bag. This is probably the
  lower-risk fix — keeps Go simpler.

---

### HIGH-1: `getRowKey` null-PK handling diverges (TS throws, Go silently writes nil)
- **TS**: `pipeline-driver.ts:3184` —
  ```ts
  function getRowKey(cols: PrimaryKey, row: Row): RowKey {
    return Object.fromEntries(cols.map(col => [col, must(row[col])]));
  }
  ```
  `must()` (mono/packages/shared/src/must.ts:1) throws `Error("Unexpected
  null/undefined value")` if any PK column is null/undefined.
- **Go**: `streamer.go:149-152` —
  ```go
  rowKey := make(map[string]interface{}, len(schema.PrimaryKey))
  for _, pk := range schema.PrimaryKey {
      rowKey[pk] = node.Row[pk]
  }
  ```
  Silently writes `nil` for a missing/null PK column. The emitted
  RowChange carries `rowKey: {pk: nil}` to the client.
- **Divergence**: a NULL PK should be impossible by schema, but if it
  ever appears (corrupted replica, column rename mid-stream, custom
  pipeline producing a row without the PK), TS panics loud — the
  ViewSyncer's `unhandledRejectionHandler` catches it and tears down the
  cg, the operator sees the error in logs. Go silently ships a wire
  message with `rowKey: {id: nil}`. The downstream comparator
  (`#shadowCompare` at line 2649) treats `stableStringify({id: null})`
  as equal between TS and Go only if TS happened to produce the same
  null, which it can't — TS would have thrown. So shadow surfaces this
  as `TS=N changes, Go=N+1 changes` (missing TS-side throw) but the
  per-row mismatch line doesn't pinpoint the null-PK as the cause.
- **Impact**: data-integrity blind spot. If a table source somehow
  produces a row missing its PK (the most likely cause: a join's
  `processParentNode` constructs a row from upstream that wasn't
  validated), Go silently writes a CVR entry with a nil-keyed row that
  no future advance will ever match (rowKey comparison is
  by-value-content, nil-keys are not findable by a subsequent Edit's
  PK lookup). The row stays in the client view forever (no REMOVE will
  ever match) — soft leak that grows with every such bad row.
- **Suggested fix**: `streamer.go:151` — `if v := node.Row[pk]; v == nil
  { panic(fmt.Sprintf("primary key %q missing from row in table %q",
  pk, schema.TableName)) } else { rowKey[pk] = v }`. Loud crash is
  preferable to a silent CVR leak. The sidecar's panic recovery
  (sidecar manager restart) will re-init and the bad row will surface
  again on the next pass — at which point the source-side bug gets
  fixed instead of being papered over.

---

### HIGH-2: Nested-relationship emission order is non-deterministic on Go
- **TS**: `pipeline-driver.ts:3165` —
  ```ts
  for (const [relationship, children] of Object.entries(relationships)) {
    const childSchema = must(schema.relationships[relationship]);
    yield* this.#streamNodes(queryID, childSchema, op, children);
  }
  ```
  `Object.entries` preserves the object's **insertion order**, which in
  turn is the order the operator chain added each relationship (Join's
  `processParentNode` at `join.ts` deterministically adds relationships
  one-by-one in AST-related order).
- **Go**: `streamer.go:165-175` —
  ```go
  for relName, childFn := range node.Relationships {
      childSchema := schema.Relationships[relName]
      ...
      for _, childNode := range childFn() {
          result = append(result, streamNodes(queryID, childSchema, op, childNode)...)
      }
  }
  ```
  Iteration over `map[string]func() []ivm.Node` is Go's randomized map
  iteration. Further upstream, `materializeNode` at `engine.go:1042-1060`
  also stores into a fresh `map[string]func() []ivm.Node` and discards
  whatever insertion order may have been on the original.
- **Divergence**: for a node with relationships `a` and `b` containing
  `aN` and `bN` rows respectively, TS always emits `a` rows then `b`
  rows in the same order; Go emits one of two orderings,
  non-deterministically per-call.
- **Impact**:
  1. **Client-visible intermediate states diverge**. Consider a query
     `SELECT u.*, photos: [...], comments: [...] FROM users WHERE id=42`
     where the user has 3 photos and 3 comments. The view-syncer streams
     chunks of RowChanges to the client; the WebSocket client applies
     them in arrival order. TS always yields `user42, photo1, photo2,
     photo3, comment1, comment2, comment3`. Go may yield `user42,
     comment1, comment2, comment3, photo1, photo2, photo3` on one
     advance and the opposite on the next. The client's intermediate
     view between consecutive applies is observably different (visible
     to UI subscribers between batched updates), and any client-side
     code that depends on RowChange arrival order (e.g., per-row
     `effect` callbacks fired in stream order) behaves
     non-deterministically.
  2. **Shadow-mode comparator papers it over**, so the soak doesn't
     catch this: `#shadowCompare` (pipeline-driver.ts:2615-2620) sorts
     by `(queryID, table, rowKey, type)` before diffing, and the
     RowChange wire shape has no inter-relationship index. So a
     shadow-clean soak does NOT prove parity for client-arrival order.
  3. **Drift audit** (pipeline-driver.ts:1602) uses the same sort, so
     it also doesn't catch this.
- **Suggested fix**: `streamer.go:165-175` — capture and sort the
  relationship names before iterating:
  ```go
  if node.Relationships != nil && schema.Relationships != nil {
      relNames := make([]string, 0, len(node.Relationships))
      for n := range node.Relationships { relNames = append(relNames, n) }
      sort.Strings(relNames)
      for _, relName := range relNames { ... }
  }
  ```
  Note: alphabetical order is NOT the same as TS's insertion order
  (which follows AST traversal). If true TS parity is required, the
  fix is bigger — both `materializeNode` and `streamNodes` would need
  to carry an ordered relationship-list alongside or in place of the
  map. Alphabetical is a deterministic-but-different fix that at least
  prevents per-call jitter; tracking AST-order is the full fix.

---

### MEDIUM-1: TS `goRowChangeToRowChange` falls back `rc.row ?? rc.rowKey` — Go never sends rowKey-as-row
- **TS**: `pipeline-driver.ts:2336` —
  ```ts
  row: type === ChangeType.REMOVE ? undefined : ((rc.row ?? rc.rowKey) as Row),
  ```
  For ADD/EDIT, if `rc.row` is missing/nullish, falls back to `rc.rowKey`.
- **Go**: `streamer.go:154-162` — Go always assigns `rc.Row = node.Row`
  on non-REMOVE (line 161). The wire encoding (`RowChange.Row` is
  `ivm.Row` with `omitempty` per line 25) emits the field when non-nil.
  For a non-REMOVE path, Go always writes `node.Row` which is
  always non-nil from `materializeNode` (line 1043 always sets `result :=
  ivm.Node{Row: node.Row}` from the upstream change).
- **Divergence**: TS has dead-code defense (`?? rc.rowKey`); Go never
  triggers it. Not a functional bug, but the comment at TS:2325-2330
  warns that the prior `row = rc.rowKey` "fix" caused a regression on
  every REMOVE — implying the defense was added after a real bug.
- **Impact**: none today. If Go ever skipped the row field (e.g., to
  save wire bytes when `row == rowKey`), TS would substitute rowKey
  silently — that would diverge from TS-native which always carries a
  full row. Reaffirms the OK-5 / REMOVE-row-shape invariant: do not
  attempt to "compress" the row field; the falsy-fallback is a trap.
- **Suggested fix**: leave both sides as-is. Recommend documenting at
  `streamer.go:160-162`: "Always set rc.Row to node.Row on non-REMOVE;
  the TS conversion path's `?? rc.rowKey` fallback is a safety net that
  must never be triggered — see REVIEW-final OK-5 inline comment at
  pipeline-driver.ts:2325."

---

### MEDIUM-2: Permissions guard is split — outer + inner — and they don't quite match
- **TS**: two guards.
  - `pipeline-driver.ts:3083-3085` in `#streamChanges` (top-level CHILD
    descent path).
  - `pipeline-driver.ts:3135-3137` in `#streamNodes` (nested relationship
    recursion path).
  Both early-return.
- **Go**: two guards.
  - `streamer.go:91-93` in `streamChanges` — but uses `continue`
    inside the loop, not `return`.
  - `streamer.go:135-137` in `streamNodes` — early-return.
- **Divergence**:
  - TS `#streamChanges` checks `schema.system === 'permissions'` ONCE
    outside the for-loop; if true, the entire generator yields nothing.
  - Go `streamChanges` checks inside the loop and `continue`s — for
    a single call this is identical, but the check executes once per
    change unnecessarily. Cosmetic.
  - Both also re-guard inside `streamNodes`/`#streamNodes` for the
    nested-relationship recursion case (where the schema flips to a
    permissions-system schema via `schema.Relationships[name]`).
- **Impact**: functionally equivalent. The Go guard is cheap so the
  inefficiency is irrelevant.
- **Suggested fix**: optional cosmetic — hoist the Go guard out of the
  loop body to match the TS pattern. Not load-bearing.

---

### MEDIUM-3: `IsScalar` guard is Go-only — TS has no schema-level equivalent (and doesn't need one)
- **TS**: no `IsScalar` field on `SourceSchema` (mono/packages/zql/src/ivm/schema.ts), no scalar-skip guard in `#streamNodes`. TS handles scalar
  EXISTS subqueries BEFORE the pipeline is built: `resolveSimpleScalarSubqueries`
  (pipeline-driver.ts:39, called at line 893) rewrites the AST,
  replacing each simple-scalar EXISTS with a literal. So the pipeline
  TS builds has NO scalar relationship in the first place, and there's
  nothing in the schema tree for the streamer to skip.
- **Go**: `ivm/operator.go:32` declares `IsScalar bool` on
  `SourceSchema`. `streamer.go:144-146` guards: `if schema.IsScalar
  { return nil }`. This drop is necessary because Go ALSO runs
  `ResolveSimpleScalarSubqueries` (engine.go:416), but for the CSQs the
  resolver could NOT simplify (no unique-key constraint), the join is
  built and the child schema gets marked `IsScalar=true` via JoinArgs
  flow (per the regression note in `scalar_unresolved_emit_test.go`).
- **Divergence**: surface-level — TS lacks the field. Behavior — both
  paths drop the same set of child-relationship row emissions.
  **Subtle wrinkle**: the test
  `scalar_unresolved_emit_test.go` (file header comment lines 1-14)
  records that propagating `Scalar` onto unresolved-CSQ joins was a
  bug — TS keeps them emitting, Go was over-dropping. Fix (commit
  971dcd1) drops `Scalar` from JoinArgs at the unresolved-CSQ call site
  so the schema's `IsScalar` stays false and the streamer emits.
- **Impact**: the current fix is correct. Risk: future changes to where
  `IsScalar=true` gets set on a child schema could regress — the test
  guards the channels-EXISTS-scalar-conv case but not other unresolved-
  CSQ shapes.
- **Suggested fix**: leave the guard, keep the test. **Add a comment at
  `streamer.go:144`** linking back to TS's `resolveSimpleScalarSubqueries`
  call site (`pipeline-driver.ts:893`) explaining the asymmetry: "TS
  rewrites scalar CSQs into AST literals BEFORE pipeline build, so no
  scalar relationship exists in the TS-side schema tree. Go runs the
  same resolver but the residual unresolved-CSQ joins reach `streamer`
  with `IsScalar=true` on the child schema. This guard is the ONLY
  place that drops them in Go — its companion in TS is the AST-rewrite,
  not a runtime guard."

---

### MEDIUM-4: TS streamer yields cooperative `'yield'` tokens; Go streamer doesn't (path divergence)
- **TS**: `pipeline-driver.ts:3070-3074, 3088-3090, 3140-3142` — the
  streamer's outer `stream()` and inner `#streamChanges` / `#streamNodes`
  generators forward the special `'yield'` sentinel for cooperative time
  slicing. The view-syncer's `TimeSliceTimer` reacts via `yieldProcess`.
- **Go**: `streamer.go` returns `[]RowChange` synchronously; no yield
  tokens. The Go-result conversion path at pipeline-driver.ts:1007-1015
  re-injects a `yield` every 100 changes:
  ```ts
  if (i > 0 && i % 100 === 0) { yield 'yield'; }
  ```
- **Divergence**: TS Streamer yields per-change-boundary inside
  `#push` (genPush yields), Go yields per-100-changes only after the
  whole Go batch arrived. The `goHydrateBatchStream` path explicitly
  DROPS yields (`pipeline-driver.ts:1213-1220`) because the batch
  consumer doesn't run TimeSliceTimer.
- **Impact**: Go-primary advance may starve the event loop for longer
  than TS-native for very large RowChange batches. Not a correctness
  issue — only a fairness one. Mitigated by Go-side chunking
  (`hydrateChunkSize = 10000` per AGENTS.md), so the worst-case
  contiguous JS work is one chunk's worth.
- **Suggested fix**: none for streamer.go itself (the wire shape
  doesn't carry yields). If event-loop starvation becomes measurable,
  drop the `% 100` to `% 50` in pipeline-driver.ts:1010, or move the
  yield insertion to the `goHydrateBatchStream` loop too.

---

### LOW-1: Go `Streamer` has no `tableSpecs` reference — symptomatic of CRITICAL-1
- **TS**: `Streamer` constructor takes `(primaryKeys, tableSpecs)` and
  uses `tableSpecs` solely for the `_0_version` bump.
- **Go**: `Streamer` is just `{mu, accumulated}`. No spec map. The
  `streamNodes` function takes only `(queryID, schema, op, node)` —
  schema carries PK + columns + relationships + IsScalar but not
  minRowVersion.
- **Divergence**: the absence of the spec dependency is the *signal*
  that the `_0_version` bump was never ported. Captured in CRITICAL-1;
  noted here as a structural marker.

---

### LOW-2: Hydration emission paths differ in shape but are equivalent
- **TS**: `hydrate()` / `hydrateInternal()` build a one-shot Streamer,
  call `accumulate(... toAdds(res))`, then `yield* streamer.stream()`.
  `toAdds` (pipeline-driver.ts:3173) converts every fetched Node into
  `[ChangeType.ADD, node, null]` (the tuple form of `AddChange`).
- **Go**: `hydrateEntry` (engine.go:453) calls `streamNodes(...
  RowChangeAdd, node)` DIRECTLY per node — skipping the
  toAdds-into-streamChanges-into-streamNodes detour. Same result.
- **Divergence**: structural only. Go's path skips one indirection
  (streamChanges → streamNodes); since the only thing streamChanges
  adds is the type switch and the permissions guard, and hydration only
  produces ADDs, the shortcut is semantically equivalent. **BUT**:
  Go's `hydrateEntry` **skips the top-level permissions guard** at
  `streamChanges:91-93`. Today the entry's schema is the main
  pipeline's input.GetSchema() — if that schema's `System` is ever
  `"permissions"`, Go would emit while TS would skip.
- **Impact**: in practice the main-pipeline schema is never the
  permissions system schema (the permissions CSQ is always a CHILD
  schema reached via a join). So this is latent.
- **Suggested fix**: add an explicit guard in `hydrateEntry` for
  defense-in-depth:
  ```go
  if entry.schema.System == "permissions" { return nil }
  ```
  And the same for each companion at `engine.go:467`.

---

### LOW-3: Companion-row queryID tagging matches TS
- **TS**: companion pipelines wire their output to the streamer tagged
  with the MAIN queryID (`pipeline-driver.ts:2013` via the companion
  setup loop) so their rows ship under the same queryID as the parent.
- **Go**: `engine.go:439-445` —
  ```go
  for _, ce := range companions {
      ce.pipeline.Input.SetOutput(&pipelineOutput{
          engine:  e,
          queryID: queryID,   // MAIN queryID
          schema:  ce.schema,
      })
  }
  ```
  Same. Both also tag hydration-time companion rows with the main
  queryID (`engine.go:468`).
- **CHECKED-OK**.

---

### LOW-4: Edit no-recurse contract matches
- **TS**: `pipeline-driver.ts:3111-3115` —
  ```ts
  case ChangeType.EDIT:
    yield* this.#streamNodes(queryID, schema, type, () => [
      {row: change[ChangeIndex.NODE].row, relationships: {}},  // empty rels
    ]);
  ```
  Edits intentionally pass an empty `relationships` so the per-node
  loop at line 3165 (`Object.entries(relationships)`) doesn't iterate.
- **Go**: `streamer.go:100-112` — Edit case constructs the RowChange
  inline (skips streamNodes entirely) and never recurses into
  `change.Node.Relationships`.
  ```go
  case ivm.ChangeTypeEdit:
      // Edit: emit the new row only (no relationship recursion for edits)
      ...
      result = append(result, RowChange{...})  // no nested stream
  ```
- **Divergence**: Go skips streamNodes for Edit, so it ALSO skips:
  - The permissions guard at `streamer.go:135-137` (mitigated by the
    outer permissions check at `streamer.go:91-93` — single call site,
    equivalent).
  - The IsScalar guard at `streamer.go:144-146` — **subtle**. If
    `schema.IsScalar` is true for an EDIT change (the parent join
    pushes an Edit into a scalar-marked child relationship), TS won't
    emit because the resolver+AST-rewrite path means scalar children
    don't exist on TS. Go's Edit path here doesn't check IsScalar, but
    the upstream pipeline shouldn't be pushing Edit into an
    IsScalar-marked child relationship anyway (scalar resolver only
    runs on the AddQuery path, so a residual IsScalar=true child
    schema's Edits would only fire if the residual CSQ join's child
    source itself edits — at which point the streamer's Edit path
    silently emits a row TS wouldn't ship).
- **Impact**: latent over-emission for the Edit-on-IsScalar-child case.
  No test currently exercises this. Detected via shadow only if the
  CVR signature catches the extra row.
- **Suggested fix**: add the `IsScalar` and `permissions` guard to the
  Go Edit path:
  ```go
  case ivm.ChangeTypeEdit:
      if schema.System == "permissions" || schema.IsScalar {
          continue
      }
      ...
  ```
  Or — cleaner — refactor Edit to also go through `streamNodes` with
  an empty-relationships node, mirroring TS exactly:
  ```go
  case ivm.ChangeTypeEdit:
      n := ivm.Node{Row: change.Node.Row}  // no Relationships
      result = append(result, streamNodes(queryID, schema, RowChangeEdit, n)...)
  ```
  The refactor is preferred — exact TS parity and centralizes the
  guards.

---

### LOW-5: Outer `streamChanges` permissions check should be re-entrant safe
- **TS**: `#streamChanges` re-enters itself for CHILD changes at line
  3108 with the child schema. Permissions guard at line 3083 catches a
  permissions-system child. CHECKED-OK.
- **Go**: `streamChanges` re-enters itself for CHILD changes at line
  118 with the child schema. Permissions guard at line 91 catches a
  permissions-system child. CHECKED-OK.

---

### CHECKED-OK-1: ADD emits node row + recurses into relationships
- TS: `pipeline-driver.ts:3094-3100`, recursion at `pipeline-driver.ts:3165`.
- Go: `streamer.go:96-97`, recursion at `streamer.go:165-175`.
- Same emission semantics (modulo HIGH-2 ordering).

### CHECKED-OK-2: REMOVE emits `row: undefined` and recurses
- TS: `pipeline-driver.ts:3094-3100` (shared switch with ADD), row
  set to `undefined` at line 3162 via the ternary `op === REMOVE ?
  undefined : row`. Recurses identically to ADD at line 3165.
- Go: `streamer.go:98-99` (shared with ADD), row NOT set when
  `op == RowChangeRemove` at lines 160-162. Recurses identically to
  ADD at lines 165-175.
- Both match the REMOVE-row-shape parity invariant (REVIEW-final § OK-5
  / inline comment at `pipeline-driver.ts:2325-2330`). Verified Go's
  test `scalar_unresolved_emit_test.go` + the inline warning together.

### CHECKED-OK-3: CHILD recurses into relationship change, no top-level emit
- TS: `pipeline-driver.ts:3102-3109` — looks up `childSchema` from
  `schema.relationships[childData.relationshipName]`, recurses
  `#streamChanges(queryID, childSchema, [child.change])`. No row emitted
  at this level.
- Go: `streamer.go:113-121` — same logic; recurses with same args.
- The schema lookup nil-check at `streamer.go:115-117` is more
  defensive than TS's `must(...)` (which would panic), but
  functionally equivalent for valid schemas. **Mild divergence**:
  if the child schema is missing (corrupted schema), TS panics, Go
  silently drops. Same flavor as HIGH-1 but lower impact (schema
  corruption is rarer than per-row PK corruption).

### CHECKED-OK-4: `rowKey` column order matches
- Both iterate `PrimaryKey` slice in order and write into the map.
  TS uses `Object.fromEntries` (insertion-ordered Object), Go uses
  `map` (unordered). The downstream wire encoder
  (msgpack on Go side) serializes the map; the TS receiver deserializes
  to a plain Object. **rowKey comparison via `stableStringify` (line
  3275-3305) deep-sorts keys before stringifying** so map vs object
  ordering doesn't matter at the comparator. Equivalent.

### CHECKED-OK-5: Per-RowChange `type` enum mapping
- TS: `ChangeType.ADD=0`, `REMOVE=1`, `EDIT=2` (zql/ChangeType
  constants reflected at `goRowChangeToRowChange:2320-2324`).
- Go: `RowChangeAdd=0`, `RowChangeRemove=1`, `RowChangeEdit=2` at
  `streamer.go:13-17`. Match.

---

## Emission contract matrix

| Change kind | TS behavior | Go behavior | Status |
|---|---|---|---|
| **ADD** top-level | Emit node row (with `_0_version` bump if needed); recurse into each relationship in INSERTION ORDER (`Object.entries`) | Emit node row (NO `_0_version` bump); recurse into each relationship in NON-DETERMINISTIC map order | CRITICAL-1 (version), HIGH-2 (order) |
| **REMOVE** top-level | Emit RowChange with `row: undefined`; recurse into each relationship | Emit RowChange with Row field unset (msgpack omits); recurse into each relationship in non-deterministic order | CHECKED-OK on shape (OK-5 invariant), HIGH-2 on recursion order |
| **EDIT** top-level | Construct empty-relationships node, call `#streamNodes` (so version-bump + guards apply) — no recursion since rels are empty | Inline emit RowChange directly, skipping `streamNodes` entirely — NO version-bump, NO IsScalar guard, NO permissions guard (relies on outer `streamChanges` guard for permissions, has no IsScalar guard at all) | CRITICAL-1 (version), LOW-4 (missing IsScalar guard) |
| **CHILD** top-level | Recurse into `#streamChanges` with `[child.change]` and `childSchema`; no top-level row emit | Recurse into `streamChanges` with `[child.Change]` and `childSchema`; nil-checks the schema lookup | CHECKED-OK (modulo defensive nil-check tolerance) |
| **ADD/REMOVE** in nested relationship | Apply per-node `_0_version` bump, permissions guard, then emit + further recurse | Apply permissions guard + IsScalar guard, emit (no version bump), further recurse | CRITICAL-1, plus HIGH-2 ordering |
| **EDIT** in nested relationship | Same as top-level EDIT — call `#streamNodes` with empty rels | Same as top-level EDIT — inline emit, NO `streamNodes` call, NO IsScalar guard | CRITICAL-1, LOW-4 |
| **rowKey computation** | `must(row[col])` per PK column — throws on null/undefined | `node.Row[pk]` per PK column — writes nil silently | HIGH-1 |
| **Permissions skip** (top-level) | Early-return in `#streamChanges` if `schema.system === 'permissions'` | `continue` per-iteration in `streamChanges` if `schema.System == "permissions"` | CHECKED-OK (functionally equivalent) |
| **Permissions skip** (nested-recursion) | Early-return in `#streamNodes` | Early-return in `streamNodes` | CHECKED-OK |
| **IsScalar skip** | No guard needed — `resolveSimpleScalarSubqueries` rewrites AST so scalar joins don't exist post-build | Guard in `streamNodes:144` only — Edit path bypass leaves IsScalar children emittable | MEDIUM-3 (asymmetry doc), LOW-4 (Edit gap) |
| **Companion-row queryID** | Tagged with MAIN queryID via output wiring at pipeline-driver.ts:2013 | Tagged with MAIN queryID via output wiring at engine.go:442 | CHECKED-OK |
| **Hydration path** | `hydrate()`/`hydrateInternal()` → `toAdds()` → `streamer.accumulate()` → `stream()` → full ADD switch | `hydrateEntry()` → direct `streamNodes(... RowChangeAdd, node)` per node, skipping `streamChanges` shell | LOW-2 (latent permissions-guard skip) |
| **Yield tokens** | Forwarded per-change in streamer generators | Absent; re-injected every 100 changes in `goHydrateBatch` conversion, dropped entirely in `goHydrateBatchStream` | MEDIUM-4 (fairness only) |

---

## Coverage notes

- **Read end-to-end** on both sides; cross-checked the streamer against
  the existing test suites (`engine_test.go::TestStreamer`,
  `streamer_ordering_test.go` D8 invariants, `scalar_unresolved_emit_test.go`).
- The OK-5 / REMOVE-row-shape invariant is documented in
  `REVIEW-final.md:146` and the inline guard comment at
  `pipeline-driver.ts:2325-2330`. Re-verified both paths match.
- Per-row transforms checked exhaustively:
  - **`_0_version` bump**: only TS path applies; Go path absent. CRITICAL-1.
  - **Permissions filter**: applied on both sides at top and nested
    recursion. CHECKED-OK with MEDIUM-2 cosmetic.
  - **IsScalar skip**: applied only on Go (TS handles upstream in AST
    rewrite). Edit path bypass = LOW-4.
- **rowKey computation** has a real divergence on null-PK handling
  (HIGH-1) that no soak has hit yet because no source has produced a
  null-PK row in tests.
- **Iteration determinism**:
  - Verified `Object.entries` is insertion-order for plain objects
    (ECMAScript spec).
  - Verified Go map iteration is randomized (Go spec, runtime hash seed).
  - `materializeNode` (engine.go:1042) is a hidden additional
    relationship-map-rebuild that erases any insertion order even if
    the upstream operator chain produced one.
- **Edge cases verified**:
  - Companion rows during hydration: chunking respects `hydrateChunkSize`
    after companion rows are appended (engine.go:601-611). Matches TS's
    queryID-tagging.
  - Streaming hydrate (`AddQueriesStream`): per-query chunking with
    `Final=true` on last frame; verified against TS streaming
    accumulator at `go-ivm-client.ts:154`.

---

## Not checked

- **MessagePack encoding fidelity** of `RowChange.Row` — handled
  separately in PORT_CORRECTNESS_ANALYSIS / T1-1 typed-Row decode.
  This audit assumes the wire correctly round-trips `ivm.Row` values
  to TS `Row` values.
- **Per-relationship row order WITHIN a single relationship's child
  array** (`childFn()` at streamer.go:171). The order is whatever the
  upstream operator (Join, FlippedJoin) produced; both TS and Go should
  walk these in the same order if the operator is correctly ported.
  This audit checked the streamer's own iteration order; per-operator
  output ordering is a separate audit surface.
- **Drift-audit Sql-ground-truth comparison** path
  (pipeline-driver.ts:1602ff) — uses its own `stableStringify`-based
  comparator that already accounts for `_0_version` differences? Not
  verified — recommend a follow-up: if the SQL ground-truth comparator
  ignores `_0_version`, the drift audit will MISS the CRITICAL-1
  divergence even after it surfaces.
- **`_0_version` interaction with mutations / put-write delta** — the
  TS bump only applies to streamer emissions; pg→sqlite replication
  writes `_0_version` directly. Whether `loadRows` into Go preserves
  the latest `_0_version` is out of streamer scope but is the supply
  side of the CRITICAL-1 problem.
- **Internal-table EDIT path** through the merged path
  `yieldMerged` (pipeline-driver.ts:2306) — assumes TS-internal +
  Go-user disjoint table partition. Disjointness is enforced
  upstream (Go never loads internal tables, TS stubs user tables) but
  was not re-validated here.
- **Order across queryIDs / pipelines** within a single advance batch
  (Streamer accumulates across pipelines) — the D8 contract notes
  this is non-deterministic under GenPushParallel; documented in
  `streamer.go:31-41`. Not re-audited; the contract is explicit.
