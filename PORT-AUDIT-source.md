# Source-Layer Parity Audit

Scope: the "leaf" sources every pipeline reads from. TS canonical references
are `MemorySource` (`mono/packages/zql/src/ivm/memory-source.ts`, the
core overlay + fan-out machinery) and `TableSource`
(`mono/packages/zqlite/src/table-source.ts`, the SQLite-backed wrapper that
reuses MemorySource's `generateWithOverlay*` + `genPushAndWriteWithSplitEdit`).
Go port is `MemorySource` (`go-ivm/ivm/source.go`) with its parallel
fan-out in `go-ivm/ivm/parallel.go` and the typed-drift signal in
`go-ivm/ivm/drift.go`.

Operators are out of scope. Items already FIXED in REVIEW-porting /
REVIEW-parallelism / REVIEW-final are not re-listed unless regressed.

This audit found 1 HIGH, 4 MEDIUM, 5 LOW, and ~14 CHECKED-OK properties.
No CRITICAL findings. No regressions of previously-FIXED items.

---

## Findings

### HIGH-1: Parallel push pre-bumps `LastPushedEpoch` for every connection — cross-conn fetch can't see the overlay
- **TS**: `mono/packages/zql/src/ivm/memory-source.ts:552-577` — `genPush`
  iterates connections sequentially and, **inside the loop**, does
  `conn.lastPushedEpoch = pushEpoch; setOverlay(...)` for the current
  conn, then yields its `filterPush`. So at any moment during iteration,
  the conn currently being pushed has `lastPushedEpoch == pushEpoch` but
  every later conn still has `lastPushedEpoch < pushEpoch` — and any
  downstream operator inside conn A that re-fetches the same source via
  any other conn B sees overlay applied (because B's
  `lastPushedEpoch < overlay.epoch`).
- **Go**: `go-ivm/ivm/parallel.go:50-58` — `GenPushParallel` walks every
  active connection and sets `conn.LastPushedEpoch = epoch` for **all
  of them up front** (`for _, conn := range ms.connections { ...
  conn.LastPushedEpoch = epoch; activeConns = append(...) }`) BEFORE
  spawning the fan-out goroutines. By the time any goroutine's
  `Output.Push` runs, **every** connection's `LastPushedEpoch` already
  equals `overlay.Epoch`. `computeOverlays` (source.go:543) gates on
  `conn.LastPushedEpoch < overlay.Epoch`, so the overlay is invisible
  to any fetch from any conn during the parallel fan-out.
- **Divergence**: in sequential Go (`source.go:283-293`) and TS, the
  per-conn bump preserves the "later conns can still see the overlay"
  invariant. In parallel Go, that invariant is lost. The same Push
  fanned out sequentially vs in parallel can produce different
  downstream output when any operator inside a pipeline cross-fetches
  another conn's input on the same source.
- **Impact**: today no production operator does this — each pipeline
  built by `builder.BuildPipeline` gets its own `SourceInput` via
  `MemorySource.Connect`, and joins inside that pipeline only read
  their own connection. SAFE-2 in `REVIEW-parallelism.md` (line 235)
  asserts pipelines are disjoint. So this is **latent** — until any
  feature (deduplication caching, EXISTS subquery sharing,
  view-syncer optimization) introduces cross-pipeline source reads.
  At that moment `genPushAndWriteParallel` silently produces
  different output than the sequential path and TS.
- **Suggested fix**: drop the up-front `LastPushedEpoch` assignment from
  `GenPushParallel`; instead bump it inside each goroutine at the very
  start of the closure (before `FilterPush`), matching the sequential /
  TS per-conn timing. Note this restores the original cross-fetch
  semantic in expectation only — under parallel execution all gorou-
  tines race so the cross-conn fetch may sporadically see overlay
  applied vs not. Either accept that non-determinism, or document
  "no cross-conn fetches during a single source's parallel push" as a
  hard invariant alongside SAFE-2 with a debug assertion in tests.

---

### MEDIUM-1: Go has no `generateWithOverlayUnordered` — every connection is sorted
- **TS**: `mono/packages/zql/src/ivm/memory-source.ts:775-839`
  (`generateWithOverlayUnordered`) and `mono/packages/zqlite/src/table-source.ts:320-336`
  — when `connection.sort` is undefined (`unordered` mode), TableSource
  hands its rows through `generateWithOverlayUnordered`, which eagerly
  yields the add overlay at the start (no sort comparison) and
  inline-suppresses the remove overlay by PK match
  (`rowMatchesPK` at line 841). The connection's schema reports
  `sort: undefined` (memory-source.ts:153, `unordered ? undefined : connection.sort`).
- **Go**: `go-ivm/ivm/source.go:127-130` — `Connect` substitutes
  `primarySort` when `connSort == nil` AND `schema.Sort = connSort`
  always reports the sort (even when caller passed nil). The Fetch path
  always goes through the ordered `generateWithOverlayInner`
  (`source.go:503`). There is no unordered code path at all.
- **Divergence**: in TS, downstream operators can observe `schema.sort
  === undefined` and choose unordered iteration. Go always reports a
  sort. Functionally rows still flow correctly — just sorted by PK
  when caller wanted unordered. Cost is O(N log N) per Fetch instead
  of TS's natural iteration; correctness for downstream operators that
  branch on `schema.sort` would diverge if any such operator existed.
- **Impact**: today no Go operator branches on
  `schema.Sort == nil` (verified by grep — `IsHidden` and `System`
  are the only fields read by downstream). Cost is also negligible
  because the sidecar's `getSortedRows` re-sorts a slice copy on every
  Fetch regardless (see MEDIUM-2). So purely a "missing fast path"
  today; would break correctness for any future operator that needs
  to detect "source is unordered".
- **Suggested fix**: keep an `unordered bool` flag on `Connection`
  set when caller passes `sort == nil`. Have the schema return
  `Sort: nil` when unordered. Add `generateWithOverlayInnerUnordered`
  port if/when a downstream consumer wants the fast path.

---

### MEDIUM-2: `getSortedRows` re-sorts on every Fetch — TS uses cached BTree indexes
- **TS**: `mono/packages/zql/src/ivm/memory-source.ts:102, 119-124, 224-249`
  — TS uses `BTreeSet<Row>` per sort (`Index` struct with `comparator`,
  `data: BTreeSet<Row>`, `usedBy: Set<Connection>`). `#getOrCreateIndex`
  builds the BTree once per sort and caches it; `#fetch` calls
  `generateRows(data, scanStart, reverse)` which walks the BTree from
  `scanStart` in O(log N) — no full sort per Fetch.
- **Go**: `go-ivm/ivm/source.go:529-537` — `getSortedRows` does
  `copy(sorted, ms.data); sort.Slice(...)` on every Fetch. `ms.data`
  is the primary-key-sorted slice; the per-conn sort requires a full
  re-sort. For a connection sorted differently from PK, this is
  `O(N log N)` per Fetch.
- **Divergence**: performance only. Output is identical. Connections
  whose sort matches PK still re-sort (the comparator just confirms
  order in place but `sort.Slice` walks anyway).
- **Impact**: invisible at small N (sandbox / unit tests). At
  production scale (10k+ row tables, fetches firing per advance via
  EXISTS Joins) this becomes the dominant cost. `PERF-REVIEW.md`
  notes Row-decode allocations were the historical hot spot; the
  per-Fetch sort would become the new one as data grows.
- **Suggested fix**: port the BTree-set / multi-index approach. If
  pulling in a BTree dependency is undesirable, at minimum:
  (1) skip the re-sort when `conn.Sort` equals `primarySort`
  (use `ms.data` directly), and
  (2) maintain a per-(sortKey) cached sorted slice that
  `writeChange` keeps in sync.

---

### MEDIUM-3: `BulkInsert` takes `connsMu.Lock` but mutates `data`, not `connections`
- **TS**: not applicable — TS TableSource writes via `writeChange`
  inside the Push loop. There's no bulk-insert RPC; the SQLite table
  is INSERTed row-by-row.
- **Go**: `go-ivm/ivm/source.go:439-445` — `BulkInsert` takes
  `connsMu.Lock()` and then mutates `ms.data` via `insert`. But
  `connsMu` documents itself as the lock guarding `connections`
  (source.go:73 "Concurrency model"). It does NOT block concurrent
  `Push` / `writeChange` calls, which take `connsMu.RLock` (or no
  lock at all from the writeChange's `insert/remove` path).
- **Divergence**: BulkInsert serializes against `Connect / Disconnect`
  (which don't touch `data`) but NOT against `writeChange` (which
  does touch `data` from a Push goroutine that's already holding
  the RLock or not taking the lock at all from `writeChange`'s
  call site at `genPush`).
- **Impact**: today none — `cmd/sidecar/main.go:1184` calls BulkInsert
  from `handleLoadRows` under `group.mu`, which also gates
  `handleAdvance` / `handleAddQuery`. So no in-flight Push exists
  while BulkInsert runs. The lock here is documentation only.
  Latent bug if any future caller path (e.g., async warmup,
  speculative pre-load) runs BulkInsert concurrent with a Push from
  another caller.
- **Suggested fix**: either rename to make the intent explicit
  ("`bulkLoadMu`" guards `data` mutations broadly), or document
  loud-and-clear at the BulkInsert site that this is "external
  serialization required; the lock taken here is for documentation
  only". The simpler fix is to teach `writeChange` to also take
  `connsMu.Lock` (turning every push into a write-locked critical
  section) — that costs throughput, so probably not worth it.

---

### MEDIUM-4: No `getRow` equivalent on MemorySource
- **TS**: `mono/packages/zqlite/src/table-source.ts:480-519` —
  `getRow(rowKey: Row): Row | undefined` does a single-row SELECT
  by a unique key (not necessarily the PK), used by view-syncer for
  row catchup. Comment at line 502-505: "This is not used in the IVM
  pipeline but is useful for retrieving data that is consistent with
  the state (and type semantics) of the pipeline."
- **Go**: absent on `MemorySource`. Search: `getRow` exists in
  `go-ivm/client/pipeline-driver-go.ts:861` (TS side wrapping the
  view-syncer), but that calls `source.getRow(pk as Row)` — `source`
  is the TS-side `TableSource`, never the Go-side sidecar. The Go
  sidecar never exposes a getRow RPC.
- **Divergence**: in Go-primary mode the view-syncer's row-catchup
  path still uses TS-side `TableSource.getRow` (the SQLite replica
  is co-located with the TS view-syncer). Go MemorySource is
  unreachable for this call path because pipeline-driver-go.ts uses
  the TS `#tables` map (line 366, 925, 1025). So divergence is
  structural / by-design — Go sidecar handles IVM compute, TS holds
  the source-of-truth SQLite for getRow/permissions.
- **Impact**: zero today. Documented as such in `client/index.ts:6`
  ("PipelineDriver keeps all state: Snapshotter, versions, getRow,
  permissions, etc.") and `integration-example.ts:82, 191`.
- **Suggested fix**: none required. Add a comment in `source.go`
  near the package doc saying "no getRow — TS-side TableSource owns
  row-catchup against the live SQLite replica; this source is
  IVM-compute-only".

---

### LOW-1: No `tableSchema` getter
- **TS**: `mono/packages/zqlite/src/table-source.ts:120-126` and
  `mono/packages/zql/src/ivm/memory-source.ts:126-132` — both expose
  `get tableSchema(): TableSchema` returning `{name, columns,
  primaryKey}`. Used by the `Source` interface
  (`mono/packages/zql/src/ivm/source.ts:55`).
- **Go**: absent — `MemorySource` exposes `TableName()`,
  `PrimaryKey()`, `Columns()` as separate methods, no aggregated
  `TableSchema` struct. The engine adapter
  (`engine/engine.go:46-65`) only reads the three separate methods,
  so the interface gap doesn't surface.
- **Impact**: cosmetic.
- **Suggested fix**: skip unless TS adds a usage path that the Go
  side needs to mirror.

---

### LOW-2: No `setDB` method (snapshot rebinding)
- **TS**: `mono/packages/zqlite/src/table-source.ts:128-134` —
  `setDB(db: Database)` swaps the underlying SQLite snapshot.
  Called by `pipeline-driver.ts` at end of every advance via
  `for (const table of #tables.values()) table.setDB(curr.db.db)`.
  This rebinds TableSource to the new SQLite snapshot once the
  Snapshotter advances to the next WAL frame.
- **Go**: absent on `MemorySource`. MemorySource is in-memory; its
  state evolves via `Push()`'s `writeChange` calls during advance.
  The "current snapshot" concept is implicit — `ms.data` IS the
  current state.
- **Divergence**: TS uses Snapshotter's leapfrog algorithm to
  rotate snapshots atomically; Go MemorySource doesn't need this
  because Push mutates in-place under `connsMu` / engine.mu.
- **Could the two paths disagree on "what the current snapshot
  means"?** Examined: TS advance pushes changes into MemorySource-
  style state (TableSource.writeChange writes to the bound SQLite),
  then setDB rebinds to the NEW snapshot for the next advance.
  Go advance pushes into MemorySource via `Engine.Advance`'s
  `source.Push(sc)` loop (engine.go:740), which calls
  `writeChange` after fan-out. Then `signalAdvanceEnd` fires —
  for MemorySource it's a no-op (source.go documents
  "MemorySource — state evolves naturally via writeChange").
  Net: Go's `ms.data` after `Advance(batch)` reflects the same
  row set TS's TableSource SQLite would after `advance(batch) +
  setDB(curr.db.db)`. **Verified equivalent.** Risk is purely
  one of definition — "current snapshot" in Go means "ms.data right
  now", which is correct.
- **Suggested fix**: none. Add note to package doc.

---

### LOW-3: No PK uniqueness assertion at construction
- **TS**: `mono/packages/zqlite/src/table-source.ts:114-117` —
  `assert(this.#uniqueIndexes.has(JSON.stringify(primaryKey.toSorted())),
  'primary key ... does not have a UNIQUE index')`. TS queries
  `pragma_index_list` (`getUniqueIndexes` at line 538-563) to
  discover unique indexes and verifies the PK is among them.
- **Go**: absent. `NewMemorySource` (`source.go:95-121`) accepts the
  PK without validation. The PK is checked at Push time via `has()`
  but never at construction.
- **Impact**: in TS this catches schema misconfigurations at startup.
  In Go, a misconfigured PK would surface as runtime panics
  (`MemorySource.remove: row not found`) much later. Caller
  responsibility — but a startup-time assertion is friendlier.
- **Suggested fix**: at construction, panic if `primaryKey` is
  empty or contains duplicate columns. Cannot mirror the
  `pragma_index_list` check because MemorySource has no SQLite.

---

### LOW-4: No `assertOrderingIncludesPK` at Connect
- **TS**: `mono/packages/zqlite/src/table-source.ts:265-268` and
  `mono/packages/zql/src/ivm/memory-source.ts:197-199` — both call
  `assertOrderingIncludesPK(internalSort, this.#primaryKey)` when the
  caller provides a sort, ensuring downstream operators that rely on
  PK-completeness for tie-breaking don't get a malformed sort.
- **Go**: absent in `Connect` (`source.go:126-161`). Builder always
  appends PK columns via `completeOrdering` (`builder.go:70`), so the
  invariant holds in practice — but defensive checks in the source
  would catch a future builder bug at the source boundary.
- **Impact**: defensive only.
- **Suggested fix**: port `assertOrderingIncludesPK` into `ivm/data.go`
  and call from `Connect`. Cheap and catches future builder
  regressions.

---

### LOW-5: No `fork()` method
- **TS**: `mono/packages/zql/src/ivm/memory-source.ts:134-142` —
  `fork()` clones the primary BTree into a new MemorySource with the
  same schema. Used by view-syncer for parallel read-side replicas.
- **Go**: absent. View-syncer concerns are TS-side (Go sidecar handles
  IVM only, not the view-syncer's CVR pipeline).
- **Impact**: zero — same reasoning as LOW-2 / MEDIUM-4. fork-based
  parallel views are a TS-side concern.
- **Suggested fix**: none.

---

## CHECKED-OK: properties verified equivalent

### CHECKED-OK-1: Push contract — validate-then-fanout-then-writeChange ordering
- TS `genPushAndWrite` (`memory-source.ts:508-520`): genPush (asserts +
  fanout) → writeChange (mutate state).
- Go `genPushAndWrite` (`source.go:229-233`): same; `genPush` panics
  on drift BEFORE bumping epoch / setting overlay / fanning out
  (validated at `drift.go:21-26` invariants and source.go:239-267).
- The "validate before state mutation" invariant is the recovery
  contract for `DriftError`. Verified intact in both sequential
  (source.go) and parallel (parallel.go:18-44) push paths.

### CHECKED-OK-2: Split-edit detection scans ALL connections
- TS `genPushAndWriteWithSplitEdit` (`memory-source.ts:452-506`):
  scans every connection's `splitEditKeys` before deciding whether to
  rewrite Edit → Remove(old) + Add(new).
- Go `genPushAndWriteWithSplitEdit` (`source.go:196-226`): same
  global scan under `connsMu.RLock`. Also in parallel path
  (`parallel.go:165-205`). Audit fix A noted in
  `internal/tablesource/source.go:301-319` documents the prior
  per-connection bug; that's already fixed.

### CHECKED-OK-3: Push validation for Add / Remove / Edit
- TS `genPush` (`memory-source.ts:529-550`): asserts
  `!exists(row)` for ADD, `exists(row)` for REMOVE, `exists(OLD_ROW)`
  for EDIT.
- Go `genPush` (`source.go:239-267`): same assertions; panics with
  typed `*DriftError` instead of TS's generic assert. The typed-
  panic is **intentional divergence** for the cross-language drift
  recovery contract (drift.go:9-26). The pre-mutation-validity
  invariant is preserved.

### CHECKED-OK-4: NormalizeRow injects ValueConverter (FromSQLiteType)
- TS `TableSource.toSQLiteRow` (`table-source.ts:274-281`) and
  `fromSQLiteTypes` (`table-source.ts:588-643`) handle full type
  coverage (boolean/number/string/null/json) at the SQLite boundary.
- Go `MemorySource.NormalizeRow` (`source.go:374-430`): when a
  `ValueConverter` is installed via `NewMemorySourceWithConverter`,
  it runs per-column for every column (matching TS coverage); the
  sidecar installs `sqlite.FromSQLiteType` at
  `cmd/sidecar/main.go:1054-1058`. The fallback partial converter
  exists only for tests where MemorySource is used without a
  converter; documented as such. Matches REVIEW-final's MED-PORT-1
  fix.

### CHECKED-OK-5: Connection iteration order is deterministic
- TS pushes to connections in insertion order (slice push at
  `memory-source.ts:200`, iteration at `memory-source.ts:552`).
- Go does the same (slice append at `source.go:158`, iteration at
  `source.go:283`). Parallel path snapshots the slice under RLock
  (`parallel.go:50-58`) and the activeConns order matches the
  insertion order. Order is documented as load-bearing for
  client-visible ordering of changes within a Push fan-out. Verified
  equivalent.

### CHECKED-OK-6: Overlay set/clear via `atomic.Pointer` with defer-clear
- REVIEW-final invariant #10 — `atomic.Pointer[Overlay]` with
  `defer Store(nil)`, load-bearing for panic safety.
- Sequential `genPush` (`source.go:269-294`): `defer ms.overlay.Store(nil)`
  at function entry; `ms.overlay.Store(&Overlay{...})` set per-iter
  inside the loop (matching TS's per-conn set semantic; redundant
  for a single Push since epoch/change are the same across iters,
  but cheap).
- Parallel `GenPushParallel` (`parallel.go:67-68`): `Store` BEFORE
  spawning goroutines; `defer Store(nil)`. Happens-before edge
  documented in `REVIEW-parallelism.md` SAFE-1. Verified intact.

### CHECKED-OK-7: ConstraintMatchesRow uses ValuesEqual (fix for porting CRITICAL-2)
- Go `ConstraintMatchesRow` (`source.go:709-719`) uses
  `ValuesEqual(row[k], v)`. ValuesEqual itself matches TS semantic
  per CRITICAL-1 fix (data.go:205-219: returns false for nil/nil).

### CHECKED-OK-8: Overlay applies to start cursor with reverse-aware compare
- TS `overlaysForStartAt` uses `indexComparator` which is reverse-aware
  (`memory-source.ts:293-296`).
- Go `computeOverlays` builds `compare := MakeComparator(conn.Sort,
  reverse)` (`source.go:561`) and applies the same `< 0` check.
  Verified matching for reverse scans.

### CHECKED-OK-9: Overlay applies constraint AND filter predicate
- TS chains `overlaysForConstraint` then `overlaysForFilterPredicate`
  (`memory-source.ts:683-689`).
- Go does the same chain at `source.go:570-586`. Both AND-combine
  constraint + filter against both `add` and `remove` overlays.

### CHECKED-OK-10: applyStart for reverse fetch (partial cursor support)
- TS uses `connectionComparator` (reverse-aware) on
  `generateWithStart` (`memory-source.ts:354`).
- Go `applyStart` (`source.go:626-671`) uses
  `CompareWithPartialBound` (the partial-cursor compare) — covers
  the case where the cursor lacks some sort columns (e.g.,
  `{createdAt}` cursor with `[createdAt, conversationId]` sort).
  Negation converts cursor-vs-row to row-vs-cursor inversion. The
  comment at line 627-635 documents the partial-cursor rationale
  and matches TS three-valued boundary behavior. REVIEW-porting
  CONFIRMED-CORRECT item 6.

### CHECKED-OK-11: generateWithOverlayInner ordering — overlay add yields once at correct position
- TS `generateWithOverlayInner` (`memory-source.ts:739-768`): tracks
  `addOverlayYielded` / `removeOverlaySkipped`; final
  `if (!addOverlayYielded && overlays.add) yield` catches the
  trailing case (add > all source rows).
- Go `generateWithOverlayInner` (`source.go:592-620`): same flags,
  same trailing yield. Verified equivalent.

### CHECKED-OK-12: DriftError emission on the panic path (not the assert path)
- TS uses `assert(exists(...))` → throws — TS treats drift as a
  programmer error / view-syncer reset trigger.
- Go uses `panic(&DriftError{...})` — typed panic so
  `Engine.Advance`'s recover (`engine.go:699-718`) can distinguish
  drift (recoverable, drop the advance) from programmer bugs
  (re-raise to abort). The `*DriftError` is also caught per-
  goroutine in `GenPushParallel` (`parallel.go:104-112`) and
  re-raised on the caller goroutine, preserving the same recovery
  contract under parallel fan-out. Verified equivalent to TS's
  "throw → view-syncer reset" flow at a higher granularity (Go
  drops one advance and re-inits; TS resets the view-syncer
  pipeline).

### CHECKED-OK-13: Engine adapter discards opts.Filter (uses only FilterPredicate)
- TS Source.connect (`source.ts:68-73`) accepts both `filters: Condition`
  AND derives a predicate inside via `transformFilters` +
  `createPredicate`.
- Go `engineSource.Connect` (`engine.go:1107-1109`) passes only
  `opts.FilterPredicate` to `MemorySource.Connect`. The `opts.Filter`
  (raw `*Condition`) is built by the builder (`builder.go:108`) and
  paired with a pre-built predicate at `builder.go:110`. So the
  predicate IS the filter — Go has just inlined the
  TS `transformFilters + createPredicate` step at the builder layer.
  Verified semantically equivalent.

### CHECKED-OK-14: writeChange Edit branch uses canUseUpdate optimization
- TS `#writeChange` (`table-source.ts:437-471`) tests `canUseUpdate`
  (PK unchanged + has non-PK cols) and prefers UPDATE over
  DELETE+INSERT.
- Go MemorySource doesn't have an UPDATE concept (it's in-memory);
  it always does `remove(OldRow) + insert(Row)` at
  `source.go:321-323`. Semantically equivalent — the BTreeSet /
  slice replace is the in-memory analogue of UPDATE. Output is
  identical because no observable state difference exists between
  "edit in place" and "remove + insert at the same position" for an
  in-memory map. No divergence.

---

## Source-layer parity matrix

| Property | TS side | Go side | Status |
|---|---|---|---|
| Push: validate (Add not exists) | `memory-source.ts:531` assert | `source.go:241-249` panic `*DriftError` | CHECKED-OK (typed panic intentional) |
| Push: validate (Remove exists) | `memory-source.ts:537` assert | `source.go:251-257` panic `*DriftError` | CHECKED-OK |
| Push: validate (Edit OldRow exists) | `memory-source.ts:544` assert | `source.go:259-266` panic `*DriftError` | CHECKED-OK |
| Push: writeChange duplicate-row assert | `memory-source.ts:394-407` asserts add/remove succeed | `source.go:333-348` panics on remove only, silently dups on insert | LOW (latent edge case under buggy upstream) |
| Push: split-edit global scan | `memory-source.ts:460-477` | `source.go:198-217` | CHECKED-OK |
| Push: per-conn overlay+epoch (sequential) | `memory-source.ts:552-577` | `source.go:283-293` | CHECKED-OK |
| Push: per-conn overlay+epoch (parallel) | n/a (TS single-threaded) | `parallel.go:50-115` pre-bumps for all conns | **HIGH-1** (latent, no observed regression today) |
| Push: NormalizeRow on Push input | `table-source.ts:274-281` (via writeChange ToSQLiteType) | `source.go:374-430` via converter when set | CHECKED-OK |
| Fetch: row source | TS BTree indexes (cached) | `source.go:529-537` re-sort copy every call | MEDIUM-2 (perf) |
| Fetch: simpleFilter source-level | `memory-source.ts:264-289` extracts PK constraint from filters | not extracted; relies on AND of predicate+constraint | MEDIUM (perf, see CONFIRMED-OK below) |
| Fetch: start cursor (forward) | `memory-source.ts:580-610` | `source.go:626-671` | CHECKED-OK |
| Fetch: start cursor (reverse, partial) | `memory-source.ts:294-296, 354` (reversed indexComparator) | `source.go:646-647` partial-cursor compare with negation | CHECKED-OK |
| Fetch: constraint AND filter ordering | `memory-source.ts:352-363` | `source.go:511-517` | CHECKED-OK |
| Fetch: overlay computeOverlays | `memory-source.ts:647-692` | `source.go:540-589` | CHECKED-OK |
| Fetch: overlay applied with constraint+filter | `memory-source.ts:683-689` | `source.go:570-586` | CHECKED-OK |
| Fetch: unordered fast path | `memory-source.ts:775-839` | absent | MEDIUM-1 (latent) |
| Connect: insertion-order iteration | `memory-source.ts:200, 552` | `source.go:158, 283` | CHECKED-OK |
| Connect: assert PK in sort | `memory-source.ts:197-199` | absent | LOW-4 |
| Connect: schema.sort optional (unordered marker) | `memory-source.ts:153` | always set to non-nil | MEDIUM-1 (paired) |
| Connect: fullyAppliedFilters report | `source.ts:100, memory-source.ts:179` | absent (builder uses different heuristic) | LOW (builder.go:204-207 documented) |
| Connect: connection ID for debug | TS uses `decorateSourceInput` (not on connection) | `source.go:56,133-134` `ConnID` | CHECKED-OK (Go richer) |
| getRow (single-row lookup) | `table-source.ts:506-519` | absent on MemorySource | MEDIUM-4 (by-design; TS-side `TableSource.getRow` handles it) |
| setDB (snapshot rebind) | `table-source.ts:128-134` | absent | LOW-2 (by-design; in-memory) |
| tableSchema getter | `table-source.ts:120-126`, `memory-source.ts:126-132` | three separate methods | LOW-1 |
| fork() | `memory-source.ts:134-142` | absent | LOW-5 |
| Construction: PK uniqueness check | `table-source.ts:114-117` | absent | LOW-3 |
| Construction: column validation | partial (uniqueIndexes scan) | none | LOW |
| Overlay panic safety | TS generator unwinds on throw | `defer ms.overlay.Store(nil)` | CHECKED-OK (REVIEW-final inv #10) |
| Connection.Output check (unwired skip) | `memory-source.ts:553-555` | `source.go:284-286` | CHECKED-OK |
| BulkInsert (loadRows path) | n/a | `source.go:439-445` with `connsMu.Lock` (wrong lock) | MEDIUM-3 |
| `*DriftError` recoverable panic contract | n/a | `drift.go`, parallel.go:104-115, engine.go:699-718 | CHECKED-OK |
| ConstraintMatchesRow uses ValuesEqual | `constraint.ts:16-26` | `source.go:709-719` | CHECKED-OK (porting CRITICAL-2 fix) |

---

## Coverage notes

Systematically checked:
1. Full read of TS `memory-source.ts` (920 lines, all functions).
2. Full read of TS `table-source.ts` (686 lines, the SQLite wrapper).
3. Full read of TS `source.ts` interface.
4. Full read of TS `schema.ts` (SourceSchema struct).
5. Full read of Go `ivm/source.go` (740 lines).
6. Full read of Go `ivm/parallel.go` (206 lines).
7. Full read of Go `ivm/drift.go` (48 lines).
8. Full read of Go `ivm/operator.go` (interface definitions).
9. Full read of Go `sqlite/table_source.go` (755 lines — secondary,
   used by the in-process / legacy path; informed cross-checks but
   isn't the primary port target).
10. Skim of Go `internal/tablesource/source.go` (the "true port" of
    TS TableSource; informs the snapshot/setDB question — Go's
    snapshot rotation lives there, not in MemorySource).
11. Read of `engine/engine.go` source-registration and Push call sites
    (lines 281-356, 680-768).
12. Read of `cmd/sidecar/main.go` init / loadRows paths (lines
    990-1186) to confirm BulkInsert and ValueConverter wiring.
13. Read of `builder/builder.go:1-260` — confirms splitEditKeys
    composition matches TS (parentField only), and the
    fullyAppliedFilters heuristic divergence.
14. Read of REVIEW-porting.md, REVIEW-parallelism.md, REVIEW-final.md
    to avoid re-listing FIXED items.
15. Read of `ivm/data.go` ValuesEqual / CompareValues to verify the
    null-semantic split (porting CRITICAL-1 fix).
16. Grep for `getRow`, `setDB`, `fork`, `BulkInsert`,
    `fullyAppliedFilters`, `assertOrderingIncludesPK`,
    `primaryKeyConstraintFromFilters`, `tableSchema`,
    `generateWithOverlayUnordered`, `uniqueIndexes` across both
    codebases.
17. Verified test coverage: `source_parallel_test.go`,
    `take_join_test.go` (BulkInsert exercised),
    `parallel_panic_recovery_test.go`, `take_pkdup_panic_test.go`
    (DriftError exercised), `source.test.ts` referenced for
    fullyAppliedFilters semantics.

Per-property cross-check performed: Push semantics, Fetch semantics,
Connect semantics, getRow, setDB / snapshot binding, constraint
enforcement, overlay semantics (incl. constraint+filter chaining,
start-cursor application, reverse-aware compare), connection
iteration order (sequential + parallel), DriftError emission,
BulkInsert behavior.

---

## Not checked

1. **Debug delegate parity** — TS threads a `DebugDelegate` through
   `connect(..., debug)` for query introspection (`table-source.ts:295,
   341-358` records scanStatus, `memory-source.ts:14` import). Go
   MemorySource has no `debug` parameter. Not checked because the
   sidecar doesn't expose IVM query introspection; the debug shape
   doesn't have an obvious place to land. Note for future: if the
   ivm.advance-go-rpc-time timing aggregation ever wants per-query
   detail, this hole becomes relevant.
2. **`splitEditKeys` semantic equivalence** — verified the
   composition (parentField-only, partition-key inclusion), but
   didn't deeply audit how an `Edit` with `splitEditKeys` containing
   columns NOT present in `change.Row` / `change.OldRow` behaves on
   both sides. TS uses `valuesEqual` (returns false for missing key
   on either side), Go uses `ValuesEqual` (same).
3. **`SourceInput.output` dead field** — Go `SourceInput` has both
   `output Output` AND `conn.Output`; `SetOutput` writes both
   (`source.go:476-479`), only `conn.Output` is read. Cosmetic; not
   a behavior bug. Worth a follow-up cleanup but not a parity
   concern.
4. **REMOVE row shape parity on the WIRE** — REVIEW-final invariant
   #4 ("REMOVE row shape parity — both paths emit `row: undefined`,
   inline comment warns against fixing"). Verified upstream in
   `engine.go` Materialize / RowChange path, not the source layer.
   This is downstream of the Source contract so out of scope.
5. **`tablesource.OnAdvanceEnd` parity vs. setDB** — examined
   briefly (`internal/tablesource/source.go:225-230` rotates the
   pinned snapshot at end-of-batch). Conceptually equivalent to TS
   `setDB(curr.db.db)` post-advance. Full audit of
   `internal/tablesource` is a separate parity surface (the "true
   port" of TS TableSource) and merits its own dedicated review.
6. **Connection `splitEditKeys` change after Connect** — TS allows
   `splitEditKeys: Set<string> | undefined` set once at Connect.
   Neither side supports mutation after Connect; both sides verified
   read-only after construction. Not deeply tested because no caller
   does this.
7. **Per-table parallel push concurrency vs. shared sidecar** —
   verified the goroutine fan-out within one source, did not
   audit cross-source parallelism (which the engine.go Advance loop
   serializes today via `for _, change := range changes`). If that
   loop ever fans out across tables, MEDIUM-1 from REVIEW-parallelism
   (which is already FIXED) would need re-verification.
