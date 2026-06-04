# DESIGN: Snapshotter-in-Go — line-by-line port of TS's `Snapshotter`

Status: **P0 + P1 + P2a/P2b DONE & SOAK-VALIDATED.** `internal/snapshotter` (13
fixture tests), `advanceToHead` RPC (Go + mono), Go-vs-TS `[go-diff-shadow]`
compare, AND frame-coordinated DRIVE mode (Go derives its own diff and drives its
own engine). **Soaks PASSED (2026-06-04, rust-test, 8u/183s/mutate):** P1 → 24
identical diffs / 0 mismatches; P2 drive → 530 `[shadow]` matches / **0 advance
mismatches** / 0 panics / 0 `database is locked` / 0 resets (2 rare hydrate
misses to chase). The hard frame-coordination problem the spike crashed on is
solved. **Remaining: P2c** (CVR version authority — stamp the user-query CVR at
Go's version for real Go-primary serving) then **P3** (lean primary). See §7.
Authored 2026-06-04.
Companion to [`DESIGN-tablesource-port.md`](./DESIGN-tablesource-port.md) and the
frame-timing deep-dive in [`PORT-AUDIT-FIXES.md`](./PORT-AUDIT-FIXES.md).

---

## 0. Why this exists (the goal, restated)

The **main goal** of the Go sidecar is *performance via parallelism*, with the
endgame being **lean Go-primary in production** — TS off the user-query hot
path entirely, Go serving alone, fanned out across goroutines.

The blocker today is the **frame-timing drift residual**: Go does not own a
snapshot. It pins a single `prev` tx at *current head* (a different instant
than TS pinned), and consumes a diff TS computed against TS's frame. Two
processes reading "head" at different instants cannot agree on a version, so
Go drifts (`driftCheckLocked` fires, Take stale-bounds, "Go produced 0").

Porting the Snapshotter into Go makes Go **self-consistent**: it derives its
own diff from a *version-addressable* log against its *own* MVCC-pinned frames.
That is simultaneously:
- the **correctness** fix (zero drift, by construction), and
- the **parallelism unlock** — safe concurrent reads require every goroutine to
  see the same frozen frame, which is exactly what an owned snapshot provides
  (`internal/tablesource/snapshot.go` exists for this reason).

This is **not** N-API. N-API (embed Go in V8) would fix drift by sharing the
`*sqlite3_snapshot` C pointer in one address space, but it (a) forfeits crash
isolation, (b) forces Go's runtime to coexist with V8, and (c) is architecturally
incompatible with "Go runs alone" — Go could never exist without the Node host.
The port is more code but it is *application logic in repos we own*, incremental,
reversible, and the only path that reaches the goal. **Do the port.**

---

## 1. How TS does it (authoritative — port THIS exactly)

Source of truth: `mono/packages/zero-cache/src/services/view-syncer/snapshotter.ts`
(590 lines). Supporting: `.../replicator/schema/change-log.ts`,
`.../replicator/schema/replication-state.ts`, `db/statements.ts`.

### 1.1 The leapfrog model (`Snapshotter`, snapshotter.ts:91-207)

The Replicator is the **sole writer**. ViewSyncers never commit — they hold
**`BEGIN CONCURRENT`** snapshots on the same wal2 replica file and *simulate*
mutations on them (applied for IVM, then rolled back). Each ViewSyncer holds
**two** connections that "leapfrog" to replay the timeline in isolation:

```
Replicator:  t1 --------------> t2 --------------> t3 -------------->
ViewSyncer:       [snapshot_a] ----> [snapshot_b] ----> [snapshot_c]
                    (conn_1)           (conn_2)           (conn_1)   ← reused
```

- `#curr: Snapshot` — the snapshot IVM is currently consistent at.
- `#prev: Snapshot` — the previous snapshot, reused as the *next* `#curr`.

**`advanceWithoutDiff()` (snapshotter.ts:183-196)** — the leapfrog core:
```ts
const next = this.#prev
  ? this.#prev.resetToHead()                 // reuse old prev conn, re-pin at head
  : Snapshot.create(lc, this.#curr.db.db.name, appID, pageCacheSizeKib);
this.#prev = this.#curr;                       // old curr becomes new prev
this.#curr = next;                             // re-pinned conn becomes new curr
return {prev: this.#prev, curr: this.#curr};
```
First advance has no `#prev`, so it opens a fresh second connection; thereafter
the two connections alternate. **Connection reuse is load-bearing** (avoids
per-advance connection + statement-prep cost).

**`advance(syncableTables, allTableNames)` (snapshotter.ts:175-181)** wraps
`advanceWithoutDiff()` in a `Diff` object (lazy — changes are computed on
iteration, not up front).

### 1.2 A `Snapshot` = pinned frame + its version (snapshotter.ts:275-396)

`Snapshot.create` (276-300):
- `new Database(lc, dbFile)`; `pragma('synchronous = OFF')` (changes are
  ephemeral, never committed); optional `cache_size`.
- **Asserts `journal_mode === 'wal2'`** (291-296) — `BEGIN CONCURRENT` requires
  the replicator's wal2 mode.

`Snapshot` constructor (306-316) — **this is how a frame is pinned**:
```ts
db.beginConcurrent();                          // BEGIN CONCURRENT
const {stateVersion} = getReplicationState(db); // a READ — actually acquires the
                                                // read lock → logically creates
                                                // the snapshot. BEGIN CONCURRENT
                                                // alone does NOT lock. (308-310)
this.version = stateVersion;
```
`getReplicationState` (replication-state.ts) = `SELECT stateVersion FROM
"_zero.replicationState"`. So **`version` is defined by when the read ran**,
frozen by MVCC for the life of the tx.

`resetToHead()` (392-395): `this.db.rollback()` then `new Snapshot(this.db,…)` —
ends the old `BEGIN CONCURRENT`, starts a fresh one on the *same* connection,
re-pinning at the new head.

`numChangesSince(prevVersion)` (318-324):
`SELECT COUNT(*) ... FROM "_zero.changeLog2" WHERE stateVersion > ?`.

`changesSince(prevVersion)` (326-337) — the diff cursor:
```sql
SELECT "stateVersion","table","rowKey","op" FROM "_zero.changeLog2"
  WHERE "stateVersion" > ? ORDER BY "stateVersion" ASC, "pos" ASC
```
Returns an **iterator** (statement-cached). This is the whole trick: the diff is
read from a **version-stamped, append-only log**, so "changes in `(prev, curr]`"
is deterministic regardless of who reads or when — **no racing pointer**.

`getRow(table, rowKey)` (339-355) / `getRows(table, keys, row)` (357-390):
read row *contents* at this snapshot's frame. `getRows` filters NULL key
columns (NULL can't violate uniqueness AND SQLite's MULTI-INDEX-OR dies on NULL
→ full scan; see PR #5542). Both use `safeIntegers(true)`.

### 1.3 The change log (`_zero.changeLog2`, change-log.ts)

```sql
CREATE TABLE "_zero.changeLog2" (
  "stateVersion" TEXT NOT NULL,   -- LexiVersion (lexicographically sortable)
  "pos"          INT  NOT NULL,   -- order within a version
  "table"        TEXT NOT NULL,
  "rowKey"       TEXT NOT NULL,   -- JSON row key; for t/r ops = the stateVersion
  "op"           TEXT NOT NULL,   -- 's' set | 'd' delete | 't' truncate | 'r' reset
  "backfillingColumnVersions" TEXT DEFAULT '{}',
  PRIMARY KEY("stateVersion","pos"),
  UNIQUE("table","rowKey")
);
```
- Ops: `SET_OP='s'`, `DEL_OP='d'`, `TRUNCATE_OP='t'`, `RESET_OP='r'`.
- Stores **identifiers only**, never contents (contents come from prev/curr
  snapshots — see §1.2).
- Table-wide ops (`t`/`r`) use **`pos = -1`** and `rowKey = stateVersion`, so
  they sort *first* within a version → the pipeline aborts (resets) as early as
  possible. `changeLogEntrySchema` maps `rowKey` → `null` for `t`/`r`.
- It is a **cross-table index of the LAST op per row, ordered by version**
  (`UNIQUE(table,rowKey)` + `INSERT OR REPLACE`). So one row appears **at most
  once** in a diff (twice only across a TRUNCATE) → bounded catch-up work.

### 1.4 Diff iteration (`Diff[Symbol.iterator]`, snapshotter.ts:398-584)

For each change-log entry `(stateVersion, table, rowKey, op)`, in
`(stateVersion ASC, pos ASC)` order:

1. `op === RESET_OP` → **throw `ResetPipelinesSignal('schema-change')`** (444-450).
2. `op === TRUNCATE_OP` → **throw `ResetPipelinesSignal('truncation')`** (451-457).
3. table not in `syncableTables`:
   - if in `allTableNames` → `continue` (skip non-syncable) (459-461);
   - else → `throw Error('change for unknown table')` (463).
4. **Assert** `(tableSpec.minRowVersion ?? '') < stateVersion` (474-479) — the
   minRowVersion/overlay invariant (a minRowVersion set is always followed by a
   RESET, so incremental catch-up is always at a later version).
5. `nextValue = op === SET_OP ? curr.getRow(tableSpec, rowKey) : null` (482-483).
6. `prevValues`:
   - if `nextValue` (a set): `prev.getRows(tableSpec, tableSpec.uniqueKeys,
     nextValue)` — **all rows that conflict on ANY unique key** with the new
     value (these must be removed) (485-490);
   - else (a delete): `prevValue = prev.getRow(rowKey); prevValues = prevValue ?
     [prevValue] : []` (491-494).
7. `nextValue === undefined` → `throw Error('Missing value')` (495-499).
8. `checkThatDiffIsValid(...)` (501, defined 556-583) — **InvalidDiffError** if:
   - `stateVersion > curr.version` (curr advanced past), or
   - any `prevValue[_0_version] > prev.version` (prev advanced past), or
   - `op === SET_OP && nextValue[_0_version] !== stateVersion` (curr advanced).
9. `prevValues.length === 0 && nextValue === null` → **`continue`** (no-op: delete
   of a row absent in prev) (503-507).
10. permissions table + `permissions` hash changed → **throw
    `ResetPipelinesSignal('permissions-change')`** (509-524).
11. **emit** `Change { table, prevValues: prevValues.map(fromSQLiteTypes),
    nextValue: nextValue ? fromSQLiteTypes(...) : null, rowKey }` (529-540).

`cleanup()` (424-431) returns the cached statement and the iterator on
done/throw/`return()`.

### 1.5 The `Change` shape (snapshotter.ts:209-220)
```ts
type Change = {
  table: string;
  prevValues: Readonly<Row>[];   // rows to REMOVE (old value, or unique-conflicts)
  nextValue: Readonly<Row> | null; // new value, or null for delete
  rowKey: RowKey;
};
```
This is **exactly** today's wire `SnapshotChange[]`
(`engine.SnapshotChange{Table, PrevValues, NextValue}`) — we already ship this
shape from TS to Go. The port moves its *production* into Go.

### 1.6 How the diff drives IVM (the consumer)
`pipeline-driver.ts #advance` iterates the `SnapshotDiff` and turns each
`Change` into source pushes: `prevValues` → removes, `nextValue` → add, the
pair → an edit (the same `prevValues`/`nextValue`/`rowKey` we buffer into
`snapshotChanges` for `#goPrimaryAdvance` today). On `ResetPipelinesSignal` it
aborts and re-hydrates all pipelines at `curr`.

---

## 2. What Go already has vs. the gap

`internal/tablesource/source.go` already implements **half** of a Snapshot:

| TS construct | Go today | Gap |
|---|---|---|
| `BEGIN CONCURRENT` pinned tx | `prevConn` + `ensurePrevTxLocked` (271) + `beginStmt` fallback | only ONE conn (the `prev`); no `curr` conn |
| ephemeral / never-commit | yes — always `ROLLBACK` (writes never land) | — |
| `resetToHead` (rollback + re-pin) | `OnAdvanceEnd` (318) re-pins `prevConn` at head | single-conn re-pin, **not** a 2-conn leapfrog |
| `sqlite3_snapshot` pinning for parallel reads | `snapshot.go` (`-tags libsqlite3`) | wired for multi-conn reads, not yet for a `curr`/`prev` pair |
| version (`stateVersion`) | **none** — Go has no version authority | read `_zero.replicationState` |
| changelog diff (`changeLog2`) | **none** — Go consumes TS's shipped `SnapshotChange[]` | the whole `Diff` |
| overlay (in-flight push visibility) | `overlay *ivm.Overlay` (memory-source parity) | keep as-is |

**So the port = add the `curr` connection, the version read, and the changelog
`Diff`, turning Go's single-prev-tx into TS's full two-snapshot leapfrog.**

---

## 3. The Go port — line-by-line mapping

New package: `internal/snapshotter` (peer to `internal/tablesource`). Mirrors
snapshotter.ts 1:1. Reuses `tablesource`'s connection/`BEGIN CONCURRENT`/snapshot
plumbing.

| TS (snapshotter.ts) | Go (`internal/snapshotter`) | Notes — preserve exactly |
|---|---|---|
| `class Snapshotter` `#curr/#prev` | `type Snapshotter struct { curr, prev *Snapshot }` | two conns, leapfrog |
| `init()` (116) | `func (s *Snapshotter) Init() error` | `curr = newSnapshot(...)`; assert not already init |
| `current()` (133) | `func (s *Snapshotter) Current() *Snapshot` | assert initialized |
| `advanceWithoutDiff()` (183) | `func (s *Snapshotter) AdvanceWithoutDiff() (prev, curr *Snapshot)` | `next = prev ? prev.ResetToHead() : newSnapshot(...)`; `prev=curr; curr=next` — **keep the conn-reuse** |
| `advance(...)` (175) | `func (s *Snapshotter) Advance(syncable, allNames) *Diff` | lazy Diff |
| `destroy()` (202) | `func (s *Snapshotter) Destroy()` | close both conns |
| `Snapshot.create` (276) | `newSnapshotConn(dbFile)` | `synchronous=OFF`; **assert `journal_mode==wal2`** |
| `Snapshot` ctor (306) | `newSnapshot(conn)` | `BEGIN CONCURRENT` **then a READ** of `_zero.replicationState.stateVersion` (the read is what locks — comment 308-310 is mandatory) |
| `resetToHead()` (392) | `(*Snapshot).ResetToHead()` | `ROLLBACK` then re-pin on same conn |
| `numChangesSince` (318) | `(*Snapshot).NumChangesSince(prevVer string) int` | `COUNT(*) … stateVersion > ?` |
| `changesSince` (326) | `(*Snapshot).ChangesSince(prevVer string) (rows, cleanup)` | `… stateVersion > ? ORDER BY stateVersion ASC, pos ASC` — **iterator, not slice** (bounded memory) |
| `getRow` (339) | `(*Snapshot).GetRow(spec, rowKey) ivm.Row` | NULL-key filtering N/A (single key) |
| `getRows` (357) | `(*Snapshot).GetRows(spec, uniqueKeys, row) []ivm.Row` | **filter NULL key cols** (correctness + MULTI-INDEX-OR perf, PR #5542) |
| `class Diff` (398) | `type Diff struct{ prev, curr *Snapshot; … }` | `changes = curr.NumChangesSince(prev.version)` |
| `Diff[Symbol.iterator]` (421) | `func (d *Diff) Iter(yield func(Change) bool)` (Go 1.23 range-over-func) | replicate steps §1.4 **in order** |
| `Change` (209) | already `engine.SnapshotChange` / `ivm.SourceChange` | reuse the wire shape |
| `ResetPipelinesSignal` (265) | `*ivm.DriftError`-style `ResetSignal{Reason}` | reasons: schema-change, truncation, permissions-change (+ existing scalar-subquery) |
| `checkThatDiffIsValid` (556) | `(*Diff).checkValid(...)` → `InvalidDiffError` | all 3 version checks |
| `getReplicationState` | `selectStateVersion(conn) string` | `SELECT stateVersion FROM "_zero.replicationState"` |
| change-log ops `s/d/t/r` | `const opSet/opDel/opTruncate/opReset = "s"/"d"/"t"/"r"` | exact bytes |
| `changeLogEntrySchema` rowKey→null on t/r | same mapping in the scan | t/r have `pos=-1`, `rowKey=version` |

### 3.1 Fidelity invariants (MUST match TS or drift returns)
1. **Diff order**: `stateVersion ASC, pos ASC`. t/r at `pos=-1` sort first → reset
   before row changes.
2. **At-most-once per row** per diff (relies on `UNIQUE(table,rowKey)` +
   `INSERT OR REPLACE` upstream — Go only reads, so this is automatic).
3. **`prevValues` = unique-key conflicts** on a SET (not just the same rowKey) —
   these are the rows IVM must REMOVE before adding `nextValue`. Getting this
   wrong = duplicate/leaked rows.
4. **No-op filter**: `prevValues.length===0 && nextValue===null` → skip.
5. **Version validity checks** (`checkThatDiffIsValid`) — these catch a diff used
   after the snapshots advanced; keep all three.
6. **Ephemeral**: never `COMMIT`; the `prev` snapshot has IVM changes *applied*
   during iteration, then the caller savepoints/rolls back (snapshotter.ts:169-173).
7. **Connection reuse** in the leapfrog (don't open a new conn per advance).
8. **The read after `BEGIN CONCURRENT`** is what acquires the lock — do not skip it.
9. **`safeIntegers(true)`** on row reads → bigint handling parity (cross-ref
   HIGH-9 / `maxSafeInteger`).

---

## 4. The advance flow: push → trigger

Today (diff-push): TS computes the `Diff`, buffers `snapshotChanges`, and
`advanceStream(snapshotChanges)` ships them to Go.

Ported (trigger): the wire call becomes **`advanceToHead()`** (no payload). Go's
own Snapshotter does `AdvanceWithoutDiff()` → `Diff` → feeds its IVM operator
trees → returns `RowChanges` **plus Go's new `version`** (and any
`ResetSignal`). TS no longer reads rows for user-query advances.

Protocol delta (mono `go-ivm-client.ts` / sidecar `cmd/sidecar/main.go`):
- replace `advanceStream(SnapshotChange[])` with `advanceToHead() ->
  {changes, version, reset?}`.
- Go reports its version; the view-syncer stamps the user-query CVR at it.
- `removeQuery`/`addQueriesStream`/hydrate unchanged (they already fetch from
  Go's source at `curr`).

---

## 5. mono-side changes (the heavy part — view-syncer)

This is where the work concentrates and why it's a major change.

1. **Version authority split.** User-query queries advance at **Go's** version;
   internal/control-plane queries (`lmids`, `<app>.permissions`,
   `<shard>.clients`) still advance via **TS's** Snapshotter (they're excluded
   from Go's data path — Fix #1). The view-syncer must drive BOTH to the **same
   target watermark `V`** each cycle and stamp **one coherent CVR at `V`**.
2. **CVR bookkeeping.** `view-syncer.ts` must accept the user-query CVR version
   from Go (not assume TS's). Reconcile with the internal-query version: only
   commit a CVR at a version both have crossed.
3. **Mode = selector at convergence** (see §6).
4. **Shadow comparison → full-state.** Per-advance 1:1 compare breaks once TS
   and Go leapfrog independently (coalescing → different advance boundaries).
   The drift-audit's re-hydrate-both-at-`V`-and-compare becomes the **primary**
   shadow mechanism; hydration-time compare (fixed snapshot) still works.

---

## 6. Both modes from ONE path (the payoff)

Upstream is identical: fork at the watermark; both PipelineDrivers (TS-native +
Go) advance to `V`; converge at the shared ViewSyncer/CVR. **Mode = which
driver's output the ViewSyncer commits:**

| | Shadow | Go-primary (lean = prod) |
|---|---|---|
| Both advance to `V` | yes | Go for user queries; TS for internal only |
| CVR stamped from | TS | Go (user) + TS (internal) |
| Other driver's output | compared (every advance) | audited (sampled) |
| Failover (Go dies) | n/a (TS already serving) | breaker → TS re-establishes snapshotter + re-hydrates |

This **unifies shadow and primary** (today they are different dispatch paths —
that divergence is exactly how CRIT-3 got *masked* by shadow). After the port,
a clean shadow run is a real guarantee about primary.

---

## 7. Phased plan

- **P0 — `internal/snapshotter` package. ✅ DONE.** Ported snapshotter.ts
  §1.1–1.4 verbatim: `Snapshotter` (curr/prev leapfrog, conn reuse,
  BEGIN CONCURRENT→BEGIN fallback), `Snapshot` (version read pins the frame,
  `resetToHead`, `NumChangesSince`, `ChangesSince`, `GetRow`, `GetRows` with
  NULL-key filtering), and `Diff` (the §1.4 iterator: reset/truncate/
  permissions signals, syncable/allTableNames handling, minRowVersion assert,
  unique-conflict prevValues, no-op filter, `checkThatDiffIsValid`). Files:
  `snapshotter.go`, `snapshot_read.go`, `spec.go`, `diff.go`. 13 fixture tests
  (`snapshotter_test.go`) cover set/edit/delete/no-op/unique-conflict/truncate/
  reset/unknown-table/non-syncable-skip/version-invalid/permissions-change/
  permissions-unchanged/leapfrog/lifecycle — all green. Pure Go, no wire/mono
  changes. **Deviation (forced):** `ChangesSince` buffers the (identifier-only)
  change-log entries before per-entry `getRow`, because Go's `database/sql`
  allows only one active query per `*sql.Conn`; row CONTENTS stay lazily
  fetched, so memory is still bounded by catch-up size, not row data.
- **P1 — advance-trigger RPC. ✅ Go side DONE; mono client pending.** Sidecar:
  `advanceToHead` RPC (`cmd/sidecar/advance_to_head.go`) gated by
  `GO_IVM_ADVANCE_TO_HEAD=true` + table mode + `GO_IVM_APP_ID` (or per-init
  `appID`). Per-CG `Snapshotter` built in `handleInit` (pinned at the hydrate
  head), torn down on re-init/destroy. The handler leapfrogs the Snapshotter and
  returns the derived `{changes, version, numChanges, reset?}` WITHOUT driving
  the engine — a pure Go-derived diff for the TS-vs-Go shadow compare. `allNames`
  comes from `sqlite_master` so replicated-but-non-syncable tables are skipped,
  not errored. protocolRev 6→7. Tests: `advance_to_head_test.go` (derive-diff +
  non-syncable-skip) green; full sidecar suite green (the two `getReplicaDB`
  probe-timing tests fail identically on the P0 baseline — pre-existing, env
  timing). **✅ mono side DONE:** `go-ivm-client.ts` `advanceToHead()` +
  `AdvanceToHeadResult`, `GoComputeBackend.advanceToHead()`, `isGoDerivedDiff`,
  protocolRev 6→7 in `sidecar-manager.ts`, `goSidecar.advanceToHead` config,
  and `#compareGoDerivedDiff` in `pipeline-driver.ts` (an order-independent
  table+prevValues+nextValue signature compare logged under `[go-diff-shadow]`).
  Off by default; runs inside shadow mode only. **P1 is now end-to-end: enable
  `shadowMode` + `advanceToHead` and the logs prove Go derives the same diff TS
  does.** **✅ SOAK-VALIDATED 2026-06-04:** rust-test shadow soak (8 users / 183s
  / `--mutate`, rev-7 sidecar + mounted P1 mono) → **24 "TS and Go derived
  identical diffs", 0 mismatches, 0 panics, 0 resets.** Unblocked by a
  pre-existing tablesource fix found mid-soak: the nullable `">"` start-cursor
  bound emitted 2 placeholders but bound 1 value (`want 2 got 1` panic) —
  `gatherStartConstraints` now binds the start value twice for that form
  (`sqlite/query_builder.go`, regression test added). The gate for P2 is now
  green.
- **P2 — view-syncer version authority + engine self-consistency** (the heavy
  part). CVR stamping from Go's version; two-snapshotter reconciliation;
  shadow→full-state compare.
  **⚠️ Spike finding (2026-06-04):** a naive "drive" mode — `advanceToHead`
  applying its own derived diff via `engine.Advance` — was prototyped and
  **proven incorrect**: the engine's `tablesource.Source` leaves re-pin their
  OWN read tx at head independently of the Snapshotter's frame, so applying a
  derived `add(row)` hits a `tablesource` prev-tx frame that *already contains
  that row* → `UNIQUE` failure / drift panic (reproduced; reverted). **The real
  fix is frame-coordination:** during a Go-primary advance the engine's leaf
  reads/writes must be bound to the Snapshotter's `prev` connection (apply the
  diff into the SAME `BEGIN CONCURRENT` frame the diff was derived against),
  rather than each `Source` owning + re-pinning its own tx.
  **✅ P2a/P2b DONE & SOAK-VALIDATED (2026-06-04):** implemented the
  frame-coordination — `tablesource.Source.BindConn/UnbindConn` + `activeConn()`
  (when bound, all prev-tx reads/writes use the Snapshotter-owned conn;
  ensurePrevTx/OnAdvanceEnd no-op), `Snapshot.Conn()`,
  `engine.BindTableSourcesToConn`, and `advanceToHead` DRIVE mode
  (`GO_IVM_ADVANCE_DRIVE`): sticky-bind leaves to curr for hydrate, flip to
  `diff.Prev()` for the apply (== the old curr post-leapfrog), rebind curr. mono:
  `goSidecar.advanceDrive` sources the shadow Go advance via `advanceToHead`
  (drive) and compares its RowChanges to TS. **Soak (8u/183s/mutate, wal2
  build): 530 `[shadow]` matches, 0 advance mismatches, 0 panics, 0
  `database is locked`, 0 resets.** The naive-spike crash is gone: writing into
  the past-pinned `prev` works under `BEGIN CONCURRENT`/wal2 (the drive unit test
  skips under plain BEGIN). **Hydrate misses FIXED:** the rare `TS=2,Go=0` was a
  LIKE-escape divergence, NOT version skew — Go's `matchLike` treated `\` as an
  escape, but TS pushes `LIKE ?` into SQLite with no `ESCAPE` (`\` literal), so
  `content LIKE '%\%%'` matched backslash-content in TS / percent-content in Go.
  `compileLikePattern` now treats `\` as literal (SQLite default). Re-soak:
  **488 matches, 0 mismatches (advance AND hydrate), 0 panics, 0 resets.**
  **Remaining for P2:** CVR two-version reconciliation (P2c) — actually stamping
  the user-query CVR at Go's version for Go-primary serving (vs today's shadow
  compare).
- **P3 — lean Go-primary.** Drop TS's user-query compute; TS = cold fallback.
  Then the PERF-REVIEW Tier-1 parallelism items (`T1-3/4/5/7`,
  `GO_IVM_PARALLEL_THRESHOLD`) carry the actual prod win. Gated on P2.

Interim stop-gap (if Go-primary correctness is needed before P2 lands): the
deep-dive's **option 4** — route Go-primary drift through the same
`ResetPipelinesSignal` full re-hydrate TS uses, instead of discarding the
re-hydrate (`go-compute-backend.ts:130-131`). Mono-only; drift still happens but
stops being a client-correctness issue.

---

## 8. Open questions / risks

1. **wal2 + snapshot in the deployed Go SQLite.** The leapfrog needs
   `BEGIN CONCURRENT` on wal2 AND (for parallel reads) `sqlite3_snapshot_*`.
   Production links rocicorp's patched `libsqlite3.so` via `-tags libsqlite3`
   (`internal/tablesource/db.go`, `snapshot.go`); confirm the same build is used
   for the snapshotter and that local (non-tagged) builds degrade to a single
   reader cleanly (snapshot_stub.go).
2. **Two-version reconciliation** in the CVR (§5.1) — the genuinely novel logic.
   Spec it before P2: define the exact rule for "CVR version when user-queries
   are at Go's `V_go` and internal at TS's `V_ts`."
3. **Failover cost** in lean primary: TS is cold, so a Go failure pays a
   re-hydrate stall. Accepted trade (don't keep a hot TS mirror — it forfeits the
   perf). Confirm the breaker thresholds make this rare.
4. **`backfillingColumnVersions`** (change-log.ts) — TS's snapshotter does NOT
   read this column for the diff (changeLogEntrySchema excludes it); confirm the
   Go diff likewise ignores it, and that the `minRowVersion < stateVersion`
   assert (§1.4 step 4) holds for our overlay handling.
5. **`InvalidDiffError` under concurrency** — in Go, multiple goroutines must not
   advance the snapshotter mid-iteration. TS relies on the single-threaded event
   loop; Go must serialize advance vs. the `Diff` iteration (same discipline as
   `tablesource.Source.mu` today).

---

## 9. One-line summary

Port `snapshotter.ts` into `internal/snapshotter` verbatim (leapfrog two
`BEGIN CONCURRENT` snapshots + a `changeLog2`-derived `Diff`), flip advance from
a TS-shipped diff to a Go-side `advanceToHead()` trigger, and move user-query CVR
version authority into the view-syncer — making Go self-consistent (zero drift),
unifying shadow & primary into one path, and unlocking the goroutine parallelism
that is the entire point of the sidecar.
