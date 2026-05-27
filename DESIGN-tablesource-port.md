# TableSource port — design

**Goal.** Replace the Go IVM sidecar's `MemorySource` leaf with a `TableSource`
that reads the same SQLite replica as TS, so Go inherits TS's row state by
construction rather than copying it via `loadRows`. This is the "true port
from TS" — fidelity becomes structural, not tested-and-monitored.

**Non-goal.** Replace the IVM operators *above* the source. Joins, GROUP BY,
EXISTS resolution, cursor walks above the leaf — all stay as they are. Only
the leaf source changes.

---

## Hard constraints (lead with these)

### 1. Parallelization MUST be preserved

The Go sidecar's current parallelism is the central reason it exists. The
port preserves it as a design property — not as an emergent post-port
benchmark. Concrete commitments:

- **Per-CG goroutine isolation stays.** Each CG continues to own its query
  tree and goroutine pool; the leaf change does not introduce a global
  source-side lock.
- **`genPushAndWriteParallel` fanout is source-agnostic.** It's a *push*
  fanout across pipeline connections; switching the leaf changes nothing.
  `GO_IVM_PARALLEL_THRESHOLD` keeps its semantics.
- **`addQueriesStream` (concurrent hydrate) keeps its concurrency model.**
  Each query hydrates on its own goroutine. Required: each goroutine gets
  its own SQLite read connection.
- **WAL mode is mandatory** (already used by the TS replicator). Without
  WAL, every reader serializes behind any writer — instantly kills
  parallelism. Enforced by the sidecar opening with `?_journal_mode=WAL`
  and assert-on-open if the file disagrees.
- **Connection pool sized to peak concurrency.** `sql.DB.SetMaxOpenConns(N)`
  where N ≥ `max(active CGs × peak per-CG concurrent queries)`. Default
  pool size should not be left at Go's default of 0 (unlimited) — that
  leaks FDs and breaks `busy_timeout` budgeting.
- **Per-CG-per-advance read transaction.** All queries in a single CG's
  advance share one `BEGIN`-ed read-tx so they see the same snapshot AND
  share the BEGIN cost. New read-tx per advance epoch; cached and reused
  for hydrates within the same epoch.
- **Prepared statements per-conn.** Hot queries reuse prepared stmts;
  driver `sql.Stmt` handles pool affinity.

### 2. Snapshot semantics MUST match TS

TS's TableSource sees a consistent snapshot per advance epoch. Go's
TableSource MUST do the same — otherwise IVM correctness breaks under
concurrent writes. Mechanism: per-advance read-tx (see above) +
replicator-driven epoch ratchet (the existing snapshot version mechanism
from `pipeline-driver.ts`).

### 3. Replicator coordination MUST stay one-way

TS replicator writes; Go reads. Never the other way around. The sidecar
opens SQLite read-only (`mode=ro`) so we cannot accidentally write.
Replicator restart → sidecar's open read-tx becomes stale at next epoch
bump → sidecar releases + re-`BEGIN`s. No two-phase commit, no leader
election.

### 4. Build complexity MUST stay manageable

CGO is a real cost. Pick the driver deliberately:

| Driver | CGO | Maturity | WAL multi-reader | Notes |
|---|---|---|---|---|
| `mattn/go-sqlite3` | Yes | Very mature | Proven | Default choice; pay the CGO tax |
| `modernc.org/sqlite` | No | Mature | Works, slower | Fallback if CGO becomes a deployment blocker |
| `crawshaw.io/sqlite` | Yes | Mature | Proven (low-level) | Lowest overhead but harder ergonomics |

**Start with `mattn/go-sqlite3`.** Benchmark `modernc.org/sqlite` once the
port works; switch only if the perf delta is < 10% and CGO is causing
real pain.

---

## What's deleted vs preserved

| Component | Status after port |
|---|---|
| `MemorySource` (Go) | Deleted |
| `loadRows` RPC | Deleted |
| `tables` payload on init | Reduced to `{table name, primary key}` only (schema read directly from SQLite) |
| `advance` RPC for row diffs | Reduced — Go pulls from SQLite; advance becomes a "new epoch is N, re-`BEGIN`" notification |
| `Row.DecodeMsgpack` numeric coercion | Deleted (TableSource reads typed SQLite values directly) |
| Per-cgID `initEpoch` handshake | Kept — still needed for cgID lifecycle |
| Drift audit (`#sqlGroundTruthCompare`, `#shadowCompare`, full audit infrastructure) | Deleted or demoted to sanity check |
| IVM operators (Join, EXISTS, GROUP BY, Filter, Sort, etc.) | Preserved unchanged |
| `genPushAndWriteParallel` | Preserved unchanged |
| `addQueriesStream` / `advanceStream` | Preserved — stream contracts identical |
| Connection-health-check ticker (externallyManaged mode) | Preserved |
| Restart-recovery `onRestart` chain | Simplified (no `loadRows` to replay) |

---

## Implementation order

Each phase ends with the sidecar still passing the existing drift audit in
shadow mode (parallel TS path) before moving to the next. That keeps every
phase shippable.

1. **Phase 0 — Scaffolding.** Add the SQLite driver to `go.mod`. Add
   `internal/tablesource/` package with the `Source` interface. Add a
   `--source=memory|table` sidecar flag defaulting to `memory`. No behavior
   change.

2. **Phase 1 — Read-only opener + pool.** Implement the connection pool,
   WAL-mode assert, busy_timeout config, per-CG-per-advance read-tx cache.
   Bench: cold start, hot read latency, concurrent reader scaling.
   Compare with MemorySource numbers from `PERF-REVIEW.md`.

3. **Phase 2 — TableSource leaf.** Implement `TableSource` matching the
   `ivm.Source` interface used by the engine: `Fetch`, `Push`, `Cleanup`.
   Mirror TS's `table-source.ts` semantics (cursor, ordering, predicate
   pushdown). Unit tests against the same test vectors TS uses.

4. **Phase 3 — Wire into engine.** Make the engine pick `TableSource` when
   `--source=table`. Run existing shadow-mode soaks at parity. Compare drift
   audit results: with TableSource, expect zero `go-vs-sql-drift` events by
   construction.

5. **Phase 4 — Replicator epoch handshake.** Subscribe to the TS replicator's
   epoch bumps via the existing snapshot-version mechanism. On epoch change,
   release + re-`BEGIN` the per-CG read-tx. Validate under sustained-write
   soak (10-minute concurrent INSERT/UPDATE/DELETE storm).

6. **Phase 5 — Retire `loadRows` + MemorySource.** Once Phase 4 has soaked
   in pre-prod for ≥ 72h with zero drift events and parity perf, delete:
   `loadRows` from RPC, `MemorySource` from the engine, the `tables`
   payload's row-state field, and the bulk of the drift-audit
   infrastructure. Keep a minimal sanity audit running on a sampled basis.

7. **Phase 6 — Pure-Go driver evaluation.** Benchmark `modernc.org/sqlite`.
   If within 10% of `mattn`, switch (no CGO is worth a small perf hit).
   Otherwise keep mattn and document the decision.

---

## Risks / open questions

- **Per-row leaf cost regression.** Today's MemorySource is a map lookup
  (~50ns). SQLite index walk with hot page cache is ~500ns–2µs. For
  query-heavy workloads with small selectivity, this could net regress.
  Mitigation: bench early (Phase 1), and consider a thin per-CG read-side
  cache for the absolute hottest rows if needed (cache invalidation on
  epoch bump is trivial).
- **Connection-pool sizing under multi-CG burst.** 14 CGs × 4 concurrent
  queries each = 56 conns. SQLite's default FD limit + WAL reader registry
  can handle this, but we need to bench at 50+ CGs to be sure. Plan: load
  test at 100 CGs in Phase 1.
- **Schema migration coordination.** Currently the TS replicator handles
  schema changes and propagates them to the Go sidecar via the `tables`
  payload. Post-port, the sidecar reads schema from SQLite — but it caches
  it in memory for query planning. Need a schema-change signal (probably
  same as the epoch bump) to invalidate the cache.
- **CGO build matrix.** GHCR builds need to produce both `linux/amd64` and
  `linux/arm64`. mattn/go-sqlite3 cross-compiles fine with the right
  cross-compiler in the builder image. Validate in Phase 0.
- **Replicator WAL checkpoint stalls.** When TS's litestream-based
  replicator checkpoints the WAL, brief reader-side stalls are possible.
  Today MemorySource is immune to this. Need to benchmark steady-state
  read latency during a checkpoint to size the impact.

---

## Out of scope (intentionally)

- Re-architecting the engine. The engine stays.
- Changing the wire protocol beyond what the leaf change forces.
- Multi-region / replication topology changes.
- Replacing IVM with anything else.

---

## Test plan checkpoints

- **End of Phase 1:** connection-pool + WAL opener microbenchmark (read
  latency, concurrent scaling) lands as a baseline in `PERF-REVIEW.md`.
- **End of Phase 3:** existing drift-audit fixture in shadow mode reports
  zero `go-vs-sql-drift` events over a 1-hour multi-CG soak.
- **End of Phase 4:** 10-minute write-storm soak with concurrent reads
  produces no snapshot-skew errors.
- **End of Phase 5:** 72-hour pre-prod soak with the new leaf running
  Go-primary, zero correctness flags, parity p99 latency vs MemorySource
  baseline (within 10%).
