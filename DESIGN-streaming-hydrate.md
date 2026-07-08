# DESIGN: True Streaming Hydrate for Go-IVM

Status: **superseded in part (2026-07-08)** · proposal v2 · Owner: TBD · Created 2026-06-29 · Replaces v1

> **Supersession note (Option B, 2026-07-08).** §3 (lanes + K = P × Cmax) and
> the §3d deadlock-freedom proof described the PER-FETCH reader-acquire model:
> every leaf fetch borrowed a pool reader, so nested fetches held readers
> while acquiring more, and K = P × Cmax was the sizing that kept that
> hold-and-wait acyclic. ABI v3's pull mode (one goroutine per query, D6 in
> DESIGN-duplex-streaming.md) broke the P-lane premise and reintroduced the
> deadlock (2026-07-07 wedge). The shipped model is now **one reader per
> hydrate pipeline** (TS's resource shape: one conn per view-syncer, nested
> `iterate()` cursors interleaved on it): a pipeline acquires its single
> reader at start while holding nothing (`ReaderPool.AcquireForPipeline`),
> every nested fetch rides that reader (`fetchViaBoundReaderStream`, raw
> driver conns — database/sql's one-Rows-per-conn limit forced the bypass),
> and K = max(hydrateReaders, hydrateLanes) is just the admission width —
> wider batches queue while holding nothing. Cmax, the per-fetch acquires,
> and the exhaustion fallback are deleted. The lazy iter.Seq operator
> plumbing this doc designed (§2, §4+) is unchanged and shipped.

Convert Go-IVM hydrate from eager `Fetch() []Node` to lazy `iter.Seq[Node]`
— matching TS's `fetch(): Stream<Node | 'yield'>` (`operator.ts:43`,
`Stream<T> = Iterable<T>`, `stream.ts:8`) — running **in parallel**, with
**bounded Go-side memory** and **no fallback path**.

The v1 doc framed the blocker as "reader-starvation / CG-stall" and proposed
spill-on-backpressure. **That framing is wrong.** Naive lazy doesn't stall —
it **deadlocks**, before any socket contention can occur. This v2 corrects
the framing, proves the deadlock, and presents one correct design with no
escape hatches.

---

## 1. Goal & non-goals

**Goal.** Convert `Input.Fetch(req) []Node` (`operator.go:75`, eager) to a
lazy `iter.Seq[Node]` (Go 1.23+ `iter` package, available in the project's Go
1.25 toolchain) threaded through the operator graph, so a query's rows stream
to the TS client as they are produced rather than being fully materialized
then replayed (`engine.go:826` → emit loop `:856-863`).

**Non-goals.**
- Not changing operator *semantics* (output rows/order stay identical —
  verified by the divergence harness).
- Not touching the advance path's read-your-writes model.
- Not N-API / SHM (boundary cost is serialization, not transport).
- **`Output.Push` stays `[]Change` (eager)** (`operator.go:79`). TS's
  `push(): Stream<'yield'>` (`operator.ts:78`) carries **only control tokens**
  (`'yield'`), not rows — the Go `[]Change` return is the faithful
  equivalent (the changes themselves, returned explicitly rather than
  yielded as a control stream). The advance path propagates changes via
  per-change callbacks depth-first (`join.ts:132-140` pushes one change;
  `join.ts:231-244` loops matching parents calling `output.push` per parent).
  Per-push output is bounded by one change's amplification (match count × 1
  child change), not the full table. **Only the fetch (hydrate) contract
  changes.** See §8 for the join child-push fan-out caveat.

**Faithfulness note on `'yield'`.** TS's `fetch()` returns
`Stream<Node | 'yield'>`, where `'yield'` is a cooperative-yield token for
JS's single-threaded event loop (`operator.ts:34-41`). Go drops it
(`operator.go:71` already comments this) — Go's runtime scheduler is
preemptive (since 1.14), so there is no event loop to yield to.
`iter.Seq[Node]` is the faithful translation of `Stream<Node>`. (The TS-side
adapter's synthetic `yield 'yield'` every 100 rows at
`pipeline-driver.ts:2094` exists only because Go currently hands back a
materialized array; it disappears once Go streams.)

**Why it's worth it.**
- **Memory:** eager allocates the full `[]Node` per query
  (`source.go:1238` builds the whole slice; `engine.go:804-805` already
  admits "Go-side memory is NOT bounded"). Lazy bounds it to one chunk —
  structurally killing the unbounded-`.all()` OOM class instead of capping
  it.
- **GC:** the documented parallelization ceiling is allocation-bound
  (`reference_parallelization_gc_ceiling`); less per-query allocation lifts
  multi-core scaling.
- **First-row latency:** rows arrive at the TS client as they're produced,
  not after the full query materializes.
- **Faithfulness:** eager `[]Node` is itself a divergence from TS's lazy
  `Stream<Node>` — this *removes* a Go-specific divergence.

---

## 2. The blocker — DEADLOCK, not stall

### 2a. The eager invariant (why eager works at K=1)

Every operator in the eager model fully materializes its `input.Fetch()`
slice — acquiring AND releasing the pool reader — **before** opening the next
cursor. This means a single goroutine holds **at most one reader at a time**.
Verified at every operator:

| Operator | Evidence | Reader sequence |
|---|---|---|
| **Source** | `fetchViaPool` (`source.go:1280-1284`): `acquire → scan all rows → defer release` | 1 reader, released at return |
| **Join** | `Join.Fetch` (`join.go:125-131`): `parent.Fetch(req)` returns complete `[]Node` (parent reader released), THEN loops building childStream closures (not yet called) | parent reader released before any child fetch |
| **UnionFanIn** | `UnionFanIn.Fetch` (`union_fan_in.go:83-88`): fetches all branches into `[][]Node` sequentially, then `MergeFetches` | each branch's reader acquired+released in turn |
| **FilterStart** | `FilterStart.Fetch` (`filter_operators.go:90`): `range fs.input.Fetch(upstreamReq)` — upstream fully materialized first | upstream reader released before EXISTS probe |
| **Take** | `Take.Fetch` (`take.go:115`): `range t.input.Fetch(req)` — upstream fully materialized first | upstream reader released |

The child relationship closures are invoked **later**, by `streamNodesInto`
(`streamer.go:227`: `node.Relationships[relName]()`) in the emit loop
(`engine.go:856-857`). By then the parent reader is already released.

This is the invariant stated at `reader_pool.go:67-70`:

> Because hydrate fetch is eager (read all rows, then return), a single
> goroutine holds at most one reader at a time, so pool size = #concurrent
> hydrate goroutines is deadlock-free.

At K=1 (the default, `main.go:741`), one goroutine needs one reader → pool
of 1 → no contention → no deadlock.

### 2b. Lazy breaks the invariant → DEADLOCK

Lazy inverts the lifetime model. `Fetch` returns `iter.Seq[Node]`; the
reader (cursor) stays open **for the lifetime of the seq**, not just the
`Fetch` call. When the consumer pulls a parent node and then invokes its
child relationship (via `streamNodes`), the child `Fetch` opens a **second
cursor while the first is still open**.

Concretely, a lazy `Join.Fetch` returning `iter.Seq[Node]`:

```
consumer pulls parent node  →  parent Source cursor open  (reader 1 held)
consumer calls streamNodes   →  child closure invoked
  child.Fetch returns seq    →  child Source cursor open   (reader 2 needed)
```

At K=1, reader 2 acquire **blocks** — the pool has 1 reader, reader 1
holds it, and the same goroutine can't release reader 1 (it's mid-iteration
of the parent seq). This is a **self-deadlock** — a single query, a single
goroutine, no socket involved, no cross-query contention. It deadlocks on
the **first parent node of the first query**.

For a 3-deep nested join (`A→B→C`), the demand is 3 simultaneous readers.
For a `UnionFanIn` with k branches doing a streaming k-way merge, the demand
is k simultaneous readers.

**The v1 doc's spill mechanism cannot help.** Spill says "when the buffer is
full, drain the seq to memory, release the reader, then block on enqueue."
But the seq **cannot be drained** — it's blocked acquiring a reader that the
same goroutine already holds. The deadlock occurs *before* any socket
backpressure can manifest.

### 2c. Why TS doesn't have this problem

TS uses `better-sqlite3`, which runs **N concurrent cursors on a single
connection**. TS's `Stream<T> = Iterable<T>` (`stream.ts:8`) is lazy, and
holding a parent cursor while opening a child cursor on the *same* connection
is fine — no pool, no acquire/release, no deadlock.

Go uses `database/sql`, which allows **one active `*sql.Rows` per
`*sql.Conn`**. N concurrent cursors need N connections. The reader pool
(`reader_pool.go`) provides these connections, but its size K must
accommodate the concurrent-cursor demand — not just the goroutine count.

This is the Go-specific constraint that shapes the entire design.

---

## 3. The design — lanes + K = P × Cmax

One mechanism. No fallback. No escape hatches.

### 3a. Bounded lanes (P workers)

Replace the unbounded per-query goroutine spawn (`engine.go:817-818`:
`go func(idx int, entry *pipelineEntry) {...}`) with **P workers** draining a
query queue. P bounds:
- **Parallelism** — at most P queries hydrate concurrently.
- **Connection demand** — at most P × Cmax readers needed (see below).

The current unbounded spawn is already capped by `hydrateReaders` (pool
size), but the goroutine count itself is unbounded (N queries → N goroutines,
all competing for K readers). Lanes make the bound explicit and structural.

### 3b. Per-query concurrent-reader demand C_q

Each query q has a statically-known maximum number of **concurrent open
cursors** its operator tree can hold. This is computed by walking the tree:

| Operator | C_q formula |
|---|---|
| Source (leaf) | 1 (one cursor) |
| Join | C_parent + C_child (parent cursor held while child fetched) |
| UnionFanIn | Σ C_branch_i (all branch cursors open for streaming k-way merge) |
| FilterStart (EXISTS) | C_upstream + C_exists_child (upstream cursor held while EXISTS probe runs) |
| Filter / Skip / Take | C_upstream (transparent — one cursor at a time) |

**Cmax** = max C_q across all queries in the batch. Since all queries share
one pool (bound via `BindTableSourcesToReaderPool`, `engine.go:513-516`), the
pool must serve the worst-case query.

Cmax is computable at pipeline-build time (`engine.go:791-795` builds
pipelines before hydrate): walk each `pipelineEntry`'s operator tree,
compute C_q, take the max. This is O(operators per query) — negligible vs
the hydrate itself.

### 3c. Pool sized K = P × Cmax

Size the reader pool to K = P × Cmax. This replaces the current
`hydrateReaders` (default 1, `main.go:741`) with a computed value.

### 3d. Deadlock-freedom proof

**Theorem.** With P lanes and K = P × Cmax readers, no lane can ever
deadlock waiting for a reader.

**Proof.** Each lane holds at most Cmax readers at any instant (by
definition of Cmax). The worst case is all P lanes simultaneously holding
Cmax−1 readers and each needing 1 more:

- Total held: P × (Cmax − 1)
- Free: K − P × (Cmax − 1) = P × Cmax − P × (Cmax − 1) = **P**
- P lanes each need 1 reader; P are free → **all acquire immediately**.

No circular wait is possible. ∎

**Corollary.** In the common case (Cmax = 1, i.e., no joins or all-shim
Phase 1), K = P × 1 = P. This is exactly today's model (pool size = #concurrent
goroutines), just with bounded lanes instead of unbounded spawn.

### 3e. Reader lifetime = cursor lifetime

In the lazy model, a reader is held for the lifetime of its `iter.Seq`, not
just the `Fetch` call. The leaf seq uses:

```go
func (s *Source) Fetch(req FetchRequest) iter.Seq[Node] {
    return func(yield func(Node) bool) {
        r, err := pool.acquire(s.ctx)    // reader held across entire seq
        if err != nil { return }
        defer pool.release(r)            // released on exhaustion OR early stop
        rows, err := r.prepared(s.ctx, q.SQL).QueryContext(s.ctx, q.Params...)
        if err != nil { return }
        defer rows.Close()               // cursor closed on exhaustion OR early stop
        for rows.Next() {
            // ... scan row ...
            if !yield(node) { return }   // consumer stopped pulling → defers run
        }
    }
}
```

The `defer` chain runs when the seq function returns — on normal exhaustion
*and* on early stop (`yield` returns false). Abandoned-while-blocked is
handled by `s.ctx` cancellation (`source.go:317` closes ctx on `Source.Close`,
unblocking `sqlite3_step`), which causes the seq to return and defers to run.
This matches TS's `Stream<T> = Iterable<T>` cleanup model (`stream.ts:5-7`:
"we know when consumer has stopped iterating the stream. This allows us to
clean up resources like sql statements").

### 3f. No fallback — backpressure is safe

When the TS consumer is slow:
1. The lane's flush blocks on the bounded enqueue channel (single flusher,
   §3g).
2. The lane holds its private readers (up to Cmax, fully accounted in
   K = P × Cmax).
3. Other lanes are **not affected** — their readers are within the budget.
4. The stalled lane's readers are in the K = P × Cmax budget → no deadlock.
5. The 120s RPC timeout (`go-ivm-client.ts:916`) fires → socket closes →
   `s.ctx` cancelled → seq returns → `defer pool.release(r)` → readers
   returned.

This is exactly TS's pull-suspension model: a slow consumer suspends the
pull, holding its resources, while other consumers continue independently.
No spill, no keyset, no re-fetch, no fallback mode. The only "degradation" is
that the stalled lane pauses — which is correct backpressure, not a bug.

### 3g. Single flusher — unified write path

One goroutine owns the blocking `writeFrame(conn, data)` socket write,
draining a bounded `chan []byte`. Lanes encode chunks and enqueue to this
channel; the flusher owns the write. This replaces the current
`writeFrameLocked` + `writeMu` + `writerSem` + `outC` dispatch machinery
(`main.go:2104-2163`).

**The flusher must be the ONLY write path.** Today, `streamW`
(`main.go:2173-2179`) bypasses `outC` and calls `writeFrameLocked` directly,
while the terminal `"done"` response (`main.go:1761`) goes through `outC` →
`respCh` → `writeFrameLocked`. Both serialize on `writeMu`, so ordering is
preserved — but only by the mutex. With a single flusher, ordering is
**structural**: partials are enqueued synchronously by the lane *before it
finishes*; `"done"` is enqueued *after* `wg.Wait` (`engine.go:891`) returns
(all lanes complete) → FIFO guarantees partials precede `"done"`.

If `outC`'s writers kept calling `writeFrame` directly (bypassing the
flusher), `"done"` could overtake buffered partials → TS throws
`addQueriesStream chunk order violation` / `missing final chunk`
(`go-ivm-client.ts:315/345`). So: one flusher, no exceptions.

The buffer is bounded. When full (slow consumer), the enqueuer (lane) blocks
— which is §3f's backpressure scenario. This is safe because the lane's
readers are in budget.

### 3h. Snapshot consistency is free

All K readers in the pool are armed to the **same WAL frame** via existing
coread (`armConn`, `coread.go:233`) or convergence (`NewReaderPool`,
`reader_pool.go:82-91`). Each pool conn has its own `BEGIN` transaction
(`reader_pool.go:111`) pinning the frame. A lazy cursor lives inside this
existing `BEGIN` — **zero additional snapshot machinery needed**.

The pool is bound during the hydrate window (`BindTableSourcesToReaderPool`,
`engine.go:513`; unbound before the first advance, `UnbindReaderPool`,
`engine.go:526`/`source.go:1318`). `group.mu` is held for the entire batch
(`main.go:1714-1715`), so no advance can rotate the frame mid-hydrate. This
is unchanged from eager — lazy doesn't touch the snapshot discipline.

### 3i. WAL/frame-pin: no new concern (M4 retracted)

The v1 doc raised M4 (WAL growth from extended frame pinning under lazy).
**This concern is retracted.** The pool's K conns all `BEGIN` and pin the
frame for the **entire bind→unbind window** (`reader_pool.go:111`),
regardless of whether individual readers are actively scanning or sitting in
the free channel. A lazy cursor adds **zero** additional pin beyond what the
pool already holds. The frame is pinned from `BindTableSourcesToReaderPool`
to `UnbindReaderPool` — whether reads take 1ms (eager) or 10s (lazy with a
slow consumer). The 120s RPC timeout provides a hard cap on pin duration.

### 3j. Multi-conn now, single-conn-multi-cursor later

Go's `database/sql` allows one active `*sql.Rows` per `*sql.Conn`. So Cmax
concurrent cursors need Cmax connections — K = P × Cmax connections.

TS's `better-sqlite3` runs N cursors on one connection. Go *could* do the
same via `conn.Raw` accessing the underlying `*C.sqlite3` and running
multiple `sqlite3_stmt` on one conn (the machinery exists:
`snapshot.go:218` `rawSQLiteHandle`). This would collapse K from P × Cmax to
just P, reducing connection count and cache pressure.

**This is a refinement, not a fallback.** Ship multi-conn first (K = P × Cmax,
tuned `cache_size`, `SetMaxOpenConns(K)`). Single-conn-multi-cursor is a
later optimization that reduces connection count without changing the
streaming model. Both are the same design — just different connection
multiplicity.

---

## 4. Phased plan

### Phase 0 — Lanes + reader provisioning + single flusher *(prerequisite; ships alone)*

Three changes to the **eager** path, each independently shippable:

1. **Single flusher.** Replace `writeFrameLocked` + `writeMu` + `writerSem`
   + `outC` dispatch (`main.go:2104-2163`) with one flusher goroutine draining
   a bounded `chan []byte`. `streamW` enqueues to the flusher instead of
   calling `writeFrameLocked` directly (`main.go:2173-2179`). The `"done"`
   response (`main.go:1761`) enqueues to the same flusher after `wg.Wait`
   (`engine.go:891`). Remove `writerSem` (its sole job was capping concurrent
   *blocking* writes — there are now none). **One flusher, no exceptions.**

2. **Lanes.** Replace `engine.go:817-818`'s unbounded
   `go func(idx, entry) {...}` with P workers draining a `chan *pipelineEntry`.
   P is configurable (env `GO_IVM_HYDRATE_LANES`, default 4). At most P
   queries hydrate concurrently.

3. **Pool sizing.** Compute Cmax from the batch's query ASTs at
   pipeline-build time. Set K = P × Cmax (default Cmax=1 while operators are
   still eager → K = P). Replace `hydrateReaders` (`main.go:741`) with this
   computed K. Call `SetMaxOpenConns(K)` on the replica DB.

All three ship with the **eager** path — no lazy code, no `iter.Seq`, no
operator changes. They are pure improvements to eager (bounded parallelism,
non-blocking writes, right-sized pool).

**Verify:**
- Suite green.
- Throttled-TS-reader soak → one slow query no longer stalls others' writes
  (flusher decouples); block profile no longer shows `writeFrame` under
  `writeMu` on the hot path.
- Deadlock-freedom trivially holds (Cmax=1, K=P, ≤P lanes each need 1 reader).

### Phase 1 — Lazy leaf + compat shim *(zero memory delta)*

**Interface migration is big-bang.** `Input.Fetch` changes from
`[]Node` to `iter.Seq[Node]` in one commit; every implementor changes
together (`Source`, `Filter`, `FilterStart/End`, `Skip`, `Take`, `Join`,
`FlippedJoin`, `UnionFanIn`, `Exists`). Non-leaf operators adopt a **compat
shim**: `slices.Collect(input.Fetch(req))` → existing logic →
`return slices.Values(out)`. The divergence harness gates output identity.

Only the **leaf** (`internal/tablesource/Source`) gets a *real* lazy
implementation: a thin `iter.Seq` over the existing cursor, single open
cursor, yielding rows from `rows.Next()` (§3e). Limit-pushdown
(`source.go:1244`) becomes "consumer stops pulling → `yield` returns false
→ seq returns → defers run."

**Zero memory delta.** With the compat shim, the first operator above the
leaf immediately `Collect`s the seq → the full slice is still materialized,
just at a different call site. Phase 1's *only* purpose is correctness
isolation — proving the lazy leaf + cursor cleanup works before threading
laziness through the graph.

Cmax is still 1 (shim Collects immediately, so only one cursor open at a
time). Pool stays K = P × 1 = P. No deadlock risk.

**Verify:**
- Divergence harness green (proves the leaf+wire change is output-identical).
- Leak test: `Take(Limit=1)` over a 10k-row source → no reader leak (the
  seq's `defer pool.release(r)` fires on early stop).

### Phase 2 — Lazy operators *(the memory win)*

Remove the compat shims operator-by-operator. The order matters:

1. **Filter, Skip, Take** — mechanical. `Filter` strips Limit and breaks
   early (`filter_operators.go:87-95`); `Take` short-circuits on Limit
   (`take.go:115`). These are single-cursor (C_q = C_upstream), no new
   deadlock risk.

2. **UnionFanIn** — streaming k-way merge. All k branch seqs are pulled
   concurrently (each holding its cursor open). C_q = Σ C_branch_i. Uses
   `iter.Pull` (Go 1.23+) to get next/stop functions for each branch, then
   merges by comparing heads. This is the most cursor-hungry operator.

3. **Join, FlippedJoin, Exists** — the delicate ones. Lazy parent seq +
   per-parent child seq = exactly TS's model (`join.ts:120-127` fetch yields
   parent nodes with lazy relationship closures; `join.ts:256-262`
   `childStream` calls `child.fetch({constraint})` on demand). C_q =
   C_parent + C_child. N+1 stays N+1, now **interleaved** → better first-row
   latency.

After shims are removed, **Cmax jumps** from 1 to the actual tree depth /
union breadth. The pool-sizing code from Phase 0 kicks in: K = P × Cmax.
This is the phase where the deadlock-freedom proof is load-bearing.

**Verify:**
- Divergence harness.
- **Deadlock-freedom test:** single lazy 3-deep join (`A→B→C`) at P=1,
  K=Cmax=3 completes. At P=1, K=2 (=Cmax−1) it deadlocks (negative test
  proving the bound is tight).
- **Heap profile** on a fat hydrate now shows the full-result slice gone
  (peak ≈ chunk + buffer); pprof CPU (cgo `sqlite3_step` still dominates,
  `range-over-func` marginal).

### Phase 3 — (merged into Phase 0; no separate phase)

The single flusher and lanes ship together in Phase 0 as eager-path
improvements. No separate Phase 3 needed — the v1 doc's Phase 3 (spill +
frame-pin protection) is eliminated: spill is unneeded (§3f), and frame-pin
is a non-concern (§3i).

---

## 5. Correctness invariants (preserved at every phase)

| Invariant | Mechanism | Why lazy keeps it |
|---|---|---|
| Frame atomicity | single flusher owns `writeFrame` | Phase 0: per-frame ordering via FIFO; partials atomic |
| Per-query row order | one lane pulls one query's seq sequentially | `ChunkIndex` monotonic per query; cross-query interleave already allowed (completion order) |
| Snapshot isolation | all K readers armed to one WAL frame via coread (`coread.go:233`); advance blocked under `group.mu` (`main.go:1714`) during hydrate | lazy holds cursor on the *same* pinned frame; no frame movement |
| No cross-query races | per-query pipeline; `minRowVersions` snapshotted under `e.mu` (`engine.go:808`); per-op Exists cache | unchanged by lazy |
| Output parity (rows, order, EXISTS) | same cursor / same ORDER BY / same merge | divergence harness gates every phase |
| Deadlock-freedom | K = P × Cmax (§3d) | worst case: P lanes × (Cmax−1) held, P free, all acquire |
| Cursor cleanup on early stop | `defer rows.Close()` + `defer pool.release(r)` in seq function | defers run when seq returns (exhaustion, early `yield→false`, or ctx cancel) |
| Within-query failure atomicity | eager: `Fetch()` is all-or-nothing per query; lazy: partial chunks may precede an error | TS accumulator handles it (verified): `onResult` fires only on `final=true` (`go-ivm-client.ts:327`); `finalized` blocks post-final resurrection (`:280/329`); `finish()` runs only after the call resolves (`:920`), so an rpcError rejection skips the missing-final check. **Add test:** partial chunks → `DriftError` within one query. |

---

## 6. Perf gates (must pass before pre-prod)

A/B matrix on the load-test harness + VictoriaMetrics + pprof:

1. **Fat hydrate (the win):** large single query / `allTickets`-class. Expect
   ↓ peak heap, ↓ GC, ↓ time-to-first-row. Gate: no p99 regression.
2. **Many-small-query, K=P (the risk):** the 29–65-query batch. Gate: batch
   wall-time and CG advance latency **no worse than eager** — Phase 0's
   flusher + lanes must keep this neutral.
3. **Deep-join deadlock test:** 3-deep join at P=1, K=Cmax=3 completes; at
   K=2 it deadlocks (proves the bound is tight).
4. **Multi-CG parallel scaling:** confirm the GC-allocation win lifts the
   documented ceiling (re-run the GOGC A/B from
   `parallelization_gc_ceiling`).
5. **Stalled-consumer soak:** throttle/freeze a TS reader; confirm advance on
   that CG and all other CGs stay live (validates deadlock-freedom under
   backpressure), and the 120s timeout self-heals (readers released). Gate:
   no permanent reader leak.

---

## 7. Rollout

- Flag-gated: `GO_IVM_LAZY_HYDRATE` (default off). Phase 0 ships unflagged
  (it's a pure improvement to the eager path).
- Shadow first (divergence harness live), then sandbox multi-CG load, then
  pre-prod with the perf gates re-checked at real scale.
- Reversible per phase; the compat shim (Phase 1) makes the leaf change a
  no-op for output.

---

## 8. Risks & open questions

- **Connection count under deep joins.** K = P × Cmax can be large for deep
  join trees (3-deep join × P=8 → K=24 connections). SQLite handles this
  (each conn is lightweight), but `SetMaxOpenConns(K)` + `cache_size` tuning
  matters. The single-conn-multi-cursor refinement (§3j) collapses K to P —
  ship it if connection count becomes a bottleneck.
- **UnionFanIn streaming k-way merge complexity.** The current `MergeFetches`
  (`union_fan_in.go:181-218`) is a simple materialized merge. A streaming
  k-way merge via `iter.Pull` is more complex (k pull iterators, heap of
  heads, dedup). If k is typically small (2–3 OR branches), the complexity is
  manageable; if large, consider keeping UnionFanIn eager (compat shim) and
  accepting C_q = 1 for it, losing streaming only on union branches.
- **`range-over-func` overhead** at huge row counts — expected marginal vs
  cgo `sqlite3_step` (pprof-dominant), confirm in Phase 2.
- **Join child-push fan-out** (advance path, not hydrate): `pushChildChange`
  (`join.ts:231-244`, Go `join.go:202-213`) loops all matching parents,
  calling `output.push` per parent. The `[]Change` return can be large if
  a child row matches many parents (pathological on huge joins). This is a
  pre-existing property of the advance path — **not introduced by lazy
  fetch** — but worth noting: if it becomes a problem, `Output.Push` could
  be made lazy in a *separate* change (the advance path, not the hydrate
  path). Out of scope for this design.
- **`group.mu` whole-batch hold** (`main.go:1714`) — left as-is. With lanes +
  K = P × Cmax, the batch's SQL always makes progress (no deadlock), so the
  hold is bounded by SQL time + the 120s timeout on any stalled lane.
  Narrowing it is a separate lock-split (deferred).
- **Cmax computation correctness.** The C_q formula (§3b) is a static tree
  walk. It must over-approximate (never under-count) — if it under-counts,
  K < P × Cmax and deadlock can occur. Conservative: round Cmax up, and if
  uncertain about an operator, count it as C_parent + 1 (join-like). Add a
  debug assert: log C_q per query; if a lane blocks on pool acquire for >
  threshold, warn (potential under-count).

See also: `project_lazy_fetch_streaming_feasibility`,
`reference_parallelization_gc_ceiling`, `project_syncer_oom_unbounded_all`,
`project_wire_format_serialization`, `project_frame_skew_cross_batch_fix`.

---

## Appendix: What changed from v1

| v1 framing | v2 correction |
|---|---|
| "Reader-starvation / CG-stall" (transitive via `group.mu`) | **Deadlock** — a single lazy join self-deadlocks at K=1 before any stall |
| Spill-on-backpressure (drain seq to memory when buffer full) | **Eliminated** — seq can't be drained while deadlocked on reader acquire |
| Option A (keyset-chunked leaf) and Option B (K≥2 single-cursor) | **Deleted** — no fallback; one design (lanes + K=P×Cmax) |
| M4 (WAL growth from extended frame pinning) | **Retracted** — pool conns pin the frame for the entire bind→unbind window regardless of lazy/eager |
| m1 ("no Union operator in Go") | **Corrected** — `UnionFanIn.Fetch` exists (`union_fan_in.go:83`), is a k-way sorted merge, and is one of the worst multi-cursor holders |
| "Output.Push stays eager because advanceStream streams independently" | **Corrected** — `Output.Push` stays eager because TS's `push()` is control-only (`Stream<'yield'>`, no rows); per-push output is bounded by one change's amplification |
| Phase 0 = flusher; Phase 3 = spill | Phase 0 = flusher + lanes + pool sizing; Phase 3 **eliminated** |
