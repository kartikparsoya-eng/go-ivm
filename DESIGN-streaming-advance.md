# DESIGN: Flatten-in-Push for the Advance Path (Go-IVM)

Status: proposal v1 · Owner: TBD · Created 2026-07-01 · Companion to
DESIGN-streaming-hydrate.md

Convert the advance path's terminal sink from **"buffer eagerly-materialized
`Change` trees, flatten later"** to **"flatten each `Change` to `RowChange`s
synchronously during `Output.Push`"** — matching how TS's view-syncer
`Streamer` flattens inside the `#push` generator (`pipeline-driver.ts:5869`,
`.stream()` at `:5875`). This removes the eager `materializeChange`
deep-copy (`engine.go:1512`) and the Nodes-then-RowChanges double-hold, at
zero change to output rows/order.

This is **"Win 1"** of the streaming-advance analysis. It is a strict
prerequisite subset of **"Win 2"** (mid-push chunked flushing for the
pathological fan-out), which is deliberately deferred — see §7–§8.

---

## 1. Goal & non-goals

**Goal.** Delete `materializeChange` / `materializeNode` (`engine.go:1511-1547`)
and move the `streamChanges` flatten (`streamer.go:106`) from `Streamer.Stream()`
(`streamer.go:90`, runs *after* the push) into `Streamer.Accumulate()`
(`streamer.go:79`, runs *during* the push). Net effect:

- Terminal-sink working set drops: we stop holding the materialized `Node`
  relationship subtree **and** its flattened `RowChange`s simultaneously.
  *Measured* (§11): total advance allocation −12% bytes / −25% allocs /
  −12–20% wall-time on a popular-parent fan-out. (The earlier "~2× peak"
  estimate was about the sink's own working set, not whole-advance peak —
  see §11 for the honest, measured framing.)
- One fewer deep copy of every pushed change's relationship tree
  (`slices.Collect` at `engine.go:1534`, recursive at `:1539`). The profile
  attributes ~32% of the OLD path's sampled `alloc_space` to
  `materializeChange` (§11).
- The flatten parallelizes across push goroutines instead of running
  single-threaded in `Stream()`.
- Removes a Go-specific divergence from TS (TS never materializes; it
  streams relationship children lazily through the `#streamNodes` generator,
  `pipeline-driver.ts:6022-6024`).

**Non-goals.**
- **Not** changing output rows, types, or wire order. `streamChanges` /
  `streamNodesInto` are called verbatim; only *when* they run moves.
- **Not** touching operator `Output.Push` signatures — they stay
  `[]Change` (`operator.go:94`). The terminal `pipelineOutput.Push` already
  returns `nil` (`engine.go:1446`); it keeps doing so.
- **Not** adding mid-push chunked flushing (that is Win 2, §7). A single
  source-change whose fan-out exceeds `advanceChunkSize` still buffers
  intact — exactly as today (`engine.go:1343-1346`), and exactly as hydrate
  does for a single node's children.
- **Not** changing the hydrate path — hydrate already flattens lazily with
  no `materializeChange` (`engine.go:897-904`, `streamer.go:227-230`). This
  change brings advance to hydrate's model.

---

## 2. Background: the Push→Stream split and why it forces eager materialize

The advance path today runs, per source-change (`engine.go:1158-1169`,
mirrored in `AdvanceStream` at `:1327-1350`):

```go
source.Push(sc)                    // 1160 — whole push runs, overlay live
rowChanges := e.streamer.Stream()  // 1161 — flatten AFTER, overlay gone
```

`source.Push` fans a `Change` through every connected pipeline; each
pipeline's terminal `pipelineOutput.Push` (`engine.go:1441`) fires **during**
the push. But the flatten (`Stream()`) runs **after** `Push` returns.

Between those two lines the source clears its mutation overlay and commits
the write:

- Sequential: `genPush` sets `ms.overlay.Store(&Overlay{...})`
  (`source.go:329`) and clears it via `defer ms.overlay.Store(nil)`
  (`source.go:314`); `genPushAndWrite` then calls `ms.writeChange(change)`
  (`source.go:272`) after `genPush` returns.
- Parallel: `GenPushParallel` does the same — `ms.overlay.Store` at
  `parallel.go:67`, `defer …Store(nil)` at `:68`, `writeChange` at
  `parallel.go:225`.

So by the time `Stream()` runs, **the overlay is nil and the change is
already written to `ms.data`.** A relationship closure that reads the overlay
(or in-progress join state, §3) would resolve against the *wrong* snapshot.

The legacy code works around this by **eagerly snapshotting** every pushed
change's relationship subtree while the overlay is still live:
`pipelineOutput.Push` calls `materializeChange(change)` (`engine.go:1444`),
which `slices.Collect`s every lazy relationship closure recursively into a
concrete `[]Node` (`materializeNode`, `engine.go:1526-1547`, comment:
*"Evaluate the closure NOW (while overlay is active)"* at `:1531`). The
deferred `Stream()` then walks those concrete slices safely.

**The eager materialize is therefore a *consequence* of the Push→Stream
split, not a semantic requirement.** Remove the split (flatten during push)
and the materialize is unnecessary.

---

## 3. The correctness invariant: flatten MUST happen inside `Output.Push`

The tempting shortcut — keep the lazy closures and re-resolve them in a
deferred `Stream()` — is **wrong**. Two independent push-scoped states make
the flatten valid *only* during `Output.Push`:

### 3a. Join in-progress child change (the load-bearing one)

`Join.pushChildChange` sets `j.inprogressChildChange = &change` and clears it
via `defer` (`join.go:201-203`). The child relationship closure built by
`processParentNode` (`join.go:227`) reads that field (`join.go:234`) to
splice the in-progress child change into the parent's child stream. It loops
**all** matching parents (`join.go:211`), pushing one `ChildChange` per
parent (`join.go:220`).

If the flatten is deferred past `Push`:
- `j.inprogressChildChange` is already `nil` (the `defer` fired).
- The child closure falls back to fetching from the child source — where
  `writeChange` has, for a Remove, **already deleted the row**.
- → the changed child's `RowChange` is **silently dropped**.

This is the real "removed row becomes invisible" hazard. It is mediated by
join push-state + child-source `writeChange`, **not** by the parent source
overlay (see 3c).

### 3b. Batch remutation

`Advance` / `AdvanceStream` loop multiple source-changes
(`engine.go:1146` / `:1319`). Source-change *N+1*'s `Push` remutates
`ms.data`. Any flatten deferred past source-change *N*'s `Push` would read
*N+1*'s state. The per-source-change `Stream()` today already relies on this
ordering; flattening in-push preserves it trivially (each change is flattened
before the next `Push`).

### 3c. Note on the parent overlay (what it does NOT do)

Worth stating precisely to avoid a wrong mental model: for a Remove, the
source overlay makes the removed row **invisible** during the push, not
visible. `computeOverlays` records `o.Remove` (`source.go:642`) and
`generateWithOverlayInner` **skips** that row (`source.go:709-714`,
`removeSkipped → continue`). So overlay-active and post-`writeChange` *agree*
that the row is gone. The removed row's own `RowChange` is emitted from the
in-hand `change.Node` (carried by the push), keyed by its PK with no row body
(`streamer.go:201`, `RowChangeRemove`) — it is **not** re-fetched. Hence the
overlay is not what protects the Remove emission; §3a (join state) is.

**Conclusion.** The flatten must run while `Output.Push` is on the stack.
`Streamer.Accumulate` is called exactly there (`engine.go:1445`), so moving
the flatten into `Accumulate` is both correct and the minimal change.

---

## 4. Why the closures are safe to walk during push (no new hazard)

`materializeNode` **already** iterates every relationship closure during the
push (`fn()` then `slices.Collect(seq)`, `engine.go:1533-1534`). The new code
path calls `streamNodesInto`, which iterates the same closures the same way
(`for childNode := range seq`, `streamer.go:227-230`). **Identical fetch
timing, identical overlay/join-state visibility, identical re-entrancy** — we
are not introducing any new access, only skipping the intermediate concrete
copy. If materialize is deadlock-free and correct today (it ships), the
in-push flatten is too.

Output equivalence: `streamNodesInto` is the *same* tree walk in both worlds.
Today it walks materialized closures (`slices.Values(captured)`,
`engine.go:1543`); after, it walks the original closures. Materialize is an
identity-preserving eager copy, so the emitted `RowChange` sequence is
byte-for-byte the same. Sibling-relationship order is fixed by
`streamNodesInto`'s own `sort.Strings(relNames)` (`streamer.go:221`),
independent of materialize, so wire order is unchanged.

---

## 5. Design: flatten in `Streamer.Accumulate`

The `Streamer` stops buffering `Change` trees and buffers flat `RowChange`s
instead. `Accumulate` does the flatten (synchronously, on the caller's push
goroutine); `Stream` becomes a hand-off of the accumulated rows.

```go
type Streamer struct {
    mu   sync.Mutex
    rows []RowChange     // was: accumulated []accEntry
}

func (s *Streamer) Accumulate(queryID string, schema *ivm.SourceSchema, changes []ivm.Change) {
    // Flatten NOW — overlay + join.inprogressChildChange are live only for
    // the duration of the Output.Push that called us (§3). streamChanges
    // walks the lazy relationship closures exactly as materializeNode did,
    // minus the intermediate concrete copy (§4).
    flat := streamChanges(queryID, schema, changes)
    if len(flat) == 0 {
        return
    }
    s.mu.Lock()
    s.rows = append(s.rows, flat...)
    s.mu.Unlock()
}

func (s *Streamer) Stream() []RowChange {
    s.mu.Lock()
    defer s.mu.Unlock()
    out := s.rows
    s.rows = nil       // hand ownership to caller; next Accumulate re-allocs
    return out
}
```

`pipelineOutput.Push` drops the materialize:

```go
func (po *pipelineOutput) Push(change ivm.Change, pusher ivm.InputBase) []ivm.Change {
    po.engine.streamer.Accumulate(po.queryID, po.schema, []ivm.Change{change})
    return nil
}
```

`materializeChange` / `materializeNode` are deleted; the now-unused `iter`
and `slices` imports come out of `engine.go` (their only uses were in
`materializeNode`).

All six `Stream()` call sites (`engine.go:1127, 1137, 1161, 1298, 1312, 1330`)
are unchanged — they still get a `[]RowChange`. The drift-drain sites
(`:1127`, `:1298`) still work: rows flattened by successful pushes before the
panic are already in `s.rows`, and `Stream()` drains them.

### 5a. Why keep the central `Streamer` (not per-query buffers)

A fully TS-shaped port would give each pipeline its own buffer and let the
engine gather after `wg.Wait`, deleting the mutex. In TS that is *less*
machinery because the view-syncer swaps a single `#streamer` per push step
(`pipeline-driver.ts:5869`, `#startAccumulating`/`#stopAccumulating`). In
**Go** it is *more* machinery: the engine gathers via `e.streamer.Stream()`
at six sites; per-query buffers would require the engine to enumerate and
drain every active `pipelineOutput` per source-change. The central
`Streamer` with an in-`Accumulate` flatten keeps the existing gather points
and shrinks the mutex critical section to a single slice append. The append
lock is negligible for dashboard fan-out (2–3 queries/source); removing it
entirely is not worth the restructure. Determinism is unchanged (still
lock-acquisition order under parallel push — §6).

---

## 6. Parallel-push safety

Under `GenPushParallel` (`parallel.go:100-116`), N goroutines each drive one
connection's `FilterPush` → `conn.Output.Push` → `pipelineOutput.Push` →
`Accumulate`. Safety of the new `Accumulate`:

- **Flatten is goroutine-local.** `streamChanges` reads only the `change`
  argument and that connection's own join state (`j.inprogressChildChange`
  belongs to that pipeline's Join, driven by this goroutine). No cross-
  goroutine shared mutable state — the same property that already makes
  parallel materialize safe (`parallel.go:3-5`).
- **Append is mutex-guarded.** `s.mu` serializes only the `append`, and the
  whole flattened slice is appended under one acquisition, so a single
  `Accumulate` call's rows stay contiguous (intra-call order preserved —
  D8 part 1, `streamer.go:50-52`).
- **Cross-call order** stays lock-acquisition order, i.e. non-deterministic
  under parallel push — unchanged from today (`streamer.go:53-57`). Callers
  needing determinism already sort (shadow-compare).
- **Flatten now parallelizes.** Today `Stream()` flattens every buffered
  entry single-threaded after the push; now each goroutine flattens its own
  change concurrently. Strictly more parallel work, smaller serial tail.

`TestStreamer_AccumulateConcurrentNoLoss` (`streamer_ordering_test.go:46`,
32 goroutines × 50 appends, `-race`) is the ready-made regression guard for
this.

---

## 7. Win 1 vs Win 2 — what this does and does NOT fix

| | Win 1 (this doc) | Win 2 (deferred) |
|---|---|---|
| Change | delete materialize, flatten-in-push | stream `RowChange`s → bounded channel → mid-push flusher |
| Removes | Nodes-then-RowChanges double-hold | holding the whole per-advance result |
| Complexity | Low — net code removal | High — reintroduces a channel+flusher, must respect parallel push + the fan-in accumulation barrier (`fan_in.go:62`) |
| Peak memory | ~2× cushion (constant) | asymptotic bound on a single source-change's fan-out |
| When it matters | always (faithfulness + peak) | only genuine >`advanceChunkSize` single-source-change fan-out |

Win 1 shrinks peak by a constant factor and is the honest TS port. It does
**not** asymptotically bound the pathological case where one source-change
fans out to >10k `RowChange`s — that output is still buffered in `s.rows`
until the next `Stream()`. Bounding *that* needs Win 2's mid-push flush,
which requires threading a flush callback through the push and merging
per-connection output under parallel push. Gate Win 2 on a profile that
actually shows a single advance's buffer blowing past `advanceChunkSize`.

Note Win 1's peak cushion is *constant* precisely on the multiplicative case
(§8): it removes the double-hold but not the parent count. Do not oversell it
as a fix for the spike; it is a faithfulness + steady-state-peak win that
also de-risks Win 2 (both require flatten-in-push).

---

## 8. The multiplicative fan-out trigger (why the spike is real)

The worst case is **not** only "one row with 10k children." `pushChildChange`
loops **all** parents matching a child change (`join.go:211`), and each
parent's `processParentNode` subtree is flattened
(`join.go:214,220`) — so a single child change hitting a **popular parent
set** (a widely-referenced entity) is multiplicative: `#parents × subtree`.
All of it lands in `s.rows` before the per-source-change `Stream()` drains.

This is a more realistic path to a memory spike than a single fat node, and
it argues for doing Win 1 sooner (constant cushion + prerequisite). But note
the correlation: a popular-parent workload has many pipeline connections →
crosses `GO_IVM_PARALLEL_THRESHOLD` (`source.go:203`) → the parallel push
path, where a mid-push flush (Win 2) needs exactly the bounded-channel +
single-flusher merge. So the multiplicative trigger and the parallel path are
correlated, and Win 2 — if built — is what bounds `#parents` there.

---

## 9. Test plan

Gate: `go build ./... && gofmt -l . && go test ./ivm/ ./engine/...`
(engine included because the edit is in `engine/`).

Regression coverage already present:
- **Streamer unit** — `engine_test.go:268` (`TestStreamer`),
  `streamer_ordering_test.go` (intra-call order, concurrent-no-loss, reset),
  `streamer_scalar_test.go` + `streamer_isscalar_test.go` (scalar/permissions
  suppression via `streamChanges`).
- **Full push→streamer path** — `advance_streaming_test.go`,
  `advance_drift_complex_test.go`,
  `advance_drift_latest_conversation_test.go` (nested relationships),
  `exists_deep_coverage_test.go`, `exists_compound_repro_test.go`,
  `channelstats_*_test.go` (real-world shapes),
  `tablesource_streaming_test.go`, `duplicate_remove_test.go`,
  `drift_recovery_test.go` (drift-drain sites still surface partial output).

These exercise joins, exists, nested relationships, and drift recovery
through the terminal sink — the exact paths whose flatten timing moves. Green
across them = output-equivalence held.

Run with `-race` to exercise the parallel-push append (`go test -race
./engine/...`).

---

## 10. Rollout / risk

- **Risk: low.** Net code removal; no signature or output changes; six
  `Stream()` sites untouched; strong existing test coverage.
- **Behavioral equivalence** rests on §4 (materialize is an identity-
  preserving copy that `streamNodesInto` walks the same either way). If a
  latent case exists where materialize's by-reference `Child` handling
  (`engine.go:1515`, it copies `change.Child` without recursing) diverged
  from a live walk, the in-push flatten resolves it *toward* TS semantics
  (TS walks children live) — correct-or-better, never worse.
- **No flag.** Like hydrate's lazy fetch, this is a straight replacement of
  an internal mechanism with its faithful equivalent; there is no cutover to
  stage.
- **Reversibility.** Single-commit revert restores `materializeChange` and
  the `accEntry` buffer if a drift audit ever disagrees.

---

## 11. Measured results (2026-07-01, Apple M4 Pro, go1.26)

Benchmark `BenchmarkAdvanceFanout_*` (`engine/advance_fanout_bench_test.go`):
a **popular-parent** shape — one `users` row (`u1`) with `childCount` `posts`
correlated to it, query `users` ⟕ `posts` (top-level `Related`). Each op is
one ADD(u1) + one REMOVE(u1); both changes carry the full `childCount`-wide
subtree through `pipelineOutput.Push`. `-benchmem`, `-count=6`, A/B by
`git stash` of the two edited files (OLD = `materializeChange`, NEW =
flatten-in-push).

| childCount | metric    | OLD (materialize) | NEW (flatten) | Δ      |
|-----------:|-----------|------------------:|--------------:|-------:|
| 100        | ns/op     |            69,012 |        60,369 | −12.5% |
| 100        | B/op      |           188,851 |       165,724 | −12.2% |
| 100        | allocs/op |               948 |           712 | −24.9% |
| 1000       | ns/op     |           590,325 |       495,006 | −16.1% |
| 1000       | B/op      |         1,674,965 |     1,474,625 | −12.0% |
| 1000       | allocs/op |             8,180 |         6,138 | −25.0% |
| 5000       | ns/op     |         2,865,774 |     2,307,153 | −19.5% |
| 5000       | B/op      |         9,787,204 |     8,662,177 | −11.5% |
| 5000       | allocs/op |            40,227 |        30,177 | −25.0% |

Consistent across scale: **−25% allocations, −12% bytes, −12–20% wall-time.**
Variance was low (<3% run-to-run); the win grows with fan-out width on time.

**Memory-profile attribution (OLD, childCount=5000, `alloc_space`):**
`materializeChange` cumulative = **1514 MB = 31.9%** of sampled allocation —
the single largest removable cost on the OLD path. Breakdown inside
`materializeNode`: `make(map…)` 238 MB + `slices.Collect(seq)` 1.17 GB +
`make([]Node…)` 81 MB. In NEW these vanish from the profile entirely
(`materializeChange` count = 0).

**Why B/op only drops ~12% while the profile attributes ~32% to materialize.**
`-benchmem` B/op is *total* (cumulative) bytes allocated, **not** peak live
heap — so it measures allocation *volume*, and this A/B is the ground truth
for that. Removing materialize does **not** remove child-node *generation*
(`generateWithOverlayInner`, ~10% in both) — those nodes are still produced
on demand inside `streamNodesInto`. What it removes is the redundant concrete
*copy* (maps, collected slices, wrapper closures) **and** the simultaneous
*retention* of the whole subtree beside the flattened rows. The 32% profile
share is the gross copy path; the 12% B/op delta is the net after the
still-required on-demand generation. **Peak live heap was not directly
measured** (would need mid-push `HeapInuse` sampling); the retention win is
bounded above by that 32% and is a strict improvement, but the original
"~2×" figure should be read as the sink working-set intuition, not a
measured whole-advance peak.

### 11a. Win-2 gate verdict — NOT justified by this profile

`TestAdvanceFanout_RowCount` measures the **un-splittable unit**: a single
popular-parent ADD emits `childCount + 1` RowChanges in one source-change,
which the advance loop does **not** split (§7).

| childCount | RowChanges (1 source-change) | advanceChunkSize | crosses? |
|-----------:|-----------------------------:|-----------------:|:--------:|
| 100        | 101                          | 10,000           | no       |
| 1000       | 1,001                        | 10,000           | no       |
| 5000       | 5,001                        | 10,000           | no       |

Even a 5,000-child parent stays comfortably under the 10k chunk threshold in
one frame. A *single* source-change only crosses the boundary at ~10k
children under one parent, or via the **multiplicative** `#parents × subtree`
path (§8) — neither of which this profile, nor any production trace to date,
exhibits. **Decision: do not build Win 2 now.** Re-open only if a production
advance profile shows a single source-change's buffer exceeding
`advanceChunkSize` (watch `pendingBytes`/`len(pending)` at `engine.go:1347`).

Reproduce:

```
go test ./engine/ -run '^$' -bench 'AdvanceFanout' -benchmem -count=6
go test ./engine/ -run TestAdvanceFanout_RowCount -v
# A/B: git stash push engine/streamer.go engine/engine.go  → rerun → stash pop
```
