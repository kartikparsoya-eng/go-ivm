# Performance Review — Go IVM Architecture

Date: 2026-05-21 (v2 — profile-driven revision)
Reviewer: post-streaming-PR audit
Scope: `engine/`, `ivm/`, `builder/`, `sqlite/`, `cmd/sidecar/` — production-readiness perf pass.

## Current state (2026-06-09 in-depth review)

Re-verified against source at HEAD `be2f2a1`. CPU is not the constraint (≈17% busy
at 20 users); allocation volume drives GC which drives cost. Standing posture:

- **Pool + fail-fast shipped (`be2f2a1`):** writable conn pool raised to 256 and
  `prevConn` acquire bounded to 30s (`internal/tablesource/source.go:324`,
  `internal/tablesource/db.go:33-41`). This raised headroom and turned exhaustion
  into a fast, diagnosable error instead of a silent 120s TS-side RPC timeout. It
  is **not** a leak fix — see the next bullet.
- **⚠ Pool-exhaustion root cause was a conn/tx LEAK — now FIXED.** Group
  teardown/re-init never closed tablesource `prevConn`/tx (the engine `Source`
  interface had no `Close()`; `Input.Destroy()`→`disconnect()` only unlinked the
  connection from the slice). ≈7 writable conns + open txns leaked per CG
  teardown — under sustained churn the 256 pool would still exhaust, just later.
  **Fixed this session:** `Close()` added to the engine `Source` interface and
  `Engine.Close()` now closes every leaf (tablesource rolls back + returns the
  conn; MemorySource no-ops). All teardown paths funnel through `engine.Close()`,
  so this plugs it everywhere. Regression test
  `engine/close_releases_source_test.go` asserts `wdb.Stats().InUse==0` after
  Close and is negative-verified (fails without the fix). Detail in
  PORT-AUDIT-FIXES.md (2026-06-09) and DESIGN-tablesource-port.md.
- **❌ Fix B (separate read-only pool for hydrate) REJECTED — profiling evidence.**
  60s mutex profile under 20-user load = 7.99ms total contention (0.013%); ZERO
  samples in `tablesource`/`fetchForConn`/`s.mu`. Block profile = 100% idle channel
  waits (request-bound, not lock-bound). A read-only pool would add complexity for
  no measurable win. If perf ever matters the targets are msgpack response encode
  (≈66% of the tiny lock budget) and per-row allocs (the `row := make(ivm.Row)` map
  at `source.go:929` is the #1 allocator), profiled under drive mode.
- **By design, not a regression — drive-mode parallel hydrate serializes on one
  conn.** `AddQueriesStream` (`engine/engine.go:720-789`) spawns one hydrate
  goroutine per query; in drive mode `BindTableSourcesToConn` (`engine.go:433-438`)
  binds every leaf to ONE shared `*sql.Conn` and `fetchForConn` holds `s.mu` across
  the whole SELECT+Scan+row-map (`source.go:870→1025`), so they serialize. This is
  intentional for frame consistency (the Snapshotter owns the pinned BEGIN
  CONCURRENT frame). Shadow-mode hydrate is unaffected (each source re-pins its own
  prev tx). Relevant to perf expectations, not a bug.

---

## Bottom line (v4 — post-soak validation)

Status as of 2026-05-21:
- ✅ **T1-1 (typed Row decode) SHIPPED and SOAK-VALIDATED.** 41% drop in total allocations on the 60s capture. Holds up under 2× 30-minute sustained-load soaks (unbounded + bounded) with 0 mismatches, 0 panics, 0 restarts. Custom `Row.DecodeMsgpack` in `ivm/data.go` replaces the bulk of the reflection-walk normalize cost.
- ❌ **P1-NEW (slice-backed Row) REJECTED on porting-fidelity grounds.** TS uses the same `map[string]Value` shape on the hot path; the 50%-of-CPU comparator cost is a V8-vs-Go runtime idiom gap, not a porting bug. Bounded-soak proved it: at constant working set, advance p50 is **218µs** (not 42ms). The comparator cost only matters when the data set grows — that's workload physics.
- ⏸ **T1-2 / T2-1 / T2-2 / T2-3** — all show zero measurable signal at current workloads, including 30-minute sustained-load soaks. Speculatively-architectural; do not pursue until a profile under truly higher concurrency points there.

Open Tier-1 items remaining: T1-3 (streamNodes pre-sizing), T1-4 (FetchRequest.Limit), T1-5 / T1-6 / T1-7 cleanups. None show in the profile top-15; all are safe, low-LoC cleanups worth doing as small PRs but with no measurable urgency.

## REPROFILE (2026-06-04, 20-user Go-primary lean) — new #1 allocator surfaced

Re-profiled under the post-snapshotter-port Go-primary lean path at **20 users**
(the doc's recommended "heavier workload" step). T1-3 + T1-5 were already shipped
(`63cdd8d`); the old T1-* items still don't appear in the top-15. The hot path has
**moved** to the snapshotter-era tablesource read, which didn't exist at the
2026-05-21 baseline:

- **CPU only ~17% busy at 20 users** — not CPU-bound. The CPU that exists is
  SQLite cgo (~26%) + GC (~20% gcDrain/scan/malloc), and the GC is driven by
  allocation volume → allocations are the lever.
- **`tablesource.(*Source).fetchForConn` = 59% of all allocations** (1.82GB flat
  / 2.55GB cum of 4.38GB over 60s). Line-level: the per-row `row := make(ivm.Row)`
  map is 1.22GB (intrinsic to the `map[string]Value` Row — the *rejected* P1-NEW),
  but the next two lines were the per-row `raw`/`ptrs` `[]any` scan buffers at
  **309MB + 280MB = 589MB**, allocated fresh every single row.

✅ **SHIPPED + RE-PROFILE-VALIDATED (`d1bb2ea`): reuse fetchForConn scan buffers.**
Hoisted `raw`/`ptrs` (+ the `&raw[i]` setup) out of the `rows.Next()` loop — they're
fixed-shape per query, and `FromSQLiteType` copies values out so reuse is safe.
Built the amd64 sidecar (Dockerfile, `-tags libsqlite3`), deployed over the baked
binary, and re-profiled under identical 20-user load:

| metric | before | after |
|---|---|---|
| scan-buffer lines (`raw`+`ptrs`) | 589 MB | **21.5 MB** (−96%) |
| `fetchForConn` flat | 1.82 GB (42.6%) | **1.15 GB (32.8%)** |
| **total sidecar allocations / 60s** | 4.38 GB | **3.60 GB (−17.9%)** |
| sidecar CPU busy | 17.4% | 14.1% |

The −17.9% total exceeds the −13% pre-estimate (the 589MB scan-buffer cut plus
reduced downstream GC re-churn; some run variance). 0 mismatches / 0 errors in both
runs. The only remaining large allocation lever is the 1.22GB per-row Row map =
revisiting the Row representation (P1-NEW, rejected on TS-parity grounds).
Artifacts: `/tmp/profile-20u/` (before), `/tmp/profile-20u-after/` (after).

**Shadow-mode log demotion (small latency cleanup, post-T1-1).** Two per-event info logs in `pipeline-driver.ts` (`#shadowAddQuery` hydrate timing at L1526 and `#shadowAdvance` PERF line at L1609) were demoted info → debug. They fire once per hydrate query and once per advance respectively, contributing ~100-300µs of synchronous template-string + sink work per advance in shadow mode. Mismatch logs are all at `error` level inside `#shadowCompare` and unaffected. The batch summary (`[shadow][batch-stream]` at L918) is kept at info because it's 1 line per batch — useful in healthy logs without dominating volume. Net effect: shadow-mode log volume in production drops ~95%, tail-latency shaved by the synchronous-log cost.

**The takeaway from the v2→v4 cycle:** read-only review predicted "engine lock granularity and streamer contention" as HIGH. Profile said neither was real, and the actual #1 allocation hotspot (msgpack walk overhead) needed a targeted fix that turned out small and correctness-clean. Sustained-load soak confirmed the fix and ruled out leaks. Always profile before you refactor; always soak before you call a fix done.

**Correctness-first ranking:** every finding below was re-read against the actual code to verify the proposed fix doesn't regress correctness. T1-1's safety was validated by a full `go test ./...` pass plus **two 30-minute soaks with 0 mismatches across 1 hour of sustained load** (unbounded + bounded).

---

## Profile baseline (2026-05-21)

Captured during a 15-client live load (`/app/load-gen-v3.cjs` run from inside the sync-proxy container, mixed simple + join queries cycling every ~4s, concurrent 3000-row PG insert burst). Sidecar built with `_ "net/http/pprof"` and `GO_IVM_PPROF_ADDR=0.0.0.0:6060`, block + mutex sample rates set to 1.

Peak window observed: `[GO-IVM][PERF] advances=15 (p50=43.8ms p95=143ms peakConc=4) hydrates=109 (p50=994µs p95=24.5ms peakConc=5)`.

Profiles saved in `/tmp/go-ivm-profiles/{cpu,allocs,block,mutex,heap-t0,goroutines}` — re-runnable via `go tool pprof -http=:8080 <file>`.

### CPU (5.07s out of 60s captured — sidecar ~8% busy)

| % cum | function | category |
|------:|----------|----------|
| **51%** | `(*MemorySource).getSortedRows.MakeComparator.func2` | row comparator |
| 41% | `runtime.mapaccess1_faststr` | string-keyed map lookups (called from comparator) |
| 13% | `(*mspan).typePointersOfUnchecked` | GC pointer-walk |
| 9% | `runtime.memclrNoHeapPointers` | new-allocation zeroing |
| 9% | `ivm.CompareValues` | numeric/string type coercion |

**Single top finding:** the row sort comparator at `ivm/data.go:142` (`return func(a, b Row) int { ... CompareValues(a[field], b[field]) }`) is the dominant cycle sink. Every sort/orderBy walks N×M times into a `map[string]interface{}`. Promoted to **P1-NEW** below.

### Allocations (1.29GB total over capture window)

| MB | % | function |
|---:|---:|----------|
| 412MB | 32% | `msgpack.Decoder.readN` |
| 218MB | 17% | `msgpack.Decoder.DecodeMap` |
| 217MB | 17% (cum) | `Server.handleLoadRows` |
| 107MB | 8% | `reflect.unsafe_New` (← the normalize walk) |
| 89MB | 7% | `readFrame` |

msgpack decoding is ~50% of total allocations; the reflection walk is 8% (real but smaller than I estimated). T1-1 (typed decode) addresses both.

### Mutex contention

**90.51ms total contention across all locks in 60s** (~1.5% of one CPU). Top callers:
- `AddQueriesStream.func1` — 9.15ms (10% of the contention budget, i.e. ~9ms total)
- `(*Join).Fetch` — 1.99ms
- `(*SourceInput).Fetch` — 0.81ms

**`Streamer.Accumulate` does not appear.** The v1-HIGH "streamer contention" claim was wrong for this workload.

### Block profile

Dominated by `runtime.chanrecv2` (~60%) and `runtime.selectgo` (~22%) — these are idle goroutines parking on work-queue channels, not blocking on locks. Sub-paths: SQL connection pool waits and OTel batch processor channels. No engine.mu blocking. **The v1-HIGH "engine.mu held across hydration" claim has zero measured signal at this workload.**

### Caveats on the profile

- **Workload limits.** 15 clients with concurrent advances surfaces SOME parallelism (peakConc=5 hydrates, peakConc=4 advances) but is still far below what a 100-client production deployment would do. Streamer contention (T1-2) and engine.mu (T2-1) could still bite at higher concurrency. Treat their downgrade as "not a current bottleneck," not "never a bottleneck."
- **Sidecar 8% busy.** Most of the time the sidecar is waiting for work. Hot-loop optimizations save ~8× more than they look here once the sidecar becomes CPU-bound.
- **Network on host broken (OrbStack port forwarding).** Couldn't drive load via the normal Traefik path; load-gen ran inside the sync-proxy container talking directly to `xyne-sandbox-rust-test-sync-proxy:8080`. Same TCP path through the proxy, just bypasses the host port mapping.
- **Profile is one point-in-time** under one workload shape. Other workloads (heavy advance, sparse hydrate, many small cgs) will show different hot paths. Re-profile when changing the workload mix.

---

## Sustained-load validation (2026-05-21)

Two 30-minute soaks under the same 15-client + continuous-PG-mutator load. Both with T1-1 applied + shared-sidecar + `ZERO_NUM_SYNC_WORKERS=1`.

### Run A — unbounded mutator (table grew ~18k rows)

| Time | Advance p50 | Advance p95 | Hydrate p50 | Hydrate p95 | Hydrate max | peakConc (adv) |
|-----:|------------:|------------:|------------:|------------:|------------:|---------------:|
| T+30s | 7ms | 22ms | 1.5ms | 38ms | 71ms | 12 |
| T+5m  | 7ms | 14ms | 1.4ms | 59ms | 77ms | 12 |
| T+10m | 13ms | 21ms | 2.2ms | 69ms | 87ms | 15 |
| T+15m | 14ms | 22ms | 1.7ms | 74ms | 93ms | 14 |
| T+20m | 19ms | 27ms | 1.9ms | 109ms | 132ms | 14 |
| T+25m | 23ms | 31ms | 2.3ms | 107ms | 295ms | 15 |
| T+30m | 42ms | 55ms | 3.4ms | 165ms | 350ms | 15 |

Latency drifted 6× for advance, 4× for hydrate over 30 min. Heap diff: **+470MB** (`Row.DecodeMsgpack` retained 326MB of working-set rows).

### Run B — bounded mutator (table held flat: insert 50 + delete 50 oldest per iteration)

| Time | Advance p50 | Advance p95 | Hydrate p50 | Hydrate p95 | Hydrate max | peakConc (adv) |
|-----:|------------:|------------:|------------:|------------:|------------:|---------------:|
| T+10m | **47µs** | **212µs** | 1.5ms | 8.2ms | 45ms | 8 |
| T+20m | **19µs** | **283µs** | 1.3ms | 6.4ms | 14ms | 6 |
| T+30m | **218µs** | **983µs** | 1.9ms | 11.4ms | 18ms | 8 |

Latency **flat across 30 minutes**. Heap diff: **+40MB** (12× less). Some sub-allocations actually shrank between t0 and t30m (GC reclaiming).

### What the two runs together prove

1. **No bounded-working-set leak in Go IVM.** Constant table size ⇒ constant latency, constant heap growth rate.
2. **The unbounded latency drift was workload physics** — `messages orderBy createdAt desc limit 20` sort cost scales with table size; not a sidecar bug.
3. **T1-1 holds under 1 hour of continuous load.** No regressions, no panics, no restarts, no drift-audit mismatches across either soak.
4. **Per-operation cost ceiling for Go IVM (this load shape, this hardware) when working set is constant:** advance sub-millisecond, hydrate low-single-digit ms p50, low-double-digit ms p95, with peakConc 6-15.

### Artifacts

- `profiles/2026-05-21-baseline/` — pre-T1-1 60s profile
- `profiles/2026-05-21-t1-1-applied/` — post-T1-1 60s profile (the −41% alloc reduction)
- `profiles/2026-05-21-30min-soak/` — unbounded 30-min soak end-state
- `profiles/2026-05-21-30min-bounded-soak/` — bounded 30-min soak end-state (the cleanest "no leak" evidence)

---

## ~~NEW P1~~ — Row as `map[string]interface{}` — **REJECTED on porting-fidelity grounds**

**Status:** REJECTED 2026-05-21. Recorded here so it doesn't get re-flagged.

**File:** `ivm/data.go:139-154` (sort comparator), `ivm/data.go` (`Row` type), all operators that read row fields.

**Profile evidence (still valid):** `Row` is `map[string]interface{}`. The sort comparator at line 142 calls `CompareValues(a[field], b[field])` per row pair per orderBy column. Each `a[field]` is a `runtime.mapaccess1_faststr` — measurably 41% of CPU on its own, 51% cumulative through the comparator closure.

**Why we're NOT changing it:** Cross-checked against TS source. `ivm.Row map[string]Value` is a faithful port of TS's canonical `Row = Record<string, Value>` (`zero-protocol/src/data.ts:31-40`), and TS's `makeComparator` (`zql/src/ivm/data.ts:91-104`) uses the same `a[field]` string-keyed access. Grep across `zql/src/ivm/` returns ZERO matches for any internal indexed-row representation — TS uses the same map shape everywhere on the hot path.

The 50%-of-CPU gap isn't a porting bug. It's a runtime gap: V8's hidden-class optimization compiles `obj[field]` to a fixed-offset property access when row shape is stable; Go's `map[string]interface{}` has no equivalent and pays full hash + bucket walk + interface unbox cost per access.

Switching to a slice-backed Row would:
1. Diverge from TS source representation — porting-fidelity loss.
2. Complicate the drift audit (currently does row-equality compare).
3. Touch every operator with a wide API change.
4. Lock the Go evolution path away from TS — future TS row-semantic changes get harder to port.

Additionally, the absolute CPU impact is small: the profile that measured "50% of CPU" was on a sidecar that was only **8% busy overall**. Even eliminating the comparator entirely leaves the sidecar at ~4% busy. The user-visible latency (p50 ~1ms hydrate) isn't constrained by this.

**Decision:** Accept the Go-vs-V8 idiom gap. Look elsewhere for measurable wins.

**Status:** REJECTED. Do not re-flag without first re-verifying against the TS source — if TS itself moves to an indexed representation, port that.

---

## Findings by safety-of-fix

### Tier 1 — Safe perf wins (ship in order)

These have **no correctness risk** — the proposed fix preserves all current invariants.

#### T1-1. Typed-decode for hot RPCs to eliminate the reflection walk ✅ **SHIPPED 2026-05-21**

**File:** `ivm/data.go` (`Row.DecodeMsgpack` + `normalizeDecodedValue`); `cmd/sidecar/main.go:85-160` (`normalizeNumericInterfaces` retained for AST literals).

**Issue:** Every decoded msgpack frame was walked via `reflect.Value` to convert `int*` → `float64` inside `interface{}` positions. For a `loadRows` with 5000 rows × 30 fields, that's 150k reflect.Value calls per RPC. Reflection in Go disables compiler inlining and walks runtime type tables — visible on the profile as 8% of total allocations (107MB / 1.29GB in `reflect.unsafe_New`).

**Why the walk existed:** IVM comparison functions (`CompareValues`, `ValuesEqual`) assume numerics are `float64`. msgpack-decoded `interface{}` positions can hold `int8/int16/int32/int64/uint*` depending on the wire value. Skipping normalization silently breaks predicates.

**Fix applied:** Custom `DecodeMsgpack` on `ivm.Row` that coerces integer column values to `float64` inline at decode time. The post-walk no longer has Row values to convert (already done), eliminating most of the reflection-walk allocation. The walk is preserved for non-Row payloads (`builder.ValuePos.Value`, AST literals) — those allocations don't dominate. Type-switch-based recursion handles JSON-typed columns (nested maps/arrays).

**Why the fix is safe (verified):**
- `Row map[string]Value` is the canonical port of TS `Record<string, Value>` — verified against `zero-protocol/src/data.ts:31-40`. The external API is unchanged: callers still access via `row[fieldName]`.
- Wire format unchanged: msgpack still encodes/decodes as a map.
- Nested map/array recursion catches the same cases the reflection walk did for JSON-typed columns.
- Full `go test ./...` passes including 105s testharness suite.
- Drift audit: 96 audits across 40s soak post-fix, **0 mismatches**. TS↔Go equivalence preserved.

**Measured result (2026-05-21, same 15-client workload):**

| Metric (60s capture) | Before | After | Δ |
|----------------------|-------:|------:|---:|
| Total allocations | 1289MB | 762MB | **−41%** |
| `msgpack.readN` allocs | 412MB | 244MB | −41% |
| `msgpack.DecodeMap` allocs | 218MB | 121MB | −45% |
| `reflect.unsafe_New` allocs | 107MB | 57MB | **−47%** |
| `handleLoadRows` cumulative | 646MB | 374MB | −42% |
| Custom `Row.DecodeMsgpack` (new) | — | 30MB | net win |

CPU profile shape unchanged — `MakeComparator.func2` + `mapaccess1_faststr` still dominate (~63% cum). The CPU win is in less GC work, not less comparator work. The comparator cost is the v1 P1-NEW finding (NOT shipping per the porting-fidelity decision); revisit if needed.

**Profiles saved:** `profiles/2026-05-21-baseline/` (before) vs `profiles/2026-05-21-t1-1-applied/` (after).

**Severity (historical):** HIGH (P2 after the dropped P1). Confirmed by profile. **Status: completed.**

---

#### T1-2. Per-goroutine accumulator buffers for the streamer ~~(HIGH)~~ → **LOW (downgraded by profile evidence)**

**File:** `engine/streamer.go:48-56` (`Streamer.Accumulate`)

**Issue (as originally claimed):** Every pipeline's `pipelineOutput.Push` calls `streamer.Accumulate` which takes `s.mu.Lock()`. Under `GenPushParallel` with N parallel pipelines, all N serialize on this single mutex during the final accumulation step.

**Why a fix would be safe:** The streamer's contract is "collect entries, return in some order via `Stream()`." There is no documented ordering invariant across pipelines (and there can't be — pipelines run in parallel goroutines whose interleaving is non-deterministic). Per-goroutine buffers preserve the existing contract.

**⚠ Profile evidence (2026-05-21):** Under 15-client load with peakConc=5 on hydrates, the mutex profile shows **`Streamer.Accumulate` doesn't appear in the top 10 contended locks**. Total cross-mutex contention was 90ms over a 60s window (~1.5% of one CPU). The streamer mutex is held for a single append; in practice the contention window is sub-microsecond and only one or two goroutines ever wait.

**Revised severity:** LOW. Architecturally cleaner but no measurable signal on realistic workloads. Don't ship until a profile under a higher-fanout workload (50+ cgs or many-query-per-cg) actually shows it on the contended-mutex list.

**If revisiting:** The fix shape (per-goroutine `*[]accEntry` passed into `GenPushParallel`, merged in `Stream()`, ~80 LoC) is still correct. Just not the right ROI right now.

---

#### T1-3. `streamNodes` recursive accumulation pre-sizing

**File:** `engine/streamer.go:115-150`

**Issue:** `var result []RowChange` then repeated `append` and recursive `append(result, streamNodes(...)...)`. For nodes with deep relationships, this is O(N log N) allocator work for what should be O(N).

**Why a fix is safe:** Pure refactor of internal accumulation; no API change. Callers receive the same `[]RowChange`.

**Fix shape:** Change signature to `streamNodes(out *[]RowChange, ...)` and pre-size at the top call with a tree-walk-counted estimate. ~30 LoC.

**Severity:** MEDIUM. Wins are proportional to relationship depth × fanout. Free win on the dashboard workload's tickets-with-assignments queries.

---

#### T1-4. Add `Limit` to `FetchRequest` for Take's bound lookup

**File:** `ivm/take.go:188-208` (Take.Push, the `size==limit` add path) and `ivm/data.go` (`FetchRequest` struct)

**Issue:** When a Take operator at full bound receives a row below the bound, it calls `input.Fetch` to find the row-just-before-the-bound. Currently `FetchRequest` has no `Limit` field, so the input may return many rows past the bound that Take immediately discards.

**Why a fix is safe:** Adding `Limit int` to `FetchRequest` is additive — existing callers leave it zero-valued, sources treat zero as "unlimited" (current behavior). Take callers explicitly pass `1`.

**Verified:** `FetchRequest` is consumed by `MemorySource.fetch` (and indirectly through `Skip`/`Take`/`Join`/etc.). All existing call sites would default to zero. Looked at `MemorySource.fetch` — easy to add an iteration early-break on `Limit`.

**Fix shape:** ~50 LoC across `data.go` + `source.go` + Take callsites.

**Severity:** MEDIUM. Only matters under high-cardinality partitions; can be O(partition_size) per displacement today.

---

#### T1-5. `pending = pending[:0]` instead of `pending = nil` in stream flush

**File:** `engine/engine.go:573` (AdvanceStream's `flush` closure), `engine/engine.go:394` (AddQueriesStream's `flush`)

**Issue:** After flushing a chunk, `pending = nil` releases the backing array. Next `append` reallocates.

**Why a fix is safe:** **Verified by reading `cmd/sidecar/main.go:1230-1239` (`writeFrameLocked`).** The sidecar calls `mpMarshal(resp)` synchronously before queuing the write. By the time `streamW(reqID, partial)` returns, the slice has already been encoded into a separate `data` byte buffer. The original slice is no longer referenced — safe to reuse.

**Fix shape:** Two-character change × 2 locations. Plus a comment explaining the synchronous-encode invariant so it doesn't regress.

**Severity:** LOW. Only matters for advances/hydrates that cross many chunk boundaries.

---

#### T1-6. Cache `hasAnySplitEditKeys` on MemorySource

**File:** `ivm/source.go:185-200`

**Issue:** `genPushAndWriteWithSplitEdit` walks all connections × all `SplitEditKeys` per Edit change to detect whether a split is needed. For sources with 1 connection without split keys, this is wasted work.

**Why a fix is safe:** Pure caching — invalidated only when connections add/remove (already under `connsMu`).

**Fix shape:** Bool field on MemorySource, recomputed in Connect/Disconnect. ~15 LoC.

**Severity:** LOW.

---

#### T1-7. `atomic.Pointer[[]Connection]` for connections slice on Push hot path

**File:** `ivm/source.go:244-249`

**Issue:** Every `genPush` does `RLock + make + copy` of the connections slice. ~50ns per push.

**Why a fix is safe:** Copy-on-write: Connect/Disconnect builds a new slice and atomic-Stores it. Pushers atomic-Load — no lock at all. Each push sees a consistent snapshot.

**Verified:** `connsMu` is also taken in `Connect`/`Disconnect`/`Destroy` paths — they would `RWMutex.Lock` (or use the same atomic pointer for store). The Push fast path becomes lock-free.

**Fix shape:** Replace `connsMu sync.RWMutex` + `connections []*Connection` with `connectionsAtomic atomic.Pointer[[]*Connection]`. ~40 LoC.

**Severity:** LOW. Real cost is small; matters at very high push rates.

---

### Tier 2 — Real wins but need careful design (do not naively apply)

These are correct as findings but the *fix* needs more care than my first-pass review suggested. Each has a specific correctness concern that was caught on re-verification.

#### T2-1. ⚠️ `Engine.mu` held across hydration goroutines — DO NOT JUST RELEASE THE LOCK

**File:** `engine/engine.go:318-414` (AddQueriesStream)

**Issue:** Lock acquired at line 318, deferred-unlocked at 319, held through `wg.Wait()` at 414. The N hydration goroutines spawned at 372 run while `e.mu` is still held by the parent — blocks any concurrent `Advance`.

**⚠ Correctness concern I missed on first pass:** I originally proposed "release `e.mu` after Phase 1, take RLock for Phase 2." That's **unsafe**:

- Operator state mutates during Push. Specifically:
  - `Take.Push` writes to `t.storage` (the operator-local SQLite). Verified at `ivm/take.go:170-185`.
  - `Join.processChildChange` allocates parent-row caches.
  - `Exists`-style operators maintain count maps.
- If a concurrent `Advance` calls `source.Push` while a hydrate is mid-Fetch through the same operator tree, the Fetch could see a half-applied state — wrong row counts, wrong bounds, wrong existence flags.

The source-level overlay handles concurrent fetch-during-push **only at the source** (it's a single atomic Overlay pointer). Operators upstream of the source have no such mechanism.

**Real fix (NOT trivial):** Either
- (a) **Snapshot operator state** before releasing the lock for Phase 2. Take would copy its `storage` pointer; Join would snapshot its child cache. Significant refactor across operators.
- (b) **Per-operator RWMutex.** Each operator declares whether its state can be safely read concurrently with a push. Fetch acquires RLock on every operator it traverses; Push acquires Lock. Lots of locks, lock-ordering concerns.
- (c) **Pipeline-cloning for hydrate.** Build a hydrate-only clone of the pipeline (sharing the source) and use it. Memory cost but lock-free.

**Severity:** ~~HIGH~~ → **SPECULATIVE (downgraded by profile evidence)** if you exercise concurrent advance+hydrate. **DO NOT TOUCH** until one of the three approaches is designed and reviewed. Until then, the lock is correct-but-coarse.

**⚠ Profile evidence (2026-05-21):** Block profile under 15-client load with peakConc=4 advances + peakConc=5 hydrates shows **zero engine.mu blocking**. Block time is dominated by `runtime.chanrecv` (idle goroutines parking on work queues) and SQL connection pool waits — neither of which involves `e.mu`. The single-engine-wide mutex is not currently a measurable bottleneck. The architectural concern remains valid at much higher concurrency (50+ active hydrates per cg), but at current workloads this isn't worth the correctness risk.

**Status:** Punt indefinitely. Re-evaluate ONLY if a profile under higher concurrency shows engine.mu in the top contended-locks list.

---

#### T2-2. ⚠️ `pipelineOutput.Push` eagerly materializes relationship closures — fix is wider than it looks

**File:** `engine/engine.go:688-720` (`materializeChange`, `materializeNode`)

**Issue:** Every push of a change walks the entire relationship tree, calls `fn()` on every lazy closure, then re-wraps the slice in a *new* closure (`func() []ivm.Node { return captured }`).

**⚠ Correctness concern I missed on first pass:** I suggested "skip the closure re-wrap." But the closure wrap is **required to satisfy the `Node.Relationships map[string]func() []Node` type**. Consumers depend on this type:

- `ivm/exists.go:218`: `relationship := node.Relationships[e.relationshipName]` — calls the closure.
- `ivm/join_utils.go:88, 155`: same pattern in join's child-stream handling.

Changing the wrap to a no-op would leave un-materialized closures live until the *next* consumer's `fn()`. But the *source overlay closes after push*, so the un-materialized closure would observe the post-push state instead of the in-push state. That's a real wire-format change — downstream subscribers would see "remove A, then fetch parent → child set including A" depending on timing.

**Real fix:** Two options:
- (a) **Change `Node.Relationships` type to `map[string][]Node` (concrete slices).** All consumers (`exists.go`, `join_utils.go`, others) update to direct indexing. Wider API change but eliminates both the eager-materialize work AND the closure re-wrap. Probably the right answer.
- (b) **Mark already-materialized closures and short-circuit re-materialization.** Track via a typed `materializedRel` struct. Less invasive but more code.

**Severity:** ~~HIGH~~ → **SPECULATIVE (downgraded by profile evidence)**. Need to commit to (a) or (b) before touching.

**⚠ Profile evidence (2026-05-21):** Neither `materializeChange` nor `materializeNode` appears in the top-15 CPU samples OR the top-15 allocation samples. The eager-materialize cost is real but not measurable on the workload — even on queries that include relationships (tickets+assignments join in the load mix), the comparator and msgpack decode dwarf it. Could matter on a query graph with deep nested relationships (5+ levels); doesn't on our load.

**Status:** Punt indefinitely. Re-evaluate ONLY if a profile shows `materializeNode` in the CPU or alloc top-15.

---

#### T2-3. ⚠️ Per-cg FIFO + engine mutex double serialization — depends on T2-1

**File:** `cmd/sidecar/main.go:516` (ClientGroup worker) + `engine/engine.go:319`

Same correctness concern as T2-1, and same profile-driven downgrade: zero engine.mu blocking at current workloads.

**Status:** Punt until T2-1 is designed AND a profile shows engine.mu contention. Currently neither precondition holds.

---

#### T2-4. LIKE/ILIKE `fmt.Sprintf("%v", x)` per row — correctness-sensitive replacement

**File:** `builder/filter.go:134-146`

**Issue:** LIKE/ILIKE call `fmt.Sprintf("%v", a)` and `fmt.Sprintf("%v", b)` per-row — two heap allocations + format dispatch per evaluation.

**⚠ Correctness concern:** The `%v` verb has specific semantics for non-string types:
- `nil` → `"<nil>"`
- `true` → `"true"`
- `42` (int) → `"42"`
- `[]int{1,2}` → `"[1 2]"`

If we naively replace with `x.(string)`, we panic on non-string. If we use `fmt.Sprint(x)`, we still allocate but skip the format-string parse — minor.

The TS LIKE on non-string operands is **undefined** in our schema, but the Go side is currently behaving like TS by stringifying. Need to verify what TS actually does for `LIKE` on non-string columns before "optimizing."

**Real fix:** Detect string-column LIKE at build time. For string-typed columns, build a string-fast-path comparator. For non-string columns... probably error at build time (since it doesn't make semantic sense), but check the TS side first.

**Severity:** MEDIUM if LIKE is common in your workloads (it isn't, for dashboards). Drop unless profile points here.

---

### Tier 3 — Findings I'd reject after re-review

Listed for the audit trail so they don't get re-flagged later.

#### T3-1. ~~"Closures capture materialized slice → memory leak"~~

Closures are GC-collected when the holding `Node` is dropped. The "leak under long-lived sessions" claim was incorrect — relationship closures don't outlive their parent node in any path I traced.

#### T3-2. ~~"`Join.processParentNode` creates new relationship maps per node"~~

The new map is required for relationship inheritance (the child stream is logically a new collection per parent). Eliminating it would require restructuring the entire join output model. Not worth it.

#### T3-3. ~~"Skip cursor re-normalization on every fetch"~~

The cursor normalize is cheap (a single row, normalized once per AddQuery call, not per row). Profile won't show this.

#### T3-4. ~~"`removeQuery` is O(pipelines) — could be O(1) map delete"~~

It already is an O(1) map delete. Misread on the first pass.

---

## Recommended order of operations (revised after 2026-05-21 profile)

1. ~~**Profile first.**~~ ✅ Done 2026-05-21.

2. ~~**P1-NEW** (slice-backed Row type).~~ ❌ REJECTED 2026-05-21 — porting-fidelity loss vs TS source. See P1-NEW section above for the rationale.

3. ~~**T1-1** (typed Row decode).~~ ✅ SHIPPED 2026-05-21. Custom `Row.DecodeMsgpack` in `ivm/data.go`. −41% total allocations measured, 0 drift-audit mismatches.

4. **T1-3** (`streamNodes` pre-sizing). Doesn't appear in profile top-15. Near-zero-risk ~30 LoC change; worth doing as a small PR but no urgency.

5. **T1-4 + T1-5 + T1-6 + T1-7** as cleanup PRs. Each independent, ~50 LoC each. None in profile top-15, all correctness-safe.

6. **REPROFILE** before considering anything below. With T1-1 shipped the hot paths may have shifted; capture a fresh baseline first.

7. **T1-2 / T2-1 / T2-2 / T2-3** — currently no profile signal. Re-evaluate only after step 6, and only under a workload that pushes substantially higher concurrency than the 15-client baseline (target: peakConc > 20, sustained advances > 50/sec).

8. **T2-4** only if a profile actually points at LIKE/ILIKE eval.

### What to do next (concrete, in order)

1. Reprofile under the same 15-client workload — confirm the alloc reduction translates to lower GC pause time / higher sustained throughput.
2. Reprofile under a heavier workload (50+ clients, sustained advance load) — see if T1-2 / T2-1 surface.
3. Bundle T1-3 + T1-4 + T1-6 + T1-7 as a "cleanup PR" — each is independent so they can be merged separately too, but they're low-risk enough to batch.
4. Re-read the doc only after a NEW profile capture justifies a change in priority.

---

## What's verified-already-good (don't change)

- **Per-cg FIFO + 30-min reaper** — correct lifecycle, bounded memory.
- **Connection-level write goroutine + ordered `outC`** — wire ordering preserved without per-message socket locking.
- **Restart machinery** — epoch tracking, init dedup, snapshot-aligned reinit. Survived the sidecar-kill drill.
- **Source overlay (`atomic.Pointer[Overlay]`)** — correct lock-free coordination for concurrent fetch-during-push at the source layer. Where the safety stops (operator layer) is T2-1's concern.
- **`recentlyTimedOut`** — prevents wrong-response routing after timeout retry. Bounded FIFO eviction.
- **Chunked streaming** (`AddQueriesStream`, `AdvanceStream`) — solid wire-level design. Engine-side parallelism gating is separate.
- **`Engine.Close` lock discipline** — takes `e.mu` before destroying pipelines; serializes with all advance/add paths. T2-1 must preserve this.

---

## Notes for future audits

- The "engine.mu is held for the duration" comment at `engine.go:311-313` is load-bearing for current correctness. If you weaken it, audit every operator's Push/Fetch interaction for shared mutable state. `Take`, `Join`, `Exists`, `Skip` all have internal state that may matter.
- The `materializeChange` pattern at `engine.go:688-720` looks like dead weight but is doing real work (closing the overlay window before the next push). The closure re-wrap is the cosmetic part; the eager-evaluation is what guarantees consistency.
- msgpack reflection walk (`walkForNumericNormalize`) was originally added to fix a CRITICAL bug where post-decode comparisons silently mis-typed. Don't remove without the typed-decode replacement.

---

## How to reproduce the profile

Baseline pprof artifacts (2026-05-21): `profiles/2026-05-21-baseline/{cpu,allocs,block,mutex,heap-t0,goroutines}.pprof`. Open with `go tool pprof -http=:8080 <file>`.

To capture a fresh baseline:

1. **Sidecar binary built with pprof.** Already in main: `_ "net/http/pprof"` registered in `cmd/sidecar/main.go`, opens an endpoint when `GO_IVM_PPROF_ADDR` is set. The binary also calls `runtime.SetBlockProfileRate(1)` and `runtime.SetMutexProfileFraction(1)` when the env var is present — those carry ~5% overhead, so leave the env var unset in prod.

2. **Sandbox setup.** `docker-compose.test-go-primary.yml` exposes pprof at host port 16060. Bring up with the workers=1 + shared-sidecar overlay (see `xy-repo/xyne-spaces/apps/sync-proxy/deploy/RUNBOOK.md` § "Go-Primary Sandbox Testing").

3. **Load.** Run `/app/load-gen-v3.cjs` from inside the `xyne-sandbox-rust-test-sync-proxy` container (script lives there from a prior copy; the source is in this session's history if you need to regenerate). `N=15 DUR=120 node load-gen-v3.cjs` drives ~15 hydrates/sec and triggers a few concurrent advances when paired with a parallel PG INSERT burst into `messages`.

4. **Capture.** During peak (after ~20s of warmup):
   ```bash
   curl -s 'http://localhost:16060/debug/pprof/profile?seconds=60' > cpu.pprof
   curl -s 'http://localhost:16060/debug/pprof/allocs' > allocs.pprof
   curl -s 'http://localhost:16060/debug/pprof/block' > block.pprof
   curl -s 'http://localhost:16060/debug/pprof/mutex' > mutex.pprof
   curl -s 'http://localhost:16060/debug/pprof/goroutine?debug=2' > goroutines.txt
   ```

5. **Analyze.** Key one-liners:
   ```bash
   go tool pprof -text -nodecount=15 cpu.pprof
   go tool pprof -text -alloc_space -nodecount=15 allocs.pprof
   go tool pprof -text -nodecount=10 mutex.pprof
   go tool pprof -list 'MakeComparator|CompareValues' cpu.pprof   # the current top finding
   ```

Workload caveats and limitations documented in "Profile baseline" § "Caveats" above.
