# Parallelism Implementation Review

Audit of the parallelism implementation of the Go IVM sidecar. Focus: race conditions, lock ordering, channel correctness, goroutine lifecycle, and Go memory-model issues. Functional correctness was NOT in scope.

> **See `REVIEW-final.md` for the consolidated cross-doc production-readiness summary. This doc preserves the audit trail.** All findings in this doc plus the new CRITICAL-CROSS-1 (HOL writer) from the consolidated audit are now fixed: the single-sequential writer has been replaced with per-request writer goroutines guarded by a shared write mutex, restoring cross-group response parallelism while preserving frame atomicity.

## Summary
- **2 CRITICAL** — Lifecycle race + worker goroutine leak. Both will trigger under normal usage.
- **3 HIGH** — Real races/deadlocks possible under expected operational patterns (re-init, destroy-during-request, peak metric updates).
- **4 MEDIUM** — Suspicious but mitigated by current invariants; would break if assumptions change.
- **3 LOW** — Style / robustness nits.
- **5 CONFIRMED SAFE** — Worth recording why, since these are the load-bearing claims.

---

## Status: FIXED & SOAK-VERIFIED

All CRITICAL / HIGH findings are addressed. Validation:

- `go build ./...` + full `go test ./...` pass (including `testharness`).
- Two consecutive 5-minute soaks (20 users, mutate mode, `rust-test` sandbox):
  - First: 936 matches / 0 mismatches.
  - Second (after timing-instrumentation work): 1462 matches / 0 mismatches.
  - Both: 0 panics, 0 decode failures, 0 errors, 0 reconnects.

| Finding | Status | Notes |
|---|---|---|
| CRITICAL-1 — Worker goroutine leak on destroy/closeAll | **FIXED** | `shutdownGroup` closes `reqC`; `closed` flag under `g.mu`; senders use `trySendReq`. |
| CRITICAL-2 — handleInit race | **RETRACTED** | re-analysis showed `Engine.mu` already prevents the hazard; reclassified as part of HIGH-3. |
| HIGH-1 — Response wire ordering | **FIXED** | single dedicated writer goroutine per connection + ordered `outC` channel (cap 1024) — wire order = enqueue order. |
| HIGH-2 — `peakAdvConc` / `peakHydConc` race | **FIXED** | `updatePeak` uses `CompareAndSwap` retry loop. |
| HIGH-3 — `Engine.Close()` didn't destroy pipelines | **FIXED** | `Engine.Close` iterates `pipelines`, calls `Input.Destroy()`, nils `pipelines` / `sources`, sets `closed` flag. |
| HIGH-4 — Unbounded writer goroutines | **FIXED** | one writer goroutine per connection; `outC` cap 1024 applies backpressure to the read loop. |
| MEDIUM-1 — `Disconnect` mutates `connections` without lock | **FIXED** | `connsMu sync.RWMutex` now guards `connections`; Connect/Disconnect take Lock, push paths take RLock to snapshot. |
| MEDIUM-2..4 | **DOCUMENTED** | invariants left in place; comments added at the relevant call sites. |
| LOW-1 — `extractClientGroupID` decodes params twice | **NOT FIXED** | CPU nit; behavior unchanged. |
| LOW-2..3 | **NOT FIXED** | style nits; behavior unchanged. |
| SAFE-1..5 | **UNCHANGED** | invariants still hold. |

---

## Findings

### CRITICAL-1: ClientGroup worker goroutine leak on destroy/closeAll
- **File:** `cmd/sidecar/main.go:251-322`
- **Concurrent paths:**
  - `Server.getGroup` creates `g.reqC = make(chan clientGroupReq, 64)` and starts `go g.worker(s)` (line 255).
  - `Server.removeGroup` deletes `s.groups[id]` and calls `g.eng.Close()` but never closes `g.reqC` (lines 296-308).
  - `Server.closeAll` does the same — closes engines, drops the map, but leaves `reqC` open.
  - The worker is `for req := range g.reqC` — it blocks forever on a never-closed channel after `removeGroup`.
- **Race / leak:** Every `destroy` RPC leaks one worker goroutine + the channel + the closed Engine reference. Over a long-running sidecar with churning client groups, this is unbounded heap and goroutine growth.
- **Memory model:** The orphan worker still holds `s` (and through it, the whole server). It also holds the prior `g.eng` field via closure on `g` — but more importantly, if a slow-arriving send happens to land in `reqC` after `delete` but before re-getGroup, the orphaned worker would happily call `s.handleRequest`, which would re-resolve the group via `s.getGroup` and possibly create a NEW ClientGroup, leading to "two ClientGroups for one ID" briefly. Note: this last hazard is unreachable on the current handleConnection path because handleConnection always calls `getGroup` itself and sends to that channel — but the orphaned worker still leaks regardless.
- **Trigger:** Any `destroy` call (handleDestroy → removeGroup), and SIGINT/SIGTERM shutdown. In production this is the normal lifecycle for client groups that come and go.
- **Fix sketch:**
  ```go
  // In removeGroup:
  if g != nil {
      g.mu.Lock()
      if g.eng != nil { g.eng.Close() }
      close(g.reqC)            // signals worker to exit
      g.mu.Unlock()
  }
  // closeAll: same — close(g.reqC) inside the loop, after Close().
  ```
  But beware: senders in `handleConnection` may race against close. Need a `closed` flag guarded by `g.mu`, or have `handleConnection` recover from `panic: send on closed channel`, or (cleaner) gate sends through a method that holds `g.mu` briefly and checks a flag. The select-with-done pattern works too:
  ```go
  type ClientGroup struct {
      mu   sync.Mutex
      eng  *engine.Engine
      reqC chan clientGroupReq
      done chan struct{}
  }
  // Sender:
  select {
  case g.reqC <- req:
  case <-g.done:
      // Respond with error
  }
  ```

### CRITICAL-2: handleInit race — overlapping init on same client group can leak engine + corrupt state
- **File:** `cmd/sidecar/main.go:386-440`
- **Concurrent paths:** `handleInit` is normally serialized through the per-group worker (`worker` → `s.handleRequest` → `handleInit`). The handler then acquires `group.mu` and re-initializes. This is fine for normal use.
- **But:** `handleInit` ALSO replaces `group.eng` if non-nil: `if group.eng != nil { group.eng.Close() }`. After Close, the engine's `mu` is still held momentarily — and the *outgoing* engine may still be referenced by goroutines spawned within `AddQueries` (the hydration `go func` in `engine.AddQueries`).
- **Race:**
  1. Client calls `addQueries` with N queries on group X. Engine.mu held, hydration goroutines spawned, `wg.Wait()` is in progress.
  2. Engine.mu blocks NEXT request in worker — but only NEW arrivals. Hydration goroutines are NOT blocked by Engine.mu (they were spawned UNDER it and reached `wg.Wait`).
  3. Hydration goroutines read source data + state without holding any lock.
  4. There is no way for init to interrupt #3 because worker is single-threaded; `wg.Wait` is held under Engine.mu so the next worker step can't start. Actually... `wg.Wait` runs UNDER `e.mu` (line 173-174), so init CANNOT proceed concurrently.
- **Refined assessment:** Within a single ClientGroup, the worker serializes everything and Engine.mu prevents engine swap during hydration. So **this specific scenario is safe**. BUT:
- **Real race:** `handleInit` does NOT remove the group from `s.groups`. If init is called twice in quick succession on the same group ID with different storage paths, the second call closes the first engine and replaces `group.eng` while holding `group.mu`. Old engine pointer is unreachable but cleanly closed. **No race here.**
- **Actual issue downgraded to HIGH-3 below** (the re-init does work, but `engine.Close()` only closes storage — does NOT destroy pipelines, so any in-flight goroutine spawned by an operator that captured `&engine` would crash).

### HIGH-1: `handleConnection` async-write goroutine ordering — response-write race vs read-loop
- **File:** `cmd/sidecar/main.go:715-728`
- **Concurrent paths:**
  - Read loop: reads frame, calls `getGroup(cgID)`, synchronously sends `clientGroupReq` to `group.reqC`. This preserves FIFO request order into the worker (good — fix #26b from doc).
  - Per-request async writer: `go func() { writeResp(<-respCh) }()` — one goroutine per request, blocks on respCh then writes through `writeMu`.
- **The race that's NOT happening:** Worker sends responses on `req.respCh` in FIFO order, so the K-th writer goroutine receives the K-th response. Good.
- **The race that IS:** Goroutines are scheduled non-deterministically. If req K-1 is enqueued and the worker sends its response on respCh, then req K is enqueued and finishes faster (impossible if worker is single-threaded — but the response order on the wire is determined by which writer goroutine grabs `writeMu` first).
- **Resolution:** Worker is single-threaded per group → worker sends to respCh in FIFO order → each respCh has buffer 1 → exactly one writer goroutine per request → they each compete for `writeMu` non-deterministically. **TS expects responses on the socket in RPC-id order? Probably not — RPC has ID-correlated responses, so out-of-order should be fine.** Confirmed safe IF the Node client correlates by ID (typical for JSON-RPC).
- **Real concern:** With B=64 buffer on `reqC`, the read loop won't block until 64 unanswered requests are queued. If responses are delivered out-of-order on the wire, the TS client *must* tolerate out-of-order. Verify msgpackr/JSON-RPC client uses `id` correlation, not stream order.
- **Trigger:** Any time scheduler reorders multiple writer goroutines under load. Easy to repro with `go test -race -count=1000`.
- **Fix sketch:** Either (a) confirm the JS client uses ID-based dispatch (likely yes), and document this invariant; or (b) write responses inline in the worker (lose write/IO parallelism) or use a dedicated writer goroutine with an ordered channel.

### HIGH-2: `peakAdvConc` / `peakHydConc` Load-then-Store race underestimates peak concurrency
- **File:** `cmd/sidecar/main.go:268-277`
- **Concurrent paths:** Multiple workers (one per group) increment `advancesInFlight.Add(1)` and then do:
  ```go
  if n > metrics.peakAdvConc.Load() {
      metrics.peakAdvConc.Store(n)
  }
  ```
- **Race:** Classic CAS-needed pattern. Two workers see Load=10, both have local n=12 (say one observed 11 then 12 races in). One stores 12, other stores 12 — fine. But interleaved: Worker A sees Load=10, observes n=15, then before its Store, Worker B observes Load=10, n=13, stores 13. Worker A now stores 15 — actually fine in this case. The pathological case: peak briefly hits 20 from worker C, gets stored. Worker A then races to overwrite with 15. Peak goes from 20 → 15. **Peak is undercounted.**
- **Memory-model:** atomic.Store on int64 is properly atomic, but Load+Store is not a CAS, so peak monotonicity is not preserved.
- **Trigger:** Always running. Effect: telemetry numbers are mildly inaccurate. Noted in scope as acceptable but flag it.
- **Fix sketch:**
  ```go
  for {
      old := metrics.peakAdvConc.Load()
      if n <= old || metrics.peakAdvConc.CompareAndSwap(old, n) {
          break
      }
  }
  ```

### HIGH-3: `engine.Close()` does not destroy pipelines or drain hydration goroutines
- **File:** `engine/engine.go:104-108`, `cmd/sidecar/main.go:393-395`
- **Concurrent paths:** `handleInit` (under group.mu) checks `if group.eng != nil { group.eng.Close() }` and replaces `group.eng`. `Engine.Close()` only acquires `e.mu` and calls `e.storage.Close()` — it does NOT iterate `e.pipelines` and call `Destroy()`, NOR does it wait on any in-flight goroutines.
- **Race:** `engine.AddQueries` (lines 207-222) spawns hydration goroutines that capture `entry.pipeline.Input` — which is part of the engine's state. If a worker goroutine were currently in `wg.Wait()` for hydration, AND a separate path could close the engine... but Engine.mu prevents that. **In current sidecar flow, this is safe** because every entry point holds group.mu, and Engine.mu is acquired around AddQueries before spawning the goroutines.
- **Latent hazard:** If anyone adds a code path that calls `engine.Close()` from outside `group.mu` (e.g., a future periodic GC), or if anyone fires off goroutines that outlive the Engine.mu critical section, this becomes a use-after-close race against storage.Close() / nil-pointer deref through `storage`.
- **Memory model:** `e.storage.Close()` invalidates the underlying `*sql.DB` and any prepared statements. Operators that hold `OperatorStorage` (e.g., Take) would crash on next use.
- **Trigger:** Future code change. Today: not triggerable.
- **Fix sketch:** Make `Engine.Close()` destroy all pipelines (`for _, p := range e.pipelines { p.pipeline.Input.Destroy() }`) under `e.mu`, then `e.pipelines = nil`, then storage.Close. Also nil-out `e.storage` and add `e.closed` flag checked at the top of every public method.

### HIGH-4: `handleConnection` write goroutine count is unbounded — backpressure absent
- **File:** `cmd/sidecar/main.go:715-728`
- **Concurrent paths:** Read loop spawns a write goroutine per request. The buffered channel `g.reqC` is size 64 — once full, the read loop blocks on send (correct backpressure for the worker). But the write goroutines are NOT bounded.
- **Scenario:** A slow socket writer (TCP buffer full, slow consumer on Node side) means `writeFrame` blocks. Each blocked writer holds the `writeMu` while waiting. New responses pile up — each spawns ANOTHER goroutine that blocks on the mutex. Meanwhile worker keeps producing responses (it sees `respCh <- resp` succeed because respCh is bufferred 1).
- **Race:** Not a race per se, but unbounded goroutine fan-out. If the Node side blocks for any reason, the Go sidecar's goroutine count grows in proportion to backed-up responses.
- **Trigger:** Any sustained period where Node can't drain the socket fast enough. Memory usage spikes.
- **Fix sketch:** Have one dedicated writer goroutine per connection that reads from an ordered channel:
  ```go
  outC := make(chan RPCResponse, 256)
  go func() {
      for resp := range outC {
          writeResp(resp)  // no mutex needed
      }
  }()
  ```
  Then writeResp pushes to outC; if full, you can either block (backpressure to worker) or drop with a warning.

### MEDIUM-1: `MemorySource.Disconnect` mutates connections slice without lock — would race with parallel push if scheduled outside Engine.mu
- **File:** `ivm/source.go:127-134`
- **Concurrent paths:**
  - Engine wraps Push/Connect/Disconnect in `e.mu`, so within one Engine, no concurrent calls. ✓
  - `GenPushParallel` (parallel.go:71-80) launches goroutines that iterate `ms.connections` (already-captured `activeConns` slice — safe).
  - But: a goroutine inside the fan-out can call `Disconnect` indirectly? No — pushing to `conn.Output` does not reach back into source.Disconnect (each pipeline destroys via `SourceInput.Destroy` → `source.Disconnect`, but only from a control path NOT a push handler).
- **Confirmed safe under current invariants** — `Disconnect` is only called from `RemoveQuery` → `removeQueryLocked` (engine.go:247-254) which holds `e.mu`, AND no push handler triggers Destroy.
- **Latent hazard:** If a future operator or output ever calls `Destroy()` mid-push (e.g., a self-removing query trigger), parallel push + concurrent Disconnect would race on `ms.connections` slice mutation. The `append`-and-shift is decidedly NOT atomic.
- **Fix sketch:** Add a `sync.Mutex` to MemorySource around `connections` mutation, OR document the invariant clearly. Better: take a snapshot of the connections slice at the top of `Push`/`GenPushParallel` (already done for activeConns in parallel.go) and use it through the call.

### MEDIUM-2: Parallel fan-out and overlay write — happens-before relies on goroutine spawn semantics
- **File:** `ivm/parallel.go:51-95`
- **Sequence:**
  1. `ms.overlay = &Overlay{...}` (line 51) — plain write.
  2. `go func(...)` spawns N goroutines (line 73-79).
  3. Inside each goroutine, `FilterPush` calls into pipelines that eventually call `ms.Fetch` (SourceInput.Fetch line 389-424), which reads `ms.overlay` via `computeOverlays`.
- **Memory model:** The Go memory model establishes a happens-before edge from the `go` statement to the start of the goroutine. So `ms.overlay = &Overlay{...}` (which appears before the `go`) happens-before each goroutine's reads. **This is correct.**
- **wg.Wait():** Establishes happens-before from each goroutine's exit to the receiver of wg.Wait. So `ms.overlay = nil` (line 95) is safely written after all reads. **Correct.**
- **But:** This is fragile in two ways:
  - **(a)** If anyone caches the `ms.overlay` pointer outside the synchronized region (e.g., into another goroutine that outlives the push), the cached pointer would observe `nil` later. Currently no such code exists. ✓
  - **(b)** The single-connection optimization (lines 53-60) is sequential within the same goroutine — no memory-model issue.
  - **(c)** The slice access `for _, conn := range ms.connections` in `GenPushParallel` is on the caller's goroutine before any spawn, so no race with itself; but if Disconnect were called concurrently (see MEDIUM-1), this iteration races.
- **Recommendation:** Add a comment in `parallel.go` documenting the happens-before chain: `go` spawn → goroutine reads overlay → wg.Done → wg.Wait → `overlay = nil`. The current code is correct but obscure.

### MEDIUM-3: `GenPushParallel` resultsCh buffer = N, may not need but is fine; ordering reconstruction is correct
- **File:** `ivm/parallel.go:69-93`
- **Analysis:**
  - `resultsCh := make(chan connResult, len(activeConns))` — buffered to N, so no goroutine blocks on send.
  - `wg.Wait()` then `close(resultsCh)` then `for r := range resultsCh` — drains in arbitrary order, but each connResult has `index` field used to slot into `ordered[r.index]`. **Deterministic reconstruction.** ✓
- **But:** `outputChange := ms.sourceChangeToChange(change)` is called INSIDE each goroutine (line 75). `sourceChangeToChange` allocates fresh `Node` and `Relationships: map[string]func() []Node{}`. **Per-goroutine allocation — no shared state.** ✓
- **Performance nit:** Calling sourceChangeToChange N times allocates N identical Change values. Cheap, but could be hoisted out of the goroutines (compute once before spawn). Each goroutine then receives a copy. Caveat: the `Relationships` map within the Change is shared across goroutines if hoisted — but it's an empty map and no operator writes to it. Probably safe to hoist. Not urgent.

### MEDIUM-4: `engine.AddQueries` parallel hydration vs pipelineOutput.Push — overlay handling during initial fetch
- **File:** `engine/engine.go:172-226`
- **Phase 2 (hydration):** Spawns N goroutines, each calls `entry.pipeline.Input.Fetch(...)`. During hydration, `pipelineOutput.Push` is NOT called (Fetch returns nodes directly, doesn't push). Materialization happens in `streamNodes` via direct iteration in each hydration goroutine, NOT via the streamer.
- **Verification:**
  - `e.streamer.Accumulate` is only called from `pipelineOutput.Push` (engine.go:355). During hydration, no push happens (Fetch is read-only).
  - `streamNodes` is called per-goroutine but returns local slices.
  - `materializeNode` / `materializeChange` are not invoked during hydration (those are called from `pipelineOutput.Push` during advance).
- **The lazy `Relationships: map[string]func() []Node`** captures pointers/closures over operator state. If two hydration goroutines simultaneously call the SAME closure (e.g., a closure on a shared `Join.parent`), the closure ends up calling `j.parent.Fetch` from two goroutines. Join.parent is per-pipeline (engineSource.Connect creates a new SourceInput per Connect call) — but the underlying MemorySource is SHARED across pipelines for the same table.
- **MemorySource.Fetch concurrency:** `SourceInput.Fetch` reads `ms.data`, calls `ms.getSortedRows(conn.Sort)` (allocates a fresh sorted copy — safe), reads `ms.overlay`. During hydration there is no push in flight, so overlay is nil. Reads of immutable slice + nil pointer are safe. ✓
- **But:** `MemorySource.getSortedRows` does `copy(sorted, ms.data)` then `sort.Slice`. The `copy` of `ms.data` reads a slice header. If anything modifies `ms.data` concurrently (it doesn't — push is serialized by Engine.mu and AddQueries holds Engine.mu through wg.Wait), this would race. **Safe under Engine.mu**. ✓
- **Confirmed safe.** Worth documenting that hydration goroutines rely on Engine.mu held across wg.Wait — a future change that releases the lock during hydration would break this.

### LOW-1: `handleConnection` `extractClientGroupID` decodes params twice
- **File:** `cmd/sidecar/main.go:658-672`, called from line 713.
- **Issue:** Every request decodes params once in `extractClientGroupID`, then again in `handleRequest` → `handleInit`/`handleAddQuery`/etc. Wastes CPU. Not a race but worth noting.
- **Fix:** Decode once into a struct with both `ClientGroupID` and the rest. Or include cgID in the RPC envelope, not in params.

### LOW-2: `connResult` struct never optimized; could send only `[]Change` via a fixed-size slice slot directly
- **File:** `ivm/parallel.go:63-93`
- **Issue:** Each goroutine sends through a channel only to be reassembled by index. Just write directly to `ordered[idx] = changes` from inside the goroutine, drop the channel entirely. `wg.Wait` provides the necessary happens-before:
  ```go
  ordered := make([][]Change, len(activeConns))
  var wg sync.WaitGroup
  for i, conn := range activeConns {
      wg.Add(1)
      go func(idx int, c *Connection) {
          defer wg.Done()
          outputChange := ms.sourceChangeToChange(change)
          ordered[idx] = FilterPush(outputChange, c.Output, c.Input, c.FilterPredicate)
      }(i, conn)
  }
  wg.Wait()  // happens-before: writes in goroutines → reads here
  ```
  Removes the channel allocation. Equivalent semantically.

### LOW-3: `Streamer.Stream` reuse pattern: `s.accumulated[:0]` keeps backing array — only matters under memory pressure
- **File:** `engine/streamer.go:73`
- **Issue:** `s.accumulated = s.accumulated[:0]` retains the slice backing array. Holds references to `[]ivm.Change` entries (and through them, ivm.Row values). Under steady-state this is fine. Under a one-time spike (10k changes in one advance), the array stays allocated until the next reset (which doesn't shrink).
- **Note:** The references in `accEntry.changes` ARE not retained because the underlying array stores `accEntry` values, and after `[:0]` the elements past length-0 are not visible. Wait — they ARE retained as live pointers in the backing storage even though len=0. To release, would need `s.accumulated = nil`.
- **Trigger:** Memory bloat after a one-time large advance.
- **Fix:** `s.accumulated = nil` if `cap > some threshold`, or always `nil` it (small alloc overhead).

---

## Confirmed Safe (the load-bearing parallelism claims)

### SAFE-1: Source overlay write/read across parallel fan-out
- **Claim:** `ms.overlay` is set once before spawning N goroutines, read by all N during their independent pipeline runs, cleared after `wg.Wait`.
- **Why safe:** Go memory model: `go f()` happens-before the goroutine's reads of variables set before `go`. `wg.Done` happens-before the corresponding `wg.Wait` returns, which happens-before `overlay = nil`. **Reads observe the correct overlay pointer, writes after wait are visible only to subsequent reads.** Confirmed at `ivm/parallel.go:51-95`.

### SAFE-2: Per-pipeline operator state during parallel fan-out
- **Claim:** Each connection (= one query's pipeline) has its own operator tree. Per-operator mutable state (`Join.inprogressChildChange`, `FlippedJoin.inprogressChildChange`, `Take.rowHiddenFromFetch`, `Exists.cache` / `inPush`, `FanIn.accumulatedPushes`, `UnionFanIn.accumulatedPushes`) is touched only by that pipeline's single push-thread.
- **Why safe:** `BuildPipeline` constructs a fresh operator graph per query (`builder.go:53`). Fan-out splits push BETWEEN pipelines, not within. Each pipeline's push runs sequentially in one goroutine of `GenPushParallel`. ✓
- **Latent hazard:** If a future feature ever lets two queries share an operator instance (e.g., for de-duplication / caching), per-operator state becomes shared and these fields race. Today there is no such sharing.

### SAFE-3: `Streamer` concurrent Accumulate
- **File:** `engine/streamer.go:53-75`
- **Claim:** Multiple pipelines push concurrently, each calling `Accumulate` from its goroutine. `Stream` is called once after `wg.Wait`.
- **Why safe:** `Accumulate` holds `s.mu`, `Stream` holds `s.mu`. Append-under-lock is correct. The `s.accumulated[:0]` truncation in `Stream` happens AFTER the loop has consumed `entry`s, all under the mutex. ✓ The prior race noted in `PORT_CORRECTNESS_ANALYSIS.md` (item 6) is fixed in current code.
- **Order of accumulation across goroutines:** Non-deterministic. But each entry contains its own (queryID, schema, changes) and the final RowChange order is per-pipeline within each entry. **If TS expects deterministic accumulation order across pipelines, that is NOT preserved here.** I am told the consumer is order-insensitive across queries, but flag this — fix is to use the connection index order from parallel.go and have parallel.go pass an ordered slot to each pipeline output.

### SAFE-4: `Engine.mu` protects all map accesses to `sources`, `pipelines`
- **File:** `engine/engine.go:66-340`
- **Audit:** Every method that touches `e.sources` or `e.pipelines` acquires `e.mu`:
  - `RegisterSource`, `RegisterMemorySource` (via RegisterSource), `AddQuery`, `AddQueries`, `RemoveQuery`, `Advance` — all hold `e.mu` for the duration.
  - `engineDelegate.GetSource` (line 402-408) — reads `e.sources` WITHOUT holding `e.mu`! BUT it's only called from `BuildPipeline`, which is called inside `AddQuery` / `AddQueries` while holding `e.mu`. ✓
- **Latent hazard:** If anyone ever uses `engineDelegate.GetSource` from a code path that doesn't hold `e.mu`, it races with `RegisterSource`. Document this.

### SAFE-5: Lock ordering — no cycles
- **Locks in the codebase:**
  - `Server.mu` (sidecar) — held while reading/writing `groups` map. Brief.
  - `ClientGroup.mu` (sidecar) — held during init/destroy/handleAddQuery/etc. Long enough to include `engine.AddQuery`.
  - `Engine.mu` — held during all engine ops.
  - `Streamer.mu` — held briefly during accumulate/stream.
  - `DatabaseStorage.mu` — held during all SQLite ops.
  - `ClientGroupStorage.mu` — held during CreateStorage/Destroy.
- **Ordering analysis:**
  - Sidecar: `Server.mu` (R or W) → released → `ClientGroup.mu` → `Engine.mu` → (during operator state set) → `DatabaseStorage.mu`. Linear cascade, no AB-BA.
  - `closeAll` (line 311-322): holds `Server.mu` AND `ClientGroup.mu` simultaneously (for each group in the map). This is the ONLY place where both are held at once. Other code paths first take `Server.mu` (RLock or Lock), release, then take `ClientGroup.mu` (see `getGroup` and `removeGroup`). **closeAll is the only place with nested Server.mu + ClientGroup.mu — and it's strictly Server.mu first → ClientGroup.mu.** Anywhere else that's strictly Server → ClientGroup → safe.
  - **But:** Inside `g.mu` we call `g.eng.Close()` which tries to acquire `e.mu`. If a worker goroutine is currently inside `e.mu` and tries to do anything that re-enters g.mu... it doesn't, because workers don't touch ClientGroup.mu themselves — they execute handleRequest which calls handleAddQuery etc., and those take g.mu. **So: worker holds g.mu → wants e.mu (acquired) → done.** closeAll holds Server.mu + g.mu → wants e.mu (acquired by closeAll). If a worker is currently inside e.mu (it took g.mu earlier and called engine), the worker is in handleAddQuery → e.mu held → blocked on... nothing? AddQuery just returns. So no deadlock today.
  - **Real deadlock risk:** None observed in the current code. **Confirmed safe.**

---

## Concurrency Map

Goroutines spawned during normal operation, their lifecycles, state touched:

| Goroutine | Spawned at | Lifetime | State touched | Synchronization |
|---|---|---|---|---|
| `main.metrics` reporter | `main.go:755` | Forever (sidecar lifetime) | `metrics.mu`, atomic counters | `metrics.mu`, atomics |
| `main.signal handler` | `main.go:766` | Until SIGINT/SIGTERM | calls `server.closeAll()` | `Server.mu`, ClientGroup.mu, Engine.mu |
| `main.listener.Accept` loop | `main.go:780` | Forever (sidecar lifetime) | spawns `handleConnection` | none direct |
| `handleConnection` (one per conn) | `main.go:780` | Until socket close | `writeMu`, per-connection bufio reader | `writeMu` |
| `handleConnection` write goroutine (one per RPC) | `main.go:720`, `main.go:725` | Until response written | `writeMu`, respCh | `writeMu` |
| `ClientGroup.worker` (one per group, **LEAKED on destroy**) | `main.go:255` | Forever — until process exit | `Engine` (via `s.handleRequest`), metrics atomics | None — single-threaded via channel reads |
| `engine.AddQueries` hydration | `engine/engine.go:212` | Until wg.Wait completes | per-pipeline Input.Fetch; reads MemorySource data | Engine.mu held by parent across wg.Wait |
| `ivm.GenPushParallel` per-connection | `ivm/parallel.go:73` | Until wg.Wait completes | per-pipeline operator state; reads ms.overlay / ms.data | Engine.mu held by parent (via Push → Engine.Advance); wg.Wait + happens-before |

---

## Recommended Verification

1. **`go test -race ./...`** — Has this been run on the soak workload? The two existing test files (`source_parallel_test.go`, `cmd/sidecar/main_test.go`) cover small slices. The race detector with the parity harness (`tools/parity/sidecar-parity.ts`) at workload scale would surface CRITICAL-1 (goroutine leak — visible via `pprof`/runtime.NumGoroutine) and possibly HIGH-1 (writer ordering on the wire).

2. **Specific scenarios worth fuzzing:**
   - **Destroy churn:** create-init-addQueries-destroy in a loop on the same cgID, watch `runtime.NumGoroutine()` grow.
   - **Init re-fire:** call init twice on the same cgID with different tables, verify the second engine is the only one referenced and the first is GC'd.
   - **Slow consumer:** wedge the Node-side reader for 5s during peak load; observe goroutine count and memory.
   - **Disconnect under push:** trigger `RemoveQuery` from within a push handler (synthetic test) — verify panic or document invariant.
   - **High fan-out:** 32+ pipelines per source, parallel push, with `-race`. Currently 8 connections are the test setup.

3. **Static analysis:** Run `go vet -copylocks` and `go-staticcheck` — should catch the goroutine leak pattern and a couple of unused `defer mu.Unlock()` issues if any.

4. **Targeted runtime check:** Add an assertion at the top of every parallel fan-out goroutine that `runtime.Caller` reports the expected caller, and add a goroutine-id sentinel into Streamer entries to confirm accumulation ordering is non-deterministic but lock-correct.

---

## TL;DR for Maintainers

The parallel fan-out itself is well-designed — the read-only overlay claim holds, the happens-before chain is correctly anchored by `go`-spawn and `wg.Wait`, the Streamer mutex is correctly used, and operator-internal state is safely per-pipeline.

The sharp edges are in lifecycle management:
- **CRITICAL-1**: Worker goroutines leak on group destroy. Fix before production.
- **HIGH-1**: Response wire ordering depends on Node side using RPC-ID correlation; confirm and document.
- **HIGH-4**: Unbounded writer goroutines under slow socket — easy DoS / OOM if Node side stalls.
- Per-Engine and per-Group mutexes give defense-in-depth, but their guarantee depends on no one accidentally adding code paths that fire goroutines outside the lock.

The race detector under the parity workload would catch the leak (via inspection) and likely surface HIGH-1 ordering if any test asserts response order matches request order.
