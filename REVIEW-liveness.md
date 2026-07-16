# Liveness / wedge audit — feat/napi-transport (go-ivm + mono)

2026-07-16, triggered by the sandbox incident: mattn/go-sqlite3 spawns a helper
goroutine + channel for EVERY `Next()`/`Exec()` when the passed context has a
`Done()` channel. The N+1 EXISTS hydrate pattern turned that into scheduler
starvation → multi-minute wedges + 65s client reload loops. Fixed for the
hydrate pool path in 6b5dfaa (`context.Background()` → mattn's synchronous
`nextSyncLocked`). This audit hunts the REST of the class: every path that can
block indefinitely, every lock held across a blocking call, every watchdog that
observes but cannot act.

Scope: go-ivm @ 6b5dfaa (feat/napi-transport), mono @ cf1c10fcd
(feat/napi-transport — identical to the deployed sandbox image).

## W0 — URGENT (process): working tree REVERTS the wedge fix

`git diff` on go-ivm shows `internal/tablesource/source.go` with `s.ctx` put
BACK into `checkoutStmt`/`queryStmt` in `fetchViaBoundReaderStream`, and the
`ctx.Err()` guard + fix comment deleted — a clean revert of 6b5dfaa's hunk
(2 insertions, 17 deletions). Almost certainly the "before" arm of an A/B run
for the new (untracked) `goroutine_overhead_test.go`. **Do not build or deploy
an image from this tree** until the revert is discarded (`git checkout --
internal/tablesource/source.go`) — shipping it brings the 17-minute wedge back.

## W1 — HIGH: advance fetch paths still pay the per-Next goroutine tax

`fetchSerial` (source.go:1634) and `fetchDuringPushStream` (source.go:1882)
query with `advanceQueryCtx()` = the advance budget context (cancellable) or
`s.ctx` (cancellable). Either way `ctx.Done() != nil` → mattn spawns a goroutine
per row fetched on the ADVANCE path — the exact mechanism that wedged hydrate.
Exposure is lower (advance row volumes are delta-sized) but is multiplied by
FlippedJoin/planner traffic and by catch-up advances after gaps.

Caveat: this one is DELIBERATE — the budget deadline interrupts a WAL-blocked
step via mattn's ctx watcher (`sqlite3_interrupt`). Dropping ctx here (as the
hydrate fix did) would remove the advance path's only mid-step liveness bound,
and the advance already has `checkAdvanceAbort` per fetched row for the
economic abort. Decision needed, in order of preference:
 1. Measure: if advance-path rows/fetch are small in prod traces, ACCEPT the
    overhead (bounded > fast) and document it.
 2. If it matters: vendor a tiny interrupt shim (we already build mattn against
    the wal2 fork) — one watchdog goroutine per ADVANCE that calls
    `sqlite3_interrupt` on budget expiry, then pass `Background()` to queries.
    One goroutine per advance, not per row.

## W2 — HIGH: apply path spawns a goroutine per row CHANGE

`execPushStmtLocked` (source.go:668) runs `st.ExecContext(s.ctx, ...)` for
every applied change. mattn's `exec()` has the same fork: cancellable ctx →
goroutine + channel per call. A GO_IVM_MAX_DIFF_CHANGES=50k advance apply =
50k goroutine spawns, inside the advance critical section with the source
lock held. Same fix shape as 6b5dfaa: check `s.ctx.Err()` once per batch (or
every N changes, alongside the existing budget clock), then
`ExecContext(context.Background(), ...)`. Unlike W1 there is no interrupt-value
lost that matters: each Exec is a single-row INSERT/UPDATE/DELETE — individual
statements never run long; the batch-level budget check bounds the loop.

## W3 — HIGH: no end-to-end liveness bound for producer-side wedges

The layered bounds all have the same blind spot, proven live by the 12m33s+
`[GO-IVM][WEDGE] cg=u3u7pgqd5imabf1ggg` in the sandbox:

 - TS pull-hydrate RPC timeout is deliberately 0 (consumer-driven), and
   `computeBoundTimeoutMs` for advance streams defaults to **0** too.
 - Go's pull idle sweeper only fires on gates with `waiters > 0`. A producer
   stuck INSIDE a cgo call (SQLite step) never reaches the gate → invisible.
 - The wedge watchdog (wedgewatch.go, 90s) logs WEDGE / WEDGE-STACKS /
   WEDGE-CLEAR — observability only, no action.
 - The idle-group reaper deliberately skips inFlight groups (correct for live
   hydrates), so a wedged CG is reap-proof: pool readers + coread WAL-frame
   pin held until pod restart.

Post-6b5dfaa the dominant cause (scheduler starvation) is gone, but a single
long `sqlite3_step` — an unindexed low-selectivity scan where one `Next()`
walks the whole table, or a WAL-blocked step — is now UNINTERRUPTIBLE on the
hydrate path (Background ctx removed the only `sqlite3_interrupt` hook; the
engine's `cancelled` atomic and the gate are only consulted between
deliveries).

Recommendation — give the wedge watchdog an escalation ladder instead of
adding new timeouts everywhere:
 1. threshold (90s): current behavior — WEDGE log + one-time stack dump.
 2. 2× threshold: `streamGates.cancel(reqID)` for the wedged RPC (frees the
    parked-waiter shape and any consumer awaiting it).
 3. 4× threshold (needs the W1 interrupt shim): `sqlite3_interrupt` on the
    CG's pool-reader conns — the only lever that unwinds a running step.
 4. Last resort: surface an engine-unhealthy signal to TS → existing
    ResetPipelinesSignal / resetEngine path (destroy + reinit) so the CG is
    rebuilt instead of leaking forever. This also closes the still-open
    "napi.send rc → restart wiring" LOW from the transport review.

## W4 — MED: hydrate cancellation latency is O(scan), not O(1)

Cancel (client `.return()`, teardown, idle sweep) is observed at delivery
points and at queued reader-acquire only. A scan-heavy, low-selectivity query
(the correlated-EXISTS shape: thousands scanned per row emitted) has few
delivery points, so a cancelled hydrate keeps scanning — holding its pool
reader and WAL pin — for up to the full remaining scan. Cheap fix: check the
gate/cancelled atomic every N rows (e.g. 1024) inside
`fetchViaBoundReaderStream`'s row loop; an atomic load per N rows is noise
next to the cgo call per row.

## W5 — MED: a wedged CG is a POD-WIDE hazard via the WAL-frame pin

The coread pin holds a wal2 read frame; the checkpointer cannot advance past
the oldest reader, so ONE wedged CG stalls checkpointing for the whole
replica → unbounded WAL growth + degraded reads for every CG on the pod. The
streamgate idle sweep was designed as exactly this bound ("bounds the
WAL-frame pin") — the waiters==0 blindness (W3) defeats it. W3's escalation
ladder is the fix; this finding is why it's worth doing.

## W6 — MED: deliverPumpFrame abandons ONE frame then keeps going

abi.go deliverPumpFrame: after 2× deliver-timeout with the TSFN queue full it
logs and `return false` — the FRAME is dropped but the pump, the RPC, and the
stream continue. For a pull stream that's a silently missing row (client gets
wrong data; best case the orphan-final guard throws much later, worst case
nothing notices). A JS loop unresponsive for that long is already fatal
territory: escalate to stream-terminal-error (settle the RPC with -32000) or
host-death instead of dropping one frame and pretending.

## W7 — LOW: stale comment invites re-breaking the fix

reader_pool.go:554 (`queryStmt`) still says "ctx cancellation (CG teardown)
interrupts a blocked step exactly as the database/sql path did" — no longer
true: the only production caller now passes `context.Background()`. Update the
comment to state the OPPOSITE contract (sync path, cancellation between
queries + conn close on teardown), so nobody "fixes" it back to `s.ctx`.

## W8 — LOW: reload-loop amplification has no server-side damper

Every pull idle-timeout surfaces to the client as Internal → the client
reconnects → identical re-hydrate → re-stall (the observed 65s loop, 93/hr).
The tiered reset breaker covers ADVANCE resets; repeated hydrate-stream
failures for the same CG have no counter/backoff/alert. At minimum add a
metric (`hydrate_stream_idle_timeouts` per CG) + alert; optionally refuse
re-hydrate for a cooling-off period after N consecutive idle-timeouts.

## Verified clean (audited, no action)

 - C boundary: `goivm_send` = leaf-mutex enqueue to unbounded sendQ (never
   blocks the JS thread); `goivm_stream_credit`/`cancel`/`queue_drained` are
   O(1) leaf-mutex direct calls; no JS→Go call can join a Go→JS backpressure
   cycle.
 - rowplane lock discipline: rp.mu never held across a blocking deliver;
   staged nonblocking flush; panic paths defer-unlock (the historical
   park-holding-mutex wedge is fixed and documented in-file).
 - TS Subscription: `cancel()`/`fail()` settle all pending `push()` results
   (`'unconsumed'`) — no hung `#push` awaits in client-handler.
 - Pull credit accounting matches on both sides (row-bearing deliveries cost
   1; defs/finals/error frames free; top-up at low-water; `onStreamOpen`
   fires before send so reqID can't be null).
 - Snapshotter reads/writes use `context.Background()` (sync mattn path).
 - Control-plane RPC timeouts: default 30s; init 120s; destroy has its own
   fast-path queue (not FIFO-blocked behind a wedged handler).
 - Reaper double-checks inFlight under lock (skip is correct; the leak is
   W3's job to bound).
 - `awaitGoInit` is restart-aware and bounded by the init RPC timeout.

## Priority order

1. W0 — restore the fix in the working tree before any build (1 command).
2. W3 escalation ladder (+ W5 falls out of it) — this is the "never any
   wedges" guarantee: every other bound can fail, this one must not.
3. W2 — mechanical, same shape as 6b5dfaa, do it with W7's comment fix.
4. W4 — cheap insurance, same PR as W2.
5. W1 — measure first; decide accept-vs-shim.
6. W6, W8 — next hardening pass.

---

# Post-implementation review (2026-07-16, commits b0bd297..80912b7)

Verdict: mechanism is sound and well-built (C-heap flag, detach-before-close,
teardown CancelAll on both pools, W6 host-death, tests prove abort-mid-scan) —
but NOT prod-ready yet: three blockers.

## Blockers
- **B1 — advance budget timer was never wired.** SetAdvanceCtx is vestigial,
  no time.AfterFunc exists anywhere, CancelBudget is never set by anyone.
  The advance path LOST its pre-existing budget interrupt (old ctx mechanism
  removed, replacement not added). Comments (source.go fetchSerial doc,
  commit 31d3974 message) describe the AfterFunc as if it exists.
- **B2 — snapshotter frame conns have NO progress handler.** Registration
  covers pool readers + Source.prevConn only. Drive mode (THE prod path)
  binds externalConn = snapshotter frame conns for advance reads
  (activeConn()); those now run Background-ctx queries with no handler =
  uninterruptible. Register at snapshotter conn creation (or BindConn).
- **B3 — gas-meter units off by 4096×.** goivm_progress_cb decrements budget
  once per CALLBACK (every progressN=4096 opcodes); defaultBudget=50M
  therefore bounds ~200 BILLION opcodes (hours), not 30-60s. Set
  defaultBudget = intended_opcodes / progressN (~12K) or decrement by
  progressN in C.

## Should-fix
- C4 half-done: CancelReason recorded but Reason() consumed NOWHERE —
  SQLITE_INTERRUPT surfaces as generic fetch panic → -32000. Map
  Stream/Teardown reasons to quiet/teardown unwind at the fetch panic sites.
- Class guard flaky: TestSourceFetchUsesSynchronousPath failed under full-
  package parallel load (observed: 1190 vs 1003 ns/row), passes 3/3 solo.
  Replace wall-clock ratio with a goroutine-count-delta assertion.
- Ladder rung 2 is a silent no-op for non-pull RPCs (every RPC has a numeric
  id → reqIDFloat!=0 always; streamGates.cancel on a non-pull id does
  nothing). Fatal rung is therefore universal (fine, mislabeled). Add
  group-level CancelAll as the 2x action for non-pull wedges.
- registerProgressHandler swallows rawSQLiteHandle errors — if mattn's
  struct changes, the whole mechanism silently disappears. Log-once + metric.
- wedgewatch uses os.Exit(1) directly; route through the shared fatal path
  for consistent flush/logging.

## Mono (unchanged at cf1c10f) — carried open items
- -32105 scalar-reset unmapped in TS (removal-sweep review).
- napi.send rc→restart wiring; T1(b) CI napi e2e job; full vitest suite
  post-v1.6.1 merge; untracked rust-ivm-driver.ts should be deleted (its
  import was removed to fix the crash-loop; the stray file invites re-add).
- D5 (TS-side generous pull-stream ceiling) not implemented — optional
  defense-in-depth.

## Verified
Full go test ./... green; -race green (tablesource, cmd/sidecar); napilib
c-shared build OK. W0 resolved (working tree clean). Flag lifecycle,
detach-before-close ordering, and teardown cancel coverage all correct.

---

# Verification of 16ac9ca (B1/B2/B3/C4 + should-fixes)

All four land as claimed; build clean, full suite green, -race green
(tablesource, cmd/sidecar). B3's C decrement has a proper underflow guard;
C4 pool-path mapping covers both the query-error and mid-scan sites with
r.cancelFlag.Reason(); rows.Err() is checked on every scan loop (no silent
truncation possible); watchdog fatal rung is now universal.

Three follow-ups, one shared root cause:

- **R1 (HIGH) — the cancel path can block on locks the wedged goroutine
  holds.** CancelAllSourceConns takes e.mu → CancelConns takes s.mu; the
  budget AfterFunc takes s.mu. fetchSerial drains its cursor UNDER s.mu and
  advances run under the engine mu — so for exactly the stuck-statement case
  these mechanisms exist for: (a) the budget timer parks on s.mu and cannot
  set the flag until the statement ends (gas meter becomes the only real
  bound), and (b) the watchdog's single scan goroutine blocks inside the 2x
  rung — stalling scans for ALL groups and this CG's own 6x fatal. A non-SQL
  wedge (mutex, non-SQL cgo) never releases → watchdog permanently dead =
  W3's blind spot rebuilt inside its own fix.
- **R2 (MED) — use-after-free window**: checkInterruptPanic at the lazy
  fetch site (:1967) runs OUTSIDE s.mu (post-F7 streaming section) despite
  its "MUST hold s.mu" contract; races BindConn/UnbindConn's Free of
  externalCancelFlag (drive mode rebinds every advance) → read of freed C
  memory.
- **Shared fix for R1+R2**: make flag access lock-free and lifetime-stable —
  atomic.Pointer[connCancelFlag] on Source/poolReader, allocate ONCE per
  slot, clearCancel+setBudget on rebind instead of Free+realloc, Free only
  at Source.Close/pool close. Then AfterFunc/CancelConns/checkInterruptPanic
  read without s.mu and CancelAllSourceConns drops e.mu.
- **R3 (MED) — classification gap at the likeliest abort site**: scanRows
  and the lazy loop's rows.Err() panics are generic — a budget abort
  surfacing MID-SCAN (the common case for a long scan) maps to -32000
  'unclassified' → the transient breaker bucket (6/60s) instead of
  rpcCodeAdvanceAborted → economic bucket. Repeated lawful budget aborts
  could trip the breaker → CG teardown (the exact reason-blind-breaker
  regression the tiered breaker fixed). Add the reason mapping to the two
  rows.Err() sites + scanRows.

LOW: C #define GOIVM_PROGRESS_N / Go progressN dual-maintained (add a test
assert); BindConn budget is cumulative per bind (per-advance in drive —
fine, watch in soak); AfterFunc re-resolves the ACTIVE flag at fire time
(may hit the successor conn on a mid-advance rebind — same advance, benign).
