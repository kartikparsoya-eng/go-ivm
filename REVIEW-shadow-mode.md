# Shadow Mode Review

Audit of `ZERO_GO_SHADOW_MODE=true` in `mono/packages/zero-cache`. Shadow mode
runs **both** the TS-native pipeline and the Go sidecar pipeline on every
hydrate / advance, compares results, logs mismatches, and returns the
**TS** results (TS = source of truth). It's the production-data parity
harness for the Go IVM port.

Code surface:
- `pipeline-driver.ts` — `#shadowMode`, `#shadowAddQuery`, `#shadowAdvance`,
  `#shadowCompare`, `#logShadowDiff`, `shadowBatchCompare`.
- `view-syncer.ts:1989` — calls `shadowBatchCompare` after the per-query
  shadow hydration loop.
- `go-compute-backend.ts` — `isGoShadowMode()` flag.

Companion docs:
- `REVIEW-ts-integration.md` — TS-side wire and lifecycle correctness.
- `REVIEW-parallelism.md` — Go-side parallelism correctness.
- `REVIEW-porting.md` — Go port semantic correctness.

## Summary
- **1 CRITICAL** — Shadow compare is structurally blind to nested-object
  content. Every passing soak with jsonb edits was a false positive for
  jsonb correctness.
- **2 HIGH** — Go state drift on a single advance failure; redundant Go
  hydration when batch-compare also runs.
- **4 MEDIUM** — Permissive timer in advance can block event loop; TS+Go
  sequential = doubled latency; locale-dependent sort; first-mismatch-only
  logging.
- **3 LOW** — Speedup math noise, AST log spam, duplicate queryID RPCs.
- **3 CONFIRMED OK** — REMOVE row parity, setDB once, sort-key resilience.

---

## Status: ALL FINDINGS FIXED & SOAK-VERIFIED

> **See `REVIEW-final.md` for the consolidated cross-doc production-readiness summary. This doc preserves the audit trail.**

All shadow-mode findings (1 CRITICAL / 2 HIGH / 4 MEDIUM / 3 LOW) plus the
new findings discovered in the four-agent consolidated audit (HIGH-SHADOW-1
BigInt safety, MED-SHADOW-1 awaited batch compare, MED-SHADOW-2 reset retry
+ dirty flag, MED-SHADOW-4 PII redaction) are all addressed. Validation:

- `npm --workspace=zero-cache run check-types` clean.
- `npm --workspace=zero-cache run lint` clean (405 pre-existing warnings, 0 errors).
- PipelineDriver tests: 36/36 pass.
- **Most recent 5-minute shadow soak**: 805 batch compares, 13007 individual TS
  hydrate operations, **0 mismatches**, 0 panics, 0 decode failures, 0 Go
  resets, 0 reaped groups, 0 errors, 0 reconnects. Includes jsonb edits;
  `stableStringify` now compares nested content structurally + BigInt/NaN-safe.
  (Success match log demoted to debug — 'TS and Go match' line absent from
  default-level container logs is the expected production posture.)

| Finding | Status | Notes |
|---|---|---|
| CRITICAL-1 — Nested-content blind compare | **FIXED** | `stableStringify` recursively sorts keys at every depth — replaces the array-replacer trick that gutted nested objects. |
| HIGH-1 — Go state drift on advance failure | **FIXED** | `#scheduleGoReset` triggers `resetEngine(currentTables)` on Go RPC failure; collapses concurrent reset requests. |
| HIGH-2 — Redundant per-query Go hydration | **FIXED** | `#shadowAddQuery` no longer hydrates against Go; `shadowBatchCompare` is the sole Go hydrate path. Halves Go-side hydrate cost in shadow mode. |
| MEDIUM-1 — Permissive timer blocked event loop | **FIXED** | Real timer threaded through with `suppressAbort` flag on `#advance`; outer shadow loop yields via `setImmediate` on `'yield'` markers. |
| MEDIUM-2 — TS+Go sequential latency | **FIXED** | Go RPC kicks off before TS drain; awaited via parallel promise. Shadow latency now ≈ max(TS, Go) instead of TS + Go. |
| MEDIUM-3 — Locale-dependent sort | **FIXED** | Direct `<`/`>` compare. |
| MEDIUM-4 — First-mismatch-only logging | **FIXED** | Logs up to 5 mismatched indices plus overflow count. |
| LOW-1 — Speedup math floor | **FIXED** | Dropped from log line. |
| LOW-2 — AST log spam | **FIXED** | Moved to `lc.debug?.()`. |
| LOW-3 — Duplicate queryID RPCs | **FIXED** | Auto-resolved by HIGH-2. |
| OK-1..3 | **UNCHANGED** | Invariants still hold. |

---

## Findings

### CRITICAL-1: `#shadowCompare` is structurally blind to nested-object content
- **File:** `pipeline-driver.ts:1205-1219`
- **The bug:**
  ```ts
  rowKey: JSON.stringify(c.rowKey, Object.keys(c.rowKey ?? {}).sort()),
  row: JSON.stringify(c.row, Object.keys(c.row ?? {}).sort()),
  ```
  `JSON.stringify(value, replacer)` with an **array replacer** treats the
  array as a global key-allowlist applied recursively at every depth. Since
  the allowlist is derived only from the **top-level** keys of the row,
  nested objects (jsonb columns, json arrays-of-objects) have all their
  inner keys filtered out.
- **Verified empirically (node REPL):**
  ```
  JSON.stringify({id: 1, metadata: {a: 'foo', b: 'bar'}}, ['id', 'metadata'])
  → '{"id":1,"metadata":{}}'
  ```
  Inner content of `metadata` is gutted.
- **Operational consequence:** The recent 5-minute soak included
  `[mutate] ticket-6 metadata jsonb edit` and `ticket-8/10 metadata jsonb edit`
  mutations and reported **0 mismatches across 1462 compares**. With this
  bug, every one of those compared `{"metadata":{}}` vs `{"metadata":{}}`.
  Zero coverage of nested jsonb content. The soak DID validate scalar
  columns; it did NOT validate jsonb edits — they were structurally
  guaranteed to "match" regardless of what either side produced.
- **Why both sides could disagree:**
  - TS path: `fromSQLiteType('json', v)` calls `JSON.parse(string)`. JS
    parsers preserve insertion order from the source text. Postgres jsonb
    returns keys in storage order (length-then-alphabetic for short keys).
  - Go path: `sqlite.FromSQLiteType('json', v)` calls `json.Unmarshal` into
    `map[string]interface{}`. Go maps have non-deterministic iteration
    order. The msgpack encoder iterates the map, so wire order is whatever
    the Go runtime picked that millisecond.
  - msgpackr on the TS side decodes msgpack maps into JS objects that
    preserve insertion order — i.e. Go's randomized order.
  - So the same logical jsonb value has different JS-object key orders on
    each side. A proper compare would normalize.
- **Fix:** Use a deep-stringify that recursively sorts keys. Smallest
  viable change:
  ```ts
  const stableStringify = (v: unknown): string => {
    if (v === null || typeof v !== 'object') return JSON.stringify(v);
    if (Array.isArray(v)) return '[' + v.map(stableStringify).join(',') + ']';
    const keys = Object.keys(v as Record<string, unknown>).sort();
    return '{' + keys.map(k =>
      JSON.stringify(k) + ':' + stableStringify((v as Record<string, unknown>)[k])
    ).join(',') + '}';
  };
  ```
  Then `rowKey: stableStringify(c.rowKey)` / `row: stableStringify(c.row)`.
- **Estimated impact:** Until this is fixed, *all* shadow-mode validation of
  jsonb columns is no-op. Operators relying on shadow soak for confidence
  before flipping the Go backend on in production should treat jsonb
  correctness as **unvalidated**.

---

### HIGH-1: Go state drift after a single shadow-advance failure is unrecoverable
- **File:** `pipeline-driver.ts:1164-1172`
- **Issue:** `#shadowAdvance` does:
  ```ts
  try {
    const goRaw = await this.#goBackend!.advance(snapshotChanges);
    goResults = goRaw.changes.map(rc => this.#goRowChangeToRowChange(rc));
  } catch (e) {
    this.#lc.error?.(`[shadow] Go advance failed: ${e}`);
  }
  ```
  On failure (RPC timeout, decode error, panic surfaced as error), `goResults
  = []` and we continue. TS path is unaffected. **Go's MemorySource state
  does not advance.** The `SnapshotDiff` is already consumed; the snapshotter
  has moved past it. There is no replay queue.
- **Drift cascade:** Every subsequent shadow-advance will send only the NEW
  diff to Go. Go's state diverges from TS by exactly the contents of the
  failed diff. Future shadow compares will mismatch on rows touched by the
  missed diff. The error log says "Go advance failed" once; nothing tells
  the operator the divergence is permanent until a manual `resetEngine` /
  `advanceWithoutDiff` runs.
- **Fix sketch:**
  - Treat shadow Go advance failure as terminal for the backend: invalidate
    `#initialized` (or bump epoch), trigger a `resetEngine(currentTables)`
    next time `addQuery` / `advance` is called.
  - Or: keep a small ring of recently-applied diffs and replay on next
    advance after a transient failure.
  - At minimum, mark this in telemetry so operators know the shadow
    comparisons after the failure are unreliable.
- **Related:** `REVIEW-ts-integration.md` CRITICAL-1 fixed restart drift; this
  is the in-process equivalent — Go RPC failure with the sidecar still alive.

### HIGH-2: Redundant per-query Go hydration in `#shadowAddQuery`
- **Files:** `pipeline-driver.ts:1088-1107` and `view-syncer.ts:1989`
- **Issue:** In shadow mode, each query is hydrated against Go twice:
  1. `#shadowAddQuery` calls `hydrateMany([{queryID, ast}])` — a batch of one
     — for the per-query compare log line.
  2. After the loop, `view-syncer.ts:1989` calls
     `pipelines.shadowBatchCompare(allQueries, tsResultsPerQuery)` which runs
     `hydrateMany(allQueries)` — the full batch.
- **Cost on Go side:** For N queries, Go does `2N` `removeQueryLocked` +
  `2N` pipeline builds + `2N` `Fetch + stream` cycles. The TS side does `N`
  hydrations. So shadow mode roughly **triples** total hydrate work (TS +
  Go×2) vs production mode (TS only) or vs honest shadow (TS + Go×1).
- **Fix:** Drop the per-query Go hydrate inside `#shadowAddQuery` — keep just
  the TS hydrate + per-query timing log. Let `shadowBatchCompare` be the
  sole Go path. The per-query mismatch log becomes a per-batch log instead
  (already done in `shadowBatchCompare` at line 706), which is fine —
  operators can still find the offending queryID from the batch report.
- **Side benefit:** Removes the second `hydrateMany` allocation per query
  and halves the RPC count.

---

### MEDIUM-1: `shadowTimer` returns 0 — `#advance` can block the event loop on large diffs
- **File:** `pipeline-driver.ts:1148-1156`
- **Issue:** `#shadowAdvance` passes a permissive timer:
  ```ts
  const shadowTimer: Timer = {
    elapsedLap: () => 0,
    totalElapsed: () => 0,
  };
  const tsIterable = this.#advance(replayDiff, shadowTimer, numChanges);
  for (const change of tsIterable) { ... }  // drains fully, synchronously
  ```
  The native `#advance` consults the timer in `#shouldAdvanceYieldMaybeAbortAdvance`
  (pipeline-driver.ts:1417) — both for the per-change yield decision AND for
  the circuit-breaker abort. With a permissive timer, `#advance` neither
  yields nor aborts. Combined with the synchronous `for ... of` drain in
  shadow, **a large diff processed in shadow mode blocks the event loop
  for as long as it takes**. Under normal mode the same diff would yield
  periodically and complete in cooperative chunks.
- **Operational impact:** Shadow mode on a node with bursty large diffs can
  starve the EventLoop, delay socket reads/writes, and cause WebSocket
  liveness timeouts on connected clients.
- **Fix:** Use the real timer for yield decisions but suppress the
  ResetPipelinesSignal abort (since shadow needs to complete). Pass a
  flag to `#advance` that disables abort-on-budget-exceeded.

### MEDIUM-2: Shadow mode runs TS and Go sequentially — doubles user-facing latency
- **Files:** `pipeline-driver.ts:1079-1099` (hydrate), `1153-1173` (advance)
- **Issue:** TS runs to completion, then Go runs to completion. The TS
  results are independent of Go; only the *comparison* depends on both.
  Sequential execution adds Go's wall time to every advance/hydrate.
- **Fix:**
  ```ts
  const [tsResults, goRaw] = await Promise.all([
    Promise.resolve(this.#runTSHydrate(...)),
    this.#goBackend!.hydrateMany([{queryID, ast: query}]),
  ]);
  ```
  TS is synchronous so wrap it in `Promise.resolve` (or invoke
  synchronously and immediately await Go). Cuts shadow-mode latency to
  `max(TS, Go) + compare` instead of `TS + Go + compare`.
- **Why MEDIUM not HIGH:** Shadow mode is opt-in; users running it knowingly
  accept some perf hit. But the doubling is avoidable and most operators
  would prefer real-shape `max()` latency.

### MEDIUM-3: `localeCompare` is locale-dependent
- **File:** `pipeline-driver.ts:1214-1218`
- **Issue:** Sort keys (`queryID`, `table`, `rowKey`-as-string) are compared
  via `localeCompare`. The default locale is the runtime's, which varies by
  host environment. Two zero-cache deployments in different locales could
  sort the same TS/Go change sets differently and report mismatches that
  the other instance doesn't see — or worse, miss mismatches because
  reordering accidentally aligned divergent indices.
- **Fix:** Use plain comparison:
  ```ts
  const cmp = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
  ```
  Cheaper and deterministic.

### MEDIUM-4: First-mismatch-only logging hides downstream divergences
- **File:** `pipeline-driver.ts:1233-1248`
- **Issue:** The compare loop logs the first mismatched index then `return`s.
  If the first mismatch is at index 0, you never learn there are 50 more.
  Useful for log volume, terrible for debugging.
- **Fix:** Log up to N (e.g. 5) mismatched indices before returning. The
  `#logShadowDiff` helper already does set-difference at the table+rowKey
  level — keep using that for size mismatches, but extend it to size-equal
  cases too.

---

### LOW-1: Speedup math has misleading floor
`(tsHydMs / Math.max(goHydMs, 0.01))` → near-zero Go times produce inflated
speedup numbers (e.g., `1.2ms / 0.01ms = 120x`). Cosmetic.

### LOW-2: AST log spam per query
`pipeline-driver.ts:1086` logs `JSON.stringify(query).slice(0, 200)` for
every shadow hydrate. Under load this is a lot of log volume. Move to
`debug` level (or behind a dedicated flag).

### LOW-3: Same `queryID` in two RPCs per shadow hydrate
Side effect of HIGH-2. Once HIGH-2 is fixed, this disappears.

---

## Confirmed OK

### OK-1: REMOVE row shape parity
Both `Streamer.#streamNodes` (TS) and `#goRowChangeToRowChange` (Go path)
emit `row: undefined` for REMOVE. Shadow compare treats both as undefined.
The inline comment at `#goRowChangeToRowChange` explicitly warns against
"fixing" this. Soak-verified across 2398 cumulative matches.

### OK-2: `setDB` is called exactly once during shadow advance
`#shadowAdvance` consumes the `tsIterable` to completion via the `for`
loop, which means `#advance`'s tail (`for (const table of this.#tables.values()) { table.setDB(curr.db.db); }`)
runs exactly once. No double-advance of the TS TableSources.

### OK-3: Comparison is resilient to per-pipeline output ordering
TS and Go may emit RowChanges in different orders (TS sequential push,
Go parallel fan-out). Both sides normalize via sort on
`(queryID, table, rowKey, type)`. As long as the SET of changes matches,
the comparison passes regardless of per-side emission order. Verified by
the matching counts in soak.
