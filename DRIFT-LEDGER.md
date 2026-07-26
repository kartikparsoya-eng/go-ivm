# Go-IVM Drift Ledger

This file records intentional Go-IVM behavior that is not a byte-for-byte port
of the current TypeScript implementation. Entries here should either become the
shared spec later, or be removed when Go and TS are aligned.

## D1: Duplicate Partition-Key Matching

Status: Intentional Go fix, not yet ported back to TS.

Go treats duplicate partition-key columns as one logical constraint key in
`ConstraintMatchesPartitionKey`: it builds a distinct-key set before comparing
against the constraint map. See `ivm/take.go:717-736`.

Current TS compares `partitionKey.length` to `Object.keys(constraint).length`
directly, so duplicate partition-key entries can fail even when the constraint
map contains the same logical key set. See `mono/packages/zql/src/ivm/take.ts:727-742`.

Reason: Go's join constraint is a map, so duplicate key names collapse before
Take sees the constraint. The Go behavior preserves the intended relationship
match instead of dropping rows from the Take state path.

Required follow-up: either port the distinct-key comparison to TS, or add a TS
test/spec note declaring duplicate partition keys unsupported before they reach
Take.

## D2: SQL Operator Whitelist

Status: ALIGNED (2026-07-22). TS now matches Go's fail-closed policy.

Go rejects operators outside its explicit SQL operator whitelist with
`*ivm.DataError` before SQL formatting. See `sqlite/query_builder.go:277-306`.

TS zqlite now carries the SAME `allowedOps` whitelist and throws before the
`sql.__dangerous__rawValue` interpolation in `simpleConditionToSQL`
(`mono/packages/zqlite/src/query-builder.ts`, parity tests in
`query-builder.test.ts`). Both engines reject the same operator set; the whole
column of previously-divergent behavior is closed.

Reason (retained for history): Go's sidecar accepts ASTs over the client
boundary; whitelisting avoids raw SQL operator interpolation from untrusted
input. An unsupported query shape is deterministic and tears down the client
group rather than silently producing an empty result.

## D3: Malformed Condition-Type Rejection

Status: ALIGNED (2026-07-22). TS now matches Go's fail-closed policy.

Go rejects condition trees whose `type` is not `simple`, `and`, or `or` with
`*ivm.DataError`. See `sqlite/query_builder.go:241-275`.

TS zqlite `filtersToSQL` now has a `default` branch that throws on an unknown
condition type instead of falling through / widening to nothing
(`mono/packages/zqlite/src/query-builder.ts`, parity test in
`query-builder.test.ts`). Both engines classify a malformed condition type
identically.

Reason (retained for history): Go receives decoded client AST data at runtime;
treating an unknown type as `TRUE` widened malformed queries, so rejecting it
preserves fail-closed behavior without letting invalid input reach SQL
generation.

## M1: Boolean Coercion from String Values

Status: Intentional Go fix, not yet ported back to TS.

Go coerces string values to boolean using JS-identical truthiness: empty
string → false, any non-empty string → true (including `"0"`, `"0.0"`,
`"false"`). See `sqlite/query_builder.go:647-655`.

Current TS coerces booleans with `!!v` (`table-source.ts:618`) — pure JS
truthiness of the raw value. The Go port matches this exactly, but the old Go
implementation used a literal-list + `ParseFloat` check that gave the opposite
answer for `"0"` (→false), a silent TS/Go divergence.

Reason: Boolean columns are stored as 0/1 INTEGER in the replica, so the
string branch is defensive. However, both implementations must agree to keep
init-vs-advance shape parity (CRIT-6).

Required follow-up: ensure TS's `!!v` coercion is documented as the canonical
behavior and add a cross-implementation test for string-to-boolean edge cases.

## M2: Engine Teardown — Awaited Destroy RPC

Status: Intentional Go hardening, not yet ported back to TS.

Go's `shutdownGroup` (called from the destroy RPC and the reaper) performs a
synchronous teardown: closes the engine, destroys the snapshotter, and waits
for the worker goroutine to exit. The TS `PipelineDriver` awaits the Go engine
teardown via `this.#goBackend?.destroy()` rather than fire-and-forget. See
`cmd/sidecar/main.go:1770-1795` and
`mono/.../pipeline-driver.ts:1198-1205`.

On a shared sidecar a rapid recycle — a new ViewSyncer for the SAME client
group starting before this one's teardown lands — could otherwise race this
group's destroy RPC against the new engine's init, tearing down freshly-
initialised state. Awaiting serialises destroy before any recreate.

Reason: TS-native fire-and-forgets the destroy because its engine is in-process
and single-threaded. The Go sidecar is multi-CG with per-CG workers, so a
destroy must complete before the same cgID's re-init to prevent the worker
from processing stale requests.

Required follow-up: none. The await is the correct behavior for the shared-
sidecar model.

## M3: Dispatch Invariant — Go Re-init Callback Runs Outside ViewSyncer Lock

Status: Intentional Go divergence, documented as safe.

The Go backend's (re-)init callback can run from the restart handler OUTSIDE
the ViewSyncer lock. This is safe ONLY because the method is fully synchronous
— the snapshot reference captured is read consistently through to the end of
the call. See `mono/.../pipeline-driver.ts:827-830`.

TS-native never calls re-init outside the lock because its engine is single-
threaded. The Go backend's restart handler is async and may fire between
ViewSyncer lock acquisitions.

Reason: The Go sidecar's destroy→re-init cycle is async (destroy RPC → await →
re-init RPC). The ViewSyncer cannot hold its lock across this cycle without
deadlocking. The synchronous snapshot read inside the callback is the
invariant that makes this safe.

Required follow-up: document this invariant in the ViewSyncer's restart
handler so future refactors don't break the synchronicity assumption.

## M4: Cost-Model Planning Is Optimisation, Not Correctness

Status: Intentional Go divergence in error handling.

Go's cost-model planner is treated as an optimisation layer: if the planner
throws on a skewed or edge-case schema, the planned AST (already correctness-
checked) is used directly. A planner fault does NOT kill the hydrate/advance.
See `mono/.../pipeline-driver.ts:1173-1176`.

TS-native's planner runs inline and a throw propagates to the caller,
aborting the operation. Go wraps the planner in a try-catch and falls back to
the pre-planned AST.

Reason: The Go planner is a port that may not cover every TS edge case. A
planner bug should not cause a CG teardown when the un-planned query is
correct. The ordering-completed AST already runs correctly on Go.

Required follow-up: align the planner's error handling between TS and Go so
both degrade gracefully, or move the planner to a shared pre-validation step.

## M5: PostgreSQL Type Mapping — `pgToZqlTypeMap` Alignment

Status: Intentional Go fix, not yet ported back to TS.

Go's `pgToZqlTypeMap` is a copy of TS's `formatTypeForLookup` in
`types/pg-data-type.ts`. The previous Go implementation used a hand-rolled
list that dropped TIME/TIMETZ, bare INT, the SERIAL family, bare FLOAT, and
never stripped `(N)` (so `varchar(255)` fell through to the unknown→string
warn path). See `mono/.../pipeline-driver.ts:3186-3189`.

The Go mapping is now byte-for-byte aligned with the canonical TS list. Both
strip array delimiters (`[]` suffix), strip any `(N)` args, and lowercase.

Reason: A divergent type map caused Go to emit `"string"` for columns TS
classified as `"varchar"`, producing false schema-mismatch warnings and
occasionally wrong coercion behavior.

Required follow-up: extract the type map into a shared protocol-level constant
so both implementations reference one source of truth.

## M6: Partition-Key Column Deduplication (same as D1)

Status: Intentional Go fix, not yet ported back to TS.

Go deduplicates partition-key column names before comparing the constraint map
length in `ConstraintMatchesPartitionKey`, ensuring duplicate column entries in
the partition key don't cause false constraint mismatches. See
`ivm/take.go:717-736`.

Current TS uses the raw `partitionKey.length` without deduplication, so a
partition key with repeated column names can fail the length check even when the
constraint map contains the correct logical key set. See
`mono/packages/zql/src/ivm/take.ts:727-742`.

Reason: Go's join constraint is a `map[string]bool`, so duplicate key names
naturally collapse. The deduplication step preserves the intended relationship
match semantics rather than dropping valid rows from the Take state path.

Required follow-up: port the distinct-key comparison to TS, or add a spec note
declaring duplicate partition-key columns unsupported before they reach Take.

## M7: Snapshotter Atomicity — Diff Before Swap

Status: Intentional Go fix, not yet ported back to TS.

Go builds the diff BEFORE swapping prev/curr snapshots in `Snapshotter.Advance`,
ensuring the diff's `NumChangesSince` read succeeds before committing the
snapshot rotation. On failure, `s.curr` (the snapshot the engine's sources are
bound to) remains untouched, making the advance retryable in place. See
`internal/snapshotter/snapshotter.go:147-192`.

Current TS swaps prev/curr first, then constructs the diff. A diff construction
failure strands the swap: the next advance would diff `(old-curr → head]` while
the engine still holds `old-prev` content, silently skipping the
`(old-prev → old-curr]` window. See
`mono/packages/zero-cache/src/services/view-syncer/snapshotter.ts:175-196`.

Reason: TS's `snapshotter.advance` is infallible in practice and a failed advance
resets the entire view-syncer. Go surfaces a retryable error
(`rpcCodeAdvanceCleanRetryable`) instead of resetting, which requires the
failure-atomic ordering to keep the engine consistent across retries.

Required follow-up: align TS with the same failure-atomic ordering, or document
that TS's reset-on-failure model makes the ordering difference unobservable.

## D4: Exclusive Partial-Cursor Boundary Tie

Status: DIVERGENT — Go drops, TS forwards. Oracle-gated alignment required.

When a pushed row's sort key ties the cursor prefix boundary in an exclusive
(`<` / `>`) direction, Go's `skip` operator drops the row while TS's `skip.ts`
forwards it. The difference is in how each engine interprets the exclusive
boundary against the partial cursor's retained prefix: Go treats the tie as
outside the range; TS treats it as inside.

See Go `ivm/skip.go` + `skip_partial_bound_parity_test.go:116-145` vs TS
`skip.ts:84-86`.

Impact: a row present on a TS-served client may be absent on a Go-served
client for the same query at the same version. This is a content-level
divergence, invisible to the row-set signature detector (which is
identity-only, not content-inclusive).

Reason: The partial-cursor boundary semantics were never formally specified;
each implementation made an independent choice. This divergence was
discovered in a test comment but absent from this ledger.

Required follow-up: align one side to the other under the differential oracle.
The TS behavior (forward the tie) is likely the intended semantics (an
exclusive boundary on the cursor prefix should not exclude rows that match
the prefix itself), but this must be oracle-gated before changing either side.
