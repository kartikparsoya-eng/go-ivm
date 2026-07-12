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

Status: Intentional Go hardening, not yet ported back to TS.

Go rejects operators outside its explicit SQL operator whitelist with
`*ivm.DataError` before SQL formatting. See `sqlite/query_builder.go:277-306`.

Current TS zqlite compiles `filter.op` with `sql.__dangerous__rawValue` on the
generic condition path, after special-casing `IN` and LIKE-family operators.
See `mono/packages/zqlite/src/query-builder.ts:194-223`. The protocol schema
defines the supported operator set at `mono/packages/zero-protocol/src/ast.ts:37-50`.

Reason: Go's sidecar accepts ASTs over the client boundary; whitelisting avoids
raw SQL operator interpolation from untrusted input. The error classification is
deliberate: an unsupported query shape is deterministic and should tear down the
client group rather than silently producing an empty result.

Required follow-up: align TS with the same whitelist/error policy, or move the
shared validation to AST ingress so both implementations reject the same op set
with the same typed error.

## D3: Malformed Condition-Type Rejection

Status: Intentional Go hardening, not yet ported back to TS.

Go rejects condition trees whose `type` is not `simple`, `and`, or `or` with
`*ivm.DataError`. See `sqlite/query_builder.go:241-275`.

Current TS zqlite relies on the typed AST union and has no runtime `default`
branch in `filtersToSQL`. See
`mono/packages/zqlite/src/query-builder.ts:169-191`.

Reason: Go receives decoded client AST data at runtime. Treating an unknown type
as `TRUE` widened malformed queries; rejecting it preserves fail-closed behavior
without letting invalid input reach SQL generation.

Required follow-up: align TS with an explicit runtime rejection or move the
shared validation to AST ingress so Go and TS classify malformed condition types
identically.

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
