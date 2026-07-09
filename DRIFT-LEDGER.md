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

Go rejects operators outside its explicit SQL operator whitelist and emits a
safe no-match predicate (`1=0`). See `sqlite/query_builder.go:291-306`.

Current TS zqlite compiles `filter.op` with `sql.__dangerous__rawValue` on the
generic condition path, after special-casing `IN` and LIKE-family operators.
See `mono/packages/zqlite/src/query-builder.ts:194-223`. The protocol schema
defines the supported operator set at `mono/packages/zero-protocol/src/ast.ts:37-50`.

Reason: Go's sidecar accepts ASTs over the client boundary; whitelisting avoids
raw SQL operator interpolation from untrusted input.

Required follow-up: align TS with the same whitelist/error policy, or change Go
from silent no-match to the agreed typed error once the TS-side classifier and
wire behavior are specified.
