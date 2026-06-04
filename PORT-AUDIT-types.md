# Type Coercion Parity Audit — TS ↔ Go IVM Sidecar

Audit scope: every PG type's coercion path (lite/SQLite ⇄ JS/Go IVM value),
NULL handling, msgpack wire encoding, and init↔advance parity. TS reference
is canonical.

## Baseline

Prior fixes I've assumed in place (per `REVIEW-porting.md` /
`REVIEW-ts-integration.md` / `REVIEW-final.md`):

- CRITICAL-3 ValueConverter wired into `MemorySource` via
  `NewMemorySourceWithConverter(sqlite.FromSQLiteType)` in
  `cmd/sidecar/main.go:1054-1059`. Verified still present — not regressed.
- MED-TS-2 `pgTypeToGoType` bytea / array explicit handling at
  `pipeline-driver.ts:3349-3373` with one-time warning. Verified present.
- T1-1 `Row.DecodeMsgpack` custom decoder at `ivm/data.go:35-58` for in-line
  int→float64 coercion. Verified present; `walkForNumericNormalize` still
  applied to non-Row payloads (AST literals, RPC envelopes) at
  `cmd/sidecar/main.go:78-164`. Not regressed.
- CRITICAL-1 `ValuesEqual(nil, nil) == false` at `ivm/data.go:206-208`. Present.

This audit looks for NEW divergences in the type-coercion surface, NOT
re-listing FIXED items.

---

## Findings

### CRITICAL-1: Snapshot init payload sends raw SQLite values; advance sends `fromSQLiteTypes`-processed values — asymmetric per-row JS shape across init vs advance

- **TS init**: `pipeline-driver.ts:602` reads `db.all('SELECT * FROM "${name}"')`
  with **no** `fromSQLiteTypes` and **no** `safeIntegers(true)`. So:
  - `boolean` columns arrive in Go's `init`/`loadRows` as JS Number `0`/`1`
    (msgpack int64).
  - `json`/`jsonb` columns arrive as the raw stored TEXT string
    (e.g. `'{"a":1}'`) — NOT parsed.
  - `bytea` columns arrive as Buffer/Uint8Array.
  - Numbers arrive as JS Number; integers > `MAX_SAFE_INTEGER` are silently
    truncated by better-sqlite3 (no safeIntegers).
- **TS advance**: `snapshotter.ts:532-537` calls `fromSQLiteTypes` on
  `prevValues` and `nextValue` BEFORE building the SnapshotChange that goes
  to Go. So:
  - `boolean` columns arrive in Go's `advance` as JS Boolean (`!!v` at
    `zqlite/table-source.ts:618`).
  - `json` columns arrive as parsed JS object/array (msgpack map/array).
  - `bytea` columns arrive as Buffer (no special TS handling for `bytea`
    inside `fromSQLiteType` — falls into `case 'string': return v`).
  - Numbers arrive as JS Number (with `safeIntegers(true)` upstream, so
    BigInts get bounds-checked and coerced).
- **Go side compensates**: `cmd/sidecar/main.go:1054-1067` (init) and
  `1170-1184` (loadRows) and `engine.snapshotToSourceChanges:960-963`
  (advance via `source.NormalizeRow`) ALL run every column through
  `sqlite.FromSQLiteType`. So for each colType, the convergence is:
  - `boolean`: init payload int64(0/1) → `val != 0` → bool. Advance: bool →
    falls through to `case bool: return val` → bool. ✓
  - `number`: init int64 → float64. Advance: float64 → returns val. ✓
  - `json`: init string → parse → object. Advance: object → falls to
    `default: return v` → object unchanged. ✓
  - `string`: init bytes/string → string. Advance: Buffer → bytes → string.
    Mostly ✓ (see HIGH-1 for UTF-8 hazard).
- **Divergence**: NONE for the normalize end-state — the Go-side per-column
  `FromSQLiteType` converges both shapes. **BUT** the convergence relies on
  Go-side `FromSQLiteType` handling each input shape. Several PG types
  (notably `'null'`, see CHECKED-OK note; and PG types unrecognized by
  `pgTypeToGoType`) silently bypass.
- **Impact**: TS init for a `bytea` column produces a Buffer that gets
  msgpack-encoded as `bin` (Go `[]byte`). Go's `FromSQLiteType("string",
  []byte)` does `string(val)`, which silently reinterprets binary as UTF-8.
  Embedded NULs / high bytes survive bytewise, but JS-side equality on a
  `bytea` Buffer vs Go-side string would not match if either is ever
  compared by serialization. Currently NOT an issue because TS never sends
  `bytea` through this path with a comparator on the Go side — but downstream
  any filter on a bytea column would silently mismatch.
- **Severity**: CRITICAL **not** because either side is wrong, but because
  the parity DEPENDS on the Go-side single-source-of-coercion (`FromSQLiteType`)
  staying complete. Any future PG type added to `pgTypeToGoType` without
  a corresponding `FromSQLiteType` case (e.g., a new `'date'` bucket) would
  silently leave the init/advance shapes inconsistent. Add a guard:
  `FromSQLiteType` should panic on unknown `colType` in a `case "null"` and
  `default` arm during the sidecar's startup self-check, OR the table column
  type whitelist should be mirrored on both sides.
- **Suggested fix**: `sqlite/query_builder.go:319-410` — add `case "null":`
  that mirrors `case "number":` (per TS `fromSQLiteType` which folds
  number/string/null together). Add a sidecar startup assertion that every
  colType in the registered MemorySource columns appears as a recognized
  case in `FromSQLiteType`.

### CRITICAL-2: `ColumnSchema.Optional` is never populated over the wire — Go-side cursor pagination on NULLable order columns silently diverges from TS

- **TS**: `pipeline-driver.ts:596-599` builds `columns[col] = {type: pgTypeToGoType(...)}`
  — ONLY `type`. `optional` is never set.
- **TS reference for cursor**: `zqlite/query-builder.ts:196-228` —
  `nullableAwareEquality` and `nullableAwareRangeComparison` use
  `columnType.optional === true` to decide between `IS ?` vs `= ?`
  and between `(field > ?)` vs `(? IS NULL OR field > ?)` /
  `(field IS NULL OR field > ?)`.
- **Go**: `sqlite/query_builder.go:36-39` defines `Optional bool` with
  msgpack tag `optional`. Since TS never sends it, Go always sees
  `Optional == false` (Go zero value). So `nullableAwareEquality:270-275`
  always emits `= ?`, and `nullableAwareRangeComparison:277-286` always
  emits `field > ?` without the `IS NULL` short-circuit.
- **Divergence**: For a cursor with `start.Row[col] == nil` on a nullable
  order column, TS emits `(? IS NULL OR field > ?)` — `? IS NULL` is true,
  so the whole branch is true and rows match unconditionally. Go emits
  `field > ?` — `field > NULL` evaluates to NULL (false in SQL), so
  ZERO rows match. Net: TS returns N rows, Go returns 0.
- **Impact**: Queries with `orderBy: [{col: nullable_col}]` paginated past
  a NULL boundary will lose data on Go-primary. Has the same shape as the
  HIGH-1 cursor-normalization bug from REVIEW-porting (which was fixed by
  normalizing the row VALUES — but doesn't fix the missing `optional` flag).
- **Suggested fix**:
  - TS: `pipeline-driver.ts:598` change to
    `columns[col] = {type: pgTypeToGoType(colSpec.dataType, warn), optional: colSpec.notNull !== true}`
  - Verify: Go-side `ColumnSchema` msgpack tag is `optional`, matching.
  - Inline test: cursor on a `varchar|TEXT_ENUM` column where the cursor
    value is `null` — TS returns all rows, Go currently returns 0.

### HIGH-1: TS `LIKE` uses regex (Unicode-aware, escape-aware, multiline-anchored); Go `LIKE` matches bytes (UTF-8-broken, no escape, line-blind)

- **TS**: `mono/packages/zql/src/builder/like.ts:37-73` builds a
  `new RegExp(pattern + '$', flags + 'm')`. `_` → `.` (matches one
  Unicode code point); `%` → `.*`; `\\x` for any x → literal x; regex
  special chars escaped. The `'m'` flag means `^`/`$` anchor per line,
  and `.` does NOT match `\n`.
- **Go**: `builder/filter.go:240-267` (`likeMatch` + `likeMatch`)
  iterates byte-by-byte. `s[si]` is a byte. `_` matches one byte. `%` matches
  any sequence. `\\` is treated as a literal char (no escape handling).
  No line-end semantics — `.`-equivalent matches `\n`.
- **Divergence — three observable cases**:
  1. Multi-byte UTF-8: pattern `_` on input `'café'` (where `é` is 2 bytes).
     TS: `_` matches `é` as one code point → returns true for `_a_é`.
     Go: `_` matches ONE byte (0xC3), leaving 0xA9 to match next char → false.
  2. Escape: pattern `'10\\%'` on input `'10%'`. TS: `\\%` → literal `%`
     in regex → matches. Go: `\\` is treated as `\` (literal backslash), so
     pattern becomes `10\%`, where `%` is the wildcard → matches anything
     starting with `10\` (which the input doesn't).
  3. Newline in field: pattern `'%foo%'` on input `"bar\nfoo"`.
     TS: `m` flag + `.` doesn't match `\n` → must be `foo` on its own line.
     Result depends on regex newline semantics. Go: `%` matches `\n` freely
     → matches.
- **Impact**: Any LIKE/ILIKE query against non-ASCII columns (user names,
  international content), or one with escaped wildcards, silently produces
  different result sets between TS and Go-primary.
- **Severity**: HIGH — silently wrong row sets, observable to clients.
- **Suggested fix**: `builder/filter.go:232-267` — port TS's regex
  translation: convert the LIKE pattern to a `regexp.Regexp` (Go's regexp
  is Unicode-aware). Use `regexp.MustCompile("^" + translated + "$")` with
  multiline flag `(?m)`. Handle `\\x` escape. Mirror `like.ts` line by line.

### HIGH-2: TS `IN`/`NOT IN` uses JS `Set.has` (reference equality for objects, value equality for primitives); Go `valueIn` uses cross-type numeric coercion + `ValuesEqual`

- **TS**: `mono/packages/zql/src/builder/filter.ts:133-145` builds
  `new Set(rhs)` then `set.has(lhs)`. JS `Set` uses SameValueZero
  comparison: `0 === -0`, `NaN === NaN`, `bigint vs number` is NOT equal
  (`Set([1]).has(1n) === false`).
- **Go**: `builder/filter.go:270-293` — for `[]interface{}`, iterates and
  calls `ivm.ValuesEqual(left, v)`. `ValuesEqual` cross-type-coerces
  numerics (int64/uint64/float64 → float64). So Go would accept
  `int64(1)` matches `float64(1)`; TS would not (would only accept
  Number `1` matching Number `1` via the post-decode coercion → both float64).
- **In practice**: Once `walkForNumericNormalize` + `Row.DecodeMsgpack` run,
  all numeric values are float64 on Go side, so this is moot. **BUT**
  `valueIn` also doesn't handle `[]float64` for case where the AST IN
  literal arrives as a typed slice (the post-walk handles `[]interface{}`
  recursively). If msgpack ever decodes IN literals into a typed
  `[]float64` (currently doesn't, AST uses `[]interface{}` from
  `DecodeInterface`), Go would still match because the explicit
  `case []float64` branch exists. **Currently CONSISTENT**, but the
  asymmetric branches are fragile.
- **Severity**: LOW (currently OK due to msgpack interface decoding) but
  flag as latent risk.
- **Suggested fix**: `builder/filter.go:270-293` — consolidate to a single
  `[]interface{}` branch using only `ValuesEqual`. Remove `case []string`
  (which uses `fmt.Sprintf("%v", left) == v` — silently differs from
  ValuesEqual for non-string `left`).

### HIGH-3: Go `valueIn` for `[]string` uses `fmt.Sprintf("%v", left)` — silently coerces non-string left to fmt-formatted string

- **File**: `builder/filter.go:278-283`
- **Code**: `case []string: ls := fmt.Sprintf("%v", left); ... if ls == v`
- **Divergence**: If `left` is `float64(5)` and the right slice is
  `[]string{"5"}`, Go: `fmt.Sprintf("%v", 5.0)` → `"5"` → matches. TS:
  JS `Set(["5"]).has(5)` → false (Number vs String). **Go matches when
  TS does not.**
- **Practical exposure**: AST IN literals arrive as `[]interface{}` not
  `[]string`, so this branch is unreachable through normal msgpack decode.
  But it's a footgun for anyone constructing `Condition` in Go code.
- **Severity**: LOW (unreachable through msgpack), MEDIUM if any callsite
  builds the Condition in pure Go.
- **Suggested fix**: Drop the `[]string` branch entirely. The
  `[]interface{}` branch with `ValuesEqual` handles all cases the wire
  protocol can produce.

### MEDIUM-1: `fromSQLiteType('boolean', v)` uses JS `!!v` while Go uses `val != 0` — string values diverge

- **TS**: `zqlite/table-source.ts:617-618` returns `!!v`.
- **Go**: `sqlite/query_builder.go:324-346` handles int64/uint64/float64 as
  `val != 0`, then string as `val == "1" || val == "1.0" || val == "true"`
  fallback to `ParseFloat` returning `f != 0`, fallback to `false`.
- **Divergence**: For a boolean column where the underlying SQLite cell
  somehow holds a string value (shouldn't happen via normal pipeline but
  can happen via direct SQL, test fixtures, or schema migration):
  - `!!"0"` is `true` (truthy non-empty string).
  - Go: `"0"` is not "1"/"1.0"/"true", `ParseFloat("0") = 0`, `0 != 0` is
    false → returns false. **TS=true, Go=false** for string `"0"`.
  - `!!"false"` is `true` (truthy non-empty string).
  - Go: not in literal list, `ParseFloat("false")` errors → falls to
    final `return false`. **TS=true, Go=false** for string `"false"`.
  - `!!""` is `false` (empty string is falsy).
  - Go: empty string is not "1"/"1.0"/"true", `ParseFloat("")` errors →
    `return false`. **MATCH**.
- **Impact**: Edge case only — boolean values from SQLite are normally
  stored as 0/1 integers (TS `liteValue` boolean → 1/0). Direct text inputs
  via fixtures or migrations would silently diverge.
- **Severity**: MEDIUM (depends on whether any code path actually loads
  string-typed bools).
- **Suggested fix**: Either mirror JS truthy-coercion (any non-empty string
  → true), or have TS reject string-typed booleans at conversion time.

### MEDIUM-2: TS throws on BigInt > `MAX_SAFE_INTEGER` for `number` columns; Go silently truncates precision

- **TS**: `zqlite/table-source.ts:622-628` throws `UnsupportedValueError`
  if the bigint is outside `±MAX_SAFE_INTEGER` (`2^53 - 1`).
- **Go**: `sqlite/query_builder.go:347-374` for "number" colType does
  `float64(int64)` or `float64(uint64)` unconditionally. For an int64 of
  `9_007_199_254_740_993` (= `2^53 + 1`), `float64(...)` returns
  `9_007_199_254_740_992` (silent precision loss; one is unrepresentable in
  float64).
- **Divergence**: A 64-bit integer column with values > 2^53 hits the
  pipeline:
  - TS init/snapshot: better-sqlite3 with `safeIntegers(true)` returns
    BigInt → `fromSQLiteType` throws → pipeline aborts.
  - Go init/loadRows: int64 → float64 with silent precision loss; downstream
    comparison may match different rows than the database actually contains.
- **Impact**: For databases with bigserial / int8 columns reaching > 2^53,
  Go-primary mode silently aliases adjacent integer keys; joins on those
  keys can match wrong rows. TS, by contrast, surfaces the issue as an
  error rather than wrong results.
- **Severity**: MEDIUM — observable but only for tables with PK / FK in
  the 9-quadrillion range, which is uncommon but exists.
- **Suggested fix**: `sqlite/query_builder.go:347-360` for `case int64` and
  `case uint64`, add bounds check matching TS: if `val > MAX_SAFE_INT_F64`
  or `val < MIN_SAFE_INT_F64`, panic with a clear error rather than
  silently corrupt. Sidecar's drift-recovery loop catches the panic.

### MEDIUM-3: TS sends ONLY `{type: ...}` per column; Go-side `ColumnSchema` has extra fields (`Optional`) and an open struct, so future schema changes silently no-op

- **TS**: `pipeline-driver.ts:596-598` — only `type` populated.
- **Go**: `sqlite/query_builder.go:36-39` — `Optional bool` exists but is
  never set over the wire. Future fields added on either side without
  coordination will be silently zero-valued.
- **Divergence**: Compounds CRITICAL-2 (Optional always false). No other
  hidden fields today, but the asymmetry is real.
- **Severity**: MEDIUM — currently manifests as CRITICAL-2; structural
  prevention.
- **Suggested fix**: Define a shared schema (or msgpack-tagged struct
  validator) such that adding a field on one side without the other is a
  compile-time error. At minimum: comment in `ColumnSchema` flagging which
  fields are wire-populated.

### MEDIUM-4: `FromSQLiteType` has no `case "null"`; falls to `default: return v` — bypasses int→float64 coercion for `'null'`-typed columns

- **File**: `sqlite/query_builder.go:319-410`
- **TS reference**: `zqlite/table-source.ts:619-621` folds `'number' |
  'string' | 'null'` into one branch with shared bigint-to-Number coercion.
- **Divergence**: If a column is typed `'null'` (TS treats this as a valid
  `ValueType`), TS coerces BigInt to Number; Go returns the value
  unchanged. For values arriving from msgpack as int64 (e.g., AST literal
  paths), they'd stay int64 in the Row and cause cross-type comparison
  issues downstream.
- **Reachability**: `pgTypeToGoType` (`pipeline-driver.ts:3323-3374`) never
  returns `'null'`. So this branch is dead via the wire. But the same
  function is used by `NormalizeRow` on `MemorySource` for tests, where
  the colType map is constructed directly in Go and could include
  `'null'`. Test paths only — not a production gap.
- **Severity**: LOW (test paths only; no production reachability).
- **Suggested fix**: `sqlite/query_builder.go:319` — add `case "null":` that
  mirrors the int→float64 coercion of `case "number":`. Defensive.

### MEDIUM-5: TS `pgTypeToGoType` does not recognize `TIME`/`TIMETZ`/`TIME WITHOUT TIME ZONE`/`TIME WITH TIME ZONE` — falls through to `'string'` with warning

- **TS reference**: `pipeline-driver.ts:3331-3373` — explicit `'number'`
  list includes `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, but NOT `TIME`, `TIMETZ`,
  `TIME WITHOUT TIME ZONE`, `TIME WITH TIME ZONE`.
- **TS canonical mapping** (`pg-data-type.ts:43-70`): `time`, `timetz`,
  `time with time zone`, `time without time zone` all map to ZQL `'number'`.
- **Divergence**: Source-of-truth `pg-data-type.ts` says TIME is a number;
  Go-bound `pgTypeToGoType` treats it as a string and logs a warning. So
  TS-side fromSQLiteType (using `liteTypeToZqlValueType` → `'number'`)
  parses values as Number, but Go-side normalizes as string. Filter
  comparisons on TIME columns will fail (number vs string in Go).
- **Impact**: Any production query filtering on a `time` column would
  silently return wrong row counts on Go-primary.
- **Severity**: MEDIUM — affects time-typed columns only. The TS path also
  has issues serializing TIME (see `types/pg.ts:103-110`), so production
  use of pure TIME columns may already be rare.
- **Suggested fix**: `pipeline-driver.ts:3332-3341` — add `TIME`, `TIMETZ`,
  `TIME WITHOUT TIME ZONE`, `TIME WITH TIME ZONE` to the `'number'` branch.

### MEDIUM-6: `pgTypeToGoType` doesn't recognize `INT` (only `INT2`/`INT4`/`INT8`/`SMALLINT`/`INTEGER`/`BIGINT`)

- **TS**: `pipeline-driver.ts:3333-3334` lists `INT2 | INT4 | INT8 | SMALLINT
  | INTEGER | BIGINT`.
- **TS canonical** (`pg-data-type.ts:6`): also includes `'int'`.
- **Divergence**: A PG column declared as `INT` (PostgreSQL alias for
  INTEGER) would fall through `pgTypeToGoType` to `'string'` with warning,
  while `fromSQLiteType` via `liteTypeToZqlValueType` produces `'number'`.
  Numeric filtering on Go-primary would compare string vs float64 — no
  match.
- **Note**: PostgreSQL's pg_catalog typically reports the canonical name
  (`integer`, not `int`), so this may not trigger in practice. But user
  schemas declared as `dataType: 'int'` would hit it.
- **Severity**: LOW (canonical PG name is `integer`, not `int`).
- **Suggested fix**: `pipeline-driver.ts:3333` — add `t === 'INT'`.

### MEDIUM-7: `pgTypeToGoType` doesn't recognize `SMALLSERIAL`/`SERIAL`/`SERIAL2`/`SERIAL4`/`SERIAL8`/`BIGSERIAL` or `DECIMAL`/`FLOAT`

- **TS**: `pipeline-driver.ts:3333-3338` does cover `NUMERIC | DECIMAL |
  REAL | DOUBLE PRECISION | FLOAT4 | FLOAT8`. **Missing: SERIAL family,
  bare FLOAT.**
- **TS canonical** (`pg-data-type.ts:13-23`): includes `smallserial`,
  `serial`, `serial2`, `serial4`, `serial8`, `bigserial`, `float`.
- **Divergence**: Same shape as MEDIUM-6 — silent string-tagging on
  Go-side, numeric on TS-side.
- **Note**: PostgreSQL transparently rewrites SERIAL to INTEGER + sequence
  at the catalog level, so `pg_attribute.atttypid` typically reports
  `int4`/`int8` for SERIAL columns. Probably no production exposure.
- **Severity**: LOW (PG catalog rewrites SERIAL to INTEGER).
- **Suggested fix**: For symmetry, add SERIAL family + bare FLOAT to
  `pipeline-driver.ts:3333-3341`.

### MEDIUM-8: Filter literal `Condition.Right.Value` for IN/NOT IN: TS uses canonical Set, Go does linear scan via per-element ValuesEqual

- **File**: `sqlite/query_builder.go:154-163` (SQL) vs
  `builder/filter.go:270-293` (predicate)
- The SQL builder for IN uses `json_each(?)` with a JSON-marshalled array.
  The predicate-builder for IN uses linear scan with ValuesEqual.
- **Difference**: The SQL path serializes to JSON, which converts JS
  numbers/strings/bools by JSON semantics. The predicate path operates on
  the decoded Go values directly. Inconsistency between the two paths
  within Go itself: if an AST literal is a `time.Time` or other unusual
  type (not msgpack-default), the SQL path JSON-marshals to a string, the
  predicate path falls through to `case []interface{}` and uses
  `ValuesEqual` which may not match.
- **Severity**: LOW — AST literals from TS are always scalars (string,
  number, bool, null), so this is theoretical.

### MEDIUM-9: TS `liteTypeToZqlValueType` strips `(N)` from `varchar(N)` / `char(N)` via `formatTypeForLookup`; Go `pgTypeToGoType` does NOT strip args

- **TS**: `pg-data-type.ts:92-98` strips `(...)` before lookup.
- **Go-bound**: `pipeline-driver.ts:3329-3330` extracts the part before
  `|`, then uppercases. Does NOT strip `(...)`.
- **Divergence**: A column declared as `VARCHAR(255)` would arrive at
  `pgTypeToGoType` with `t === 'VARCHAR(255)'`, which doesn't match
  `'VARCHAR'`. Falls through to `'string'` with warning. **End result is
  still `'string'`** (the `default fall through` arm). So this is
  cosmetic in terms of correctness but produces noisy warnings.
- **Severity**: LOW (correct outcome, noisy warnings).
- **Suggested fix**: `pipeline-driver.ts:3329-3330` — strip the leading
  `(...)` like `pg-data-type.ts:formatTypeForLookup`.

### LOW-1: TS `fromSQLiteType` for `'json'` throws `UnsupportedValueError` on parse failure; Go returns the raw string

- **TS**: `zqlite/table-source.ts:631-641` — throws on `JSON.parse` failure.
- **Go**: `sqlite/query_builder.go:381-384` — returns the raw string.
- **Divergence**: For corrupted JSON in a `json` column, TS bubbles an
  error up; Go silently passes the string through. Downstream operators
  comparing JSON-as-objects would diverge for the corrupt row.
- **Impact**: Only on actually-corrupted data — schema migration mistakes,
  manual edits. In normal operation both should agree.
- **Suggested fix**: `sqlite/query_builder.go:380-383` — change `return val`
  to `panic` matching TS, with a wrapped error type the drift-recovery
  loop can detect.

### LOW-2: TS `fromSQLiteType` for `'null'` colType (folded into number/string/null arm) coerces BigInt → Number; Go has no `'null'` case

- Covered by MEDIUM-4 above. Listed separately at LOW because the
  reachability is test-only.

### LOW-3: `valueIn` for `[]float64` exists but is dead code

- **File**: `builder/filter.go:285-290`
- **Code**: `case []float64: ... ValuesEqual(left, v)`
- AST literals never arrive as typed `[]float64` from msgpack — they're
  `[]interface{}` with elements walked through `walkForNumericNormalize`.
- **Severity**: cosmetic, dead branch.
- **Suggested fix**: drop the branch.

### LOW-4: `pgTypeToGoType` doesn't lowercase before comparing; relies on uppercase normalize. TS canonical lowercase. Edge cases: schema-named types

- **File**: `pipeline-driver.ts:3329-3330` does `.toUpperCase()`.
- **TS canonical** uses `.toLocaleLowerCase()`.
- For pure ASCII enum types this is fine. For schema-qualified type names
  containing dots or odd chars, uppercase-vs-lowercase normalization can
  differ for non-ASCII alphabets (Turkish I etc.).
- **Severity**: cosmetic; PG type names are ASCII in practice.

---

## CHECKED-OK (verified equivalent)

These are type pairs I verified converge correctly between TS and Go:

- **smallint / integer / bigint / int2 / int4 / int8**: TS `'number'` →
  fromSQLiteType BigInt→Number; Go `'number'` → FromSQLiteType
  int64→float64 (with MEDIUM-2 precision caveat). ✓
- **real / double precision / float4 / float8**: TS `'number'`; Go
  `'number'`. ✓
- **numeric / decimal**: TS `'number'`; Go `'number'`. (PG NUMERIC actually
  has arbitrary precision; both sides force to float64. Precision loss
  on either side is identical — both lose at 2^53. ✓ for parity.)
- **boolean / bool**: stored as 0/1 int. TS fromSQLiteType `!!v` → bool.
  Go FromSQLiteType int64→`val != 0` → bool. Both produce native bool.
  ✓ (with MEDIUM-1 string-edge caveat).
- **text / varchar / char / bpchar / citext / name**: TS `'string'`; Go
  `'string'`. Stored as text in SQLite. Both pass through unchanged. ✓
- **uuid**: TS `'string'`; Go `'string'`. better-sqlite3 returns text,
  Go modernc/sqlite returns text/[]byte. FromSQLiteType("string", []byte)
  → string(val). Both produce canonical lowercase. ✓
- **json / jsonb**: TS `'json'`; Go `'json'`.
  - Init: TS sends raw text. Go parses via FromSQLiteType. ✓
  - Advance: TS sends parsed object. Go FromSQLiteType for non-string/byte
    types passes through unchanged. ✓
  - Nested map/array values recursed for int→float64 by
    `normalizeDecodedValue` (data.go:67-103). ✓
- **timestamp / timestamptz / timestamp with time zone / date**: TS
  `'number'` (microseconds from epoch as Number); Go `'number'`. Numeric
  comparison aligns. ✓
- **enums**: TS `pg-to-lite.ts:23-27` marks via `TEXT_ENUM` attribute.
  `liteTypeToZqlValueType` returns `'string'`. `pgTypeToGoType` doesn't
  see the attribute — but since the underlying type after stripping `|`
  is the enum name (a custom type), it falls through to `'string'` with
  warning. End result `'string'`. ✓ (noisy warning).
- **array types (text[], int4[], jsonb[], etc.)**: TS lite.ts stores as
  JSON-stringified text. `pgTypeToGoType` returns `'string'` for `[]`
  suffix. Both sides see the same string. ✓ (opaque — no filter
  introspection).
- **bytea**: Both sides see Buffer/[]byte at init, Buffer at advance, both
  reduced to Go string via `FromSQLiteType("string", []byte)`. Bytewise
  equivalent; UTF-8 interpretation hazard noted in CRITICAL-1.
- **NULL handling**:
  - TS `fromSQLiteType` line 613: `if (v === null) return null;` —
    null-in / null-out for all types.
  - Go `FromSQLiteType` line 320-322: `if v == nil return nil` — matches.
  - `ValuesEqual(nil, nil) == false` on both sides for join semantics
    (CRITICAL-1 fix). ✓
  - `CompareValues(nil, nil) == 0` on Go for sort semantics; TS uses JS
    `===` which has nil==nil true via dedicated path. ✓
- **ValueConverter wiring on advance**: `cmd/sidecar/main.go:1054-1059`
  injects `sqlite.FromSQLiteType` into MemorySource. Not regressed.
  `engine.snapshotToSourceChanges:960-963` calls
  `source.NormalizeRow(change.NextValue)` and for each `prevRow`. ✓
- **`Row.DecodeMsgpack` typed decoder**: `ivm/data.go:35-58`. ✓ Still
  present (per T1-1 perf fix). `walkForNumericNormalize` still applied to
  non-Row payloads (AST literals, RPC envelopes) via `mpUnmarshal`
  (`cmd/sidecar/main.go:78-87`). ✓

---

## Type coercion matrix

| PG type | TS ZQL ValueType | TS JS shape | Go shape | Wire encoding | Status |
|---|---|---|---|---|---|
| `bool` / `boolean` | `boolean` | bool | bool | msgpack bool | ✓ CHECKED-OK |
| `int2` / `smallint` | `number` | Number | float64 | msgpack int → float64 | ✓ CHECKED-OK |
| `int4` / `integer` | `number` | Number | float64 | msgpack int → float64 | ✓ CHECKED-OK |
| `int8` / `bigint` | `number` | Number (bounds-throw via TS) | float64 (silent precision-loss) | msgpack int → float64 | MEDIUM-2 |
| `int` | `number` (canonical) | Number | (untagged) string via `pgTypeToGoType` | string | MEDIUM-6 |
| `serial*` / `bigserial` | `number` (canonical) | Number | string (untagged) | string | MEDIUM-7 (PG rewrites internally) |
| `numeric` / `decimal` | `number` | Number | float64 | msgpack float | ✓ CHECKED-OK |
| `real` / `float4` | `number` | Number | float64 | msgpack float | ✓ CHECKED-OK |
| `double precision` / `float8` | `number` | Number | float64 | msgpack float | ✓ CHECKED-OK |
| `float` (bare) | `number` (canonical) | Number | string (untagged) | string | MEDIUM-7 |
| `text` | `string` | string | string | msgpack string | ✓ CHECKED-OK |
| `varchar` / `varchar(n)` | `string` | string | string | msgpack string | ✓ CHECKED-OK; LOW-4 noisy warning for varchar(n) |
| `char` / `bpchar` | `string` | string | string | msgpack string | ✓ CHECKED-OK |
| `citext` / `name` | `string` | string | string | msgpack string | ✓ CHECKED-OK |
| `uuid` | `string` | string | string | msgpack string | ✓ CHECKED-OK |
| `json` / `jsonb` | `json` | object / array | map / []interface{} (recursed) | msgpack map/array | ✓ CHECKED-OK |
| `date` | `number` | Number | float64 | msgpack float | ✓ CHECKED-OK |
| `timestamp` / `timestamptz` | `number` (epoch ms with fp ns) | Number | float64 | msgpack float | ✓ CHECKED-OK |
| `time` / `timetz` | `number` (canonical) | Number | string (untagged via pgTypeToGoType) | string | MEDIUM-5 |
| `bytea` | (TS: unmapped — passes Buffer through) | Buffer/Uint8Array (init), Buffer (advance) | string (UTF-8-reinterpreted bytes) | msgpack bin → []byte → string(...) | CRITICAL-1 hazard |
| `enum` (TEXT_ENUM) | `string` | string | string | msgpack string | ✓ CHECKED-OK (warning) |
| `<type>[]` (any) | `json` (per `dataTypeToZqlValueType`) | parsed JSON array | `[]interface{}` post-walk | msgpack array | ✓ CHECKED-OK on equality; opaque to filters |
| `<type>[][]` (multi-d) | `json` | parsed nested | nested `[]interface{}` | msgpack nested | ✓ CHECKED-OK |
| `hstore` | (unsupported via `pgToZqlTypeMap`) | undefined | n/a | n/a | NOT CHECKED (would skip column per warnIfDataTypeSupported) |
| `interval` | (unsupported) | undefined | n/a | n/a | NOT CHECKED (skipped) |
| `null` (TS ValueType) | `null` (folds with number/string in coercion) | Number / string / null | unchanged (no `case "null"` in Go) | n/a | MEDIUM-4 (test only) |

Cursor/order-by/null branch behavior:

| Case | TS behavior | Go behavior | Status |
|---|---|---|---|
| Cursor on non-null col, basis=after | `field > ?` | `field > ?` | ✓ |
| Cursor on optional col, value=null | `(? IS NULL OR field > ?)` | `field > ?` (because Optional always false) | CRITICAL-2 |
| Cursor at basis | full row equality via `=`/`IS` | `=` only | minor (subset of CRITICAL-2) |

Predicate ops:

| Op | TS | Go | Status |
|---|---|---|---|
| `IS` | strict `===` (null-safe) | `valuesIdentical` (null=null=true) | ✓ |
| `IS NOT` | strict `!==` | `!valuesIdentical` | ✓ |
| `=` / `!=` | short-circuit null=false, then `===`/`!==` | short-circuit null=false, then `valuesIdentical` w/ numeric coercion | ✓ |
| `<` / `>` / `<=` / `>=` | short-circuit null=false, then JS `<`/`>` | short-circuit null=false, then `CompareValues` (numeric cross-coerce) | ✓ |
| `LIKE` / `ILIKE` | Unicode regex, escape-aware | byte-level, no escape, line-blind | HIGH-1 |
| `NOT LIKE` / `NOT ILIKE` | not(LIKE) | not(LIKE) | HIGH-1 (inherits) |
| `IN` / `NOT IN` | `Set.has(lhs)` (SameValueZero) | linear scan via `ValuesEqual` | LOW (currently equivalent post-walk); HIGH-3 for `[]string` branch dead code |

---

## Coverage notes

What I verified in source:
- TS `fromSQLiteType` and `fromSQLiteTypes` at `mono/packages/zqlite/src/table-source.ts:588-643`.
- TS `liteValue` and `liteTypeToZqlValueType` at `mono/packages/zero-cache/src/types/lite.ts:73-92, 198-212`.
- TS `dataTypeToZqlValueType` at `mono/packages/zero-cache/src/types/pg-data-type.ts:72-89`.
- TS `pgTypeToGoType` at `mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:3322-3374`.
- TS `nullableAwareEquality` / `nullableAwareRangeComparison` at
  `mono/packages/zqlite/src/query-builder.ts:196-228`.
- TS `getLikePredicate` / `patternToRegExp` at `mono/packages/zql/src/builder/like.ts:1-73`.
- TS `createPredicateImpl` at `mono/packages/zql/src/builder/filter.ts:108-150`.
- TS Snapshotter advance path at `mono/packages/zero-cache/src/services/view-syncer/snapshotter.ts:339-553`.
- TS init payload construction at
  `mono/packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:562-624`.
- TS msgpack codec config at
  `mono/packages/zero-cache/src/services/view-syncer/go-sidecar/go-ivm-client.ts:30-44`.

- Go `FromSQLiteType` / `ToSQLiteType` at `go-ivm/sqlite/query_builder.go:289-410`.
- Go `MemorySource.NormalizeRow` and `NewMemorySourceWithConverter` at
  `go-ivm/ivm/source.go:84-121, 367-430`.
- Go `Row.DecodeMsgpack` / `normalizeDecodedValue` at
  `go-ivm/ivm/data.go:35-103`.
- Go `walkForNumericNormalize` / `mpUnmarshal` at
  `go-ivm/cmd/sidecar/main.go:62-179`.
- Go sidecar init/loadRows column normalization at
  `go-ivm/cmd/sidecar/main.go:1054-1085, 1170-1184`.
- Go advance normalization via `engine.snapshotToSourceChanges` at
  `go-ivm/engine/engine.go:955-998`.
- Go `evalOp` for predicates at `go-ivm/sqlite/table_source.go:723-754`
  AND `go-ivm/builder/filter.go:106-208`.
- Go `nullableAwareEquality` / `nullableAwareRangeComparison` at
  `go-ivm/sqlite/query_builder.go:270-286`.
- Go LIKE matcher at `go-ivm/builder/filter.go:232-267`.
- Go `valueIn` at `go-ivm/builder/filter.go:270-293`.
- Go `tablesource.Source.NormalizeRow` at
  `go-ivm/internal/tablesource/source.go:238-253`.

Wire encoding verified:
- msgpack codec config matches on both sides (no records, no BigInt
  extension, maps as objects).
- `UseLooseInterfaceDecoding(true)` plus `walkForNumericNormalize` plus
  `Row.DecodeMsgpack` collectively guarantee numeric → float64 on Go.
- `mapsAsObjects: true` on TS side means JSON columns serialize as plain
  msgpack maps, decoded by Go as `map[string]interface{}`. NormalizeRow's
  recursion through `normalizeDecodedValue` handles nested ints.

Tests run / checked: NONE — this is a static audit. The shadow-mode soak
machinery would catch the active divergences here (specifically CRITICAL-2
on cursor pagination, HIGH-1 on non-ASCII LIKE, MEDIUM-5 on time columns)
if exercised; the absence of shadow-mode mismatches in the documented
45-min soak (REVIEW-final) suggests the production workload doesn't yet
hit these cases.

---

## Not checked

- **`hstore`**, **`interval`**, **PostGIS geometric types**, **network
  types** (`inet`, `cidr`, `macaddr`), **range types** (`int4range`,
  `tstzrange`, etc.): None of these are in `pgToZqlTypeMap`. TS's
  `liteTypeToZqlValueType` returns `undefined`, triggering
  `warnIfDataTypeSupported` to skip the column entirely. Go-side via
  `pgTypeToGoType` would emit a warning and fall through to `'string'` —
  but those columns won't be in the schema sent to Go because they're
  filtered out upstream. So this is "consistent skip" rather than active
  divergence.
- **`tsvector` / `tsquery`** (full-text): same as above.
- **`xml`**: not in canonical map.
- **`citext`**: returned as `'string'` by both sides; case-insensitive
  collation is a PG operator concern, not a coercion concern. Filters
  against citext columns would behave per pure-string comparison on both
  sides — TS via JS string compare, Go via byte compare. Whether this
  matches PG citext semantics (case-insensitive) is a separate (already-
  documented) issue, not a TS↔Go parity issue.
- **`money` / `pg_lsn`**: rare; not in canonical map.
- **PG composite types**: would arrive as text representation; treated as
  opaque string on both sides. Not verified end-to-end.
- **Custom DOMAIN types**: would inherit the underlying type's mapping in
  PG, which gets reported via `pg_attribute.atttypid`. Coverage depends on
  whether the underlying base type is in the recognized list.
- **Empty arrays vs NULL array**: `liteValue` for `null` returns `null`;
  for empty `[]` returns `'[]'` string. Both sides see one as `null` and
  the other as a string `"[]"`. Comparison would correctly differentiate.
  Not separately verified for filter equality semantics in Go.
- **NaN / Infinity in numeric columns**: NaN and ±Infinity from PG numeric
  / double precision serialize as `'NaN'`, `'Infinity'`, `'-Infinity'`
  string literals through some PG drivers; behavior across the boundary
  depends on which driver is active. NOT investigated.
- **Embedded NUL bytes in strings**: msgpack-safe but downstream SQL or
  view-syncer comparisons may not be. NOT investigated.
- **Aliases like `varchar` returned by PG as `character varying`**: PG's
  pg_catalog reports the canonical name. Coverage assumed via documented
  type-name list, not verified by integration test.

---

## Recommended priority order for fixes

1. **CRITICAL-2** — pass `optional` from TS to Go. Cheap (~3 LOC TS, ~0 LOC
   Go). Unblocks correct cursor pagination on nullable columns.
2. **HIGH-1** — port TS LIKE regex semantics to Go. Medium effort
   (~30 LOC); use Go's `regexp` package with the same translation rules.
3. **MEDIUM-5** — add TIME/TIMETZ to `pgTypeToGoType`'s number list.
   Trivial (~5 LOC).
4. **CRITICAL-1 hardening** — add startup assertion in sidecar that every
   schema colType is recognized by `FromSQLiteType`. Defensive; catches
   future type additions before they corrupt.
5. **MEDIUM-2** — bounds-check int64 / uint64 against MAX_SAFE_INT in
   `FromSQLiteType("number", ...)` to match TS throw behavior. Trivial.
6. Everything else as cleanup.
