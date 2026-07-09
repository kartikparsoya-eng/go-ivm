package sqlite

// Builds SELECT SQL queries with constraints, filters, ordering, and start cursors.

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// maxSafeInteger is JS Number.MAX_SAFE_INTEGER (2^53 - 1). int64/uint64 values
// beyond ±this cannot round-trip through float64 without precision loss, which
// is how TS's number model (all numbers are float64) bounds integers (HIGH-9).
const maxSafeInteger = int64(1)<<53 - 1

// boxFloat64 shares a single immutable box for common numeric column values to
// avoid a per-row heap allocation. See ivm.BoxFloat64 — the cache lives in the
// lower ivm package so the advance/wire path (normalizeDecodedValue) shares it.
func boxFloat64(f float64) ivm.Value { return ivm.BoxFloat64(f) }

// Condition represents a filter condition from the query AST.
type Condition struct {
	Type       string       // "simple", "and", "or"
	Op         string       // for simple: "=", "!=", ">", "<", ">=", "<=", "LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE", "IN", "NOT IN", "IS", "IS NOT"
	Left       ValuePos     // for simple
	Right      ValuePos     // for simple
	Conditions []*Condition // for and/or
}

// ValuePos represents a value position in a condition.
type ValuePos struct {
	Type  string    // "column", "literal", "static"
	Name  string    // for column
	Value ivm.Value // for literal
}

// ColumnSchema describes a column's type and nullability.
//
// Struct tags are required because msgpack decoding (unlike Go's encoding/json)
// is case-sensitive on field names — without the tag, TS's `{"type":"number"}`
// would leave Type empty.
type ColumnSchema struct {
	Type     string `json:"type"` // "boolean", "number", "string", "null", "json"
	Optional bool   `json:"optional"`
}

// QueryResult holds the generated SQL and bind parameters.
type QueryResult struct {
	SQL    string
	Params []interface{}
}

// BuildSelectQuery generates a SELECT query matching the TS buildSelectQuery
// (zqlite/query-builder.ts @ 1.7.0, incl. the multiConstraints batched-IN
// clauses from #5928).
func BuildSelectQuery(
	tableName string,
	columns map[string]ColumnSchema,
	constraint *ivm.Constraint,
	filters *Condition,
	order ivm.Ordering,
	reverse bool,
	start *ivm.Start,
	multiConstraints []ivm.MultiConstraint,
) QueryResult {
	var params []interface{}
	colNames := sortedColumnNames(columns)

	// SELECT columns FROM table.
	//
	// Every result column is wrapped in SQLite's unary `+` no-op and aliased
	// back to its bare name: `+"col" AS "col"`. The unary + returns its
	// operand unchanged for every storage class (INTEGER/REAL/TEXT/BLOB/NULL)
	// but turns the result column into an EXPRESSION, and expressions carry
	// no declared type — sqlite3_column_decltype returns NULL.
	//
	// That kills mattn/go-sqlite3's decltype-driven value conversions in
	// Rows.Next (sqlite3.go:2571-2638 @ v1.14.44), which TS's better-sqlite3
	// does not have — TS ships the raw cell:
	//
	//   - INTEGER in a column declared exactly "timestamp"/"datetime"/"date"
	//     (nullable temporal columns; NOT-null ones are declared
	//     "timestamp|NOT_NULL" which dodges the exact-string match) became
	//     time.Time via a magnitude heuristic: |v| <= 1e12 ⇒ SECONDS, else
	//     ms. Reversing with UnixMilli() multiplied every pre-2001 epoch-ms
	//     value (|v| <= 1e12, negatives included) by 1000 — a real
	//     hydration-vs-TS data divergence (ART G15, 2026-07-07: replica 1 →
	//     Go 1000, TS 1). A smarter reversal is provably impossible (stored
	//     2e9 and 2e12 produce the identical time.Time), so the conversion
	//     must not happen at all.
	//   - TEXT in a temporal column parsed to time.Time (zero time on parse
	//     failure) instead of shipping the raw string.
	//   - INTEGER in a column declared exactly "boolean" became Go bool via
	//     `val > 0`, which disagrees with TS's `!!v` truthiness for negative
	//     integers. FromSQLiteType's boolean case applies `val != 0` to the
	//     raw integer, matching TS.
	//
	// The alias back to the bare name is load-bearing: every scan site maps
	// values by rows.Columns() name (tablesource source.go scanRows /
	// fetchDuringPushStream; snapshotter scanRawRow uses spec order but the
	// harness reads names too) — without it the result column would be
	// named `+"col"` and the schema lookup would drop every value.
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = "+" + quoteIdent(c) + " AS " + quoteIdent(c)
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(quotedCols, ", "), quoteIdent(tableName))

	// WHERE clauses
	var constraints []string

	// Constraint (join key equality)
	if constraint != nil {
		for _, key := range sortedConstraintKeys(*constraint) {
			value := (*constraint)[key]
			colSchema := columns[key]
			sqlVal := ToSQLiteType(value, colSchema.Type)
			constraints = append(constraints, fmt.Sprintf("%s = ?", quoteIdent(key)))
			params = append(params, sqlVal)
		}
	}

	// Multi-constraints (batched IN clauses). Empty entries are skipped —
	// same as TS buildSelectQuery's `mc.length > 0` guard.
	for _, mc := range multiConstraints {
		if len(mc) == 0 {
			continue
		}
		mcSQL, mcParams := multiConstraintToSQL(mc, columns)
		constraints = append(constraints, mcSQL)
		params = append(params, mcParams...)
	}

	// Start cursor
	if start != nil {
		// TS query-builder.ts:50-53 — assert(order !== undefined, 'start
		// requires ordering'): a cursor bound is meaningless without a sort,
		// and unordered (Cap/EXISTS-child) connections must never see one.
		if order == nil {
			panic("start requires ordering")
		}
		startSQL, startParams := gatherStartConstraints(*start, reverse, order, columns)
		constraints = append(constraints, startSQL)
		params = append(params, startParams...)
	}

	// Filters
	if filters != nil {
		filterSQL, filterParams := filtersToSQL(filters)
		constraints = append(constraints, filterSQL)
		params = append(params, filterParams...)
	}

	if len(constraints) > 0 {
		query += " WHERE " + strings.Join(constraints, " AND ")
	}

	// ORDER BY
	if order != nil && len(order) > 0 {
		query += " " + orderByToSQL(order, reverse)
	}

	return QueryResult{SQL: query, Params: params}
}

// multiConstraintToSQL builds a single batched IN clause from a
// MultiConstraint (TS multiConstraintToSQL, zqlite/query-builder.ts @
// 1.7.0). All entries must share the same column shape; FlippedJoin derives
// them from the same parentKey for all children.
//
// Single-column form: `"col" IN (?, ?, ?)`
// Compound form:      `("a", "b") IN (VALUES (?, ?), (?, ?), …)`
//
// SQLite optimizes `col IN (literal-list)` using the column's index
// (verified upstream via EXPLAIN QUERY PLAN).
//
// Go deviation: TS takes the column list from Object.keys(entry[0])
// (JS insertion order); Go map iteration is randomized, so the keys are
// SORTED for a deterministic clause — required for prepared-statement
// cache hits and semantically identical (the tuple order pairs each
// column with its own values either way).
//
// Panics with a DataError on heterogeneous entry shapes (TS asserts).
// MultiConstraints are engine-generated (FlippedJoin), never client-sent,
// so this is an internal-invariant check, not an input-validation boundary.
func multiConstraintToSQL(mc ivm.MultiConstraint, columns map[string]ColumnSchema) (string, []interface{}) {
	keys := make([]string, 0, len(mc[0]))
	for k := range mc[0] {
		keys = append(keys, k)
	}
	sortStrings(keys)
	if len(keys) == 0 {
		panic(ivm.NewDataError("multiConstraintToSQL: entries must have at least one key"))
	}
	for i := 1; i < len(mc); i++ {
		if len(mc[i]) != len(keys) {
			panic(ivm.NewDataError("multiConstraintToSQL: entries must share the same keys (entry 0 has %d, entry %d has %d)", len(keys), i, len(mc[i])))
		}
		for _, k := range keys {
			if _, ok := mc[i][k]; !ok {
				panic(ivm.NewDataError("multiConstraintToSQL: entry %d missing key %q", i, k))
			}
		}
	}

	if len(keys) == 1 {
		key := keys[0]
		colType := columns[key].Type
		placeholders := make([]string, len(mc))
		params := make([]interface{}, len(mc))
		for i, c := range mc {
			placeholders[i] = "?"
			params[i] = ToSQLiteType(c[key], colType)
		}
		return fmt.Sprintf("%s IN (%s)", quoteIdent(key), strings.Join(placeholders, ",")), params
	}

	// Compound: `("a", "b") IN (VALUES (?, ?), …)`
	quotedKeys := make([]string, len(keys))
	rowPlaceholders := make([]string, len(keys))
	for i, k := range keys {
		quotedKeys[i] = quoteIdent(k)
		rowPlaceholders[i] = "?"
	}
	rowForm := "(" + strings.Join(rowPlaceholders, ", ") + ")"
	rows := make([]string, len(mc))
	params := make([]interface{}, 0, len(mc)*len(keys))
	for i, c := range mc {
		rows[i] = rowForm
		for _, k := range keys {
			params = append(params, ToSQLiteType(c[k], columns[k].Type))
		}
	}
	return fmt.Sprintf("(%s) IN (VALUES %s)",
		strings.Join(quotedKeys, ", "), strings.Join(rows, ",")), params
}

// filtersToSQL converts a Condition tree to SQL.
func filtersToSQL(cond *Condition) (string, []interface{}) {
	if cond == nil {
		return "TRUE", nil
	}
	switch cond.Type {
	case "simple":
		return simpleConditionToSQL(cond)
	case "and":
		if len(cond.Conditions) == 0 {
			return "TRUE", nil
		}
		parts := make([]string, len(cond.Conditions))
		var params []interface{}
		for i, c := range cond.Conditions {
			s, p := filtersToSQL(c)
			parts[i] = s
			params = append(params, p...)
		}
		return "(" + strings.Join(parts, " AND ") + ")", params
	case "or":
		if len(cond.Conditions) == 0 {
			return "FALSE", nil
		}
		parts := make([]string, len(cond.Conditions))
		var params []interface{}
		for i, c := range cond.Conditions {
			s, p := filtersToSQL(c)
			parts[i] = s
			params = append(params, p...)
		}
		return "(" + strings.Join(parts, " OR ") + ")", params
	}
	return "TRUE", nil
}

// allowedOps is the exhaustive set of operators simpleConditionToSQL will
// emit into generated SQL. S4: cond.Op originates from a CLIENT-SENT AST
// (the sidecar accepts addQuery with an `ast` field over the wire), so it is
// untrusted and was previously interpolated raw at the Sprintf below — a
// malicious op like "; DROP TABLE x; --" would be spliced straight into the
// query. The whitelist closes that: any op not in this set is treated as
// unsupported and short-circuits to "1=0" (a safe no-match that interpolates
// NO client data), rather than panicking — this is a client-input boundary
// shared across an entire client group, and a bad op must not take the engine
// down (same reasoning as matchLike returning false on a bad pattern). The
// no-match is observable as an empty result; a genuinely-unsupported op query
// simply yields no rows. Every op the Zero client emits is listed here; if a
// new op is added upstream it must be added to this set or it will silently
// no-match.
var allowedOps = map[string]bool{
	"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
	"LIKE": true, "NOT LIKE": true,
	"ILIKE": true, "NOT ILIKE": true, // mapped to LIKE/NOT LIKE below
	"IN": true, "NOT IN": true,
	"IS": true, "IS NOT": true,
}

// simpleConditionToSQL handles a simple condition.
func simpleConditionToSQL(cond *Condition) (string, []interface{}) {
	op := cond.Op
	// S4: reject any operator outside the whitelist BEFORE it reaches the
	// Sprintf that interpolates op raw. Safe no-match, no crash, no injection.
	if !allowedOps[op] {
		return "1=0", nil
	}
	if op == "LIKE" || op == "NOT LIKE" || op == "ILIKE" || op == "NOT ILIKE" {
		return likeConditionToSQL(cond, op)
	}

	if op == "IN" || op == "NOT IN" {
		leftSQL, leftParams := valuePositionToSQL(cond.Left)
		// For IN, right value must be a JSON array string for json_each()
		rightJSON, err := json.Marshal(cond.Right.Value)
		if err != nil {
			rightJSON = []byte("[]")
		}
		return fmt.Sprintf("%s %s (SELECT value FROM json_each(?))", leftSQL, op),
			append(leftParams, string(rightJSON))
	}

	leftSQL, leftParams := valuePositionToSQL(cond.Left)
	rightSQL, rightParams := valuePositionToSQL(cond.Right)
	return fmt.Sprintf("%s %s %s", leftSQL, op, rightSQL),
		append(leftParams, rightParams...)
}

// likeConditionToSQL mirrors TS's likeConditionToSQL (zqlite/query-builder.ts
// @ v1.7.0), which aligned SQL pattern matching with Postgres / the in-memory
// IVM matcher (like.ts):
//   - LIKE is case-sensitive: the replica connections run with
//     `PRAGMA case_sensitive_like = ON` (see internal/tablesource/db.go), so
//     the bare LIKE operator is case-sensitive.
//   - ILIKE is case-insensitive: lower() both operands. TS uses the
//     Unicode-aware ICU lower() from @rocicorp/zero-sqlite3; our connections
//     override SQLite's ASCII-only built-in lower() with a full Unicode case
//     mapping (db.go ConnectHook) to match.
//   - Backslash is the default escape character in Postgres and in the IVM
//     matcher, but SQLite has no default — emit `ESCAPE '\'` explicitly.
//     (SQLite string literals don't process backslash escapes, so '\' in the
//     SQL text is a single literal backslash.)
//
// Pre-1.7.0 this mapped ILIKE→LIKE and relied on SQLite's case-insensitive
// default with no escape; upstream changed the contract.
func likeConditionToSQL(cond *Condition, op string) (string, []interface{}) {
	caseInsensitive := op == "ILIKE" || op == "NOT ILIKE"
	likeOp := "LIKE"
	if op == "NOT LIKE" || op == "NOT ILIKE" {
		likeOp = "NOT LIKE"
	}
	leftSQL, leftParams := valuePositionToSQL(cond.Left)
	rightSQL, rightParams := valuePositionToSQL(cond.Right)
	params := append(leftParams, rightParams...)
	if caseInsensitive {
		return fmt.Sprintf(`lower(%s) %s lower(%s) ESCAPE '\'`, leftSQL, likeOp, rightSQL), params
	}
	return fmt.Sprintf(`%s %s %s ESCAPE '\'`, leftSQL, likeOp, rightSQL), params
}

func valuePositionToSQL(vp ValuePos) (string, []interface{}) {
	switch vp.Type {
	case "column":
		return quoteIdent(vp.Name), nil
	case "literal":
		// M3 (napi review): TS types filter literals by the LITERAL's OWN JS
		// type — valuePositionToSQL → toSQLiteType(v, getJsType(v))
		// (zqlite/query-builder.ts:257,265) — NOT by the column's schema
		// type. The old ColType plumbing coerced by COLUMN type, so e.g. a
		// string literal compared against a json column was bound as its
		// JSON encoding ('"x"' instead of x), and a string literal against a
		// boolean column ('true') was bound as 1 — both silently matching
		// different rows than TS. Constraints and cursors are DIFFERENT: TS
		// types those by column schema (table-source toSQLiteTypes), which
		// the constraint/start paths here still do.
		return "?", []interface{}{ToSQLiteType(vp.Value, jsValueType(vp.Value))}
	case "static":
		panic("Static parameters must be replaced before conversion to SQL")
	}
	return "?", []interface{}{vp.Value}
}

// jsValueType mirrors zqlite/query-builder.ts getJsType: the ValueType of a
// LITERAL as JS typeof sees it — null, string, number, boolean, else json.
func jsValueType(v ivm.Value) string {
	switch v.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "number"
	case bool:
		return "boolean"
	default:
		return "json"
	}
}

// orderByToSQL generates ORDER BY clause.
func orderByToSQL(order ivm.Ordering, reverse bool) string {
	parts := make([]string, len(order))
	for i, o := range order {
		dir := o[1]
		if reverse {
			if dir == "asc" {
				dir = "desc"
			} else {
				dir = "asc"
			}
		}
		parts[i] = fmt.Sprintf("%s %s", quoteIdent(o[0]), dir)
	}
	return "ORDER BY " + strings.Join(parts, ", ")
}

// gatherStartConstraints builds the cursor constraint for pagination.
func gatherStartConstraints(
	start ivm.Start,
	reverse bool,
	order ivm.Ordering,
	columns map[string]ColumnSchema,
) (string, []interface{}) {
	var orClauses []string
	var params []interface{}

	for i := range order {
		iField := order[i][0]

		// Partial cursor: stop at the first sort column not present in start.Row.
		// A partial cursor (e.g. start={createdAt: X} over sort [createdAt, pk])
		// means "everything strictly after createdAt=X for ANY value of pk". TS's
		// SQL gets this for free: toSQLiteType(undefined) binds NULL, and
		// "col > NULL" evaluates to NULL in SQLite — disabling the clause. But
		// when the column is Optional the nullable-aware form "(? IS NULL OR col > ?)"
		// short-circuits to TRUE (because NULL IS NULL → TRUE), admitting the
		// boundary row. The correct semantic — matching CompareWithPartialBound on
		// the push path — is to not generate OR clauses beyond what the cursor
		// specifies. This is the SQL-path counterpart of the ed7a302 push fix.
		if _, ok := start.Row[iField]; !ok {
			break
		}

		var andParts []string
		iDirection := order[i][1]

		for j := 0; j <= i; j++ {
			if j == i {
				colSchema := columns[iField]
				constraintValue := ToSQLiteType(start.Row[iField], colSchema.Type)
				var op string
				if iDirection == "asc" {
					if reverse {
						op = "<"
					} else {
						op = ">"
					}
				} else {
					if reverse {
						op = ">"
					} else {
						op = "<"
					}
				}
				rangeSQL := nullableAwareRangeComparison(iField, op, colSchema)
				andParts = append(andParts, rangeSQL)
				params = append(params, constraintValue)
				// The optional-column ">" form is "(? IS NULL OR col > ?)" —
				// TWO placeholders, both bound to the same start value (the
				// leading "? IS NULL" tests whether the cursor value itself is
				// NULL). Every other form ("col > ?", "(col IS NULL OR col < ?)")
				// has a single placeholder. Without this second bind the
				// statement has 2 params-wanted / 1 given and SQLite panics
				// "not enough args to execute query: want 2 got 1" the first
				// time a Take/Skip cursor hydrates over a nullable order column.
				if op == ">" && colSchema.Optional {
					params = append(params, constraintValue)
				}
			} else {
				jField := order[j][0]
				colSchema := columns[jField]
				value := ToSQLiteType(start.Row[jField], colSchema.Type)
				eqSQL := nullableAwareEquality(jField, colSchema)
				andParts = append(andParts, eqSQL)
				params = append(params, value)
			}
		}
		orClauses = append(orClauses, "("+strings.Join(andParts, " AND ")+")")
	}

	// Inclusive (basis == "at"): add equality for order fields present in start.Row.
	// Same partial-cursor rule: skip columns not specified in the cursor.
	// Guard the degenerate cursor (Row lacks even the FIRST order column):
	// appending "(" + join(nothing) + ")" would emit the literal "()", a SQL
	// syntax error that panics every Fetch — the 'at' twin of the empty-
	// orClauses guard below, and the same disposition: a cursor pinning no
	// position imposes no constraint.
	if start.Basis == "at" {
		var andParts []string
		for _, o := range order {
			field := o[0]
			if _, ok := start.Row[field]; !ok {
				break
			}
			colSchema := columns[field]
			value := ToSQLiteType(start.Row[field], colSchema.Type)
			eqSQL := nullableAwareEquality(field, colSchema)
			andParts = append(andParts, eqSQL)
			params = append(params, value)
		}
		if len(andParts) > 0 {
			orClauses = append(orClauses, "("+strings.Join(andParts, " AND ")+")")
		}
	}

	// Defensive: if the cursor specified no usable order column (start.Row lacks
	// the FIRST sort column and basis != "at"), orClauses is empty. Returning
	// "(" + "" + ")" = "()" is a SQL syntax error that panics every Fetch. A
	// cursor that pins no position imposes no lower bound, so the correct
	// constraint is "no constraint" (TRUE). Not reachable from today's Take/Skip
	// cursors (a partial cursor is always a prefix of the sort, so column 0 is
	// present), but unguarded interpolation of "()" is a latent crash.
	if len(orClauses) == 0 {
		return "TRUE", nil
	}

	return "(" + strings.Join(orClauses, " OR ") + ")", params
}

func nullableAwareEquality(field string, col ColumnSchema) string {
	if col.Optional {
		return fmt.Sprintf("%s IS ?", quoteIdent(field))
	}
	return fmt.Sprintf("%s = ?", quoteIdent(field))
}

func nullableAwareRangeComparison(field string, op string, col ColumnSchema) string {
	comparison := fmt.Sprintf("%s %s ?", quoteIdent(field), op)
	if !col.Optional {
		return comparison
	}
	if op == ">" {
		return fmt.Sprintf("(? IS NULL OR %s)", comparison)
	}
	return fmt.Sprintf("(%s IS NULL OR %s)", quoteIdent(field), comparison)
}

// ToSQLiteType converts a Go value to SQLite-compatible type.
//
// Mirrors TS toSQLiteType (query-builder.ts:278-289), which has NO top-level
// null short-circuit — null handling is per-arm: boolean maps null→null
// explicitly, number/string/null pass it through, and the json arm
// JSON.stringify's it. A former top-level `if v == nil { return nil }` here
// silently overrode the json arm, storing SQL NULL where TS stores the TEXT
// 'null' (the one on-disk divergence found by the porting-correctness review).
func ToSQLiteType(v ivm.Value, colType string) interface{} {
	switch colType {
	case "boolean":
		// TS: `v === null ? null : v ? 1 : 0` (query-builder.ts:281).
		if v == nil {
			return nil
		}
		if b, ok := v.(bool); ok {
			if b {
				return 1
			}
			return 0
		}
		return v
	case "json":
		// ALWAYS marshal — never passthrough, and NO nil short-circuit. TS's
		// toSQLiteType ALWAYS JSON.stringify's (query-builder.ts:287), even for
		// a string, so a JSON string value is stored as "\"x\"" not bare x. A
		// bare-string passthrough here is what made FromSQLiteType panic on the
		// next read ("Payment Failures" → invalid JSON). And JSON.stringify(null)
		// === 'null', so a null json value is stored as the 4-char TEXT 'null',
		// never SQL NULL — json.Marshal(nil) == "null" matches exactly.
		b, err := json.Marshal(v)
		if err != nil {
			// json.Marshal rejects NaN/±Inf; JSON.stringify encodes them as the
			// literal "null" rather than throwing. Match TS exactly: return "null"
			// (valid JSON, round-trips to nil) — NOT the old fmt.Sprintf("%v", v),
			// which wrote bare "NaN"/"+Inf" that FromSQLiteType then panicked on
			// (the same corruption class as the string-passthrough bug), and NOT a
			// panic: the codebase's invariant is that Go fails ONLY where TS fails,
			// and TS does not fail here. Unreachable for real json-column values
			// (they originate from json.Unmarshal and never hold NaN/Inf), but kept
			// symmetric so a stray non-finite float can't desync Go from TS.
			return "null"
		}
		return string(b)
	default:
		return v
	}
}

// FromSQLiteType converts a SQLite value back to Go IVM Value.
func FromSQLiteType(v interface{}, colType string) ivm.Value {
	if v == nil {
		return nil
	}
	// A time.Time here is a PLUMBING BUG, never a data condition. TS's
	// better-sqlite3 has no decltype conversion — it ships raw cells — and
	// every Go-side row SELECT strips the declared type with the unary-+
	// wrap (`+"col" AS "col"` — BuildSelectQuery and the snapshotter's
	// selectColList), so mattn/go-sqlite3's decltype-driven time.Time
	// conversion (sqlite3.go:2571-2638 @ v1.14.44) can no longer fire.
	//
	// History: this used to be `v = t.UnixMilli()` (2026-06-08, fixing
	// nullable timestamps msgpack-encoding as `{}`), but mattn builds the
	// time.Time with a |v| <= 1e12 ⇒ SECONDS heuristic, so the reversal
	// multiplied every pre-2001 epoch-ms value by 1000 (ART G15,
	// 2026-07-07). No reversal can be correct — stored 2e9 (seconds path)
	// and 2e12 (ms path) yield the identical time.Time — so reaching this
	// branch means some SELECT site exposes a bare temporal decltype and
	// must be given the unary-+ wrap. Panic loudly instead of shipping a
	// silently wrong value; the engine's recover surfaces it as an RPC
	// error → CG teardown.
	if t, ok := v.(time.Time); ok {
		panic(fmt.Sprintf(
			"FromSQLiteType: time.Time %v reached coercion — a SELECT site exposes a bare temporal decltype to mattn's driver conversion; wrap its result columns in `+\"col\" AS \"col\"` (see BuildSelectQuery)", t))
	}
	switch colType {
	case "boolean":
		switch val := v.(type) {
		case int64:
			return val != 0
		case uint64:
			return val != 0
		case float64:
			return val != 0
		case string:
			// MED-1 (types): TS coerces booleans with `!!v` (table-source.ts:618),
			// i.e. pure JS truthiness of the RAW value — so ANY non-empty string is
			// true, including "0", "0.0" and "false". The old literal-list +
			// ParseFloat check gave the opposite answer for those ("0"→false), a
			// silent TS/Go divergence. Match JS exactly: empty string → false,
			// everything else → true. (Boolean columns are stored as 0/1 INTEGER in
			// the replica so this string branch is defensive, but it must still
			// agree with TS to keep init-vs-advance shape parity — CRIT-6.)
			return val != ""
		case []byte:
			// A SQLite blob in a boolean column never happens in practice, but JS
			// treats any Buffer as truthy; mirror "non-empty → true".
			return len(val) != 0
		case bool:
			return val
		default:
			return v
		}
	case "number":
		switch val := v.(type) {
		case int64:
			// HIGH-9: int64 above JS Number.MAX_SAFE_INTEGER (±2^53-1) cannot
			// round-trip through float64 — silent precision loss aliases PKs to
			// adjacent integers and makes joins match wrong rows. TS throws
			// UnsupportedValueError on the same input; panic to match (the
			// engine's recover surfaces it instead of corrupting silently).
			if val > maxSafeInteger || val < -maxSafeInteger {
				panic(ivm.NewDataError("FromSQLiteType(number): int64 %d exceeds JS MAX_SAFE_INTEGER (±2^53-1); float64 coercion loses precision", val))
			}
			return boxFloat64(float64(val))
		case uint64:
			if val > uint64(maxSafeInteger) {
				panic(ivm.NewDataError("FromSQLiteType(number): uint64 %d exceeds JS MAX_SAFE_INTEGER (2^53-1); float64 coercion loses precision", val))
			}
			return boxFloat64(float64(val))
		case float64:
			return boxFloat64(val)
		case string:
			// User's-audit item (numeric-string coercion on the advance path):
			// TS's fromSQLiteType 'number' branch NEVER parses strings — it
			// only downcasts bigint and returns everything else AS-IS
			// (table-source.ts fromSQLiteType), and the TS push path consumes
			// incoming change rows without re-coercing at all. The old
			// ParseFloat here (a modernc.org/sqlite legacy; mattn returns REAL
			// as float64, so the hydrate path never took it) fired on the
			// ADVANCE path via NormalizeRow: a numeric-looking string in a
			// number column became float64 in Go while TS kept the string —
			// row-content drift between engines. Pass through, like TS.
			return val
		case []byte:
			// better-sqlite3 surfaces TEXT as a JS string, so TS's `return v`
			// yields a STRING for text stored in a number column; mattn hands
			// us []byte — convert the representation, never the value.
			return string(val)
		case bool:
			// mattn's decltype conversion can surface BOOLEAN-declared INTEGER
			// columns as bool; better-sqlite3 would have handed TS 0/1 →
			// Number. Normalize back to the numeric the TS engine holds.
			if val {
				return boxFloat64(1)
			}
			return boxFloat64(0)
		default:
			return v
		}
	case "json":
		// JSON columns are stored as text; parse them. TS's coerceValue
		// (table-source.ts:632-641) throws UnsupportedValueError on
		// JSON.parse failure — surfacing the error instead of silently
		// shipping the raw string to the client (which would desync the
		// init-vs-advance shape: init sends a parsed object, advance sends
		// the raw string). Panic to match; the engine's recover surfaces
		// it as an RPC error.
		switch val := v.(type) {
		case string:
			var parsed interface{}
			if err := json.Unmarshal([]byte(val), &parsed); err != nil {
				panic(ivm.NewDataError("FromSQLiteType(json): parse failed for %q: %v", val, err))
			}
			return parsed
		case []byte:
			var parsed interface{}
			if err := json.Unmarshal(val, &parsed); err != nil {
				panic(ivm.NewDataError("FromSQLiteType(json): parse failed for %q: %v", string(val), err))
			}
			return parsed
		default:
			return v
		}
	case "string":
		// TS folds 'string' into the same branch as 'number'|'null'
		// (table-source.ts fromSQLiteType): bigint → bounds-check → Number,
		// everything else returned AS-IS. So an INTEGER stored in a string
		// column surfaces in TS's engine as a JS NUMBER — not the formatted
		// text the old FormatInt/FormatFloat produced (user's-audit coercion
		// item; same class as the 'null'-branch divergence fixed 2026-07-03).
		// []byte→string stays: better-sqlite3 hands TS TEXT as a JS string,
		// mattn hands us []byte — representation conversion, not value.
		switch val := v.(type) {
		case []byte:
			return string(val)
		case string:
			return val
		case int64:
			if val > maxSafeInteger || val < -maxSafeInteger {
				panic(ivm.NewDataError("FromSQLiteType(string): int64 %d exceeds JS MAX_SAFE_INTEGER (±2^53-1)", val))
			}
			return boxFloat64(float64(val))
		case uint64:
			if val > uint64(maxSafeInteger) {
				panic(ivm.NewDataError("FromSQLiteType(string): uint64 %d exceeds JS MAX_SAFE_INTEGER (2^53-1)", val))
			}
			return boxFloat64(float64(val))
		default:
			return v
		}
	case "null":
		// TS folds 'null' with 'number'|'string' into ONE branch
		// (table-source.ts fromSQLiteType): `typeof v === 'bigint'` →
		// bounds-check → `return Number(v)`. So an INTEGER-stored value in a
		// 'null'-typed column comes back a JS NUMBER, not a bigint. The Go
		// port must therefore CONVERT int64/uint64 to float64 after the
		// bounds check — the previous passthrough (return raw int64) was a
		// porting divergence (full-scale review 2026-07-03): within Go it
		// left int64 vs float64 mixing in comparators/equality for the same
		// logical value, and it diverged from what TS's engine holds.
		switch val := v.(type) {
		case int64:
			if val > maxSafeInteger || val < -maxSafeInteger {
				panic(ivm.NewDataError("FromSQLiteType(null): int64 %d exceeds JS MAX_SAFE_INTEGER (±2^53-1)", val))
			}
			return float64(val)
		case uint64:
			if val > uint64(maxSafeInteger) {
				panic(ivm.NewDataError("FromSQLiteType(null): uint64 %d exceeds JS MAX_SAFE_INTEGER (2^53-1)", val))
			}
			return float64(val)
		}
		return v
	default:
		return v
	}
}

// SelfCheckCoercion validates the init-vs-advance shape-convergence contract
// (CRIT-6) at sidecar startup. TS's init path sends raw SQLite values (bool as
// 0/1 int, etc.) while the advance path sends pre-coerced JS shapes (bool,
// number); both land in the same source and only stay consistent because
// FromSQLiteType maps BOTH shapes to the same canonical value for every
// colType. A future PG type added to TS's pgTypeToGoType without a matching
// FromSQLiteType case would silently hit the default passthrough and desync the
// two paths — invisible corruption. This asserts the invariant up front so that
// regression fails loud at boot instead of mis-shipping client data later.
func SelfCheckCoercion() error {
	checks := []struct {
		colType  string
		rawShape interface{} // representative shape the init path sends
		jsShape  interface{} // representative shape the advance path sends
	}{
		{"boolean", int64(1), true},
		{"boolean", int64(0), false},
		{"number", int64(42), float64(42)},
		// 'string' type: the replica stores TEXT for string columns, so the
		// init path's raw shape is []byte (mattn) where better-sqlite3 hands
		// TS a JS string; the advance path ships the string directly. The
		// previous {int64(7), "7"} pair modeled the OLD FormatInt behavior —
		// which itself diverged from TS (TS's shared number|string|null
		// branch turns bigint into Number, never into text), fixed under the
		// user's-audit coercion item.
		{"string", []byte("7"), "7"},
		// 'null' type: TS's shared 'number|string|null' branch converts
		// bigint→Number, so an int64 from the replica and a float64 from an
		// advance row must converge on the same float64.
		{"null", int64(42), float64(42)},
	}
	for _, c := range checks {
		a := FromSQLiteType(c.rawShape, c.colType)
		b := FromSQLiteType(c.jsShape, c.colType)
		if a != b {
			return fmt.Errorf(
				"coercion self-check FAILED for colType=%q: init-shape %T(%v)→%v "+
					"vs advance-shape %T(%v)→%v diverge — init and advance rows "+
					"would have mismatched per-column shapes",
				c.colType, c.rawShape, c.rawShape, a, c.jsShape, c.jsShape, b)
		}
	}
	// JSON write/read symmetry — the string-passthrough bug class. A json column
	// value MUST survive ToSQLiteType→FromSQLiteType unchanged; the original bug
	// passed a Go string through ToSQLiteType un-quoted, so the next read panicked
	// ("Payment Failures" → invalid JSON). TestToSQLiteType_JSONRoundTrip guards
	// this in CI, but the bug reached PRODUCTION, so re-assert it on the live
	// build/driver before serving. Scalar payloads only: they exercise the
	// stringify/parse symmetry and stay !=-comparable after parse.
	for _, jv := range []interface{}{"Payment Failures", "PROD", float64(42), true} {
		rt, err := jsonRoundTrip(jv)
		if err != nil {
			return fmt.Errorf("coercion self-check FAILED: json round-trip of %T(%v) errored: %w "+
				"— ToSQLiteType/FromSQLiteType asymmetric for json (string-passthrough bug class)",
				jv, jv, err)
		}
		if rt != jv {
			return fmt.Errorf("coercion self-check FAILED: json round-trip of %T(%v) produced %T(%v) "+
				"— ToSQLiteType/FromSQLiteType asymmetric for json (string-passthrough bug class)",
				jv, jv, rt, rt)
		}
	}
	return nil
}

// jsonRoundTrip runs v through ToSQLiteType then FromSQLiteType for a json
// column, recovering any panic into an error so SelfCheckCoercion keeps a
// uniform error contract (return, not crash) if a future change reintroduces
// an asymmetry that makes the re-read panic.
func jsonRoundTrip(v interface{}) (out interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic on re-read: %v", r)
		}
	}()
	return FromSQLiteType(ToSQLiteType(v, "json"), "json"), nil
}

// --- Helpers ---

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func sortedColumnNames(columns map[string]ColumnSchema) []string {
	names := make([]string, 0, len(columns))
	for k := range columns {
		names = append(names, k)
	}
	// Sort for deterministic output
	sortStrings(names)
	return names
}

func sortedConstraintKeys(c ivm.Constraint) []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sortStrings(keys)
	return keys
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
