package sqlite

// Builds SELECT SQL queries with constraints, filters, ordering, and start cursors.

import (
	"encoding/json"
	"fmt"
	"strconv"
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
	Type    string    // "column", "literal", "static"
	Name    string    // for column
	Value   ivm.Value // for literal
	ColType string    // optional: column type for literal conversion
}

// ColumnSchema describes a column's type and nullability.
//
// Struct tags are required because msgpack decoding (unlike Go's encoding/json)
// is case-sensitive on field names — without the tag, TS's `{"type":"number"}`
// would leave Type empty.
type ColumnSchema struct {
	Type     string `json:"type"`     // "boolean", "number", "string", "null", "json"
	Optional bool   `json:"optional"`
}

// QueryResult holds the generated SQL and bind parameters.
type QueryResult struct {
	SQL    string
	Params []interface{}
}

// BuildSelectQuery generates a SELECT query matching the TS buildSelectQuery.
func BuildSelectQuery(
	tableName string,
	columns map[string]ColumnSchema,
	constraint *ivm.Constraint,
	filters *Condition,
	order ivm.Ordering,
	reverse bool,
	start *ivm.Start,
) QueryResult {
	var params []interface{}
	colNames := sortedColumnNames(columns)

	// SELECT columns FROM table
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = quoteIdent(c)
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

	// Start cursor
	if start != nil && order != nil {
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
	// SQLite LIKE is case-insensitive, so ILIKE → LIKE
	switch op {
	case "ILIKE":
		op = "LIKE"
	case "NOT ILIKE":
		op = "NOT LIKE"
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

func valuePositionToSQL(vp ValuePos) (string, []interface{}) {
	switch vp.Type {
	case "column":
		return quoteIdent(vp.Name), nil
	case "literal":
		// Convert literal value through ToSQLiteType if column type known
		converted := vp.Value
		if vp.ColType != "" {
			converted = ToSQLiteType(vp.Value, vp.ColType)
		}
		return "?", []interface{}{converted}
	case "static":
		panic("Static parameters must be replaced before conversion to SQL")
	}
	return "?", []interface{}{vp.Value}
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
		orClauses = append(orClauses, "("+strings.Join(andParts, " AND ")+")")
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
func ToSQLiteType(v ivm.Value, colType string) interface{} {
	if v == nil {
		return nil
	}
	switch colType {
	case "boolean":
		if b, ok := v.(bool); ok {
			if b {
				return 1
			}
			return 0
		}
		return v
	case "json":
		// JSON stored as text
		if s, ok := v.(string); ok {
			return s
		}
		// Marshal non-string values to JSON
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
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
	// mattn/go-sqlite3 auto-converts columns whose declared SQLite type is
	// EXACTLY "timestamp"/"datetime"/"date" (case-insensitive) into time.Time
	// in Rows.Next. Zero's replica declares NULLABLE temporal columns as bare
	// "timestamp"/"date" but NON-null ones as "timestamp|NOT_NULL" — the
	// "|NOT_NULL" suffix dodges mattn's exact-string decltype match, so ONLY
	// nullable temporal columns reach us as time.Time (non-null ones arrive as
	// the raw int64 epoch and flow through the numeric path below). TS models
	// every timestamp as an epoch-MILLISECOND number, so an unconverted
	// time.Time would (a) skip the numeric coercion below and (b) msgpack-encode
	// as `{}` (a struct with only unexported fields), shipping the client an
	// empty object instead of the timestamp — a real go-vs-TS content drift
	// caught by the shadow SQL oracle on channel_user_status.conversationSeenCutoffAt
	// + updatedAt. Normalize back to epoch ms here so the value rejoins the
	// normal numeric path identically to a NOT_NULL temporal column. mattn builds
	// the time.Time with a >1e12 ⇒ milliseconds heuristic, so UnixMilli()
	// round-trips every real (post-2001) timestamp exactly.
	if t, ok := v.(time.Time); ok {
		v = t.UnixMilli()
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
				panic(fmt.Sprintf("FromSQLiteType(number): int64 %d exceeds JS MAX_SAFE_INTEGER (±2^53-1); float64 coercion loses precision", val))
			}
			return boxFloat64(float64(val))
		case uint64:
			if val > uint64(maxSafeInteger) {
				panic(fmt.Sprintf("FromSQLiteType(number): uint64 %d exceeds JS MAX_SAFE_INTEGER (2^53-1); float64 coercion loses precision", val))
			}
			return boxFloat64(float64(val))
		case float64:
			return boxFloat64(val)
		case string:
			// modernc.org/sqlite may return REAL values as strings
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return v
			}
			return boxFloat64(f)
		case []byte:
			f, err := strconv.ParseFloat(string(val), 64)
			if err != nil {
				return v
			}
			return boxFloat64(f)
		case bool:
			if val {
				return boxFloat64(1)
			}
			return boxFloat64(0)
		default:
			return v
		}
	case "json":
		// JSON columns are stored as text; parse them
		switch val := v.(type) {
		case string:
			var parsed interface{}
			if err := json.Unmarshal([]byte(val), &parsed); err != nil {
				return val // Return raw string on parse failure
			}
			return parsed
		case []byte:
			var parsed interface{}
			if err := json.Unmarshal(val, &parsed); err != nil {
				return string(val)
			}
			return parsed
		default:
			return v
		}
	case "string":
		switch val := v.(type) {
		case []byte:
			return string(val)
		case string:
			return val
		case int64:
			return strconv.FormatInt(val, 10)
		case float64:
			return strconv.FormatFloat(val, 'f', -1, 64)
		default:
			return fmt.Sprintf("%v", v)
		}
	case "null":
		// CRIT-6: TS folds 'null' with 'number'|'string' (table-source.ts:619-630)
		// — pass the value through unchanged, but reject int64/uint64 beyond
		// ±2^53 that cannot round-trip as a JS number (same bound as HIGH-9).
		// Previously 'null' silently hit the default below with no bounds check.
		switch val := v.(type) {
		case int64:
			if val > maxSafeInteger || val < -maxSafeInteger {
				panic(fmt.Sprintf("FromSQLiteType(null): int64 %d exceeds JS MAX_SAFE_INTEGER (±2^53-1)", val))
			}
		case uint64:
			if val > uint64(maxSafeInteger) {
				panic(fmt.Sprintf("FromSQLiteType(null): uint64 %d exceeds JS MAX_SAFE_INTEGER (2^53-1)", val))
			}
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
		{"string", int64(7), "7"},
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
	return nil
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
