package sqlite

// Builds SELECT SQL queries with constraints, filters, ordering, and start cursors.

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// maxSafeInteger is JS Number.MAX_SAFE_INTEGER (2^53 - 1). int64/uint64 values
// beyond ±this cannot round-trip through float64 without precision loss, which
// is how TS's number model (all numbers are float64) bounds integers (HIGH-9).
const maxSafeInteger = int64(1)<<53 - 1

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

// simpleConditionToSQL handles a simple condition.
func simpleConditionToSQL(cond *Condition) (string, []interface{}) {
	op := cond.Op
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
		var andParts []string
		iField := order[i][0]
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

	// Inclusive (basis == "at"): add equality for all order fields
	if start.Basis == "at" {
		var andParts []string
		for _, o := range order {
			field := o[0]
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
			return float64(val)
		case uint64:
			if val > uint64(maxSafeInteger) {
				panic(fmt.Sprintf("FromSQLiteType(number): uint64 %d exceeds JS MAX_SAFE_INTEGER (2^53-1); float64 coercion loses precision", val))
			}
			return float64(val)
		case float64:
			return val
		case string:
			// modernc.org/sqlite may return REAL values as strings
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return v
			}
			return f
		case []byte:
			f, err := strconv.ParseFloat(string(val), 64)
			if err != nil {
				return v
			}
			return f
		case bool:
			if val {
				return float64(1)
			}
			return float64(0)
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
