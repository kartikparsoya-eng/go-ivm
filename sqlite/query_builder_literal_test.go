package sqlite

// M3 (napi hostile review): filter LITERALS must be typed by the LITERAL's
// own JS type — mirroring TS zqlite/query-builder.ts valuePositionToSQL →
// toSQLiteType(v, getJsType(v)) — never by the column's schema type. The
// old ColType plumbing (stamped from the column side in tablesource's
// convertSimpleCondition) marshaled a string literal on a json column into
// its JSON encoding ('"x"') and coerced literals on boolean columns, both
// binding different values than TS binds for the identical AST.

import (
	"testing"
)

func litCond(op string, col string, lit interface{}) *Condition {
	return &Condition{
		Type:  "simple",
		Op:    op,
		Left:  ValuePos{Type: "column", Name: col},
		Right: ValuePos{Type: "literal", Value: lit},
	}
}

func TestFilterLiteralsBindByLiteralType(t *testing.T) {
	cases := []struct {
		name string
		lit  interface{}
		want interface{}
	}{
		// getJsType('x') = 'string' → toSQLiteType passes through. Pre-fix,
		// ColType="json" marshaled this to the JSON text "\"x\"".
		{"string literal stays bare string", "Payment Failures", "Payment Failures"},
		// getJsType(5) = 'number' → passthrough. Pre-fix on a json column it
		// became the text "5".
		{"number literal stays number", float64(5), float64(5)},
		// getJsType(true) = 'boolean' → 1 (same result under either typing —
		// pinned so the fix can't over-rotate).
		{"bool literal binds 1", true, 1},
		// getJsType(null) = 'null' → passthrough nil.
		{"nil literal binds NULL", nil, nil},
		// Non-scalar literal → 'json' → JSON.stringify, like TS.
		{"array literal binds JSON text", []interface{}{"a", "b"}, `["a","b"]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, params := filtersToSQL(litCond("=", "j", c.lit))
			if len(params) != 1 {
				t.Fatalf("params = %v, want exactly 1", params)
			}
			if got := params[0]; got != c.want {
				// interface compare is fine for these scalar/string wants.
				t.Fatalf("literal %v (%T) bound as %v (%T); TS binds %v (%T)",
					c.lit, c.lit, got, got, c.want, c.want)
			}
		})
	}
}

// jsValueType is the getJsType mirror — pin its mapping directly.
func TestJSValueType(t *testing.T) {
	for _, c := range []struct {
		v    interface{}
		want string
	}{
		{nil, "null"},
		{"s", "string"},
		{float64(1), "number"},
		{int64(1), "number"},
		{true, "boolean"},
		{map[string]interface{}{"a": 1}, "json"},
		{[]interface{}{1}, "json"},
	} {
		if got := jsValueType(c.v); got != c.want {
			t.Errorf("jsValueType(%T) = %q, want %q", c.v, got, c.want)
		}
	}
}
