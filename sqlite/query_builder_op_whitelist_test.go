package sqlite

import (
	"strings"
	"testing"
)

// S4 parity tests for the simpleConditionToSQL Op whitelist. cond.Op arrives
// from a CLIENT-SENT AST and was previously interpolated raw into SQL. These
// pin three things the fix must guarantee:
//  1. Every whitelisted op still generates the SAME SQL it did before the
//     whitelist (no generated-SQL regression — especially the ILIKE→LIKE and
//     IN→json_each special-cases).
//  2. A malicious op is neutralized to "1=0" with no interpolation of the
//     payload (no injection).
//  3. The neutralization never panics on a client-input path.

// colLit builds a simple "col <op> literal" condition.
func colLit(col, op string, lit interface{}) *Condition {
	return &Condition{
		Type: "simple",
		Op:   op,
		Left: ValuePos{Type: "column", Name: col},
		Right: ValuePos{
			Type:    "literal",
			Value:   lit,
			ColType: "text",
		},
	}
}

func TestSimpleCondition_AllWhitelistedComparisonOps(t *testing.T) {
	// Every comparison op from the Condition struct comment must round-trip to
	// "col <op> ?" with the literal as a param. This is the pre-whitelist
	// behavior; the whitelist must not change it.
	cases := []struct {
		op   string
		want string
	}{
		{"=", `"col" = ?`},
		{"!=", `"col" != ?`},
		{">", `"col" > ?`},
		{"<", `"col" < ?`},
		{">=", `"col" >= ?`},
		{"<=", `"col" <= ?`},
		{"LIKE", `"col" LIKE ?`},
		{"NOT LIKE", `"col" NOT LIKE ?`},
		{"IS", `"col" IS ?`},
		{"IS NOT", `"col" IS NOT ?`},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			got, params := simpleConditionToSQL(colLit("col", c.op, "x"))
			if got != c.want {
				t.Fatalf("op=%q: got %q, want %q", c.op, got, c.want)
			}
			if len(params) != 1 || params[0] != "x" {
				t.Fatalf("op=%q: params=%v, want [x]", c.op, params)
			}
		})
	}
}

func TestSimpleCondition_ILIKEMapsToLIKE(t *testing.T) {
	// SQLite LIKE is case-insensitive, so ILIKE → LIKE. The whitelist must NOT
	// break this special-case (it runs after the allowedOps check).
	got, params := simpleConditionToSQL(colLit("col", "ILIKE", "x"))
	if got != `"col" LIKE ?` {
		t.Fatalf("ILIKE: got %q, want %q", got, `"col" LIKE ?`)
	}
	if len(params) != 1 || params[0] != "x" {
		t.Fatalf("ILIKE: params=%v, want [x]", params)
	}
	// NOT ILIKE → NOT LIKE
	got, _ = simpleConditionToSQL(colLit("col", "NOT ILIKE", "x"))
	if got != `"col" NOT LIKE ?` {
		t.Fatalf("NOT ILIKE: got %q, want %q", got, `"col" NOT LIKE ?`)
	}
}

func TestSimpleCondition_INJsonEach(t *testing.T) {
	// IN must emit the json_each() subquery form with the RHS marshalled as a
	// JSON array param. The whitelist must not perturb this special-case.
	cond := &Condition{
		Type: "simple",
		Op:   "IN",
		Left: ValuePos{Type: "column", Name: "col"},
		Right: ValuePos{
			Type:  "literal",
			Value: []interface{}{"a", "b"},
		},
	}
	got, params := simpleConditionToSQL(cond)
	// The RHS becomes a json_each subquery; left is a column, so no left param.
	if got != `"col" IN (SELECT value FROM json_each(?))` {
		t.Fatalf("IN: got %q, want json_each form", got)
	}
	if len(params) != 1 {
		t.Fatalf("IN: params=%v, want 1 (the json array)", params)
	}
	// NOT IN mirrors it.
	cond.Op = "NOT IN"
	got, _ = simpleConditionToSQL(cond)
	if got != `"col" NOT IN (SELECT value FROM json_each(?))` {
		t.Fatalf("NOT IN: got %q, want json_each form", got)
	}
}

func TestSimpleCondition_UnknownOpNeutralizedNoInjection(t *testing.T) {
	// The injection case: a client-sent op carrying SQL. It must be neutralized
	// to "1=0" with NO interpolation of the payload — the payload string must
	// not appear anywhere in the output SQL.
	payloads := []string{
		"; DROP TABLE x; --",
		")) OR 1=1 --",
		"UNION SELECT password FROM users",
		"",
		"   ",
		"bogus",
	}
	for _, p := range payloads {
		t.Run(p, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("op=%q: panicked (must not on client-input path): %v", p, r)
				}
			}()
			got, params := simpleConditionToSQL(colLit("col", p, "x"))
			if got != "1=0" {
				t.Fatalf("op=%q: got %q, want 1=0", p, got)
			}
			if len(params) != 0 {
				t.Fatalf("op=%q: params=%v, want nil (no-match carries no params)", p, params)
			}
			// The payload must not leak into the SQL.
			if strings.Contains(got, p) && p != "" {
				t.Fatalf("op=%q: payload leaked into output %q", p, got)
			}
		})
	}
}
