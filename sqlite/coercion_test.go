package sqlite

import "testing"

// TestFromSQLiteTypeBooleanString covers types MED-1: TS coerces booleans with
// `!!v` (table-source.ts), pure JS truthiness of the RAW value, so ANY
// non-empty string is true — including "0", "0.0" and "false". The old
// literal-list + ParseFloat check gave the opposite for those. Go must agree
// to keep init-vs-advance shape parity (CRIT-6).
func TestFromSQLiteTypeBooleanString(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want interface{}
	}{
		// JS `!!v` truthiness for strings: empty → false, everything else → true.
		{"empty string false", "", false},
		{"zero string true (JS !!\"0\")", "0", true},
		{"zero-float string true", "0.0", true},
		{"false string true (JS !!\"false\")", "false", true},
		{"true string true", "true", true},
		{"space string true", " ", true},
		{"one string true", "1", true},
		// Numeric shapes still compare against zero.
		{"int 0 false", int64(0), false},
		{"int 1 true", int64(1), true},
		{"float 0 false", float64(0), false},
		{"float nonzero true", float64(2.5), true},
		// Native bool passes through.
		{"bool true", true, true},
		{"bool false", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := FromSQLiteType(c.in, "boolean")
			if got != c.want {
				t.Fatalf("FromSQLiteType(%#v, boolean) = %#v, want %#v", c.in, got, c.want)
			}
		})
	}
}

// TestSelfCheckCoercionStillPasses guards CRIT-6: the boolean coercion change
// (MED-1) must not break the init-vs-advance shape-convergence self-check.
func TestSelfCheckCoercionStillPasses(t *testing.T) {
	if err := SelfCheckCoercion(); err != nil {
		t.Fatalf("SelfCheckCoercion failed after MED-1 boolean change: %v", err)
	}
}
