package sqlite

import (
	"strings"
	"testing"
	"time"
)

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

// TestFromSQLiteTypeTimeValue pins the time.Time invariant. History, in
// order:
//
//  1. mattn/go-sqlite3 converts INTEGER result columns declared exactly
//     "timestamp"/"datetime"/"date" (zero's NULLABLE temporal columns;
//     NOT-null ones carry "|NOT_NULL" and dodge the exact match) into
//     time.Time. Unhandled, that msgpack-encoded as `{}` (shadow SQL
//     oracle, 2026-06-08) — so FromSQLiteType normalized with UnixMilli().
//  2. But mattn's heuristic reads |v| <= 1e12 as SECONDS, so the UnixMilli
//     reversal multiplied every pre-2001 epoch-ms value by 1000 (ART G15,
//     2026-07-07). No reversal is correct — stored 2e9 and 2e12 yield the
//     identical time.Time.
//  3. Now the conversion is killed at the SQL level: every row SELECT wraps
//     its result columns in the unary-+ no-op (`+"col" AS "col"`), which
//     strips the declared type, so the raw integer ships exactly as TS's
//     better-sqlite3 ships it. A time.Time reaching FromSQLiteType is
//     therefore a PLUMBING BUG (an unwrapped SELECT site) and must panic
//     loudly instead of silently shipping a maybe-×1000 value.
func TestFromSQLiteTypeTimeValue(t *testing.T) {
	mattnTime := time.UnixMilli(1779813865070).UTC()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("FromSQLiteType(time.Time) did not panic — the silent-normalization path is back; a pre-2001 value would ship ×1000")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "SELECT site") {
			t.Fatalf("panic = %v, want the unwrapped-SELECT-site diagnostic", r)
		}
	}()
	FromSQLiteType(mattnTime, "number")
}

func TestFromSQLiteTypeNilNumber(t *testing.T) {
	if got := FromSQLiteType(nil, "number"); got != nil {
		t.Fatalf("FromSQLiteType(nil, number) = %#v, want nil", got)
	}
}
