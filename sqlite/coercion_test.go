package sqlite

import (
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

// TestFromSQLiteTypeTimeValue guards the mattn time.Time gotcha: mattn/go-sqlite3
// auto-converts columns whose declared SQLite type is exactly
// "timestamp"/"datetime"/"date" into time.Time. Zero's NULLABLE temporal columns
// arrive bare (e.g. "timestamp") so they hit that path, while NOT-null ones carry
// a "|NOT_NULL" suffix that dodges mattn's exact match and stay int64. Before the
// fix, an unconverted time.Time skipped numeric coercion and msgpack-encoded as
// `{}` — shipping clients an empty object for nullable timestamps
// (channel_user_status.conversationSeenCutoffAt + updatedAt, found via the shadow
// SQL oracle 2026-06-08). FromSQLiteType must normalize time.Time back to the
// epoch-millisecond number TS uses, identically to a NOT-null temporal column.
func TestFromSQLiteTypeTimeValue(t *testing.T) {
	// mattn builds the time.Time from an epoch-ms integer (>1e12 ⇒ ms heuristic)
	// via time.Unix(0, ms*1e6).UTC(); time.UnixMilli(ms).UTC() reproduces that.
	const epochMs int64 = 1779813865070 // a real channel_user_status value
	mattnTime := time.UnixMilli(epochMs).UTC()

	// Zero's clientType for a timestamp column is "number", so the converted
	// time.Time must land on float64(epochMs) — the same value (and Go type) a
	// NOT-null temporal column produces from its raw int64.
	got := FromSQLiteType(mattnTime, "number")
	want := float64(epochMs)
	if got != want {
		t.Fatalf("FromSQLiteType(time.Time(%d ms), number) = %#v (%T), want %#v (float64)",
			epochMs, got, got, want)
	}

	// The nullable (time.Time) and NOT-null (raw int64) paths must produce the
	// IDENTICAL value so the two render the same on the wire (init/advance parity);
	// before the fix the time.Time path diverged (msgpack `{}`).
	if a, b := FromSQLiteType(epochMs, "number"), FromSQLiteType(mattnTime, "number"); a != b {
		t.Fatalf("int64 path %#v != time.Time path %#v — nullable and NOT-null temporal columns diverge", a, b)
	}

	// nil (a NULL nullable timestamp) must still pass through as nil, not 0.
	if got := FromSQLiteType(nil, "number"); got != nil {
		t.Fatalf("FromSQLiteType(nil, number) = %#v, want nil", got)
	}
}
