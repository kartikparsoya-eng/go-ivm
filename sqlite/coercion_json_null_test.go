package sqlite

import "testing"

// TestToSQLiteType_NullJSONStoresJSONNullText pins the TS-faithful handling of
// a NULL value in a json column: TS's toSQLiteType (query-builder.ts:278-289)
// has NO null short-circuit for the json arm — `case 'json': return
// JSON.stringify(v)` — and JSON.stringify(null) === 'null', so TS stores the
// 4-character TEXT 'null', never SQL NULL. Go's old top-level `if v == nil {
// return nil }` sat ABOVE the json case and defeated the rule the json arm's
// own comment states ("ALWAYS marshal — never passthrough"), writing SQL NULL
// where TS writes 'null' TEXT. Any nullable json column rewritten through the
// push path (rowToInsertArgs / rowToPKArgs / rowToNonPKArgs ↔ TS
// #writeChange, table-source.ts:429) then diverged on disk, and every
// downstream IS NULL / ordering / cursor comparison on that column diverged
// with it.
func TestToSQLiteType_NullJSONStoresJSONNullText(t *testing.T) {
	got := ToSQLiteType(nil, "json")
	s, ok := got.(string)
	if !ok {
		t.Fatalf("ToSQLiteType(nil, json) = %#v (%T), want the TEXT \"null\" — TS JSON.stringify(null) === 'null' (query-builder.ts:287), never SQL NULL", got, got)
	}
	if s != "null" {
		t.Fatalf("ToSQLiteType(nil, json) = %q, want \"null\"", s)
	}
	// The stored TEXT must round-trip back to nil (JSON.parse('null') → null).
	if rt := FromSQLiteType(got, "json"); rt != nil {
		t.Fatalf("FromSQLiteType(%#v, json) = %#v, want nil", got, rt)
	}
}

// TestToSQLiteType_NullNonJSONStaysSQLNull pins the boundary of the fix: for
// every non-json column type TS passes null through unchanged — boolean:
// `v === null ? null : v ? 1 : 0` (query-builder.ts:281); number/string/null:
// `return v` (query-builder.ts:282-285) — so Go must keep returning SQL NULL
// there. Only the json arm marshals.
func TestToSQLiteType_NullNonJSONStaysSQLNull(t *testing.T) {
	for _, colType := range []string{"boolean", "number", "string", "null", "timestamp"} {
		if got := ToSQLiteType(nil, colType); got != nil {
			t.Fatalf("ToSQLiteType(nil, %s) = %#v, want nil (SQL NULL) — TS passes null through for non-json types", colType, got)
		}
	}
}
