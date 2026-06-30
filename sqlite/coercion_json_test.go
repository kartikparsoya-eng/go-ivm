package sqlite

// D3: FromSQLiteType json-parse-failure + int-precision tests.
//
// The json type path previously returned the raw string on JSON.parse
// failure (silent passthrough), while TS throws UnsupportedValueError
// (table-source.ts:637-640). This divergence meant Go would ship a
// string to the client where TS ships an error — desyncing the
// init-vs-advance value shape. The fix panics on parse failure to
// match TS; the engine's recover surfaces it as an RPC error.
//
// Also covers the HIGH-9 bounds check for the number type (int >2^53
// panics) and confirms the string type preserves full int64 precision
// via decimal string conversion (no float64 coercion).

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

// TestFromSQLiteType_JSONValidParse verifies that valid JSON strings
// are correctly parsed into Go maps/slices/scalars.
func TestFromSQLiteType_JSONValidParse(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want interface{}
	}{
		{"object", `{"a":1,"b":"x"}`, map[string]interface{}{"a": float64(1), "b": "x"}},
		{"array", `[1,2,3]`, []interface{}{float64(1), float64(2), float64(3)}},
		{"nested", `{"inner":{"val":42}}`, map[string]interface{}{"inner": map[string]interface{}{"val": float64(42)}}},
		{"string in json", `"hello"`, "hello"},
		{"number in json", `42`, float64(42)},
		{"bool in json", `true`, true},
		{"null in json", `null`, nil},
		{"empty object", `{}`, map[string]interface{}{}},
		{"empty array", `[]`, []interface{}{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := FromSQLiteType(c.in, "json")
			if !deepEqual(got, c.want) {
				t.Fatalf("FromSQLiteType(%v, json) = %#v, want %#v", c.in, got, c.want)
			}
		})
	}
}

// TestFromSQLiteType_JSONInvalidPanics verifies that invalid JSON in
// a json-typed column panics instead of silently returning the raw
// string. TS throws UnsupportedValueError (table-source.ts:637-640);
// Go must match by panicking (the engine's recover surfaces it).
func TestFromSQLiteType_JSONInvalidPanics(t *testing.T) {
	invalidInputs := []struct {
		name string
		in   interface{}
	}{
		{"truncated object", `{"a":`},
		{"truncated array", `[1,2`},
		{"bareword", `hello`},
		{"trailing comma", `{"a":1,}`},
		{"single quote", `{'a':1}`},
	}
	for _, c := range invalidInputs {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("FromSQLiteType(%q, json) should have panicked, got success", c.in)
				}
			}()
			FromSQLiteType(c.in, "json")
		})
	}
}

// TestFromSQLiteType_JSONInvalidBytesPanics verifies the same panic
// behavior for []byte input (SQLite may return blobs for text columns).
func TestFromSQLiteType_JSONInvalidBytesPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("FromSQLiteType([]byte, json) should have panicked on invalid JSON")
		}
	}()
	FromSQLiteType([]byte(`{"broken`), "json")
}

// TestFromSQLiteType_NumberHigh9Int64Panics verifies the HIGH-9 guard:
// int64 values beyond ±2^53-1 panic because they cannot round-trip
// through float64 without precision loss. TS throws
// UnsupportedValueError (table-source.ts:623-627); Go panics to match.
func TestFromSQLiteType_NumberHigh9Int64Panics(t *testing.T) {
	cases := []struct {
		name string
		val  int64
	}{
		{"positive overflow", maxSafeInteger + 1},
		{"negative overflow", -maxSafeInteger - 1},
		{"very large", 1 << 62},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("FromSQLiteType(int64(%d), number) should have panicked", c.val)
				}
			}()
			FromSQLiteType(c.val, "number")
		})
	}
}

// TestFromSQLiteType_NumberHigh9Uint64Panics verifies the same guard
// for uint64.
func TestFromSQLiteType_NumberHigh9Uint64Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("FromSQLiteType(uint64(%d), number) should have panicked", uint64(maxSafeInteger)+1)
		}
	}()
	FromSQLiteType(uint64(maxSafeInteger)+1, "number")
}

// TestFromSQLiteType_NumberHigh9AtBoundarySucceeds verifies that values
// AT the boundary (exactly ±2^53-1) do NOT panic — they are the last
// values that float64 can represent exactly.
func TestFromSQLiteType_NumberHigh9AtBoundarySucceeds(t *testing.T) {
	cases := []struct {
		name string
		val  interface{}
		want float64
	}{
		{"max safe int", maxSafeInteger, float64(maxSafeInteger)},
		{"min safe int", -maxSafeInteger, float64(-maxSafeInteger)},
		{"zero", int64(0), 0},
		{"max safe uint", uint64(maxSafeInteger), float64(maxSafeInteger)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := FromSQLiteType(c.val, "number")
			if got != c.want {
				t.Fatalf("FromSQLiteType(%v, number) = %#v, want %#v", c.val, got, c.want)
			}
		})
	}
}

// TestFromSQLiteType_StringPreservesLargeInt verifies that the string
// type path preserves full int64 precision — converting to a decimal
// string via strconv.FormatInt, NOT through float64. A string column
// holding a large int must not lose precision.
func TestFromSQLiteType_StringPreservesLargeInt(t *testing.T) {
	large := int64(1) << 60 // 1152921504606846976 — far beyond 2^53
	got := FromSQLiteType(large, "string")
	want := "1152921504606846976"
	if got != want {
		t.Fatalf("FromSQLiteType(int64(%d), string) = %#v, want %#v", large, got, want)
	}
}

// TestFromSQLiteType_NullHigh9Panics verifies the HIGH-9 guard on the
// null type (which TS folds with number/string per table-source.ts:619).
func TestFromSQLiteType_NullHigh9Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("FromSQLiteType(int64(maxSafeInteger+1), null) should have panicked")
		}
	}()
	FromSQLiteType(maxSafeInteger+1, "null")
}

// TestToSQLiteType_JSONRoundTrip verifies that ToSQLiteType always
// produces valid JSON for json-typed columns, matching TS's
// JSON.stringify behavior (query-builder.ts:192). The Go side
// previously had a string passthrough (query_builder.go:383-384)
// that returned Go strings as-is without JSON-quoting, causing
// FromSQLiteType to panic on the next read:
//
//	SQLite: "Payment Failures" (valid JSON string)
//	FromSQLiteType → "Payment Failures" (Go string, quotes stripped)
//	ToSQLiteType (bug) → "Payment Failures" (no quotes — NOT valid JSON)
//	FromSQLiteType → panic: invalid character 'P'
//
// After the fix, ToSQLiteType always json.Marshal's, producing
// "\"Payment Failures\"" which round-trips correctly.
func TestToSQLiteType_JSONRoundTrip(t *testing.T) {
	panicValues := []string{
		"Payment Failures",
		"Others",
		"PROD",
		"Details about txn/refund",
		"NA",
		"done",
		"Yes",
		"GENERATED",
		"REVIEWED",
		"Dashboard",
		"Call with gateway",
		"hello",
		"",
	}
	for _, val := range panicValues {
		t.Run(val, func(t *testing.T) {
			// Simulate what the SQLite replica stores: a JSON-encoded string
			jsonInSQLite, _ := json.Marshal(val) // e.g. "\"Payment Failures\""

			// Step 1: FromSQLiteType reads valid JSON → returns Go string
			goVal := FromSQLiteType(string(jsonInSQLite), "json")
			goStr, ok := goVal.(string)
			if !ok {
				t.Fatalf("FromSQLiteType(%q, json) = %T, want string", jsonInSQLite, goVal)
			}

			// Step 2: ToSQLiteType must produce valid JSON that can round-trip
			sqliteOut := ToSQLiteType(goStr, "json")
			sqliteStr, ok := sqliteOut.(string)
			if !ok {
				t.Fatalf("ToSQLiteType(%q, json) = %T, want string", goStr, sqliteOut)
			}

			// Step 3: FromSQLiteType must be able to re-read the output
			// This would panic before the fix if sqliteStr was not valid JSON
			roundTripped := FromSQLiteType(sqliteStr, "json")
			if roundTripped != goStr {
				t.Fatalf("round-trip failed: FromSQLiteType(ToSQLiteType(%q)) = %#v, want %q",
					goStr, roundTripped, goStr)
			}
		})
	}
}

// TestToSQLiteType_JSONNonStringValues verifies that non-string JSON
// values (objects, arrays, numbers, booleans) are correctly marshalled.
func TestToSQLiteType_JSONNonStringValues(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want string
	}{
		{"object", map[string]interface{}{"a": float64(1)}, `{"a":1}`},
		{"array", []interface{}{float64(1), float64(2)}, `[1,2]`},
		{"number", float64(42), `42`},
		{"bool", true, `true`},
		{"null", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ToSQLiteType(c.in, "json")
			if c.in == nil {
				if got != nil {
					t.Fatalf("ToSQLiteType(nil, json) = %#v, want nil", got)
				}
				return
			}
			gotStr, ok := got.(string)
			if !ok {
				t.Fatalf("ToSQLiteType(%v, json) = %T, want string", c.in, got)
			}
			if gotStr != c.want {
				t.Fatalf("ToSQLiteType(%v, json) = %q, want %q", c.in, gotStr, c.want)
			}
		})
	}
}

// TestToSQLiteType_JSONMarshalFailureMatchesTS verifies the json-column
// marshal-failure path. json.Marshal rejects NaN/±Inf, but JS JSON.stringify
// (query-builder.ts:192) encodes them as the literal "null" rather than
// throwing. Go must match: return "null" (valid JSON that round-trips to nil),
// NOT the old fmt.Sprintf("%v", v) fallback which wrote bare "NaN"/"+Inf" that
// FromSQLiteType then panicked on — the same corruption class as the original
// string-passthrough bug. Panicking here would be wrong too: it would be the
// first place Go fails where TS succeeds, violating the Go-fails-iff-TS-fails
// invariant. (Unreachable for real json-column values, which come from
// json.Unmarshal and never hold non-finite floats, but kept symmetric.)
func TestToSQLiteType_JSONMarshalFailureMatchesTS(t *testing.T) {
	for _, v := range []interface{}{math.NaN(), math.Inf(1), math.Inf(-1)} {
		t.Run(fmt.Sprintf("%v", v), func(t *testing.T) {
			out := ToSQLiteType(v, "json")
			if out != "null" {
				t.Fatalf("ToSQLiteType(%v, json) = %#v, want \"null\" (matching JSON.stringify(NaN/Inf))", v, out)
			}
			// Must re-read without panicking (the old "NaN" fallback panicked here).
			if got := FromSQLiteType(out, "json"); got != nil {
				t.Fatalf("FromSQLiteType(%#v, json) = %#v, want nil", out, got)
			}
		})
	}
}

// TestJSONWriteReadRoundTrip_AllShapes is the cross-boundary fidelity guard for
// the SQLite write↔read boundary: for EVERY json value shape, the value must
// survive ToSQLiteType (write) → FromSQLiteType (read) byte-for-byte unchanged.
// The original string-passthrough bug was ONE shape (a scalar string) silently
// diverging; this asserts the whole shape space round-trips, so a future change
// to either half that breaks any shape fails loudly here. Mirrors TS, where
// toSQLiteType (JSON.stringify, query-builder.ts:192) and fromSQLiteType
// (JSON.parse, table-source.ts:633) are exact inverses. The stored form must
// ALWAYS be JSON text (a Go string) — never a bare passthrough value, which is
// exactly the invariant the bug violated.
func TestJSONWriteReadRoundTrip_AllShapes(t *testing.T) {
	shapes := []struct {
		name string
		v    interface{}
	}{
		{"scalar string (the bug class)", "Payment Failures"},
		{"scalar string with embedded quotes", `he said "hi"`},
		{"empty string", ""},
		{"scalar number", float64(42)},
		{"scalar bool true", true},
		{"scalar bool false", false},
		{"object", map[string]interface{}{"a": float64(1), "b": "x"}},
		{"array", []interface{}{float64(1), "two", true}},
		{"nested", map[string]interface{}{"o": map[string]interface{}{"k": []interface{}{float64(1), float64(2)}}}},
		{"empty object", map[string]interface{}{}},
		{"empty array", []interface{}{}},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			stored := ToSQLiteType(s.v, "json")
			if _, ok := stored.(string); !ok {
				t.Fatalf("ToSQLiteType(%#v, json) = %T, want string (JSON text — never a bare passthrough)", s.v, stored)
			}
			got := FromSQLiteType(stored, "json")
			if !deepEqual(got, s.v) {
				t.Fatalf("round-trip mismatch for %s: FromSQLiteType(ToSQLiteType(%#v)) = %#v", s.name, s.v, got)
			}
		})
	}
	// nil short-circuits in ToSQLiteType (stored as SQL NULL), so it never
	// reaches the json marshal path; assert that explicitly.
	if got := ToSQLiteType(nil, "json"); got != nil {
		t.Fatalf("ToSQLiteType(nil, json) = %#v, want nil", got)
	}
}

// deepEqual is a minimal deep-equality check for interface{} values
// covering maps, slices, and scalars. We avoid reflect.DeepEqual because
// msgpack-decoded maps have type map[string]interface{} which
// reflect.DeepEqual handles, but we want a simple explicit check.
func deepEqual(a, b interface{}) bool {
	switch av := a.(type) {
	case map[string]interface{}:
		bv, ok := b.(map[string]interface{})
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !deepEqual(v, bv[k]) {
				return false
			}
		}
		return true
	case []interface{}:
		bv, ok := b.([]interface{})
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
