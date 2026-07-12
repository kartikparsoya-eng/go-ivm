package sqlite

// FromSQLiteType json-parse-failure + int-precision tests.
//
// The json type path panics on JSON.parse failure instead of silently
// returning the raw string; the engine's recover surfaces it as an RPC
// error.
//
// Also covers the int-precision bounds check for the number type
// (int >2^53 panics) and confirms the string type converts integers to
// float64 (no precision-preserving decimal string conversion).

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
// string.
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

// TestFromSQLiteType_NumberHigh9Int64Panics verifies the int-precision
// guard: int64 values beyond ±2^53-1 panic because they cannot round-trip
// through float64 without precision loss.
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

// TestFromSQLiteType_StringMirrorsTS verifies that the 'string' type
// folds integers into float64 (like JS Number), with values beyond
// ±(2^53-1) panicking. An INTEGER stored in a string column surfaces as
// a float64, matching the reference implementation.
func TestFromSQLiteType_StringMirrorsTS(t *testing.T) {
	// In-range int64 → float64, like TS's Number(bigint).
	if got := FromSQLiteType(int64(42), "string"); got != float64(42) {
		t.Fatalf("FromSQLiteType(int64(42), string) = %#v, want float64(42) (TS Number)", got)
	}
	// float64 passthrough (TS `return v`).
	if got := FromSQLiteType(float64(1.5), "string"); got != float64(1.5) {
		t.Fatalf("FromSQLiteType(1.5, string) = %#v, want 1.5 passthrough", got)
	}
	// Beyond MAX_SAFE → panic, mirroring TS's UnsupportedValueError throw.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("FromSQLiteType(int64(1<<60), string) should panic like TS's UnsupportedValueError")
		}
	}()
	FromSQLiteType(int64(1)<<60, "string")
}

// TestFromSQLiteType_NumberStringNotParsed verifies that the 'number'
// type never parses strings — a numeric-looking string is returned
// unchanged.
func TestFromSQLiteType_NumberStringNotParsed(t *testing.T) {
	if got := FromSQLiteType("123.45", "number"); got != "123.45" {
		t.Fatalf("FromSQLiteType(%q, number) = %#v; TS returns the string unchanged", "123.45", got)
	}
	if got := FromSQLiteType([]byte("67.8"), "number"); got != "67.8" {
		t.Fatalf("FromSQLiteType([]byte %q, number) = %#v; want the TEXT as string, unparsed", "67.8", got)
	}
}

// TestFromSQLiteType_NullHigh9Panics verifies the int-precision guard
// on the null type.
func TestFromSQLiteType_NullHigh9Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("FromSQLiteType(int64(maxSafeInteger+1), null) should have panicked")
		}
	}()
	FromSQLiteType(maxSafeInteger+1, "null")
}

// TestToSQLiteType_JSONRoundTrip verifies that ToSQLiteType always
// produces valid JSON for json-typed columns. The Go side must always
// json.Marshal its output so that FromSQLiteType can re-read it without
// panicking.
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
		// JSON.stringify(null) === 'null' — the stored form is the TEXT
		// 'null' for a null json value, never SQL NULL.
		{"null", nil, `null`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ToSQLiteType(c.in, "json")
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
// encodes them as the literal "null" rather than throwing. Go must match:
// return "null" (valid JSON that round-trips to nil), not a bare string
// fallback that FromSQLiteType would panic on. Panicking here would be wrong
// too: it would be the first place Go fails where the reference succeeds.
// (Unreachable for real json-column values, which come from json.Unmarshal
// and never hold non-finite floats, but kept symmetric.)
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
// the SQLite write↔read boundary: for every json value shape, the value must
// survive ToSQLiteType (write) → FromSQLiteType (read) byte-for-byte unchanged.
// The stored form must always be JSON text (a Go string) — never a bare
// passthrough value.
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
	// nil does NOT short-circuit: the stored form is the TEXT 'null', which
	// round-trips back to nil via JSON.parse.
	if got := ToSQLiteType(nil, "json"); got != "null" {
		t.Fatalf("ToSQLiteType(nil, json) = %#v, want the TEXT \"null\"", got)
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

// TestFromSQLiteType_NullTypeConvertsIntsToFloat64 verifies that the
// 'null' type folds integers into float64, matching the reference
// implementation's fromSQLiteType behavior. An INTEGER-stored value in a
// 'null'-typed column must come back as float64, not the raw int64.
func TestFromSQLiteType_NullTypeConvertsIntsToFloat64(t *testing.T) {
	if got := FromSQLiteType(int64(42), "null"); got != float64(42) {
		t.Fatalf("FromSQLiteType(int64(42), null) = %T(%v), want float64(42)", got, got)
	}
	if got := FromSQLiteType(uint64(7), "null"); got != float64(7) {
		t.Fatalf("FromSQLiteType(uint64(7), null) = %T(%v), want float64(7)", got, got)
	}
	// Non-integer values still pass through unchanged (TS `return v`).
	if got := FromSQLiteType("s", "null"); got != "s" {
		t.Fatalf("string passthrough broken: %v", got)
	}
	if got := FromSQLiteType(float64(1.5), "null"); got != float64(1.5) {
		t.Fatalf("float passthrough broken: %v", got)
	}
	// Bounds check preserved: beyond ±2^53 still panics DataError.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected DataError panic for out-of-range int64 in null-typed column")
			}
		}()
		FromSQLiteType(int64(maxSafeInteger+1), "null")
	}()
}
