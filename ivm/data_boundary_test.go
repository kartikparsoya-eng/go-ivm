package ivm

import (
	"math"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// Boundary type-conversion correctness: the TS↔Go seams are (1) msgpack wire
// decode (Row.DecodeMsgpack / normalizeDecodedValue) and (2) the comparator
// contract ported from zql data.ts (compareValues / valuesEqual). These pin
// the full matrix, including the previously-crashing non-scalar cases: a
// JSON column (map/array) or blob reaching a comparator used to panic with
// an opaque `runtime error: comparing uncomparable type` (a -32000 reset
// loop until the breaker trips) instead of TS's clean `Unsupported type`
// throw. Now: CompareValues → *DataError (-32102 teardown, deterministic
// data problem), ValuesEqual → false (TS reference-equality semantics:
// independently decoded objects are never ===).

func mustRecoverDataError(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic, got none")
		}
		if _, ok := r.(*DataError); !ok {
			t.Fatalf("panic must be *DataError (→ -32102 teardown), got %T: %v", r, r)
		}
	}()
	fn()
}

func TestCompareValues_NonScalar_DataErrorNotRuntimePanic(t *testing.T) {
	jsonA := map[string]interface{}{"x": 1.0}
	jsonB := map[string]interface{}{"x": 2.0}
	arrA := []interface{}{1.0}
	arrB := []interface{}{2.0}
	blobA := []byte{1}
	blobB := []byte{2}

	// Same non-comparable dynamic type on both sides — the exact shape that
	// used to trip interface == with a raw runtime panic.
	mustRecoverDataError(t, func() { CompareValues(jsonA, jsonB) })
	mustRecoverDataError(t, func() { CompareValues(arrA, arrB) })
	mustRecoverDataError(t, func() { CompareValues(blobA, blobB) })
	// Self-comparison (identical reference) is equally unsupported in TS.
	mustRecoverDataError(t, func() { CompareValues(jsonA, jsonA) })
	// Non-scalar vs scalar = cross-type → same DataError policy.
	mustRecoverDataError(t, func() { CompareValues(jsonA, "s") })
	mustRecoverDataError(t, func() { CompareValues(1.0, arrA) })
	// Scalar cross-type stays DataError too.
	mustRecoverDataError(t, func() { CompareValues("s", 1.0) })
	mustRecoverDataError(t, func() { CompareValues(true, "s") })
}

func TestCompareValues_NilOrdersBeforeEverything_IncludingNonScalar(t *testing.T) {
	// TS runs its null checks BEFORE the unsupported-type throw (data.ts:62-75)
	// so nil-vs-object is ORDERED, not an error.
	cases := []Value{1.0, "s", true, map[string]interface{}{}, []interface{}{}, []byte{1}}
	for _, v := range cases {
		if got := CompareValues(nil, v); got != -1 {
			t.Errorf("CompareValues(nil, %T) = %d, want -1", v, got)
		}
		if got := CompareValues(v, nil); got != 1 {
			t.Errorf("CompareValues(%T, nil) = %d, want 1", v, got)
		}
	}
	if got := CompareValues(nil, nil); got != 0 {
		t.Errorf("CompareValues(nil, nil) = %d, want 0", got)
	}
}

func TestCompareValues_NumericWidthMatrix(t *testing.T) {
	// Every width msgpack DecodeInterface can hand us must live in ONE
	// numeric space (JS has a single Number type). A missing toFloat64 case
	// makes an equal pair look cross-type → spurious DataError panic.
	widths := []Value{
		int8(5), int16(5), int32(5), int64(5), int(5),
		uint8(5), uint16(5), uint32(5), uint64(5), uint(5),
		float32(5), float64(5),
	}
	for _, a := range widths {
		for _, b := range widths {
			if got := CompareValues(a, b); got != 0 {
				t.Errorf("CompareValues(%T(5), %T(5)) = %d, want 0", a, b, got)
			}
			if !ValuesEqual(a, b) {
				t.Errorf("ValuesEqual(%T(5), %T(5)) = false, want true", a, b)
			}
		}
	}
	if got := CompareValues(int16(4), float64(5)); got != -1 {
		t.Errorf("CompareValues(int16(4), float64(5)) = %d, want -1", got)
	}
	// 2^53 boundary: values are exactly representable up to MAX_SAFE_INTEGER;
	// the sidecar's ingestion guard (FromSQLiteType high-9 panic) keeps
	// anything beyond it out, so equality at the boundary must hold.
	maxSafe := int64(1) << 53
	if !ValuesEqual(maxSafe, float64(maxSafe)) {
		t.Error("2^53 int64 must equal its float64 form")
	}
}

func TestCompareValues_StringOrderIsUTF8_MatchingTSAndSQLite(t *testing.T) {
	// TS compares strings with compareUTF8 ON PURPOSE (zql data.ts:40-49) to
	// match SQLite's BINARY collation; Go's byte-wise strings.Compare is the
	// same order. The discriminating pair: U+E000 (BMP, 3-byte UTF-8) vs
	// U+10000 (astral, 4-byte UTF-8). UTF-8/code-point order puts U+E000
	// FIRST; naive JS UTF-16 code-unit order would put the astral char's
	// D800 surrogate first. If someone "optimizes" this to a UTF-16-style
	// comparison, Take bounds and cursors stop agreeing with SQLite ORDER BY.
	bmp := ""
	astral := string(rune(0x10000))
	if got := CompareValues(bmp, astral); got != -1 {
		t.Fatalf("CompareValues(U+E000, U+10000) = %d, want -1 (UTF-8/code-point order)", got)
	}
	if got := CompareValues("a", "b"); got >= 0 {
		t.Fatalf(`CompareValues("a","b") = %d, want <0`, got)
	}
	if got := CompareValues("héllo", "héllo"); got != 0 {
		t.Fatalf("identical unicode strings must compare 0, got %d", got)
	}
}

func TestValuesEqual_TSReferenceSemantics(t *testing.T) {
	// nil never equals anything, including nil (join semantics — a NULL FK
	// must not match a NULL FK; data.ts:112-118).
	if ValuesEqual(nil, nil) {
		t.Error("nil must not equal nil")
	}
	if ValuesEqual(nil, 1.0) || ValuesEqual("x", nil) {
		t.Error("nil must not equal any value")
	}
	// Non-scalars: TS === is reference equality; independently decoded
	// objects are never identical references → unequal, never a crash.
	// (Used by editChangesSplitKeys — a JSON column in a query's sort key
	// hits this on every Edit of that column.)
	m1 := map[string]interface{}{"a": 1.0}
	m2 := map[string]interface{}{"a": 1.0}
	if ValuesEqual(m1, m2) {
		t.Error("distinct JSON objects must be unequal (TS === semantics)")
	}
	if ValuesEqual([]interface{}{1.0}, []interface{}{1.0}) {
		t.Error("distinct JSON arrays must be unequal")
	}
	if ValuesEqual([]byte{1}, []byte{1}) {
		t.Error("distinct blobs must be unequal")
	}
	// Scalars unchanged.
	if !ValuesEqual("x", "x") || !ValuesEqual(true, true) || !ValuesEqual(3.5, 3.5) {
		t.Error("scalar equality broken")
	}
	if ValuesEqual("x", "y") || ValuesEqual(true, false) || ValuesEqual("1", 1.0) {
		t.Error("scalar inequality broken")
	}
}

// --- msgpack wire decode (Row.DecodeMsgpack) ---

// TS encodes rows with @msgpack/msgpack: JS numbers become the narrowest
// integer encoding (small ints → fixint/int8/int16...) or float64. Go must
// land EVERY numeric at float64 — at every nesting depth (JSON columns carry
// objects/arrays) — or comparators see cross-type pairs.
func TestRowDecodeMsgpack_NumericNormalization_AllDepths(t *testing.T) {
	wire := map[string]interface{}{
		"tiny":     int8(3),
		"small":    int16(300),
		"med":      int32(70000),
		"big":      int64(5_000_000_000),
		"utiny":    uint8(200),
		"negative": int8(-5),
		"f32":      float32(1.5),
		"f64":      float64(2.5),
		"str":      "s",
		"boolean":  true,
		"nothing":  nil,
		"json_obj": map[string]interface{}{
			"n":      int16(7),
			"nested": map[string]interface{}{"deep": int8(1)},
			"arr":    []interface{}{int8(1), "x", nil, map[string]interface{}{"m": int64(9)}},
		},
		"json_arr": []interface{}{int32(4), float32(0.5)},
	}
	data, err := msgpack.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	var row Row
	if err := msgpack.Unmarshal(data, &row); err != nil {
		t.Fatal(err)
	}

	assertF64 := func(path string, v Value, want float64) {
		t.Helper()
		f, ok := v.(float64)
		if !ok {
			t.Fatalf("%s: got %T(%v), want float64", path, v, v)
		}
		if f != want {
			t.Fatalf("%s: got %v, want %v", path, f, want)
		}
	}
	assertF64("tiny", row["tiny"], 3)
	assertF64("small", row["small"], 300)
	assertF64("med", row["med"], 70000)
	assertF64("big", row["big"], 5_000_000_000)
	assertF64("utiny", row["utiny"], 200)
	assertF64("negative", row["negative"], -5)
	assertF64("f32", row["f32"], 1.5)
	assertF64("f64", row["f64"], 2.5)

	if row["str"] != "s" || row["boolean"] != true || row["nothing"] != nil {
		t.Fatalf("scalar passthrough broken: %+v", row)
	}

	obj, ok := row["json_obj"].(map[string]interface{})
	if !ok {
		t.Fatalf("json_obj: got %T", row["json_obj"])
	}
	assertF64("json_obj.n", obj["n"], 7)
	nested, _ := obj["nested"].(map[string]interface{})
	assertF64("json_obj.nested.deep", nested["deep"], 1)
	arr, _ := obj["arr"].([]interface{})
	if len(arr) != 4 {
		t.Fatalf("json_obj.arr len = %d", len(arr))
	}
	assertF64("json_obj.arr[0]", arr[0], 1)
	if arr[1] != "x" || arr[2] != nil {
		t.Fatalf("json_obj.arr passthrough broken: %+v", arr)
	}
	inner, _ := arr[3].(map[string]interface{})
	assertF64("json_obj.arr[3].m", inner["m"], 9)

	jarr, _ := row["json_arr"].([]interface{})
	assertF64("json_arr[0]", jarr[0], 4)
	assertF64("json_arr[1]", jarr[1], 0.5)

	// Decoded values must be USABLE by the comparators — the end-to-end
	// reason normalization exists.
	if CompareValues(row["small"], float64(300)) != 0 {
		t.Fatal("decoded int16 column does not compare equal to float64(300)")
	}
}

func TestRowDecodeMsgpack_NilMapAndSafeInteger(t *testing.T) {
	// nil row on the wire (e.g. absent OldRow) → nil map, not empty map.
	data, err := msgpack.Marshal((map[string]interface{})(nil))
	if err != nil {
		t.Fatal(err)
	}
	var row Row
	if err := msgpack.Unmarshal(data, &row); err != nil {
		t.Fatal(err)
	}
	if row != nil {
		t.Fatalf("nil wire map must decode to nil Row, got %+v", row)
	}

	// MAX_SAFE_INTEGER passes through exactly.
	maxSafe := int64(math.MaxInt64>>10) & ((1 << 53) - 1) // 2^53-1
	data, err = msgpack.Marshal(map[string]interface{}{"v": maxSafe})
	if err != nil {
		t.Fatal(err)
	}
	var row2 Row
	if err := msgpack.Unmarshal(data, &row2); err != nil {
		t.Fatal(err)
	}
	if f, ok := row2["v"].(float64); !ok || int64(f) != maxSafe {
		t.Fatalf("2^53-1 round-trip broken: %T %v", row2["v"], row2["v"])
	}
}
