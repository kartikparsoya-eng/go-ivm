package ivm

import (
	"fmt"
	"iter"
	"runtime/debug"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

// Value represents a column value. Mirrors zero-protocol/src/data.ts Value type.
// We normalize undefined → nil (same as TS normalizeUndefined).
type Value interface{}

// smallFloatCacheMax bounds boxedSmallFloat below. 1024 covers the values that
// dominate real numeric columns — booleans-as-int (0/1), enums, status codes,
// small counts, and low primary keys.
const smallFloatCacheMax = 1024

// boxedSmallFloat holds pre-boxed Value (interface{}) wrappers for the small
// non-negative integers above. Boxing a float64 into an interface heap-allocates
// 8 bytes PER value; numeric column coercion (FromSQLiteType on the hydrate path,
// normalizeDecodedValue on the advance/wire path) was ~30% of all allocations in
// the 1k-row hydrate profile, almost entirely these boxes. The boxed value is
// immutable — every downstream reader (comparators, the view, msgpack encode)
// only READS it and interface equality is by value — so handing the SAME box to
// every row with an equal value is indistinguishable from a fresh box, at zero
// per-row allocation. Built once at init.
var boxedSmallFloat = func() [smallFloatCacheMax]Value {
	var a [smallFloatCacheMax]Value
	for i := range a {
		a[i] = float64(i)
	}
	return a
}()

// BoxFloat64 wraps f as a Value, reusing a shared immutable box for small
// non-negative integer values to avoid a per-row heap allocation. Out-of-range
// or non-integral values fall through to a fresh box (the normal interface
// conversion). Hot path: kept small enough to inline.
func BoxFloat64(f float64) Value {
	if f >= 0 && f < smallFloatCacheMax {
		if i := int(f); float64(i) == f {
			return boxedSmallFloat[i]
		}
	}
	return f
}

// Row is a map of column name to value.
type Row map[string]Value

// DecodeMsgpack normalizes integer column values to float64 at decode time so
// downstream comparators (which assume the TS single-Number model — see
// `toFloat64`) don't need a separate post-walk to coerce types.
//
// This replaces the legacy `walkForNumericNormalize` reflection walk for the
// Row portion of decoded payloads. That walk was a significant share of total
// allocations (reflection-driven boxing), and most msgpack decoding cost flows
// through Row-bearing payloads (loadRows / advance). Decoding straight into
// float64 instead of int* → walk-and-rebox eliminates both.
//
// Wire-format compatibility:
//   - Numbers on the wire decode to msgpack-native int* / uint* / float types;
//     we coerce all to float64 here, matching what the post-walk used to do.
//   - Strings, bools, nil, nested maps, nested arrays are passed through.
//   - Nested maps/arrays are recursed to handle JSON-typed columns whose
//     values are objects/arrays (e.g., metadata jsonb).
func (r *Row) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	if n < 0 {
		*r = nil
		return nil
	}
	m := make(Row, n)
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return fmt.Errorf("Row.DecodeMsgpack: key %d: %w", i, err)
		}
		v, err := dec.DecodeInterface()
		if err != nil {
			return fmt.Errorf("Row.DecodeMsgpack: value for %q: %w", key, err)
		}
		m[key] = normalizeDecodedValue(v)
	}
	*r = m
	return nil
}

// normalizeDecodedValue coerces an interface{} produced by msgpack's
// DecodeInterface to the TS-compatible numeric model: every integer type
// becomes float64, including ints found inside nested maps/arrays. Strings,
// bools, nil, and other types pass through unchanged.
//
// Recursion handles JSON-typed column values (e.g., tickets.metadata) that
// decode to map[string]interface{} / []interface{} on the wire.
func normalizeDecodedValue(v interface{}) interface{} {
	switch x := v.(type) {
	case int8:
		return BoxFloat64(float64(x))
	case int16:
		return BoxFloat64(float64(x))
	case int32:
		return BoxFloat64(float64(x))
	case int64:
		return BoxFloat64(float64(x))
	case int:
		return BoxFloat64(float64(x))
	case uint8:
		return BoxFloat64(float64(x))
	case uint16:
		return BoxFloat64(float64(x))
	case uint32:
		return BoxFloat64(float64(x))
	case uint64:
		return BoxFloat64(float64(x))
	case uint:
		return BoxFloat64(float64(x))
	case float32:
		return BoxFloat64(float64(x))
	case map[string]interface{}:
		for k, vv := range x {
			x[k] = normalizeDecodedValue(vv)
		}
		return x
	case []interface{}:
		for i, vv := range x {
			x[i] = normalizeDecodedValue(vv)
		}
		return x
	}
	return v
}

// Node is a row flowing through the pipeline, plus its relationships.
// Relationships are generated lazily as read.
type Node struct {
	Row           Row
	Relationships map[string]func() iter.Seq[Node]
}

// Comparator compares two rows. Returns <0, 0, or >0.
type Comparator func(r1, r2 Row) int

// toFloat64 converts any numeric Value into a float64.
// Mirrors JS's single-Number-type model so values that arrived as int64/uint64
// (e.g., msgpack-decoded AST literals) compare equal to float64 row values.
// The matrix is complete over every width msgpack's DecodeInterface can
// produce (small ints decode as int8/int16 on the wire) — a missing case
// would make an equal pair look cross-type and panic instead of comparing.
func toFloat64(v Value) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint32:
		return float64(n), true
	case float32:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint:
		return float64(n), true
	}
	return 0, false
}

// CompareValues compares two values. The values must be of the same logical
// type, but numeric values are compared in a unified float64 space (matching
// TS's single JS Number type). nil compares equal to nil here (unlike SQL).
// Join code handles null separately via ValuesEqual.
//
// Structure note: there is deliberately NO leading `if a == b` fast path.
// Interface == panics with an opaque runtime error ("comparing uncomparable
// type") when both operands hold the same non-comparable dynamic type — a
// JSON column's map[string]interface{}, a JSON array, or a blob's []byte.
// TS compareValues (zql data.ts:75) throws a clean `Unsupported type` Error
// for non-scalar operands instead; we match that by letting non-scalars fall
// through every scalar branch into the DataError below. Scalar equality is
// handled inside each typed branch, so dropping the fast path costs nothing.
func CompareValues(a, b Value) int {
	// Numeric cross-type: every integer width + float compare as float64.
	if af, ok := toFloat64(a); ok {
		if bf, ok := toFloat64(b); ok {
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		}
	}

	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			// Byte-wise compare == UTF-8 code-point order — the SAME order TS
			// chose on purpose (compareUTF8, data.ts:40-49) to match SQLite's
			// BINARY collation. Do not switch to a locale/UTF-16 comparison.
			return strings.Compare(av, bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			if av == bv {
				return 0
			}
			if av {
				return 1
			}
			return -1
		}
	}

	// Null orders before ANY value — including non-scalar ones. TS runs its
	// null checks (data.ts:62-67) before the unsupported-type throw, so
	// nil-vs-object is ordered, not an error. Same here.
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}

	// Cross-type or non-scalar (JSON object/array column, blob) comparison =
	// deterministic data/schema mismatch (non-retryable): tag as DataError so
	// the sidecar maps it to RPC_CODE_DATA_ERROR (teardown, not reset-retry —
	// a reset re-reads the same rows and re-panics forever). Mirrors both TS
	// throws (`Cannot compare values of different types` / `Unsupported type`).
	panic(NewDataError("cannot compare values: unsupported or mismatched types %T(%v) and %T(%v)\n%s", a, a, b, b, string(debug.Stack())))
}

// ValuesEqual checks if two values are equal — matches TS valuesEqual.
//
// IMPORTANT: TS deliberately treats null as unequal to itself
// (see data.ts:106-118). This is required for correct join semantics; a NULL
// FK on parent must NOT match a NULL FK on child. Do not "fix" this by
// returning true for nil/nil — that would break joins.
//
// Cross-type numeric comparison (int64 vs float64 etc.) is supported so that
// msgpack-decoded AST literals match normalized row values.
func ValuesEqual(a, b Value) bool {
	if a == nil || b == nil {
		return false
	}
	// Numeric cross-type
	if af, ok := toFloat64(a); ok {
		bf, ok := toFloat64(b)
		return ok && af == bf
	}
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	}
	// Non-scalar (JSON object/array column, blob): TS valuesEqual is `a === b`
	// — REFERENCE equality (data.ts:117). Two independently-decoded objects
	// are always distinct references, so they compare UNEQUAL in TS even with
	// identical content. Return false to match (a bare interface == here would
	// instead panic "comparing uncomparable type" on same-typed maps/slices).
	// Known micro-divergence: TS would return true for the literally-same
	// object reference on both sides; that cannot occur on our push paths
	// (Row and OldRow are decoded separately) and checking map identity would
	// need reflection on a hot path.
	return false
}

// Ordering represents a sort specification.
// Each element is [columnName, "asc"|"desc"].
type Ordering [][2]string

// MakeComparator creates a Comparator from an ordering.
func MakeComparator(order Ordering, reverse bool) Comparator {
	return func(a, b Row) int {
		for _, ord := range order {
			field := ord[0]
			comp := CompareValues(a[field], b[field])
			if comp != 0 {
				if ord[1] == "desc" {
					comp = -comp
				}
				if reverse {
					comp = -comp
				}
				return comp
			}
		}
		return 0
	}
}

// MakePartialBoundComparator is MakeComparator except the SECOND argument is
// treated as a PARTIAL bound (a pagination cursor): comparison stops at the
// first sort column ABSENT from `b`, treating the remaining columns as equal.
//
// This mirrors SQL's three-valued logic where `col > NULL` is NULL — a cursor
// `{createdAt}` against sort `[createdAt, conversationId]` produces a dead
// `conversationId > NULL` clause, so a row equal on createdAt compares EQUAL to
// the cursor (not strictly after). The plain MakeComparator instead hits
// `CompareValues(row.conversationId, nil)` → +1 (nil < non-nil) and wrongly
// ranks the boundary row strictly AFTER the cursor — which made the overlay
// start-gate (overlayRowAtOrAfterStart) keep a Basis:"after" boundary row that
// should be dropped. Same nil<non-nil asymmetry the Skip operator fixed via
// CompareWithPartialBound.
//
// When `b` is a COMPLETE row (every sort column present — e.g. comparing two
// real fetched rows for insertion order) this is byte-identical to
// MakeComparator, so it is a safe drop-in wherever the second operand is
// sometimes a partial cursor and sometimes a full row.
func MakePartialBoundComparator(order Ordering, reverse bool) Comparator {
	return func(a, b Row) int {
		for _, ord := range order {
			field := ord[0]
			if _, ok := b[field]; !ok {
				return 0
			}
			comp := CompareValues(a[field], b[field])
			if comp != 0 {
				if ord[1] == "desc" {
					comp = -comp
				}
				if reverse {
					comp = -comp
				}
				return comp
			}
		}
		return 0
	}
}
