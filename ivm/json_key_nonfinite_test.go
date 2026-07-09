package ivm

// Hardening batch (porting-correctness review, theoretical items): state/cache
// key builders must encode non-finite floats the way JS JSON.stringify does —
// JSON.stringify(NaN) === JSON.stringify(Infinity) === 'null' — instead of
// panicking. json.Marshal rejects NaN/±Inf outright, so the old code PANICKED
// where TS succeeds, violating the Go-fails-iff-TS-fails invariant. The TS
// encoding also means a NaN partition/join value produces the SAME key as an
// actual null (a deliberate collision TS has); Go must reproduce it exactly.
//
// TS oracles: take.ts:710-725 (getTakeStateKey), cap.ts:300-313
// (getCapStateKey), cap.ts:315-317 (serializePK), exists.ts:224-230
// (#getCacheKey) — all plain JSON.stringify with no try/catch.
//
// Unreachable in production (FromSQLiteType never yields NaN/Inf; json columns
// come from json.Unmarshal), but a stray non-finite float64 must not desync Go
// from TS. Truly non-JS values (channels etc.) still panic — pinned by
// json_marshal_panic_test.go.

import (
	"iter"
	"math"
	"testing"
)

func TestGetTakeStateKey_NonFiniteMatchesJSONStringify(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		got := GetTakeStateKey(PartitionKey{"x"}, Row{"x": v})
		if got != `["take",null]` {
			t.Fatalf("GetTakeStateKey(%v) = %q, want %q (JSON.stringify encodes NaN/Inf as null, take.ts:724)", v, got, `["take",null]`)
		}
	}
	// The TS key COLLISION: NaN and actual null produce the identical key.
	if a, b := GetTakeStateKey(PartitionKey{"x"}, Row{"x": math.NaN()}),
		GetTakeStateKey(PartitionKey{"x"}, Row{"x": nil}); a != b {
		t.Fatalf("NaN key %q must collide with null key %q exactly as TS does", a, b)
	}
}

func TestGetCapStateKey_NonFiniteMatchesJSONStringify(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		got := GetCapStateKey(PartitionKey{"x"}, Row{"x": v})
		if got != `["cap",null]` {
			t.Fatalf("GetCapStateKey(%v) = %q, want %q (cap.ts:312)", v, got, `["cap",null]`)
		}
	}
}

func TestCapSerializePK_NonFiniteMatchesJSONStringify(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		got := serializePK(Row{"id": v}, []string{"id"})
		if got != `[null]` {
			t.Fatalf("serializePK(%v) = %q, want %q (cap.ts:315-317)", v, got, `[null]`)
		}
	}
}

func TestExistsGetCacheKey_NonFiniteMatchesJSONStringify(t *testing.T) {
	input := &mockFilterInput{
		schema: &SourceSchema{
			TableName:  "parent",
			PrimaryKey: []string{"id"},
			Columns:    map[string]string{"id": "number"},
			Relationships: map[string]*SourceSchema{
				"children": {
					TableName:  "child",
					PrimaryKey: []string{"id"},
					Columns:    map[string]string{"id": "string", "parentId": "number"},
				},
			},
		},
	}
	exists := NewExists(input, "children", CompoundKey{"id"}, ExistsTypeExists)
	out := &mockOutput{}
	exists.SetFilterOutput(out)

	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		node := Node{
			Row: Row{"id": v},
			Relationships: map[string]func() iter.Seq[Node]{
				"children": func() iter.Seq[Node] { return func(yield func(Node) bool) {} },
			},
		}
		got := exists.getCacheKey(node, exists.parentJoinKey)
		if got != `[null]` {
			t.Fatalf("getCacheKey(%v) = %q, want %q (JSON.stringify via exists.ts:229)", v, got, `[null]`)
		}
	}
}
