package ivm

import (
	"slices"
	"testing"
)

// Pins union-fan-in.ts:52-88: branch relationships merge into the fan-in
// schema via Object.entries in BRANCH ORDER — first input's novel names
// first, then the second's — not alphabetically. Fixture: branch 1 joins a
// child as "zzz", branch 2 as "aaa"; the merged RelationshipOrder must be
// [zzz, aaa].
//
// Also pins the SourceSchema invariant the wire emitter relies on:
// RelationshipOrder and Relationships carry the same key set.
func TestUnionFanIn_RelationshipOrder_BranchOrder(t *testing.T) {
	src := NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	src.BulkInsert([]Row{{"id": "a"}})
	conn := src.Connect(Ordering{{"id", "asc"}}, nil, nil)
	ufo := NewUnionFanOut(conn)

	childZ := NewMemorySource("cz",
		map[string]string{"id": "string", "tid": "string"}, []string{"id"})
	childA := NewMemorySource("ca",
		map[string]string{"id": "string", "tid": "string"}, []string{"id"})

	branch := func(child *MemorySource, name string) Input {
		fs := NewFilterStart(ufo)
		f := NewFilter(fs, func(Row) bool { return true })
		end := NewFilterEnd(fs, f)
		return NewJoin(JoinArgs{
			Parent:           end,
			Child:            child.Connect(Ordering{{"id", "asc"}}, nil, nil),
			ParentKey:        CompoundKey{"id"},
			ChildKey:         CompoundKey{"tid"},
			RelationshipName: name,
			System:           "client",
		})
	}
	b1 := branch(childZ, "zzz")
	b2 := branch(childA, "aaa")

	ufi := NewUnionFanIn(ufo, []Input{b1, b2})
	schema := ufi.GetSchema()

	if want := []string{"zzz", "aaa"}; !slices.Equal(schema.RelationshipOrder, want) {
		t.Fatalf("RelationshipOrder = %v, want branch order %v", schema.RelationshipOrder, want)
	}
	if len(schema.RelationshipOrder) != len(schema.Relationships) {
		t.Fatalf("invariant broken: %d ordered names vs %d map entries",
			len(schema.RelationshipOrder), len(schema.Relationships))
	}
	for _, name := range schema.RelationshipOrder {
		if schema.Relationships[name] == nil {
			t.Fatalf("ordered name %q missing from Relationships map", name)
		}
	}
}
