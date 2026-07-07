package ivm

import "testing"

// An unordered connect (nil sort — the Cap/EXISTS-child path) yields a
// schema with NO sort, matching TS MemorySource #getSchema
// (memory-source.ts:154: `sort: unordered ? undefined : connection.sort`).
// The connection still scans in primary-index order internally, but
// surfacing that as the schema sort would let order-dependent operators
// (Take, UnionFanIn) build over an input TS rejects.
func TestMemorySource_UnorderedConnectSchemaHasNoSort(t *testing.T) {
	cols := map[string]string{"id": "string", "name": "string"}
	source := NewMemorySource("items", cols, []string{"id"})

	unordered := source.Connect(nil, nil, nil)
	if got := unordered.GetSchema().Sort; got != nil {
		t.Fatalf("unordered schema.Sort = %v, want nil", got)
	}
	if unordered.GetSchema().CompareRows == nil {
		t.Fatal("unordered schema must still carry a PK compareRows")
	}

	ordered := source.Connect(Ordering{{"id", "asc"}}, nil, nil)
	if got := ordered.GetSchema().Sort; got == nil {
		t.Fatal("ordered schema.Sort = nil, want the explicit sort")
	}
}
