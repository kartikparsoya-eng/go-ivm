package ivm

import (
	"slices"
	"strconv"
	"testing"
)

// Benchmarks for the chained-FlippedJoin drain — the per-change cost behind
// the 2026-07-13 advance SLOW profile (one child-change push re-fetches the
// Take window, which drains the chained join). MemorySource-backed, so this
// isolates the ivm-operator cost (canonicalKey, comparators, heap merge,
// node assembly) from SQLite decode. Run with:
//
//	go test ./ivm/ -bench BenchmarkFlippedJoin -benchmem \
//	  -cpuprofile /tmp/fj-cpu.prof -memprofile /tmp/fj-mem.prof
func benchFlipSources(b *testing.B, n int) (*MemorySource, *MemorySource) {
	b.Helper()
	parent := NewMemorySource("parent", map[string]string{"id": "string", "label": "string"}, []string{"id"})
	child := NewMemorySource("child", map[string]string{"id": "string", "parentId": "string"}, []string{"id"})
	var parents, children []Row
	for i := 1; i <= n; i++ {
		parents = append(parents, Row{"id": pID(i), "label": "Parent"})
		children = append(children, Row{"id": cID(i), "parentId": pID(i)})
	}
	parent.BulkInsert(parents)
	child.BulkInsert(children)
	return parent, child
}

// BenchmarkFlippedJoinChunkedFetch: single join, n children → n deduped multi
// entries → chunked path (n/256 chunks), full drain.
func BenchmarkFlippedJoinChunkedFetch(b *testing.B) {
	const n = 10_000
	parent, child := benchFlipSources(b, n)
	fj := NewFlippedJoin(FlippedJoinArgs{
		Parent:           parent.Connect(Ordering{{"id", "asc"}}, nil, nil),
		Child:            child.Connect(Ordering{{"id", "asc"}}, nil, nil),
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"parentId"},
		RelationshipName: "children",
		System:           "client",
	})
	b.ResetTimer()
	for b.Loop() {
		got := slices.Collect(fj.Fetch(FetchRequest{}))
		if len(got) != n {
			b.Fatalf("got %d, want %d", len(got), n)
		}
	}
}

// BenchmarkFlippedJoinChained: grandchild→child inner join feeding a
// child→parent outer join — the wedge stack's shape. The outer Fetch
// eagerly collects the inner's whole output to build its IN-list.
func BenchmarkFlippedJoinChained(b *testing.B) {
	const n = 10_000
	parent, child := benchFlipSources(b, n)
	grandchild := NewMemorySource("grandchild", map[string]string{"id": "string", "childId": "string"}, []string{"id"})
	var gcs []Row
	for i := 1; i <= n; i++ {
		gcs = append(gcs, Row{"id": "g" + strconv.Itoa(i), "childId": cID(i)})
	}
	grandchild.BulkInsert(gcs)

	inner := NewFlippedJoin(FlippedJoinArgs{
		Parent:           child.Connect(Ordering{{"id", "asc"}}, nil, nil),
		Child:            grandchild.Connect(Ordering{{"id", "asc"}}, nil, nil),
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"childId"},
		RelationshipName: "grandchildren",
		System:           "client",
	})
	outer := NewFlippedJoin(FlippedJoinArgs{
		Parent:           parent.Connect(Ordering{{"id", "asc"}}, nil, nil),
		Child:            inner,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"parentId"},
		RelationshipName: "children",
		System:           "client",
	})
	b.ResetTimer()
	for b.Loop() {
		got := slices.Collect(outer.Fetch(FetchRequest{}))
		if len(got) != n {
			b.Fatalf("got %d, want %d", len(got), n)
		}
	}
}
