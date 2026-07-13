package ivm

import (
	"iter"
	"slices"
	"testing"
)

// stubParentAlways returns the same row for every Fetch call regardless of
// the Start cursor — simulating what the SQL keyset does when a NULL cursor
// value collapses the WHERE clause to TRUE (the bug that caused the infinite
// loop before the same-row guard was added).
type stubParentAlways struct {
	row     Row
	schema  *SourceSchema
	fetches int
}

func (s *stubParentAlways) GetSchema() *SourceSchema { return s.schema }
func (s *stubParentAlways) Destroy()                 {}
func (s *stubParentAlways) SetOutput(o Output)       {}
func (s *stubParentAlways) Fetch(req FetchRequest) iter.Seq[Node] {
	s.fetches++
	return func(yield func(Node) bool) {
		yield(Node{Row: s.row})
	}
}

// TestFlippedJoinChunkedNonAdvancingParentTerminates pins the structural
// wedge fix: the chunk-buffered merge fetches each chunk exactly once, so a
// parent whose results ignore the Start cursor (what the NULL-cursor SQL
// degeneracy produced — WHERE collapsed to TRUE) CANNOT loop: there is no
// keyset re-fetch to not-advance. The old shape re-fetched the winning
// chunk per yielded row and looped forever on such a parent (the 6-21+
// minute live advance wedge); a same-row panic guard then converted the
// loop to a teardown. Both the loop and the guard are gone — this test
// pins termination + the one-fetch-per-chunk contract against the exact
// adversarial parent that used to wedge.
func TestFlippedJoinChunkedNonAdvancingParentTerminates(t *testing.T) {
	// chunkSize=1 with 2 distinct parent keys (p1, p2) → 2 chunks → enters
	// the chunked path. The stub returns p1 for every fetch regardless of
	// constraints or Start.
	t.Cleanup(SetMultiConstraintChunkSizeForTest(1))

	stub := &stubParentAlways{
		row: Row{"id": "p1", "label": "stuck"},
		schema: &SourceSchema{
			TableName:  "parent",
			Columns:    map[string]string{"id": "string", "label": "string"},
			PrimaryKey: []string{"id"},
			Sort:       Ordering{{"id", "asc"}},
		},
	}
	stub.schema.CompareRows = MakeComparator(stub.schema.Sort, false)

	child := NewMemorySource("child",
		map[string]string{"id": "string", "parentId": "string"},
		[]string{"id"})
	child.BulkInsert([]Row{
		{"id": "c1", "parentId": "p1"},
		{"id": "c2", "parentId": "p2"},
	})
	childConn := child.Connect(Ordering{{"id", "asc"}}, nil, nil)

	fj := NewFlippedJoin(FlippedJoinArgs{
		Parent:           stub,
		Child:            childConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"parentId"},
		RelationshipName: "children",
		System:           "client",
	})

	// The fetch must TERMINATE (the old shape looped forever here) with
	// exactly one parent fetch per chunk. Output: each chunk's buffer is
	// the single p1 row the stub returns; both pass the parent-key filter
	// (p1 is a known key), so p1 is yielded once per chunk.
	got := slices.Collect(fj.Fetch(FetchRequest{}))
	if len(got) != 2 {
		t.Fatalf("got %d nodes, want 2 (one buffered p1 per chunk)", len(got))
	}
	for _, n := range got {
		if n.Row["id"] != "p1" {
			t.Fatalf("unexpected row %v", n.Row)
		}
	}
	if stub.fetches != 2 {
		t.Fatalf("parent fetched %d times, want exactly 2 (one per chunk, never per row)", stub.fetches)
	}
}

// TestFlippedJoinChunkedNullSortColumnTerminates verifies that a chunked
// fetch over a nullable sort column with NULL-valued rows terminates
// correctly (does not infinite-loop). Before the SQL fix, a NULL cursor
// value collapsed the keyset WHERE clause to TRUE, causing the chunked
// fetch to re-fetch the same head forever.
func TestFlippedJoinChunkedNullSortColumnTerminates(t *testing.T) {
	t.Cleanup(SetMultiConstraintChunkSizeForTest(2))

	// Parents with a nullable "score" column: p1 has NULL, p2 and p3 have
	// non-NULL scores. The sort is [score asc, id asc] — NULL sorts first.
	parent := NewMemorySource("parent",
		map[string]string{"id": "string", "score": "number", "label": "string"},
		[]string{"id"})
	parent.BulkInsert([]Row{
		{"id": "p1", "score": nil, "label": "null-score"},
		{"id": "p2", "score": float64(10), "label": "low"},
		{"id": "p3", "score": float64(20), "label": "high"},
	})

	child := NewMemorySource("child",
		map[string]string{"id": "string", "parentId": "string"},
		[]string{"id"})
	child.BulkInsert([]Row{
		{"id": "c1", "parentId": "p1"},
		{"id": "c2", "parentId": "p2"},
		{"id": "c3", "parentId": "p3"},
	})
	childConn := child.Connect(Ordering{{"id", "asc"}}, nil, nil)

	// Connect parent with score as the sort column (nullable).
	parentConn := parent.Connect(Ordering{{"score", "asc"}, {"id", "asc"}}, nil, nil)
	rec := &recordingInput{inner: parentConn}

	fj := NewFlippedJoin(FlippedJoinArgs{
		Parent:           rec,
		Child:            childConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"parentId"},
		RelationshipName: "children",
		System:           "client",
	})

	// This must terminate — before the fix it would infinite-loop.
	result := slices.Collect(fj.Fetch(FetchRequest{}))

	// All 3 parents should be returned, sorted by score asc (NULL first).
	if got, want := fetchedParentIDs(result), []string{"p1", "p2", "p3"}; !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v (NULL score sorts first)", got, want)
	}

	// Each parent should have exactly 1 child.
	for _, n := range result {
		if c := childCount(t, n, "children"); c != 1 {
			t.Fatalf("parent %v children = %d, want 1", n.Row["id"], c)
		}
	}
}
