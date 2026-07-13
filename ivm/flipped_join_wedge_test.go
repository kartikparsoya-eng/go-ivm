package ivm

import (
	"iter"
	"slices"
	"strings"
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

// TestFlippedJoinChunkedNonAdvancingCursorPanics verifies the defense-in-depth
// guard: if fetchChunkHead returns the same row that was just advanced past,
// fetchChunkedSequential panics instead of looping forever. Before the guard,
// a non-advancing cursor (caused by the NULL-cursor SQL bug) would re-yield
// the same head eternally, burning a core at 100% CPU with no backstop able
// to interrupt it.
func TestFlippedJoinChunkedNonAdvancingCursorPanics(t *testing.T) {
	// chunkSize=1 with 2 distinct parent keys (p1, p2) → 2 chunks → enters
	// the chunked path. The stub always returns p1, so after yielding
	// chunk 0's head (p1), the re-fetch with Start={p1, after} returns
	// p1 again → the same-row guard fires.
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

	// The fetch should panic on the non-advancing cursor.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from non-advancing cursor guard, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		// Verify the panic message mentions non-advancing cursor.
		if !strings.Contains(msg, "non-advancing cursor") {
			t.Fatalf("panic message should mention 'non-advancing cursor', got: %q", msg)
		}
	}()

	_ = slices.Collect(fj.Fetch(FetchRequest{}))
	t.Fatal("should have panicked before reaching here")
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
