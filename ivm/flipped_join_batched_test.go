package ivm

import (
	"iter"
	"slices"
	"strconv"
	"testing"
)

// recordingInput wraps an Input and records every FetchRequest it receives.
// The Go analog of TS's Snitch for fetch-call assertions.
type recordingInput struct {
	inner   Input
	fetches []FetchRequest
}

func (r *recordingInput) GetSchema() *SourceSchema { return r.inner.GetSchema() }
func (r *recordingInput) Destroy()                 { r.inner.Destroy() }
func (r *recordingInput) SetOutput(o Output)       { r.inner.SetOutput(o) }
func (r *recordingInput) Fetch(req FetchRequest) iter.Seq[Node] {
	r.fetches = append(r.fetches, req)
	return r.inner.Fetch(req)
}

// newFlipFixture builds parent/child memory sources wired into a FlippedJoin
// keyed on parent.id / child.parentId, with the parent side recorded.
// Parents are p1..pN; children are 1:1 c1→p1 .. cN→pN unless childRows is
// given. Mirrors the makeSetup helper in upstream
// flipped-join.chunked.test.ts.
func newFlipFixture(t *testing.T, parentCount int, parentRows []Row, childRows []Row) (*FlippedJoin, *recordingInput) {
	t.Helper()
	parent := NewMemorySource("parent", map[string]string{"id": "string", "label": "string", "active": "boolean"}, []string{"id"})
	child := NewMemorySource("child", map[string]string{"id": "string", "parentId": "string"}, []string{"id"})

	if parentRows == nil {
		for i := 1; i <= parentCount; i++ {
			parentRows = append(parentRows, Row{"id": pID(i), "label": "Parent", "active": true})
		}
	}
	parent.BulkInsert(parentRows)

	if childRows == nil {
		for i := 1; i <= parentCount; i++ {
			childRows = append(childRows, Row{"id": cID(i), "parentId": pID(i)})
		}
	}
	child.BulkInsert(childRows)

	parentConn := parent.Connect(Ordering{{"id", "asc"}}, nil, nil)
	childConn := child.Connect(Ordering{{"id", "asc"}}, nil, nil)
	rec := &recordingInput{inner: parentConn}

	fj := NewFlippedJoin(FlippedJoinArgs{
		Parent:           rec,
		Child:            childConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"parentId"},
		RelationshipName: "children",
		Hidden:           false,
		System:           "client",
	})
	return fj, rec
}

func pID(i int) string { return "p" + strconv.Itoa(i) }
func cID(i int) string { return "c" + strconv.Itoa(i) }

func fetchedParentIDs(nodes []Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.Row["id"].(string)
	}
	return ids
}

func childCount(t *testing.T, n Node, rel string) int {
	t.Helper()
	gen, ok := n.Relationships[rel]
	if !ok {
		t.Fatalf("node %v missing relationship %q", n.Row, rel)
	}
	return len(slices.Collect(gen()))
}

// TestFlippedJoinFetchBatchesParentFetch is the core #5928 port assertion
// (zero 1.7.0): a FlippedJoin hydrate issues ONE batched parent fetch
// carrying the deduped child keys as a MultiConstraint — not one fetch per
// child. Proven failing pre-port: the old per-child strategy issued 5 parent
// fetches, each with a scalar Constraint and no MultiConstraints.
func TestFlippedJoinFetchBatchesParentFetch(t *testing.T) {
	fj, rec := newFlipFixture(t, 5, nil, nil)

	result := slices.Collect(fj.Fetch(FetchRequest{}))

	if got, want := fetchedParentIDs(result), []string{"p1", "p2", "p3", "p4", "p5"}; !slices.Equal(got, want) {
		t.Fatalf("parent order = %v, want %v", got, want)
	}
	for _, n := range result {
		if c := childCount(t, n, "children"); c != 1 {
			t.Fatalf("parent %v has %d children, want 1", n.Row["id"], c)
		}
	}

	if len(rec.fetches) != 1 {
		t.Fatalf("parent saw %d fetches, want 1 (batched); requests: %+v", len(rec.fetches), rec.fetches)
	}
	req := rec.fetches[0]
	if req.Constraint != nil {
		t.Fatalf("batched parent fetch must not carry a per-child Constraint; got %v", *req.Constraint)
	}
	if len(req.MultiConstraints) != 1 {
		t.Fatalf("batched parent fetch MultiConstraints = %d entries, want 1", len(req.MultiConstraints))
	}
	if got := len(req.MultiConstraints[0]); got != 5 {
		t.Fatalf("multi has %d key entries, want 5", got)
	}
}

// TestSkipForwardsMultiConstraints pins the pass-through contract for Skip
// (the one Go operator that rebuilds FetchRequest field-by-field): TS
// skip.ts spreads `...req`, so MultiConstraints must survive Skip.
// Proven failing pre-port: Skip's literal construction dropped the field.
func TestSkipForwardsMultiConstraints(t *testing.T) {
	src := NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	src.BulkInsert([]Row{{"id": "a"}, {"id": "b"}})
	conn := src.Connect(Ordering{{"id", "asc"}}, nil, nil)
	rec := &recordingInput{inner: conn}
	skip := NewSkip(rec, Bound{Row: Row{"id": "a"}, Exclusive: false})

	multis := []MultiConstraint{{{"id": "b"}}}
	got := slices.Collect(skip.Fetch(FetchRequest{MultiConstraints: multis}))

	if len(rec.fetches) != 1 || len(rec.fetches[0].MultiConstraints) != 1 {
		t.Fatalf("Skip dropped MultiConstraints on forward: %+v", rec.fetches)
	}
	if len(got) != 1 || got[0].Row["id"] != "b" {
		t.Fatalf("Skip+multi fetch = %v, want just b", fetchedParentIDs(got))
	}
}
