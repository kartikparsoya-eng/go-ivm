package ivm

import (
	"slices"
	"testing"
)

// ufiFixture wires the canonical OR-of-subqueries shape the builder produces
// (builder.go applyFilterWithFlips "or"): source → UnionFanOut → two Filter
// branches → UnionFanIn. Branch predicates OVERLAP (c,d) so the merge's
// adjacency dedup is exercised.
func ufiFixture(t *testing.T) *UnionFanIn {
	t.Helper()
	src := NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	src.BulkInsert([]Row{
		{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}, {"id": "e"}, {"id": "f"},
	})
	conn := src.Connect(Ordering{{"id", "asc"}}, nil, nil)
	ufo := NewUnionFanOut(conn)
	branch := func(pred func(Row) bool) Input {
		fs := NewFilterStart(ufo)
		f := NewFilter(fs, pred)
		return NewFilterEnd(fs, f)
	}
	b1 := branch(func(r Row) bool { return r["id"].(string) <= "d" }) // a..d
	b2 := branch(func(r Row) bool { return r["id"].(string) >= "c" }) // c..f
	return NewUnionFanIn(ufo, []Input{b1, b2})
}

func ufiIDs(nodes []Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Row["id"].(string)
	}
	return out
}

// TestUnionFanInFetchReverse pins upstream #5980 (union-fan-in.ts fetch):
// when req.Reverse is set every branch yields a DESCENDING stream, so the
// k-way merge must select heads by the NEGATED comparator. Pre-fix the Go
// merge used the ascending comparator unconditionally — with descending
// inputs it emitted scrambled order AND re-emitted the overlap rows (the
// adjacency dedup only works when equal rows surface consecutively):
// [d c b a f e d c] instead of [f e d c b a]. Reachable in production via
// Take's reverse fetches on its bound-recompute paths over an OR query.
func TestUnionFanInFetchReverse(t *testing.T) {
	ufi := ufiFixture(t)

	got := ufiIDs(slices.Collect(ufi.Fetch(FetchRequest{Reverse: true})))
	if want := []string{"f", "e", "d", "c", "b", "a"}; !slices.Equal(got, want) {
		t.Fatalf("reverse merge = %v, want %v", got, want)
	}

	// Forward path unchanged.
	got = ufiIDs(slices.Collect(ufi.Fetch(FetchRequest{})))
	if want := []string{"a", "b", "c", "d", "e", "f"}; !slices.Equal(got, want) {
		t.Fatalf("forward merge = %v, want %v", got, want)
	}
}

// TestUnionFanInFetchReverseWithStart: reverse + start cursor rides through
// the branches (Skip/Take shapes); the merge must stay descending from the
// cursor.
func TestUnionFanInFetchReverseWithStart(t *testing.T) {
	ufi := ufiFixture(t)
	got := ufiIDs(slices.Collect(ufi.Fetch(FetchRequest{
		Reverse: true,
		Start:   &Start{Row: Row{"id": "d"}, Basis: "at"},
	})))
	if want := []string{"d", "c", "b", "a"}; !slices.Equal(got, want) {
		t.Fatalf("reverse+start merge = %v, want %v", got, want)
	}
}
