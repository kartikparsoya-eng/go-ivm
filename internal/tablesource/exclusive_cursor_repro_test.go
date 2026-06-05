package tablesource

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Repro for the shadow-soak exclusive-cursor boundary over-include:
// conversations list, sort [createdAt asc, <pk> asc], start={createdAt}
// (PARTIAL — no pk), exclusive=true, where the cursor's createdAt is the
// NEWEST row. Replica ground-truth: correct page is EMPTY (nothing strictly
// after the cursor). TS over-included the boundary row (+1); Go over-included
// the boundary PLUS the next-older row (+2). Here `score` stands in for
// `createdAt` and `id` for the pk; score=90 (alice) is the max.
func TestExclusiveCursorPartialBound_FetchPath(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	sort := ivm.Ordering{{"score", "asc"}, {"id", "asc"}}

	// (a) Source.Fetch directly with the Start that Skip.getStart synthesizes
	//     for a partial exclusive bound: Basis="after", Row={score:90}.
	in := src.Connect(sort, nil, nil)
	direct := in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{Row: ivm.Row{"score": float64(90)}, Basis: "after"},
	})
	if len(direct) != 0 {
		t.Errorf("Source.Fetch(after partial {score:90}): got %d rows, want 0; rows=%v",
			len(direct), rowsOf(direct))
	}

	// (b) Full Skip path (what the pipeline actually builds).
	in2 := src.Connect(sort, nil, nil)
	skip := ivm.NewSkip(in2, ivm.Bound{
		Row:       ivm.Row{"score": float64(90)},
		Exclusive: true,
	})
	viaSkip := skip.Fetch(ivm.FetchRequest{})
	if len(viaSkip) != 0 {
		t.Errorf("Skip.Fetch(exclusive partial {score:90}): got %d rows, want 0; rows=%v",
			len(viaSkip), rowsOf(viaSkip))
	}
}

func rowsOf(nodes []ivm.Node) []ivm.Row {
	out := make([]ivm.Row, len(nodes))
	for i, n := range nodes {
		out[i] = n.Row
	}
	return out
}
