package ivm

// Parity pins for Skip with a PARTIAL bound (a pagination cursor that
// specifies only a prefix of the sort columns, e.g. {createdAt} over sort
// [createdAt, id]). Skip has TWO comparison regimes and they deliberately
// differ — these tests pin both so a future "fix" on either side surfaces
// as a test failure instead of a live shadow drift:
//
//  1. getStart uses the FULL comparator (skip.go:153), exactly like TS
//     skip.ts #getStart. TS compareValues coerces undefined→null→-1
//     (data.ts:33-34,62-63), so a partial bound ranks BEFORE any full row
//     that ties on the specified prefix — the cmp==0 branches never fire
//     for partial cursors in EITHER engine, and an explicit req.Start at
//     the boundary row wins over the bound. TS-parity; do not "fix" one
//     side alone.
//
//  2. shouldBePresent (the push path + reverse-fetch filter) uses
//     CompareWithPartialBound — a DELIBERATE divergence from TS skip.ts
//     #shouldBePresent (skip.ts:84-86), which uses the full comparator and
//     would FORWARD a pushed row tying the exclusive partial cursor
//     (undefined→null→-1 ⇒ cmp<0 ⇒ present). Go instead matches SQL's
//     three-valued semantics (`col > NULL` excludes) so its push path is
//     self-consistent with its own SQL hydrate path (gatherStartConstraints
//     early-break) and the overlay gate (MakePartialBoundComparator).
//     Consequence: a row pushed with the sort prefix EXACTLY equal to an
//     exclusive partial cursor is dropped by Go but forwarded by TS — a
//     known, intentional shadow MISMATCH shape (Go is the SQL-correct
//     side). Requires an exact tie on the cursor value, so it is rare in
//     live traffic.

import "testing"

// skipCaptureOutput records pushes forwarded by the Skip under test.
type skipCaptureOutput struct {
	pushes []Change
}

func (c *skipCaptureOutput) Push(change Change, _ InputBase) []Change {
	c.pushes = append(c.pushes, change)
	return nil
}

func newSkipParityFixture(t *testing.T, exclusive bool) (*MemorySource, *Skip, *skipCaptureOutput) {
	t.Helper()
	ms := NewMemorySource(
		"items",
		map[string]string{"createdAt": "number", "id": "string"},
		[]string{"id"},
	)
	ms.BulkInsert([]Row{
		{"createdAt": float64(99), "id": "a"},
		{"createdAt": float64(100), "id": "b"},
		{"createdAt": float64(100), "id": "c"},
		{"createdAt": float64(101), "id": "d"},
	})
	input := ms.Connect(Ordering{{"createdAt", "asc"}, {"id", "asc"}}, nil, nil)
	// PARTIAL bound: only createdAt, no id.
	sk := NewSkip(input, Bound{Row: Row{"createdAt": float64(100)}, Exclusive: exclusive})
	out := &skipCaptureOutput{}
	sk.SetOutput(out)
	return ms, sk, out
}

// Pin (1): getStart with a partial exclusive bound and an explicit req.Start
// AT the boundary row returns req.Start unchanged — the full comparator ranks
// the partial bound strictly before the full start row (missing id ⇒
// nil < "b"), so the cmp==0 "upgrade to after" branch never fires. Matches TS
// skip.ts #getStart + data.ts undefined→null→-1.
func TestSkipGetStartPartialBoundFullComparatorParity(t *testing.T) {
	_, sk, _ := newSkipParityFixture(t, true)

	reqStart := &Start{Row: Row{"createdAt": float64(100), "id": "b"}, Basis: "at"}
	got, empty := sk.getStart(FetchRequest{Start: reqStart})
	if empty {
		t.Fatalf("getStart returned empty for a forward fetch")
	}
	if got != reqStart {
		t.Fatalf("getStart = %+v, want req.Start unchanged (full-comparator parity with TS skip.ts)", got)
	}

	// Functional consequence: fetching from that explicit start INCLUDES the
	// boundary-tied rows b and c even though the bound is exclusive — same
	// quirk in TS. The bound only takes over when it ranks >= req.Start.
	nodes := sk.Fetch(FetchRequest{Start: reqStart})
	gotIDs := idsOf(nodes)
	wantIDs := []string{"b", "c", "d"}
	if !stringSlicesEqual(gotIDs, wantIDs) {
		t.Fatalf("Fetch from explicit boundary start = %v, want %v", gotIDs, wantIDs)
	}
}

// Pin (2a): a plain hydrate (no req.Start) over an exclusive partial bound
// EXCLUDES every row tying the cursor prefix — SQL `createdAt > 100`
// semantics, consistent across the in-memory applyStart, the SQL
// gatherStartConstraints early-break, and the overlay gate.
func TestSkipFetchPartialExclusiveBoundExcludesTies(t *testing.T) {
	_, sk, _ := newSkipParityFixture(t, true)

	gotIDs := idsOf(sk.Fetch(FetchRequest{}))
	wantIDs := []string{"d"}
	if !stringSlicesEqual(gotIDs, wantIDs) {
		t.Fatalf("Fetch = %v, want %v (rows at createdAt==100 must be excluded)", gotIDs, wantIDs)
	}

	// Inclusive bound keeps the ties.
	_, skIncl, _ := newSkipParityFixture(t, false)
	gotIDs = idsOf(skIncl.Fetch(FetchRequest{}))
	wantIDs = []string{"b", "c", "d"}
	if !stringSlicesEqual(gotIDs, wantIDs) {
		t.Fatalf("inclusive Fetch = %v, want %v", gotIDs, wantIDs)
	}
}

// Pin (2b): the push path. Go DROPS an add that ties the exclusive partial
// cursor (CompareWithPartialBound ⇒ 0 ⇒ excluded). TS skip.ts would FORWARD
// it (#shouldBePresent uses the full comparator: undefined→null ranks the
// partial bound before the row ⇒ present). This asymmetry is intentional —
// Go sides with SQL/its own hydrate path; see the file header.
func TestSkipPushPartialExclusiveBoundDropsBoundaryTie(t *testing.T) {
	ms, _, out := newSkipParityFixture(t, true)

	// Tie on the cursor prefix → dropped.
	ms.Push(MakeSourceChangeAdd(Row{"createdAt": float64(100), "id": "x"}))
	if len(out.pushes) != 0 {
		t.Fatalf("boundary-tie add was forwarded (%d pushes), want dropped — "+
			"Go pins SQL semantics here, deliberately diverging from TS skip.ts push", len(out.pushes))
	}

	// Strictly before → dropped.
	ms.Push(MakeSourceChangeAdd(Row{"createdAt": float64(99.5), "id": "y"}))
	if len(out.pushes) != 0 {
		t.Fatalf("pre-bound add was forwarded, want dropped")
	}

	// Strictly after → forwarded.
	ms.Push(MakeSourceChangeAdd(Row{"createdAt": float64(100.5), "id": "z"}))
	if len(out.pushes) != 1 {
		t.Fatalf("post-bound add: got %d pushes, want 1", len(out.pushes))
	}
	if got := out.pushes[0].Node.Row["id"]; got != "z" {
		t.Fatalf("forwarded row id = %v, want z", got)
	}
}

func idsOf(nodes []Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.Row["id"].(string))
	}
	return ids
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
