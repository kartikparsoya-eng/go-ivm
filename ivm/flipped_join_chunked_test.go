package ivm

import (
	"math"
	"slices"
	"strconv"
	"testing"
)

// Port of upstream zql/src/ivm/flipped-join.chunked.test.ts (zero 1.7.0,
// #5928) — the chunked multi-constraint fetch scenarios — onto Go's
// MemorySource fixtures. Two upstream cases are intentionally NOT ported:
//   - ".return() propagation to sub-streams": Go's chunked path collects
//     each chunk eagerly (deadlock-safe deviation documented on Fetch), so
//     there are no live sub-streams to release; early consumer stop is
//     covered by TestFlippedJoinChunkedEarlyStop instead.
//   - "yield forwarding": Go drops TS's 'yield' scheduling tokens entirely
//     (operator.go header).

// chunk2Fixture: 5 parents, 1:1 children, chunk size 2 → 3 chunks (2+2+1).
// p2 is inactive so req.Constraint tests can exclude it.
func chunk2Fixture(t *testing.T) (*FlippedJoin, *recordingInput) {
	t.Helper()
	t.Cleanup(SetMultiConstraintChunkSizeForTest(2))
	var parents []Row
	for i := 1; i <= 5; i++ {
		parents = append(parents, Row{"id": pID(i), "label": "Parent " + strconv.Itoa(i), "active": i != 2})
	}
	return newFlipFixture(t, 5, parents, nil)
}

func TestFlippedJoinChunkedMergesSortedChunks(t *testing.T) {
	fj, rec := chunk2Fixture(t)

	result := slices.Collect(fj.Fetch(FetchRequest{}))

	// 5 parents, in id order, each with the matching child grouped under it.
	if got, want := fetchedParentIDs(result), []string{"p1", "p2", "p3", "p4", "p5"}; !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for _, n := range result {
		children := slices.Collect(n.Relationships["children"]())
		if len(children) != 1 {
			t.Fatalf("parent %v children = %d, want 1", n.Row["id"], len(children))
		}
		if children[0].Row["parentId"] != n.Row["id"] {
			t.Fatalf("parent %v grouped with wrong child %v", n.Row["id"], children[0].Row)
		}
	}

	// 3 parent fetches (chunks of 2, 2, 1), each carrying one
	// multiConstraints entry capped at the chunk size.
	if len(rec.fetches) != 3 {
		t.Fatalf("parent fetches = %d, want 3", len(rec.fetches))
	}
	for i, wantLen := range []int{2, 2, 1} {
		if got := len(rec.fetches[i].MultiConstraints[0]); got != wantLen {
			t.Fatalf("chunk %d multi len = %d, want %d", i, got, wantLen)
		}
	}
}

func TestFlippedJoinChunkedDedupesSharedParentKeys(t *testing.T) {
	// 3 distinct parents but 6 children (2 per parent): the multi must have
	// 3 unique entries, so chunk size 2 → 2 fetches (2+1), not 3.
	t.Cleanup(SetMultiConstraintChunkSizeForTest(2))
	var children []Row
	n := 1
	for i := 1; i <= 3; i++ {
		children = append(children, Row{"id": cID(n), "parentId": pID(i)})
		n++
		children = append(children, Row{"id": cID(n), "parentId": pID(i)})
		n++
	}
	fj, rec := newFlipFixture(t, 3, nil, children)

	result := slices.Collect(fj.Fetch(FetchRequest{}))

	if len(result) != 3 {
		t.Fatalf("parents = %d, want 3", len(result))
	}
	for _, nd := range result {
		if c := childCount(t, nd, "children"); c != 2 {
			t.Fatalf("parent %v children = %d, want 2", nd.Row["id"], c)
		}
	}
	if len(rec.fetches) != 2 {
		t.Fatalf("parent fetches = %d, want 2 (deduped 3 keys / chunk 2)", len(rec.fetches))
	}
	if len(rec.fetches[0].MultiConstraints[0]) != 2 || len(rec.fetches[1].MultiConstraints[0]) != 1 {
		t.Fatalf("chunk sizes = %d,%d want 2,1",
			len(rec.fetches[0].MultiConstraints[0]), len(rec.fetches[1].MultiConstraints[0]))
	}
}

func TestFlippedJoinChunkedReverse(t *testing.T) {
	fj, rec := chunk2Fixture(t)

	result := slices.Collect(fj.Fetch(FetchRequest{Reverse: true}))

	if got, want := fetchedParentIDs(result), []string{"p5", "p4", "p3", "p2", "p1"}; !slices.Equal(got, want) {
		t.Fatalf("reverse order = %v, want %v", got, want)
	}
	for _, n := range result {
		children := slices.Collect(n.Relationships["children"]())
		if len(children) != 1 || children[0].Row["parentId"] != n.Row["id"] {
			t.Fatalf("parent %v wrong children %v", n.Row["id"], children)
		}
	}
	if len(rec.fetches) != 3 {
		t.Fatalf("parent fetches = %d, want 3", len(rec.fetches))
	}
	for i, f := range rec.fetches {
		if !f.Reverse {
			t.Fatalf("chunk %d did not carry Reverse", i)
		}
	}
}

func TestFlippedJoinChunkedStartAt(t *testing.T) {
	fj, rec := chunk2Fixture(t)

	start := &Start{Row: Row{"id": "p3"}, Basis: "at"}
	result := slices.Collect(fj.Fetch(FetchRequest{Start: start}))

	if got, want := fetchedParentIDs(result), []string{"p3", "p4", "p5"}; !slices.Equal(got, want) {
		t.Fatalf("start-at = %v, want %v", got, want)
	}
	// Each chunk's parent fetch carries the start parameter through.
	if len(rec.fetches) != 3 {
		t.Fatalf("parent fetches = %d, want 3", len(rec.fetches))
	}
	for i, f := range rec.fetches {
		if f.Start == nil || f.Start.Row["id"] != "p3" || f.Start.Basis != "at" {
			t.Fatalf("chunk %d start = %+v, want p3/at", i, f.Start)
		}
	}
}

func TestFlippedJoinChunkedStartAfter(t *testing.T) {
	fj, _ := chunk2Fixture(t)
	result := slices.Collect(fj.Fetch(FetchRequest{Start: &Start{Row: Row{"id": "p3"}, Basis: "after"}}))
	if got, want := fetchedParentIDs(result), []string{"p4", "p5"}; !slices.Equal(got, want) {
		t.Fatalf("start-after = %v, want %v", got, want)
	}
}

func TestFlippedJoinChunkedStartReverse(t *testing.T) {
	fj, _ := chunk2Fixture(t)
	result := slices.Collect(fj.Fetch(FetchRequest{
		Start:   &Start{Row: Row{"id": "p3"}, Basis: "at"},
		Reverse: true,
	}))
	if got, want := fetchedParentIDs(result), []string{"p3", "p2", "p1"}; !slices.Equal(got, want) {
		t.Fatalf("start+reverse = %v, want %v", got, want)
	}
}

func TestFlippedJoinChunkedConstraintOnNonJoinColumn(t *testing.T) {
	fj, rec := chunk2Fixture(t)

	// `active` is not in parentKey, so FlippedJoin can't translate it to a
	// child constraint — children are fetched unconstrained, all 5 land in
	// the multi, and the constraint rides along with each chunk's parent
	// fetch for the source to apply alongside the IN list. p2 is inactive.
	constraint := Constraint{"active": true}
	result := slices.Collect(fj.Fetch(FetchRequest{Constraint: &constraint}))

	if got, want := fetchedParentIDs(result), []string{"p1", "p3", "p4", "p5"}; !slices.Equal(got, want) {
		t.Fatalf("constrained = %v, want %v", got, want)
	}
	if len(rec.fetches) != 3 {
		t.Fatalf("parent fetches = %d, want 3", len(rec.fetches))
	}
	for i, f := range rec.fetches {
		if f.Constraint == nil || (*f.Constraint)["active"] != true {
			t.Fatalf("chunk %d lost req.Constraint: %+v", i, f.Constraint)
		}
	}
}

func TestFlippedJoinChunkedEarlyStop(t *testing.T) {
	// Breaking after the first parent must terminate cleanly on the chunked
	// path (the merge loop must observe yield()==false and stop).
	fj, _ := chunk2Fixture(t)
	var got []string
	for n := range fj.Fetch(FetchRequest{}) {
		got = append(got, n.Row["id"].(string))
		break
	}
	if !slices.Equal(got, []string{"p1"}) {
		t.Fatalf("early stop got %v, want [p1]", got)
	}
}

func TestFlippedJoinChainedJoinsANDMultiConstraints(t *testing.T) {
	// Two chained FlippedJoins: the inner one contributes a multi on
	// `assigneeId`, the outer on `creatorId`. The leaf must receive BOTH
	// (ANDed), and the pass-through (inner join forwarding a fetch whose
	// multi it didn't compute) must not drop rows it can't match —
	// the outer join's canonical-key miss filter handles the row routing.
	//
	//   tickets(id, assigneeId, creatorId)
	//   inner FlippedJoin: parent=tickets child=assignments(parentId=assigneeId… )
	//
	// Simplest faithful shape: parent=tickets; two child tables keyed on
	// different ticket columns is not chainable directly (both flips share
	// the same parent). Instead chain on the PARENT input: outer flip's
	// parent is the INNER flip. The inner flip forwards the outer's multi
	// unchanged to the tickets leaf (pass-through contract) and appends its
	// own — the leaf sees two multis ANDed.
	tickets := NewMemorySource("tickets", map[string]string{"id": "string"}, []string{"id"})
	tickets.BulkInsert([]Row{{"id": "t1"}, {"id": "t2"}, {"id": "t3"}})

	comments := NewMemorySource("comments", map[string]string{"id": "string", "ticketId": "string"}, []string{"id"})
	comments.BulkInsert([]Row{
		{"id": "m1", "ticketId": "t1"},
		{"id": "m2", "ticketId": "t2"},
		{"id": "m3", "ticketId": "t3"},
	})
	labels := NewMemorySource("labels", map[string]string{"id": "string", "ticketId": "string"}, []string{"id"})
	labels.BulkInsert([]Row{
		{"id": "l1", "ticketId": "t2"},
		{"id": "l2", "ticketId": "t3"},
	})

	ticketConn := tickets.Connect(Ordering{{"id", "asc"}}, nil, nil)
	rec := &recordingInput{inner: ticketConn}

	inner := NewFlippedJoin(FlippedJoinArgs{
		Parent:           rec,
		Child:            comments.Connect(Ordering{{"id", "asc"}}, nil, nil),
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"ticketId"},
		RelationshipName: "comments",
		System:           "client",
	})
	outer := NewFlippedJoin(FlippedJoinArgs{
		Parent:           inner,
		Child:            labels.Connect(Ordering{{"id", "asc"}}, nil, nil),
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"ticketId"},
		RelationshipName: "labels",
		System:           "client",
	})

	result := slices.Collect(outer.Fetch(FetchRequest{}))

	// Only t2 and t3 have BOTH a comment and a label.
	if got, want := fetchedParentIDs(result), []string{"t2", "t3"}; !slices.Equal(got, want) {
		t.Fatalf("chained = %v, want %v", got, want)
	}
	// The tickets leaf saw the outer's multi (labels: t2,t3) AND the
	// inner's multi (comments: t1,t2,t3) in one request.
	if len(rec.fetches) != 1 {
		t.Fatalf("leaf fetches = %d, want 1", len(rec.fetches))
	}
	multis := rec.fetches[0].MultiConstraints
	if len(multis) != 2 {
		t.Fatalf("leaf multis = %d, want 2 (chained joins AND)", len(multis))
	}
	if len(multis[0]) != 2 || len(multis[1]) != 3 {
		t.Fatalf("multi sizes = %d,%d want 2 (labels), 3 (comments)", len(multis[0]), len(multis[1]))
	}
}

func TestFlippedJoinNullFKChildExcluded(t *testing.T) {
	// A child with a null FK builds no join constraint and must not reach
	// the multi (BuildJoinConstraint returns nil — same as pre-port).
	children := []Row{
		{"id": "c1", "parentId": "p1"},
		{"id": "c2", "parentId": nil},
	}
	fj, rec := newFlipFixture(t, 2, nil, children)

	result := slices.Collect(fj.Fetch(FetchRequest{}))

	if got, want := fetchedParentIDs(result), []string{"p1"}; !slices.Equal(got, want) {
		t.Fatalf("null-FK = %v, want %v", got, want)
	}
	if len(rec.fetches) != 1 || len(rec.fetches[0].MultiConstraints[0]) != 1 {
		t.Fatalf("multi should have exactly the non-null key; fetches: %+v", rec.fetches)
	}
}

func TestCanonicalKeyTypeTags(t *testing.T) {
	// canonicalKey backs the multi dedup map and the parent→children lookup.
	// Type-tag collisions would conflate distinct children into one bucket.
	pk := CompoundKey{"k"}
	cases := []struct {
		name string
		a, b Row
	}{
		{"number vs string same lexical form", Row{"k": float64(1)}, Row{"k": "1"}},
		{"bool vs string", Row{"k": true}, Row{"k": "true"}},
		{"null vs literal n string", Row{"k": nil}, Row{"k": "n"}},
		{"int64 vs float64", Row{"k": int64(1)}, Row{"k": float64(1)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if canonicalKey(c.a, pk) == canonicalKey(c.b, pk) {
				t.Fatalf("canonicalKey collision: %v vs %v -> %q", c.a, c.b, canonicalKey(c.a, pk))
			}
		})
	}
	// Compound keys separate parts with \x00.
	ck := CompoundKey{"a", "b"}
	if canonicalKey(Row{"a": "x", "b": "y"}, ck) == canonicalKey(Row{"a": "xy", "b": ""}, ck) {
		t.Fatal("compound canonicalKey must not concatenate ambiguously")
	}
	// Same value same key (dedup relies on it).
	if canonicalKey(Row{"a": "x", "b": float64(2)}, ck) != canonicalKey(Row{"a": "x", "b": float64(2)}, ck) {
		t.Fatal("identical records must produce identical keys")
	}
	// ±0 conflate (review M2): JS String(-0) === "0", so TS keys both zeros
	// as "d0" (flipped-join.ts:607). A child keyed -0.0 must match a parent
	// fetched back as +0.0 (SQLite's int-serial encoding normalizes
	// integral REALs); pre-fix Go emitted "d-0" and the parent row was
	// silently dropped.
	negZero := math.Copysign(0, -1)
	if got, want := canonicalKey(Row{"k": negZero}, pk), canonicalKey(Row{"k": float64(0)}, pk); got != want {
		t.Fatalf("canonicalKey(-0.0) = %q, canonicalKey(0.0) = %q; JS conflates ±0", got, want)
	}
	if got := canonicalKey(Row{"k": negZero}, pk); got != "d0" {
		t.Fatalf("canonicalKey(-0.0) = %q; want %q (TS 'd' + String(-0))", got, "d0")
	}
}
