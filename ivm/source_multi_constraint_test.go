package ivm

import (
	"slices"
	"testing"
)

// Port of the multiConstraints MemorySource cases from upstream
// zql/src/ivm/source.test.ts (present since 1.6.1; first exercised by
// FlippedJoin's batched fetch in 1.7.0 #5928).

func multiFixture(t *testing.T) *SourceInput {
	t.Helper()
	src := NewMemorySource("t", map[string]string{"id": "number", "s": "string", "b": "boolean"}, []string{"id"})
	src.BulkInsert([]Row{
		{"id": float64(1), "s": "a", "b": true},
		{"id": float64(2), "s": "b", "b": false},
		{"id": float64(3), "s": "c", "b": true},
		{"id": float64(4), "s": "d", "b": false},
		{"id": float64(5), "s": "e", "b": true},
	})
	return src.Connect(Ordering{{"id", "asc"}}, nil, nil)
}

func mcIDs(nodes []Node) []float64 {
	out := make([]float64, len(nodes))
	for i, n := range nodes {
		out[i] = n.Row["id"].(float64)
	}
	return out
}

func TestMemorySourceMultiConstraints(t *testing.T) {
	conn := multiFixture(t)

	cases := []struct {
		name   string
		req    FetchRequest
		wantID []float64
	}{
		{
			// source.test.ts:646 — unordered entry values, ordered output.
			"single multi returns matches in sort order",
			FetchRequest{MultiConstraints: []MultiConstraint{{{"id": float64(4)}, {"id": float64(1)}, {"id": float64(3)}}}},
			[]float64{1, 3, 4},
		},
		{
			"no matches yields empty",
			FetchRequest{MultiConstraints: []MultiConstraint{{{"id": float64(99)}, {"id": float64(100)}}}},
			nil,
		},
		{
			// source.test.ts:663 — empty multis list = no filtering.
			"empty multis list is full scan",
			FetchRequest{MultiConstraints: []MultiConstraint{}},
			[]float64{1, 2, 3, 4, 5},
		},
		{
			// source.test.ts:676 — an EMPTY entry is ignored, not empty-set.
			"empty entry ignored alongside real one",
			FetchRequest{MultiConstraints: []MultiConstraint{{}, {{"id": float64(2)}, {"id": float64(4)}}}},
			[]float64{2, 4},
		},
		{
			// source.test.ts:917 — two multis AND together.
			"two multis AND",
			FetchRequest{MultiConstraints: []MultiConstraint{
				{{"id": float64(4)}, {"id": float64(5)}},
				{{"s": "d"}},
			}},
			[]float64{4},
		},
		{
			"two multis AND no overlap",
			FetchRequest{MultiConstraints: []MultiConstraint{
				{{"id": float64(4)}, {"id": float64(5)}},
				{{"s": "q"}},
			}},
			nil,
		},
		{
			// source.test.ts:787 — boolean values.
			"boolean multi",
			FetchRequest{MultiConstraints: []MultiConstraint{{{"b": false}}}},
			[]float64{2, 4},
		},
		{
			// Constraint ANDs with multis.
			"constraint plus multi",
			FetchRequest{
				Constraint:       &Constraint{"b": true},
				MultiConstraints: []MultiConstraint{{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(3)}}},
			},
			[]float64{1, 3},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mcIDs(slices.Collect(conn.Fetch(c.req)))
			if !slices.Equal(got, c.wantID) {
				t.Fatalf("ids = %v, want %v", got, c.wantID)
			}
		})
	}
}

// TestMemorySourceMultiConstraintNullValueNoMatch: TS constraintMatchesRow
// uses valuesEqual, which treats null as UNEQUAL to null — a null entry
// value never matches (source.test.ts:772 uses {org: null} amid real
// values; only the real values match).
func TestMemorySourceMultiConstraintNullValueNoMatch(t *testing.T) {
	src := NewMemorySource("t", map[string]string{"id": "number", "org": "string"}, []string{"id"})
	src.BulkInsert([]Row{
		{"id": float64(1), "org": "a"},
		{"id": float64(2), "org": nil},
		{"id": float64(3), "org": "b"},
	})
	conn := src.Connect(Ordering{{"id", "asc"}}, nil, nil)

	got := mcIDs(slices.Collect(conn.Fetch(FetchRequest{
		MultiConstraints: []MultiConstraint{{{"org": "a"}, {"org": nil}, {"org": "b"}}},
	})))
	if want := []float64{1, 3}; !slices.Equal(got, want) {
		t.Fatalf("ids = %v, want %v (null entry must not match null row)", got, want)
	}
}

// multiFetchingOutput fetches with MultiConstraints from inside a push
// fanout, capturing what a downstream operator would see mid-push (the
// overlay window) — port of source.test.ts's "fetch during push" pattern.
type multiFetchingOutput struct {
	conn   *SourceInput
	multis []MultiConstraint
	seen   [][]float64
}

func (o *multiFetchingOutput) Push(change Change, pusher InputBase) []Change {
	o.seen = append(o.seen, mcIDs(slices.Collect(o.conn.Fetch(FetchRequest{MultiConstraints: o.multis}))))
	return nil
}

func TestMemorySourceMultiConstraintsGateOverlayDuringPush(t *testing.T) {
	// source.test.ts:2497+ — a fetch during push must see the in-flight
	// overlay row IFF it matches the multi.
	mk := func(multis []MultiConstraint) *multiFetchingOutput {
		src := NewMemorySource("t", map[string]string{"id": "number"}, []string{"id"})
		src.BulkInsert([]Row{{"id": float64(1)}, {"id": float64(2)}})
		conn := src.Connect(Ordering{{"id", "asc"}}, nil, nil)
		out := &multiFetchingOutput{conn: conn, multis: multis}
		conn.SetOutput(out)
		src.Push(MakeSourceChangeAdd(Row{"id": float64(6)}))
		return out
	}

	// Overlay row 6 matches the multi → visible mid-push.
	out := mk([]MultiConstraint{{{"id": float64(1)}, {"id": float64(6)}}})
	if len(out.seen) != 1 || !slices.Equal(out.seen[0], []float64{1, 6}) {
		t.Fatalf("matching overlay: seen = %v, want [[1 6]]", out.seen)
	}

	// Overlay row 6 does NOT match the multi → filtered out mid-push.
	out = mk([]MultiConstraint{{{"id": float64(1)}}})
	if len(out.seen) != 1 || !slices.Equal(out.seen[0], []float64{1}) {
		t.Fatalf("non-matching overlay: seen = %v, want [[1]]", out.seen)
	}
}
