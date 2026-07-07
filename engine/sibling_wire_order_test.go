package engine

// Pins TS sibling wire order. TS streams a node's sibling relationships in
// Object.entries insertion order of node.relationships
// (pipeline-driver.ts:2861), and every operator inserts relationship names in
// schema-merge order (join.ts:86-96 / 295-301) — for `related` subqueries
// that is the AST `related` array order, NOT alphabetical order.
//
// Pre-fix Go sorted sibling relationship names in streamNodesInto
// (engine/streamer.go), so a query declaring related aliases [zzz, aaa]
// emitted aaa's child rows first. This test declares aliases whose insertion
// order is the reverse of their alphabetical order and asserts the wire
// sequence on both the hydrate and advance paths.

import (
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// siblingWireOrderAST: users with two related subqueries. Insertion order
// (zzz → posts, aaa → comments) is the reverse of alphabetical so a sorted
// emitter provably flips the wire order.
func siblingWireOrderAST() builder.AST {
	return builder.AST{
		Table:   "users",
		OrderBy: ivm.Ordering{{"id", "asc"}},
		Related: []builder.CorrelatedSubquery{
			{
				Correlation: builder.Correlation{
					ParentField: []string{"id"},
					ChildField:  []string{"userId"},
				},
				Subquery: builder.AST{
					Table:   "posts",
					Alias:   "zzz",
					OrderBy: ivm.Ordering{{"id", "asc"}},
				},
			},
			{
				Correlation: builder.Correlation{
					ParentField: []string{"id"},
					ChildField:  []string{"userId"},
				},
				Subquery: builder.AST{
					Table:   "comments",
					Alias:   "aaa",
					OrderBy: ivm.Ordering{{"id", "asc"}},
				},
			},
		},
	}
}

func TestSiblingWireOrder_MatchesTSInsertionOrder(t *testing.T) {
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string"}, []string{"id"})
	posts := ivm.NewMemorySource("posts",
		map[string]string{"id": "string", "userId": "string"}, []string{"id"})
	comments := ivm.NewMemorySource("comments",
		map[string]string{"id": "string", "userId": "string"}, []string{"id"})

	// u1 hydrates with one child per relationship; u2's children are seeded
	// now and surface via the advance leg below.
	users.BulkInsert([]ivm.Row{{"id": "u1"}})
	posts.BulkInsert([]ivm.Row{
		{"id": "p1", "userId": "u1"},
		{"id": "p2", "userId": "u2"},
	})
	comments.BulkInsert([]ivm.Row{
		{"id": "c1", "userId": "u1"},
		{"id": "c2", "userId": "u2"},
	})

	eng, err := NewEngine(EngineConfig{StoragePath: filepath.Join(t.TempDir(), "storage.db")})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(posts)
	eng.RegisterMemorySource(comments)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetTableUniqueKeys("posts", [][]string{{"id"}})
	eng.SetTableUniqueKeys("comments", [][]string{{"id"}})

	tables := func(changes []RowChange) []string {
		out := make([]string, len(changes))
		for i, rc := range changes {
			out[i] = rc.Table
		}
		return out
	}
	equal := func(a, b []string) bool {
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

	// Hydrate leg: u1 must stream as [users, posts(zzz), comments(aaa)] —
	// insertion order. A sorted emitter yields [users, comments, posts].
	changes, _, err := eng.AddQuery("q-sibling", siblingWireOrderAST())
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	want := []string{"users", "posts", "comments"}
	if got := tables(changes); !equal(got, want) {
		t.Fatalf("hydrate wire order = %v, want TS insertion order %v", got, want)
	}

	// Advance leg: ADD u2 flows through the same streamNodesInto — its
	// relationship subtree must stream zzz (posts) before aaa (comments).
	var advTables []string
	err = eng.AdvanceStream(
		[]SnapshotChange{{Table: "users", NextValue: ivm.Row{"id": "u2"}}},
		func(p AdvanceStreamPartial) {
			for _, rc := range p.Changes {
				advTables = append(advTables, rc.Table)
			}
		},
	)
	if err != nil {
		t.Fatalf("AdvanceStream: %v", err)
	}
	if !equal(advTables, want) {
		t.Fatalf("advance wire order = %v, want TS insertion order %v", advTables, want)
	}
}
