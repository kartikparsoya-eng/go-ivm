package engine

// Profiling harness for the flatten-in-push change (DESIGN-streaming-advance.md,
// "Win 1"). These benchmarks isolate the exact cost the change targets: the
// per-advance materialize deep-copy of a relationship subtree.
//
// Shape: a "popular parent" — ONE users row (u1) with childCount posts rows all
// correlated to u1. The query is `users` with a top-level Related → posts, so
// the pipeline output for u1 is a single Add/Remove Change whose Node carries a
// relationship closure yielding childCount child Nodes. That subtree is what the
// legacy pipelineOutput.Push materialized eagerly (materializeChange →
// materializeNode, deep-copying every child) and what the new path flattens
// straight to RowChanges during push.
//
// Run:
//
//	go test ./engine/ -run '^$' -bench 'AdvanceFanout' -benchmem
//
// Compare NEW (flatten-in-push, working tree) vs OLD (materialize) by stashing:
//
//	go test ./engine/ -run '^$' -bench 'AdvanceFanout' -benchmem  > /tmp/new.txt
//	git stash                                                     # restore materialize
//	go test ./engine/ -run '^$' -bench 'AdvanceFanout' -benchmem  > /tmp/old.txt
//	git stash pop
//	# eyeball, or: benchstat /tmp/old.txt /tmp/new.txt

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// fanoutRelatedAST: users ⟕ posts (top-level relationship, no filter). Every
// emitted user Node carries a "posts" relationship — the subtree under test.
func fanoutRelatedAST() builder.AST {
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
					Alias:   "posts",
					OrderBy: ivm.Ordering{{"id", "asc"}},
				},
			},
		},
	}
}

// newFanoutEngine registers users + posts, seeds childCount posts all under u1,
// and hydrates the Related query. u1 itself is NOT seeded — the benchmark loop
// adds and removes it so each op re-drives the full subtree through the sink.
func newFanoutEngine(tb testing.TB, childCount int) *Engine {
	tb.Helper()
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	posts := ivm.NewMemorySource("posts",
		map[string]string{"id": "string", "userId": "string"},
		[]string{"id"})
	seed := make([]ivm.Row, childCount)
	for i := 0; i < childCount; i++ {
		seed[i] = ivm.Row{"id": fmt.Sprintf("p%06d", i), "userId": "u1"}
	}
	posts.BulkInsert(seed)

	eng, err := NewEngine(EngineConfig{StoragePath: filepath.Join(tb.TempDir(), "storage.db")})
	if err != nil {
		tb.Fatalf("NewEngine: %v", err)
	}
	tb.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(posts)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetTableUniqueKeys("posts", [][]string{{"id"}})

	if _, _, err := eng.AddQuery("q-fanout", fanoutRelatedAST()); err != nil {
		tb.Fatalf("AddQuery: %v", err)
	}
	return eng
}

var u1Row = ivm.Row{"id": "u1", "name": "Alice"}

// benchAdvanceFanout drives one ADD(u1)+REMOVE(u1) pair per op. Both changes
// carry the childCount-wide subtree through pipelineOutput.Push, so allocs/op
// and B/op capture the materialize-vs-flatten delta directly.
func benchAdvanceFanout(b *testing.B, childCount int) {
	eng := newFanoutEngine(b, childCount)
	addU1 := []SnapshotChange{{Table: "users", NextValue: u1Row}}
	removeU1 := []SnapshotChange{{Table: "users", PrevValues: []ivm.Row{u1Row}}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if r := eng.Advance(addU1); r.Drift != nil {
			b.Fatalf("add drift: %v", r.Drift)
		}
		if r := eng.Advance(removeU1); r.Drift != nil {
			b.Fatalf("remove drift: %v", r.Drift)
		}
	}
}

func BenchmarkAdvanceFanout_100(b *testing.B)  { benchAdvanceFanout(b, 100) }
func BenchmarkAdvanceFanout_1000(b *testing.B) { benchAdvanceFanout(b, 1000) }
func BenchmarkAdvanceFanout_5000(b *testing.B) { benchAdvanceFanout(b, 5000) }

// TestAdvanceFanout_RowCount documents how many RowChanges a SINGLE
// source-change (one popular-parent ADD) fans out to. This is the Win-2 gate
// input from DESIGN-streaming-advance §7: a single source-change's output is
// NOT split (engine.go — "we don't split an individual source-change's
// RowChange list"), so this count is the un-splittable unit that Win 1 still
// buffers whole. Compare against advanceChunkSize (default 10000).
func TestAdvanceFanout_RowCount(t *testing.T) {
	for _, childCount := range []int{100, 1000, 5000} {
		eng := newFanoutEngine(t, childCount)
		r := eng.Advance([]SnapshotChange{{Table: "users", NextValue: u1Row}})
		if r.Drift != nil {
			t.Fatalf("childCount=%d: unexpected drift: %v", childCount, r.Drift)
		}
		t.Logf("childCount=%5d → single-ADD emits %d RowChanges (advanceChunkSize=%d, splits=%d)",
			childCount, len(r.Changes), advanceChunkSize,
			len(r.Changes)/advanceChunkSize)
		if len(r.Changes) == 0 {
			t.Fatalf("childCount=%d: expected fan-out rows, got 0", childCount)
		}
	}
}
