package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Edge: a batch addQueries/addQueriesStream where one query's BUILD panics
// (builder.BuildPipeline raises *ivm.DataError on an unknown table — the
// "client sent an AST the replica schema doesn't have" shape). The whole
// batch RPC rejects on the TS side, so no query in the batch may stay
// registered on the Go side. Pre-fix, queries built BEFORE the failing one
// stayed registered with wired outputs but were never hydrated — orphan
// pipelines that (a) emitted RowChanges for queryIDs TS considers absent and
// (b) could panic on the next advance (e.g. Take pushes before its bound is
// initialized by the hydrate fetch), converting one bad query into an
// advance-time reset loop.

func buildUnwindEngine(t *testing.T) *Engine {
	t.Helper()
	users := ivm.NewMemorySource(
		"users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"},
	)
	users.BulkInsert([]ivm.Row{{"id": "u1", "name": "Alice"}})
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)
	return eng
}

func goodAndBadBatch() []QuerySpec {
	return []QuerySpec{
		{QueryID: "q-good", AST: builder.AST{Table: "users", OrderBy: ivm.Ordering{{"id", "asc"}}}},
		{QueryID: "q-bad", AST: builder.AST{Table: "no_such_table"}},
	}
}

// assertBatchBuildUnwound drives the shared assertions after a batch build
// panic: the panic is a *ivm.DataError (so the RPC layer classifies it as
// -32102 teardown, not a reset loop), NOTHING from the batch stays
// registered, the next advance emits zero RowChanges (no orphan pipeline
// output), and the good query is cleanly re-addable afterwards.
func assertBatchBuildUnwound(t *testing.T, eng *Engine, recovered any) {
	t.Helper()
	if recovered == nil {
		t.Fatal("expected the unknown-table build panic to escape, got clean return")
	}
	if _, ok := recovered.(*ivm.DataError); !ok {
		t.Fatalf("build panic should stay a *ivm.DataError (RPC -32102 classification), got %T: %v",
			recovered, recovered)
	}

	eng.mu.Lock()
	nPipelines := len(eng.pipelines)
	eng.mu.Unlock()
	if nPipelines != 0 {
		t.Fatalf("batch build panic must unwind ALL entries registered by the call; %d pipeline(s) leaked",
			nPipelines)
	}

	// An orphan (registered-but-unhydrated) pipeline would emit here — or
	// panic on push. Zero pipelines → zero output, no panic.
	result := eng.Advance([]SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u2", "name": "Bob"}},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift on post-unwind advance: %v", result.Drift)
	}
	if len(result.Changes) != 0 {
		t.Fatalf("orphan pipeline leaked output after failed batch build: %+v", result.Changes)
	}

	// The good query must be re-addable and hydrate the full current state.
	results, err := eng.AddQueries([]QuerySpec{
		{QueryID: "q-good", AST: builder.AST{Table: "users", OrderBy: ivm.Ordering{{"id", "asc"}}}},
	})
	if err != nil {
		t.Fatalf("re-adding the good query after unwind failed: %v", err)
	}
	if len(results) != 1 || len(results[0].Changes) != 2 { // u1 seed + u2 advance
		t.Fatalf("re-added query should hydrate u1+u2, got %+v", results)
	}
}

func TestAddQueries_BuildPanicMidBatch_UnwindsRegistered(t *testing.T) {
	eng := buildUnwindEngine(t)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = eng.AddQueries(goodAndBadBatch())
	}()
	assertBatchBuildUnwound(t, eng, recovered)
}

func TestAddQueriesStream_BuildPanicMidBatch_UnwindsRegistered(t *testing.T) {
	eng := buildUnwindEngine(t)

	var recovered any
	frames := 0
	func() {
		defer func() { recovered = recover() }()
		_ = eng.AddQueriesStream(goodAndBadBatch(), func(QueryResult) { frames++ })
	}()
	// Build is Phase 1 — it precedes every hydrate lane, so no partial
	// frame may have reached the sink before the panic.
	if frames != 0 {
		t.Fatalf("no hydrate frames may be emitted when the batch build fails, got %d", frames)
	}
	assertBatchBuildUnwound(t, eng, recovered)
}
