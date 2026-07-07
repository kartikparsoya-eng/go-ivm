package engine

// Structural pin for the TS-faithful scalar-CSQ pipeline shape (2026-07-07
// faithfulness sweep). TS strips a RESOLVED simple scalar subquery from the
// AST before building (resolveSimpleScalarSubqueries — zqlite/src/
// resolve-scalar-subqueries.ts): the literal is baked into the plan, NO join
// exists for the subquery, and the companion pipeline is the only live
// element. Go runs the same resolver before BuildPipeline
// (engine.buildAndRegisterLocked → builder.ResolveSimpleScalarSubqueries),
// so the same must hold here.
//
// Historically Go marked a still-built join's child schema IsScalar and
// suppressed its rows at the streamer. That machinery became dead code once
// the resolver landed (no production path ever set JoinArgs.Scalar after
// commit 971dcd1 stopped propagating it for non-simple CSQs) and was deleted
// in this sweep. This test pins the invariant that made it dead — a resolved
// scalar CSQ must never build a Join/FlippedJoin/Exists — so a regression
// back to join-building (which would double-emit subquery rows against the
// companion, or require re-inventing suppression) fails loudly.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestAddQuery_ResolvedScalarCSQ_BuildsNoJoin(t *testing.T) {
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	users.BulkInsert([]ivm.Row{{"id": "u1", "name": "Alice"}})
	issues := ivm.NewMemorySource("issues",
		map[string]string{"id": "string", "ownerId": "string"},
		[]string{"id"})
	issues.BulkInsert([]ivm.Row{
		{"id": "i1", "ownerId": "Alice"},
		{"id": "i2", "ownerId": "Bob"},
	})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(issues)
	// users.id is a unique key and the subquery WHERE equality-constrains it
	// with a literal — IsSimpleSubquery is true, so the resolver MUST rewrite.
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})

	ast := builder.AST{
		Table: "issues",
		Where: &builder.Condition{
			Type:   "correlatedSubquery",
			Op:     "EXISTS",
			Scalar: true,
			Related: &builder.CorrelatedSubquery{
				Correlation: builder.Correlation{
					ParentField: []string{"ownerId"},
					ChildField:  []string{"name"},
				},
				Subquery: builder.AST{
					Table: "users",
					Where: &builder.Condition{
						Type:  "simple",
						Op:    "=",
						Left:  &builder.ValuePos{Type: "column", Name: "id"},
						Right: &builder.ValuePos{Type: "literal", Value: "u1"},
					},
				},
			},
		},
	}

	if _, _, err := eng.AddQuery("q-nojoin", ast); err != nil {
		t.Fatal(err)
	}

	eng.mu.Lock()
	entry := eng.pipelines["q-nojoin"]
	eng.mu.Unlock()
	if entry == nil {
		t.Fatal("pipeline entry missing after AddQuery")
	}

	// TS structural shape: the resolver replaced the scalar EXISTS with a
	// literal condition, so the built operator graph must contain NO join
	// operator of any kind for it.
	for _, edge := range entry.pipeline.Edges {
		for _, node := range edge {
			switch node.(type) {
			case *ivm.Join:
				t.Fatalf("resolved scalar CSQ built an *ivm.Join — TS builds no join for it (edge %T→%T)",
					edge[0], edge[1])
			case *ivm.FlippedJoin:
				t.Fatalf("resolved scalar CSQ built an *ivm.FlippedJoin — TS builds no join for it")
			case *ivm.Exists:
				t.Fatalf("resolved scalar CSQ built an *ivm.Exists — TS builds no EXISTS operator for it")
			}
		}
	}

	// The companion pipeline must be the one live element tracking the
	// subquery — it is what re-detects the scalar value changing
	// (ScalarResetError → reset + re-resolve).
	if got := len(entry.companions); got != 1 {
		t.Fatalf("companions = %d, want 1 (the resolver-recorded subquery pipeline)", got)
	}

	// And the main schema must carry NO relationship for the subquery —
	// nothing to stream, so no suppression machinery is needed anywhere
	// downstream (the deleted IsScalar guards had nothing to guard).
	if rels := entry.schema.Relationships; len(rels) != 0 {
		names := make([]string, 0, len(rels))
		for n := range rels {
			names = append(names, n)
		}
		t.Fatalf("main schema has relationships %v, want none for a resolved scalar", names)
	}
}
