package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Verifies the Phase 2 scalar-subquery resolver inside the engine path:
// at hydrate, a scalar EXISTS pointing at a unique-key row is replaced by
// a literal equality and the matched subquery row ships as a companion.
//
// Layout mirrors the smallest realistic shadow-mode case:
//   - parent table "issues" with column "ownerId" (the EXISTS correlation
//     parentField) and a string "title"
//   - scalar table "users" with primary key "id"
//   - the original WHERE pre-resolution is whereExists('users', q =>
//     q.where('id', '=', 'u1'), {scalar: true}) on correlation
//     ownerId = users.name (deliberately picking a non-PK childField so
//     the resolver path is exercised end-to-end)
func TestAddQuery_ResolvesScalarAndEmitsCompanion(t *testing.T) {
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	users.BulkInsert([]ivm.Row{
		{"id": "u1", "name": "Alice"},
	})
	issues := ivm.NewMemorySource("issues",
		map[string]string{"id": "string", "ownerId": "string", "title": "string"},
		[]string{"id"})
	issues.BulkInsert([]ivm.Row{
		{"id": "i1", "ownerId": "Alice", "title": "first"},
		{"id": "i2", "ownerId": "Bob", "title": "ignored"},
	})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(issues)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetTableUniqueKeys("issues", [][]string{{"id"}})

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

	changes, _, err := eng.AddQuery("q", ast)
	if err != nil {
		t.Fatal(err)
	}

	// Collect by table to assert independent of ordering. Hydrate yields
	// issues first then companions, but the goal is to confirm both sides
	// are present rather than fix an order contract.
	byTable := map[string][]RowChange{}
	for _, c := range changes {
		if c.QueryID != "q" {
			t.Fatalf("companion row tagged with wrong queryID: %+v", c)
		}
		byTable[c.Table] = append(byTable[c.Table], c)
	}

	if len(byTable["issues"]) != 1 {
		t.Fatalf("expected exactly one matching issue (i1), got %d in %v",
			len(byTable["issues"]), byTable["issues"])
	}
	if byTable["issues"][0].Row["id"] != "i1" {
		t.Fatalf("expected issue id=i1, got %v", byTable["issues"][0].Row)
	}
	if len(byTable["users"]) != 1 {
		t.Fatalf("expected exactly one companion user row, got %d in %v",
			len(byTable["users"]), byTable["users"])
	}
	if byTable["users"][0].Row["id"] != "u1" {
		t.Fatalf("expected companion user id=u1, got %v", byTable["users"][0].Row)
	}
}

// Verifies the companion's live scalar-value-changed reset: the resolver
// baked users.name="Alice" into the issues query as a literal. Editing the
// resolved user row's child field (name: Alice -> Alicia) makes that
// literal stale, so the advance must RESET (panic with *ScalarResetError,
// which the sidecar maps to the scalar-reset RPC code and TS turns into a
// re-hydrate), NOT emit a companion EDIT. Direct port of TS's
// CompanionPipeline ResetPipelinesSignal('scalar-subquery') behavior
// (pipeline-driver.ts:1468).
func TestAddQuery_CompanionLiveTracking(t *testing.T) {
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	users.BulkInsert([]ivm.Row{{"id": "u1", "name": "Alice"}})
	issues := ivm.NewMemorySource("issues",
		map[string]string{"id": "string", "ownerId": "string"},
		[]string{"id"})
	issues.BulkInsert([]ivm.Row{{"id": "i1", "ownerId": "Alice"}})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(issues)
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
	if _, _, err := eng.AddQuery("q", ast); err != nil {
		t.Fatal(err)
	}

	// Edit the resolved companion row's child field (the baked scalar). The
	// source's Push fans out to the companion's connection; its output runs
	// the scalar-value-changed check and panics with the reset. The panic
	// escapes Advance (no partial output returns), so no companion EDIT can
	// leak — TS's throw has the same effect.
	sre := advanceScalarResetPanic(t, eng, []SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
		},
	})
	if sre.Table != "users" {
		t.Fatalf("expected ScalarResetError.Table=users, got %q", sre.Table)
	}
	// Message mirrors TS: `Scalar subquery value changed for users: Alice -> Alicia`
	if got := sre.Error(); got != "Scalar subquery value changed for users: Alice -> Alicia" {
		t.Fatalf("unexpected reset message: %q", got)
	}
}

// Verifies the companion does NOT over-reset: editing a NON-child column
// of the resolved row leaves the scalar value unchanged, so the advance
// must accumulate a normal companion EDIT (no reset). This is the
// scalarValuesEqual==true branch of TS's companion push (the row still
// streams via streamer.accumulate). Guards against J's reset firing on
// every companion push.
func TestAddQuery_CompanionEditNonScalarFieldNoReset(t *testing.T) {
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string", "email": "string"},
		[]string{"id"})
	users.BulkInsert([]ivm.Row{{"id": "u1", "name": "Alice", "email": "a@x.com"}})
	issues := ivm.NewMemorySource("issues",
		map[string]string{"id": "string", "ownerId": "string"},
		[]string{"id"})
	issues.BulkInsert([]ivm.Row{{"id": "i1", "ownerId": "Alice"}})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(issues)
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
	if _, _, err := eng.AddQuery("q", ast); err != nil {
		t.Fatal(err)
	}

	// Edit only the email; the child field (name) is unchanged.
	res := eng.Advance([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice", "email": "a@x.com"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alice", "email": "b@x.com"},
		},
	})

	found := false
	for _, c := range res.Changes {
		if c.Table == "users" && c.Type == RowChangeEdit && c.QueryID == "q" &&
			c.Row["email"] == "b@x.com" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected companion EDIT row email=b@x.com under q, got %+v", res.Changes)
	}
}

// Verifies that removing the parent query also tears down companion
// connections — i.e., subsequent source.Push on the subquery table
// doesn't keep producing change events tagged with the (now-removed)
// queryID. Critical to prevent companion-Connection leaks across the
// long-lived MemorySource.
func TestAddQuery_RemoveQueryCleansCompanions(t *testing.T) {
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	users.BulkInsert([]ivm.Row{{"id": "u1", "name": "Alice"}})
	issues := ivm.NewMemorySource("issues",
		map[string]string{"id": "string", "ownerId": "string"},
		[]string{"id"})
	issues.BulkInsert([]ivm.Row{{"id": "i1", "ownerId": "Alice"}})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(issues)
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
	if _, _, err := eng.AddQuery("q", ast); err != nil {
		t.Fatal(err)
	}
	eng.RemoveQuery("q")

	// After removal, an advance on the companion's table must not produce
	// any RowChange tagged with the removed query.
	res := eng.Advance([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
		},
	})
	for _, c := range res.Changes {
		if c.QueryID == "q" {
			t.Fatalf("unexpected post-remove change for q: %+v", c)
		}
	}
}

// TestAddQuery_ScalarNoMatchToMatchedNullThenValue exercises the no-match →
// matched-NULL → matched-value lifecycle with TS's tri-state resolvedValue.
//
// Hydrate with NO matching user (users empty) resolves undefined
// (pipeline-driver.ts:1195-1199) and bakes ALWAYS_FALSE, so the issue is
// filtered out. A later INSERT of users{id:u1, name:NULL} moves the scalar
// from "no row" (undefined) to "row, NULL value" (null) — and TS's
// scalarValuesEqual is strict ===, where null !== undefined
// (pipeline-driver.ts:3025-3034), so a reset FIRES ("undefined -> null").
// TS then rebuilds the pipeline against the advanced replica: the re-resolve
// sees matched-NULL → null, and the re-baked predicate is the SAME
// ALWAYS_FALSE (resolve-scalar-subqueries.ts:171-174) — the issue stays
// filtered; the reset changed the companion's resolved state, not the rows.
// Only when the value then becomes non-NULL (name: NULL→"Zoe") does the baked
// literal go stale again and a second reset fire ("null -> Zoe").
//
// (An earlier version of this test pinned NO reset for the first transition,
// justified as "matching TS null===null" — wrong: TS compares null ===
// undefined there, which is false. The streaming/advance audit's F1.)
func TestAddQuery_ScalarNoMatchToMatchedNullThenValue(t *testing.T) {
	// users starts EMPTY — the scalar subquery (id='u1') matches nothing.
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	issues := ivm.NewMemorySource("issues",
		map[string]string{"id": "string", "ownerId": "string"},
		[]string{"id"})
	issues.BulkInsert([]ivm.Row{{"id": "i1", "ownerId": "Alice"}})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(issues)
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

	// Hydrate: no match → resolvedValue undefined, ALWAYS_FALSE baked → no
	// issue rows emitted.
	changes, _, err := eng.AddQuery("q", ast)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Table == "issues" {
			t.Fatalf("no-match hydrate must emit zero issue rows (ALWAYS_FALSE); got %+v", c)
		}
	}

	// Transition 1: INSERT users{id:u1, name:NULL} — matched, value NULL.
	// scalarValuesEqual(null, undefined) is false (strict ===) → RESET.
	sre := advanceScalarResetPanic(t, eng, []SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": nil}},
	})
	if got := sre.Error(); got != "Scalar subquery value changed for users: undefined -> null" {
		t.Fatalf("unexpected first reset message: %q", got)
	}

	// Mimic the TS reset lifecycle: the view-syncer destroys the pipelines,
	// the replica finishes advancing (the reset panic aborted the engine push
	// BEFORE the source write — genPushAndWrite pushes then writes — so apply
	// the pending insert directly, as the replica would hold it), and the
	// query re-registers, re-running the resolver against current truth.
	eng.RemoveQuery("q")
	users.BulkInsert([]ivm.Row{{"id": "u1", "name": nil}})
	changes, _, err = eng.AddQuery("q", ast)
	if err != nil {
		t.Fatal(err)
	}
	// Re-resolve sees matched-NULL → resolvedValue null → the SAME
	// ALWAYS_FALSE predicate: the issue stays filtered (output-equivalent to
	// no-match), and the companion row now ships.
	for _, c := range changes {
		if c.Table == "issues" {
			t.Fatalf("matched-NULL re-hydrate must emit zero issue rows (ALWAYS_FALSE); got %+v", c)
		}
	}

	// Transition 2: EDIT users{id:u1} name NULL→"Zoe" — the baked literal is
	// genuinely stale again → second reset, now null -> Zoe.
	sre = advanceScalarResetPanic(t, eng, []SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": nil}},
			NextValue:  ivm.Row{"id": "u1", "name": "Zoe"},
		},
	})
	if got := sre.Error(); got != "Scalar subquery value changed for users: null -> Zoe" {
		t.Fatalf("unexpected second reset message: %q", got)
	}
}
