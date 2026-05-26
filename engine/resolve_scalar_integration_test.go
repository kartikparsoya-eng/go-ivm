package engine

import (
	"sort"
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

// Verifies the companion's source push fans out to the live companion
// connection: editing the resolved user row produces an EDIT RowChange
// under the parent queryID (companion live tracking is implicit because
// the resolver built a real Pipeline connected to the subquery source).
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

	// Edit the resolved companion row. The source's Push fans out to the
	// companion's connection; its pipeline produces an EDIT change tagged
	// with the parent queryID via the wired pipelineOutput.
	res := eng.Advance([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
		},
	})

	// Sort changes by (table, type) so test isn't order-fragile.
	sort.Slice(res.Changes, func(i, j int) bool {
		if res.Changes[i].Table != res.Changes[j].Table {
			return res.Changes[i].Table < res.Changes[j].Table
		}
		return res.Changes[i].Type < res.Changes[j].Type
	})

	found := false
	for _, c := range res.Changes {
		if c.Table == "users" && c.Type == RowChangeEdit && c.QueryID == "q" &&
			c.Row["name"] == "Alicia" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected EDIT companion row name=Alicia under q, got %+v", res.Changes)
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
