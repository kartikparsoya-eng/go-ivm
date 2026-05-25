package engine

import (
	"os"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func tempStoragePath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "engine-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	t.Cleanup(func() { os.Remove(path) })
	return path
}

// TestSnapshotToSourceChanges tests the conversion of snapshot changes to source changes.
func TestSnapshotToSourceChanges(t *testing.T) {
	ms := ivm.NewMemorySource("users", map[string]string{"id": "string", "name": "string"}, []string{"id"})
	source := &memorySourceAdapter{ms: ms}

	// Test ADD (no prevValues, has nextValue)
	changes := snapshotToSourceChanges(SnapshotChange{
		Table:      "users",
		PrevValues: nil,
		NextValue:  ivm.Row{"id": "1", "name": "Alice"},
	}, source)

	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Type != ivm.ChangeTypeAdd {
		t.Fatalf("expected ADD, got %d", changes[0].Type)
	}
	if changes[0].Row["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", changes[0].Row["name"])
	}

	// Test REMOVE (has prevValues, no nextValue)
	changes = snapshotToSourceChanges(SnapshotChange{
		Table:      "users",
		PrevValues: []ivm.Row{{"id": "1", "name": "Alice"}},
		NextValue:  nil,
	}, source)

	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Type != ivm.ChangeTypeRemove {
		t.Fatalf("expected REMOVE, got %d", changes[0].Type)
	}

	// Test EDIT (prevValues has matching PK to nextValue)
	changes = snapshotToSourceChanges(SnapshotChange{
		Table:      "users",
		PrevValues: []ivm.Row{{"id": "1", "name": "Alice"}},
		NextValue:  ivm.Row{"id": "1", "name": "Alicia"},
	}, source)

	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Type != ivm.ChangeTypeEdit {
		t.Fatalf("expected EDIT, got %d", changes[0].Type)
	}
	if changes[0].Row["name"] != "Alicia" {
		t.Fatalf("expected Alicia, got %v", changes[0].Row["name"])
	}
	if changes[0].OldRow["name"] != "Alice" {
		t.Fatalf("expected old=Alice, got %v", changes[0].OldRow["name"])
	}

	// Test constraint conflict: prevValues has non-matching PK + nextValue (REMOVE + ADD)
	changes = snapshotToSourceChanges(SnapshotChange{
		Table:      "users",
		PrevValues: []ivm.Row{{"id": "2", "name": "Bob"}},
		NextValue:  ivm.Row{"id": "1", "name": "Alice"},
	}, source)

	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(changes))
	}
	if changes[0].Type != ivm.ChangeTypeRemove {
		t.Fatalf("expected first=REMOVE, got %d", changes[0].Type)
	}
	if changes[1].Type != ivm.ChangeTypeAdd {
		t.Fatalf("expected second=ADD, got %d", changes[1].Type)
	}
}

// TestEngineAdvance tests the full advance flow with a direct source + output wiring.
func TestEngineAdvance(t *testing.T) {
	ms := ivm.NewMemorySource("users", map[string]string{"id": "string", "name": "string", "age": "number"}, []string{"id"})

	storagePath := tempStoragePath(t)
	eng, err := NewEngine(EngineConfig{StoragePath: storagePath})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.RegisterMemorySource(ms)

	// Manually connect a pipeline (bypass builder for unit test simplicity)
	sort := ivm.Ordering{{"name", "asc"}, {"id", "asc"}}
	input := ms.Connect(sort, nil, nil)

	// Wire output to streamer
	schema := input.GetSchema()
	queryID := "test-query-1"
	input.SetOutput(&pipelineOutput{
		engine:  eng,
		queryID: queryID,
		schema:  schema,
	})

	// Advance: add a row
	result := eng.Advance([]SnapshotChange{
		{
			Table:     "users",
			NextValue: ivm.Row{"id": "1", "name": "Alice", "age": float64(30)},
		},
	})

	if len(result.Changes) != 1 {
		t.Fatalf("expected 1 row change, got %d", len(result.Changes))
	}
	if result.Changes[0].Type != RowChangeAdd {
		t.Fatalf("expected ADD, got %d", result.Changes[0].Type)
	}
	if result.Changes[0].QueryID != queryID {
		t.Fatalf("expected queryID=%s, got %s", queryID, result.Changes[0].QueryID)
	}
	if result.Changes[0].Row["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", result.Changes[0].Row["name"])
	}

	// Advance: add another row
	result = eng.Advance([]SnapshotChange{
		{
			Table:     "users",
			NextValue: ivm.Row{"id": "2", "name": "Bob", "age": float64(25)},
		},
	})

	if len(result.Changes) != 1 {
		t.Fatalf("expected 1 row change, got %d", len(result.Changes))
	}
	if result.Changes[0].Row["name"] != "Bob" {
		t.Fatalf("expected Bob, got %v", result.Changes[0].Row["name"])
	}

	// Advance: edit a row
	result = eng.Advance([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "1", "name": "Alice", "age": float64(30)}},
			NextValue:  ivm.Row{"id": "1", "name": "Alicia", "age": float64(31)},
		},
	})

	if len(result.Changes) != 1 {
		t.Fatalf("expected 1 row change, got %d", len(result.Changes))
	}
	if result.Changes[0].Type != RowChangeEdit {
		t.Fatalf("expected EDIT, got %d", result.Changes[0].Type)
	}

	// Advance: remove a row
	result = eng.Advance([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "2", "name": "Bob", "age": float64(25)}},
			NextValue:  nil,
		},
	})

	if len(result.Changes) != 1 {
		t.Fatalf("expected 1 row change, got %d", len(result.Changes))
	}
	if result.Changes[0].Type != RowChangeRemove {
		t.Fatalf("expected REMOVE, got %d", result.Changes[0].Type)
	}
}

// TestEngineMultipleConnections tests that changes fan out to multiple pipelines on the same source.
func TestEngineMultipleConnections(t *testing.T) {
	ms := ivm.NewMemorySource("items", map[string]string{"id": "string", "name": "string", "price": "number"}, []string{"id"})

	storagePath := tempStoragePath(t)
	eng, err := NewEngine(EngineConfig{StoragePath: storagePath})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.RegisterMemorySource(ms)

	// Connect two pipelines to same source
	sort := ivm.Ordering{{"name", "asc"}, {"id", "asc"}}
	input1 := ms.Connect(sort, nil, nil)
	input2 := ms.Connect(sort, nil, nil)

	schema1 := input1.GetSchema()
	schema2 := input2.GetSchema()

	input1.SetOutput(&pipelineOutput{engine: eng, queryID: "q1", schema: schema1})
	input2.SetOutput(&pipelineOutput{engine: eng, queryID: "q2", schema: schema2})

	// Advance: add a row — should produce changes for both queries
	result := eng.Advance([]SnapshotChange{
		{
			Table:     "items",
			NextValue: ivm.Row{"id": "a", "name": "Widget", "price": float64(9.99)},
		},
	})

	if len(result.Changes) != 2 {
		t.Fatalf("expected 2 row changes (one per pipeline), got %d", len(result.Changes))
	}

	// Check both query IDs are present
	qids := map[string]bool{}
	for _, rc := range result.Changes {
		qids[rc.QueryID] = true
	}
	if !qids["q1"] || !qids["q2"] {
		t.Fatalf("expected changes for q1 and q2, got %v", qids)
	}
}

// TestStreamer tests the streamer directly.
func TestStreamer(t *testing.T) {
	s := NewStreamer()

	schema := &ivm.SourceSchema{
		TableName:  "users",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string", "name": "string"},
	}

	// Accumulate an ADD change
	s.Accumulate("q1", schema, []ivm.Change{
		ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": "1", "name": "Alice"}}),
	})

	// Stream
	result := s.Stream()
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	if result[0].Type != RowChangeAdd {
		t.Fatalf("expected ADD")
	}
	if result[0].Table != "users" {
		t.Fatalf("expected table=users, got %s", result[0].Table)
	}
	if result[0].RowKey["id"] != "1" {
		t.Fatalf("expected rowKey.id=1, got %v", result[0].RowKey["id"])
	}

	// Stream again — should be empty
	result = s.Stream()
	if len(result) != 0 {
		t.Fatalf("expected 0 after second stream, got %d", len(result))
	}
}
