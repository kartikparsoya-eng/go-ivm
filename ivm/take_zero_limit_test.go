package ivm

import (
	"slices"
	"testing"
)

// Test for audit fix #10: Take.initialFetch (and Fetch) must return nil/empty
// when limit is 0 — no rows should be fetched or state recorded.
// The NewTake constructor panics on limit < 0, so the <= 0 guard in
// initialFetch is defense-in-depth for that path; here we test the
// reachable limit == 0 case.

func TestTake_ZeroLimit_FetchReturnsNil(t *testing.T) {
	cols := map[string]string{"id": "string", "name": "string"}
	source := NewMemorySource("items", cols, []string{"id"})
	source.BulkInsert([]Row{
		{"id": "a", "name": "Alice"},
		{"id": "b", "name": "Bob"},
	})

	conn := source.Connect(Ordering{{"id", "asc"}}, nil, nil)
	storage := NewMemoryTakeStorage()
	take := NewTake(conn, storage, 0, nil)

	// Wire a collecting output.
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 0 {
		t.Fatalf("expected 0 rows from Take with limit=0, got %d", len(result))
	}
}

func TestTake_NegativeLimit_ConstructorPanics(t *testing.T) {
	cols := map[string]string{"id": "string"}
	source := NewMemorySource("items", cols, []string{"id"})
	conn := source.Connect(Ordering{{"id", "asc"}}, nil, nil)
	storage := NewMemoryTakeStorage()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on negative limit, but NewTake returned normally")
		}
	}()
	NewTake(conn, storage, -1, nil)
}
