package ivm

import "testing"

// mockFilterInput is a minimal FilterInput for testing Filter in isolation.
type mockFilterInput struct {
	schema *SourceSchema
	output FilterOutput
}

func (m *mockFilterInput) GetSchema() *SourceSchema       { return m.schema }
func (m *mockFilterInput) Destroy()                       {}
func (m *mockFilterInput) SetFilterOutput(o FilterOutput) { m.output = o }

// mockOutput collects pushed changes for assertions.
type mockOutput struct {
	pushed []Change
}

func (m *mockOutput) Push(change Change, pusher InputBase) []Change {
	m.pushed = append(m.pushed, change)
	return []Change{change}
}

func (m *mockOutput) BeginFilter()          {}
func (m *mockOutput) EndFilter()            {}
func (m *mockOutput) Filter(node Node) bool { return true }

func TestFilter_Push_Add_PassesPredicate(t *testing.T) {
	input := &mockFilterInput{}
	f := NewFilter(input, func(row Row) bool {
		return row["age"].(int) > 18
	})

	out := &mockOutput{}
	f.SetFilterOutput(out)

	node := Node{Row: Row{"age": 25, "name": "Alice"}}
	changes := f.Push(MakeAddChange(node), input)

	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Type != ChangeTypeAdd {
		t.Fatalf("expected ADD change, got %d", changes[0].Type)
	}
}

func TestFilter_Push_Add_FailsPredicate(t *testing.T) {
	input := &mockFilterInput{}
	f := NewFilter(input, func(row Row) bool {
		return row["age"].(int) > 18
	})

	out := &mockOutput{}
	f.SetFilterOutput(out)

	node := Node{Row: Row{"age": 10, "name": "Bob"}}
	changes := f.Push(MakeAddChange(node), input)

	if len(changes) != 0 {
		t.Fatalf("expected 0 changes, got %d", len(changes))
	}
	if len(out.pushed) != 0 {
		t.Fatalf("expected nothing pushed to output, got %d", len(out.pushed))
	}
}

func TestFilter_Push_Edit_SplitsToAdd(t *testing.T) {
	input := &mockFilterInput{}
	f := NewFilter(input, func(row Row) bool {
		return row["age"].(int) > 18
	})

	out := &mockOutput{}
	f.SetFilterOutput(out)

	oldNode := Node{Row: Row{"age": 10, "name": "Bob"}}
	newNode := Node{Row: Row{"age": 25, "name": "Bob"}}
	changes := f.Push(MakeEditChange(newNode, oldNode), input)

	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Type != ChangeTypeAdd {
		t.Fatalf("expected ADD change (split from edit), got %d", changes[0].Type)
	}
}

func TestFilter_Push_Edit_SplitsToRemove(t *testing.T) {
	input := &mockFilterInput{}
	f := NewFilter(input, func(row Row) bool {
		return row["age"].(int) > 18
	})

	out := &mockOutput{}
	f.SetFilterOutput(out)

	oldNode := Node{Row: Row{"age": 25, "name": "Bob"}}
	newNode := Node{Row: Row{"age": 10, "name": "Bob"}}
	changes := f.Push(MakeEditChange(newNode, oldNode), input)

	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Type != ChangeTypeRemove {
		t.Fatalf("expected REMOVE change (split from edit), got %d", changes[0].Type)
	}
}

func TestFilter_Push_Edit_BothPass(t *testing.T) {
	input := &mockFilterInput{}
	f := NewFilter(input, func(row Row) bool {
		return row["age"].(int) > 18
	})

	out := &mockOutput{}
	f.SetFilterOutput(out)

	oldNode := Node{Row: Row{"age": 25, "name": "Bob"}}
	newNode := Node{Row: Row{"age": 30, "name": "Bob"}}
	changes := f.Push(MakeEditChange(newNode, oldNode), input)

	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Type != ChangeTypeEdit {
		t.Fatalf("expected EDIT change (both pass), got %d", changes[0].Type)
	}
}

func TestFilter_Push_Edit_NeitherPasses(t *testing.T) {
	input := &mockFilterInput{}
	f := NewFilter(input, func(row Row) bool {
		return row["age"].(int) > 18
	})

	out := &mockOutput{}
	f.SetFilterOutput(out)

	oldNode := Node{Row: Row{"age": 10, "name": "Bob"}}
	newNode := Node{Row: Row{"age": 15, "name": "Bob"}}
	changes := f.Push(MakeEditChange(newNode, oldNode), input)

	if len(changes) != 0 {
		t.Fatalf("expected 0 changes, got %d", len(changes))
	}
}

func TestFilter_Filter_PassesPredicate(t *testing.T) {
	input := &mockFilterInput{}
	f := NewFilter(input, func(row Row) bool {
		return row["active"].(bool)
	})

	// Terminal output that always returns true
	out := &mockOutput{}
	f.SetFilterOutput(out)

	node := Node{Row: Row{"active": true}}
	if !f.Filter(node) {
		t.Fatal("expected filter to pass")
	}
}

func TestFilter_Filter_FailsPredicate(t *testing.T) {
	input := &mockFilterInput{}
	f := NewFilter(input, func(row Row) bool {
		return row["active"].(bool)
	})

	out := &mockOutput{}
	f.SetFilterOutput(out)

	node := Node{Row: Row{"active": false}}
	if f.Filter(node) {
		t.Fatal("expected filter to fail")
	}
}
