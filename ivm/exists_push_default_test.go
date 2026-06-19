package ivm

import "testing"

func TestExists_Push_UnknownChangeType_Panics(t *testing.T) {
	input := &mockFilterInput{
		schema: &SourceSchema{
			TableName:  "parent",
			PrimaryKey: []string{"id"},
			Columns:    map[string]string{"id": "string"},
			Relationships: map[string]*SourceSchema{
				"children": {
					TableName:  "child",
					PrimaryKey: []string{"id"},
					Columns:    map[string]string{"id": "string", "parentId": "string"},
				},
			},
		},
	}
	exists := NewExists(input, "children", CompoundKey{"id"}, ExistsTypeExists)

	// Wire a collecting output so Push can forward.
	out := &mockOutput{}
	exists.SetFilterOutput(out)

	// A bogus ChangeType (99) that no case matches.
	bogusChange := Change{
		Type: ChangeType(99),
		Node: Node{Row: Row{"id": "x"}},
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on unknown ChangeType, but Push returned normally")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if msg != "Exists.Push: unreachable change type" {
			t.Fatalf("wrong panic message: %q", msg)
		}
	}()
	exists.Push(bogusChange, input)
}
