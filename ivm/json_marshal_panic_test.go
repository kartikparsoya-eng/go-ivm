package ivm

import (
	"strings"
	"testing"
)

func TestGetTakeStateKey_NonSerializableValue_Panics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on non-serializable partition key value")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "GetTakeStateKey: json.Marshal:") {
			t.Fatalf("wrong panic message: %q", msg)
		}
	}()
	GetTakeStateKey(PartitionKey{"x"}, Row{"x": make(chan int)})
}

func TestGetCacheKey_NonSerializableValue_Panics(t *testing.T) {
	input := &mockFilterInput{
		schema: &SourceSchema{
			TableName:  "parent",
			PrimaryKey: []string{"id", "name"},
			Columns:    map[string]string{"id": "string", "name": "string"},
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
	out := &mockOutput{}
	exists.SetFilterOutput(out)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on non-serializable join key value in getCacheKey")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "getCacheKey: json.Marshal:") {
			t.Fatalf("wrong panic message: %q", msg)
		}
	}()

	bogusNode := Node{
		Row: Row{"id": make(chan int)},
		Relationships: map[string]func() []Node{
			"children": func() []Node { return nil },
		},
	}
	exists.Filter(bogusNode)
}
