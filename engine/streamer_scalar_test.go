package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestStreamer_EditOnScalarSchema_ProducesNoRowChanges(t *testing.T) {
	s := NewStreamer()
	schema := &ivm.SourceSchema{
		TableName:  "t",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string", "val": "string"},
		IsScalar:   true,
	}
	changes := []ivm.Change{
		{Type: ivm.ChangeTypeEdit, Node: ivm.Node{Row: ivm.Row{"id": "a", "val": "new"}}, OldNode: &ivm.Node{Row: ivm.Row{"id": "a", "val": "old"}}},
	}
	s.Accumulate("q", schema, changes)
	result := s.Stream()
	if len(result) != 0 {
		t.Fatalf("expected 0 RowChanges for Edit on IsScalar schema, got %d", len(result))
	}
}

func TestStreamer_AddRemoveOnScalarSchema_AlsoSuppressed(t *testing.T) {
	s := NewStreamer()
	schema := &ivm.SourceSchema{
		TableName:  "t",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string"},
		IsScalar:   true,
	}
	changes := []ivm.Change{
		ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": "a"}}),
		ivm.MakeRemoveChange(ivm.Node{Row: ivm.Row{"id": "b"}}),
	}
	s.Accumulate("q", schema, changes)
	result := s.Stream()
	if len(result) != 0 {
		t.Fatalf("expected 0 RowChanges for Add/Remove on IsScalar schema, got %d", len(result))
	}
}

func TestStreamer_EditOnNonScalarSchema_ProducesRowChange(t *testing.T) {
	s := NewStreamer()
	schema := &ivm.SourceSchema{
		TableName:  "t",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string", "val": "string"},
		IsScalar:   false,
	}
	changes := []ivm.Change{
		{Type: ivm.ChangeTypeEdit, Node: ivm.Node{Row: ivm.Row{"id": "a", "val": "new"}}, OldNode: &ivm.Node{Row: ivm.Row{"id": "a", "val": "old"}}},
	}
	s.Accumulate("q", schema, changes)
	result := s.Stream()
	if len(result) != 1 {
		t.Fatalf("expected 1 RowChange for Edit on non-scalar schema, got %d", len(result))
	}
	if result[0].Type != RowChangeEdit {
		t.Fatalf("expected Edit type, got %d", result[0].Type)
	}
}
