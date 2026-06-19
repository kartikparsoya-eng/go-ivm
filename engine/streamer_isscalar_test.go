package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Test for audit fix #9: the Edit path in streamChanges must skip schemas
// with IsScalar=true, matching the Add/Remove path (streamNodesInto :180).
// Pre-fix, an Edit on a scalar CSQ row was emitted while Add/Remove for the
// same schema were suppressed — an inconsistency.

func TestStreamChanges_EditSkippedForIsScalar(t *testing.T) {
	scalarSchema := &ivm.SourceSchema{
		TableName:  "scalar_csq",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string", "val": "number"},
		IsScalar:   true,
	}

	// An Edit change on the scalar schema.
	editChange := ivm.Change{
		Type:    ivm.ChangeTypeEdit,
		Node:    ivm.Node{Row: ivm.Row{"id": "r1", "val": 42}},
		OldNode: &ivm.Node{Row: ivm.Row{"id": "r1", "val": 41}},
	}

	result := streamChanges("q1", scalarSchema, []ivm.Change{editChange})
	if len(result) != 0 {
		t.Fatalf("expected 0 RowChanges for IsScalar Edit, got %d: %+v", len(result), result)
	}
}

func TestStreamChanges_AddSkippedForIsScalar(t *testing.T) {
	scalarSchema := &ivm.SourceSchema{
		TableName:  "scalar_csq",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string"},
		IsScalar:   true,
	}

	addChange := ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": "r1"}})

	result := streamChanges("q1", scalarSchema, []ivm.Change{addChange})
	if len(result) != 0 {
		t.Fatalf("expected 0 RowChanges for IsScalar Add, got %d", len(result))
	}
}

func TestStreamChanges_EditEmittedForNonScalar(t *testing.T) {
	normalSchema := &ivm.SourceSchema{
		TableName:  "normal_table",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string", "val": "number"},
		IsScalar:   false,
	}

	editChange := ivm.Change{
		Type:    ivm.ChangeTypeEdit,
		Node:    ivm.Node{Row: ivm.Row{"id": "r1", "val": 42}},
		OldNode: &ivm.Node{Row: ivm.Row{"id": "r1", "val": 41}},
	}

	result := streamChanges("q1", normalSchema, []ivm.Change{editChange})
	if len(result) != 1 {
		t.Fatalf("expected 1 RowChange for non-scalar Edit, got %d", len(result))
	}
	if result[0].Type != RowChangeEdit {
		t.Fatalf("expected RowChangeEdit, got %d", result[0].Type)
	}
	if result[0].RowKey["id"] != "r1" {
		t.Fatalf("expected rowKey id=r1, got %v", result[0].RowKey["id"])
	}
}
