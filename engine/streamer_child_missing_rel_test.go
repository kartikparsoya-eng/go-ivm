package engine

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// F2 from the streaming/advance audit: TS's #streamChanges resolves a CHILD
// change's relationship with must(schema.relationships[child.relationshipName])
// (pipeline-driver.ts:2798-2802) — a CHILD change naming a relationship its
// schema doesn't know is a pipeline-construction bug and TS THROWS, tearing
// down the client group. Go's old `if childSchema != nil` guard silently
// DROPPED the descendant change instead: silent data loss where TS fails loud,
// violating the Go-fails-iff-TS-fails invariant — and internally inconsistent
// with streamNodesInto, which already panics for the identical class
// (streamer.go "missing from schema" tripwire).

func childChangeStreamPanics(t *testing.T, schema *ivm.SourceSchema, change ivm.Change, wantSubstr string) {
	t.Helper()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		streamChanges("q", schema, []ivm.Change{change})
	}()
	if recovered == nil {
		t.Fatalf("expected streamChanges to panic (TS must() throws, pipeline-driver.ts:2800); it silently dropped the CHILD change instead")
	}
	msg, ok := recovered.(string)
	if !ok {
		t.Fatalf("expected string panic, got %T: %v", recovered, recovered)
	}
	if !strings.Contains(msg, wantSubstr) {
		t.Fatalf("panic message %q missing %q", msg, wantSubstr)
	}
}

// TestStreamChanges_ChildMissingRelationshipPanics: a CHILD change whose
// relationshipName is absent from schema.Relationships must panic, not skip.
func TestStreamChanges_ChildMissingRelationshipPanics(t *testing.T) {
	schema := &ivm.SourceSchema{
		TableName:  "parent",
		PrimaryKey: []string{"id"},
		// No relationships at all — "ghost" cannot resolve.
	}
	change := ivm.MakeChildChange(
		ivm.Node{Row: ivm.Row{"id": "p1"}},
		ivm.ChildData{
			RelationshipName: "ghost",
			Change:           ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": "c1"}}),
		},
	)
	childChangeStreamPanics(t, schema, change, `relationship "ghost" missing from schema`)
}

// TestStreamChanges_ChildNilPayloadPanics: a ChangeTypeChild carrying no Child
// payload is malformed — TS would TypeError reading child.relationshipName of
// undefined (pipeline-driver.ts:2799-2801); Go must fail loud, not skip.
func TestStreamChanges_ChildNilPayloadPanics(t *testing.T) {
	schema := &ivm.SourceSchema{
		TableName:  "parent",
		PrimaryKey: []string{"id"},
	}
	change := ivm.Change{Type: ivm.ChangeTypeChild, Node: ivm.Node{Row: ivm.Row{"id": "p1"}}}
	childChangeStreamPanics(t, schema, change, "no child payload")
}
