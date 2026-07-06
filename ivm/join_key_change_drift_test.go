package ivm

import (
	"strings"
	"testing"
)

// buildTestJoin wires a minimal parent⋈child Join for the key-change assert
// tests. Parent "tickets" PK=id, child "assignments" joined on ticketId.
func buildTestJoin(t *testing.T) *Join {
	t.Helper()
	parent := NewMemorySource("tickets", map[string]string{"id": "string", "title": "string"}, []string{"id"})
	child := NewMemorySource("assignments", map[string]string{"id": "string", "ticketId": "string"}, []string{"id"})
	pConn := parent.Connect(Ordering{{"id", "asc"}}, nil, nil)
	cConn := child.Connect(Ordering{{"id", "asc"}}, nil, nil)
	j := NewJoin(JoinArgs{
		Parent:           pConn,
		Child:            cConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"ticketId"},
		RelationshipName: "assignments",
	})
	j.SetOutput(&testOutput{})
	return j
}

// A parent Edit that changes the join key must panic with the plain
// source-drift error (joinKeyChangeError) — TS asserts (throws) here and the
// view-syncer tears the client group down; the panic must be an `error`
// carrying the table + old row's PK so the teardown log is attributable.
func TestJoin_ParentKeyChange_PanicsPlainError(t *testing.T) {
	j := buildTestJoin(t)
	edit := MakeEditChange(
		Node{Row: Row{"id": "t2", "title": "x"}},
		Node{Row: Row{"id": "t1", "title": "x"}},
	)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on parent join-key change, got none")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("expected error panic, got %T: %v", r, r)
		}
		msg := err.Error()
		if !strings.Contains(msg, "source drift: table=tickets") {
			t.Errorf("panic message = %q, want table=tickets", msg)
		}
		if !strings.Contains(msg, "Join-parent-key-change") {
			t.Errorf("panic message = %q, want Join-parent-key-change op", msg)
		}
		if !strings.Contains(msg, "t1") {
			t.Errorf("panic message = %q, want old row's key t1", msg)
		}
	}()
	j.pushParent(edit)
}

// A child Edit that changes the join key likewise panics with the plain error.
func TestJoin_ChildKeyChange_PanicsPlainError(t *testing.T) {
	j := buildTestJoin(t)
	edit := MakeEditChange(
		Node{Row: Row{"id": "a1", "ticketId": "t2"}},
		Node{Row: Row{"id": "a1", "ticketId": "t1"}},
	)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on child join-key change, got none")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("expected error panic, got %T: %v", r, r)
		}
		if !strings.Contains(err.Error(), "source drift: table=assignments") {
			t.Errorf("panic message = %q, want table=assignments", err.Error())
		}
	}()
	j.pushChild(edit)
}

// A FlippedJoin parent Edit that changes the join key panics with the plain error.
func TestFlippedJoin_ParentKeyChange_PanicsPlainError(t *testing.T) {
	parent := NewMemorySource("tickets", map[string]string{"id": "string"}, []string{"id"})
	child := NewMemorySource("assignments", map[string]string{"id": "string", "ticketId": "string"}, []string{"id"})
	pConn := parent.Connect(Ordering{{"id", "asc"}}, nil, nil)
	cConn := child.Connect(Ordering{{"id", "asc"}}, nil, nil)
	fj := NewFlippedJoin(FlippedJoinArgs{
		Parent:           pConn,
		Child:            cConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"ticketId"},
		RelationshipName: "assignments",
	})
	fj.SetOutput(&testOutput{})
	// pushParent's inner-join guard returns early unless the NEW node has a
	// matching child; give the new key (t2) a child so execution reaches the
	// key-change assertion (same reachability as the original raw panic).
	child.BulkInsert([]Row{{"id": "a1", "ticketId": "t2"}})
	edit := MakeEditChange(
		Node{Row: Row{"id": "t2"}}, // new node (Node)
		Node{Row: Row{"id": "t1"}}, // old node (OldNode)
	)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on flipped-join parent key change, got none")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("expected error panic, got %T: %v", r, r)
		}
		if !strings.Contains(err.Error(), "FlippedJoin-parent-key-change") {
			t.Errorf("panic message = %q, want FlippedJoin-parent-key-change op", err.Error())
		}
	}()
	fj.pushParent(edit)
}
