package ivm

import "testing"

// buildTestJoin wires a minimal parent⋈child Join for the key-change drift
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

// A parent Edit that changes the join key must surface as a recoverable
// *DriftError (drop the advance + re-init), NOT a raw string panic that would
// crash the shared sidecar. Mirrors the Take stale-bound contract.
func TestJoin_ParentKeyChange_RaisesDriftError(t *testing.T) {
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
		d, ok := r.(*DriftError)
		if !ok {
			t.Fatalf("expected *DriftError, got %T: %v", r, r)
		}
		if d.Table != "tickets" {
			t.Errorf("DriftError.Table = %q, want tickets", d.Table)
		}
		if d.PK["id"] != "t1" {
			t.Errorf("DriftError.PK[id] = %v, want t1 (old row's key)", d.PK["id"])
		}
	}()
	j.pushParent(edit)
}

// A child Edit that changes the join key likewise raises *DriftError.
func TestJoin_ChildKeyChange_RaisesDriftError(t *testing.T) {
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
		if d, ok := r.(*DriftError); !ok {
			t.Fatalf("expected *DriftError, got %T: %v", r, r)
		} else if d.Table != "assignments" {
			t.Errorf("DriftError.Table = %q, want assignments", d.Table)
		}
	}()
	j.pushChild(edit)
}

// A FlippedJoin parent Edit that changes the join key raises *DriftError.
func TestFlippedJoin_ParentKeyChange_RaisesDriftError(t *testing.T) {
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
		if _, ok := r.(*DriftError); !ok {
			t.Fatalf("expected *DriftError, got %T: %v", r, r)
		}
	}()
	fj.pushParent(edit)
}
