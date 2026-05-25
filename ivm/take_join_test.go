package ivm

import (
	"fmt"
	"testing"
)

// testOutput collects changes pushed to it.
type testOutput struct {
	changes []Change
}

func (o *testOutput) Push(change Change, pusher InputBase) []Change {
	o.changes = append(o.changes, change)
	return []Change{change}
}

// TestTakeWithJoinBoundaryDisplacement tests that when a Take operator
// has a downstream Join, edits that change the sort key produce correct
// boundary displacement (REMOVE + ADD).
func TestTakeWithJoinBoundaryDisplacement(t *testing.T) {
	// Setup: tickets sorted by updatedAt DESC, id ASC (tiebreaker)
	ticketCols := map[string]string{"id": "string", "updatedAt": "number", "title": "string"}
	ticketSource := NewMemorySource("tickets", ticketCols, []string{"id"})

	// Assignment source for the related join
	assignCols := map[string]string{"id": "string", "ticketId": "string", "userId": "string"}
	assignSource := NewMemorySource("assignments", assignCols, []string{"id"})

	// Load initial data via BulkInsert (simulates hydration snapshot)
	ticketSource.BulkInsert([]Row{
		{"id": "t1", "updatedAt": float64(300), "title": "Ticket 1"},
		{"id": "t2", "updatedAt": float64(200), "title": "Ticket 2"},
		{"id": "t3", "updatedAt": float64(100), "title": "Ticket 3"},
	})
	assignSource.BulkInsert([]Row{
		{"id": "a1", "ticketId": "t3", "userId": "user1"},
	})

	// Sort: updatedAt DESC, id ASC
	ticketSort := Ordering{{"updatedAt", "desc"}, {"id", "asc"}}
	assignSort := Ordering{{"id", "asc"}}

	// splitEditKeys includes "id" (correlation parentField for related join)
	splitEditKeys := map[string]bool{"id": true}

	// Connect sources
	ticketConn := ticketSource.Connect(ticketSort, nil, splitEditKeys)
	assignConn := assignSource.Connect(assignSort, nil, nil)

	// Build pipeline: ticketConn → Take(2) → Join(assignments) → collector
	takeStorage := NewMemoryTakeStorage()
	take := NewTake(ticketConn, takeStorage, 2, nil)

	join := NewJoin(JoinArgs{
		Parent:           take,
		Child:            assignConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"ticketId"},
		RelationshipName: "assignments",
		Hidden:           false,
	})

	// Collector output
	out := &testOutput{}
	join.SetOutput(out)

	// Hydrate by fetching — this triggers initialFetch, setting Take state (size=2, bound=t2)
	fetched := join.Fetch(FetchRequest{})
	if len(fetched) != 2 {
		t.Fatalf("expected 2 fetched nodes, got %d", len(fetched))
	}
	t.Logf("Hydration OK: %v, %v", fetched[0].Row["id"], fetched[1].Row["id"])

	// Clear any collected changes
	out.changes = nil

	// Now push an EDIT that changes t2's updatedAt from 200 → 50
	// In DESC order: t2(50) now sorts AFTER t3(100), so t2 leaves the window, t3 enters
	// The bound is t2{updatedAt:200}. 
	// oldCmp = compareRows(oldRow{updatedAt:200}, bound{updatedAt:200}) = 0
	// newCmp = compareRows(newRow{updatedAt:50}, bound{updatedAt:200}):
	//   CompareValues(50, 200) = -1, negated for desc → +1
	//   Then id tiebreaker: same "t2" = 0
	//   Result: +1 (new row sorts AFTER bound → outside window)
	// This triggers pushEditChange case: oldCmp==0, newCmp > 0 → displacement
	fmt.Printf("\n=== Pushing edit: t2 updatedAt 200→50 ===\n")
	ticketSource.Push(SourceChange{
		Type:   ChangeTypeEdit,
		Row:    Row{"id": "t2", "updatedAt": float64(50), "title": "Ticket 2"},
		OldRow: Row{"id": "t2", "updatedAt": float64(200), "title": "Ticket 2"},
	})

	fmt.Printf("=== Collected %d changes ===\n", len(out.changes))
	for i, c := range out.changes {
		fmt.Printf("  [%d] type=%d row.id=%v row.updatedAt=%v\n", i, c.Type, c.Node.Row["id"], c.Node.Row["updatedAt"])
		if c.OldNode != nil {
			fmt.Printf("       oldRow.id=%v oldRow.updatedAt=%v\n", c.OldNode.Row["id"], c.OldNode.Row["updatedAt"])
		}
	}

	if len(out.changes) < 2 {
		t.Fatalf("expected at least 2 changes (REMOVE + ADD for displacement), got %d", len(out.changes))
	}

	// Verify displacement: REMOVE(t2) + ADD(t3)
	hasRemove := false
	hasAdd := false
	for _, c := range out.changes {
		if c.Type == ChangeTypeRemove {
			hasRemove = true
			if c.Node.Row["id"] != "t2" {
				t.Errorf("expected REMOVE for t2, got %v", c.Node.Row["id"])
			}
		}
		if c.Type == ChangeTypeAdd {
			hasAdd = true
			if c.Node.Row["id"] != "t3" {
				t.Errorf("expected ADD for t3, got %v", c.Node.Row["id"])
			}
		}
	}
	if !hasRemove || !hasAdd {
		t.Fatalf("expected REMOVE+ADD displacement pair, got hasRemove=%v hasAdd=%v", hasRemove, hasAdd)
	}
}
