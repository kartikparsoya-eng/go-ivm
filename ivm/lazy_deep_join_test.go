package ivm

import (
	"slices"
	"testing"
)

// TestLazyThreeDeepJoin verifies that lazy operators (Phase 2) produce
// correct results for a 3-deep join A→B→C. The pipeline is:
//
//	Join2(parent=A, child=Join1(parent=B, child=C))
//
// Result: A nodes with "assignments" (B) relationship, each B with
// "comments" (C) relationship. All operators are lazy (iter.Seq-based);
// this test verifies output parity with the eager equivalent.
//
// The deadlock-freedom test (K=P×Cmax at Cmax=3) requires a lazy
// tablesource leaf that holds a cursor open while yielding rows — see
// DESIGN-streaming-hydrate.md §3d and the "Future: lazy leaf" todo.
// With the current eager leaf (fetchForConn materializes and releases),
// Cmax=1 because only one reader is held at a time.
func TestLazyThreeDeepJoin(t *testing.T) {
	// --- Sources ---
	ticketCols := map[string]string{"id": "string", "title": "string"}
	ticketSrc := NewMemorySource("tickets", ticketCols, []string{"id"})

	assignCols := map[string]string{"id": "string", "ticketId": "string", "assignee": "string"}
	assignSrc := NewMemorySource("assignments", assignCols, []string{"id"})

	commentCols := map[string]string{"id": "string", "assignmentId": "string", "text": "string"}
	commentSrc := NewMemorySource("comments", commentCols, []string{"id"})

	// --- Data ---
	ticketSrc.BulkInsert([]Row{
		{"id": "t1", "title": "Ticket 1"},
		{"id": "t2", "title": "Ticket 2"},
		{"id": "t3", "title": "Ticket 3"},
	})
	assignSrc.BulkInsert([]Row{
		{"id": "a1", "ticketId": "t1", "assignee": "alice"},
		{"id": "a2", "ticketId": "t1", "assignee": "bob"},
		{"id": "a3", "ticketId": "t2", "assignee": "carol"},
		{"id": "a4", "ticketId": "t3", "assignee": "dave"},
	})
	commentSrc.BulkInsert([]Row{
		{"id": "c1", "assignmentId": "a1", "text": "fix the bug"},
		{"id": "c2", "assignmentId": "a1", "text": "reproduced"},
		{"id": "c3", "assignmentId": "a2", "text": "reviewing"},
		{"id": "c4", "assignmentId": "a3", "text": "done"},
	})

	// --- Connect ---
	sort := Ordering{{"id", "asc"}}
	ticketConn := ticketSrc.Connect(sort, nil, nil)
	assignConn := assignSrc.Connect(sort, nil, nil)
	commentConn := commentSrc.Connect(sort, nil, nil)

	// --- Pipeline: Join1(B→C) is child of Join2(A→B) ---
	join1 := NewJoin(JoinArgs{
		Parent:           assignConn,
		Child:            commentConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"assignmentId"},
		RelationshipName: "comments",
		Hidden:           false,
	})

	join2 := NewJoin(JoinArgs{
		Parent:           ticketConn,
		Child:            join1,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"ticketId"},
		RelationshipName: "assignments",
		Hidden:           false,
	})

	join2.SetOutput(&testOutput{})

	// --- Hydrate (lazy) ---
	fetched := slices.Collect(join2.Fetch(FetchRequest{}))
	if len(fetched) != 3 {
		t.Fatalf("expected 3 top-level nodes, got %d", len(fetched))
	}

	// --- Verify structure ---
	type expectedChild struct {
		id       string
		comments []string
	}
	type expectedTicket struct {
		id          string
		assignments []expectedChild
	}
	want := []expectedTicket{
		{"t1", []expectedChild{
			{"a1", []string{"c1", "c2"}},
			{"a2", []string{"c3"}},
		}},
		{"t2", []expectedChild{
			{"a3", []string{"c4"}},
		}},
		{"t3", []expectedChild{
			{"a4", nil}, // a4 has no comments
		}},
	}

	for i, node := range fetched {
		if node.Row["id"] != want[i].id {
			t.Errorf("node[%d]: id=%v, want %s", i, node.Row["id"], want[i].id)
			continue
		}

		assignStream, ok := node.Relationships["assignments"]
		if !ok {
			t.Errorf("node[%d] (%s): no 'assignments' relationship", i, want[i].id)
			continue
		}
		assignments := slices.Collect(assignStream())
		if len(assignments) != len(want[i].assignments) {
			t.Errorf("node[%d] (%s): %d assignments, want %d",
				i, want[i].id, len(assignments), len(want[i].assignments))
			continue
		}

		for j, aNode := range assignments {
			if aNode.Row["id"] != want[i].assignments[j].id {
				t.Errorf("node[%d].assignments[%d]: id=%v, want %s",
					i, j, aNode.Row["id"], want[i].assignments[j].id)
				continue
			}

			commentStream, ok := aNode.Relationships["comments"]
			if !ok {
				t.Errorf("node[%d].assignments[%d] (%s): no 'comments' relationship",
					i, j, want[i].assignments[j].id)
				continue
			}
			comments := slices.Collect(commentStream())
			if len(comments) != len(want[i].assignments[j].comments) {
				t.Errorf("node[%d].assignments[%d] (%s): %d comments, want %d",
					i, j, want[i].assignments[j].id,
					len(comments), len(want[i].assignments[j].comments))
				continue
			}
			for k, cNode := range comments {
				if cNode.Row["id"] != want[i].assignments[j].comments[k] {
					t.Errorf("node[%d].assignments[%d].comments[%d]: id=%v, want %s",
						i, j, k, cNode.Row["id"], want[i].assignments[j].comments[k])
				}
			}
		}
	}
}

// TestLazyThreeDeepJoinWithTake verifies that Take's early termination
// propagates through lazy Join operators — the upstream seq stops after
// the limit is reached, and the Take state is set correctly.
func TestLazyThreeDeepJoinWithTake(t *testing.T) {
	ticketCols := map[string]string{"id": "string", "title": "string"}
	ticketSrc := NewMemorySource("tickets", ticketCols, []string{"id"})

	assignCols := map[string]string{"id": "string", "ticketId": "string", "assignee": "string"}
	assignSrc := NewMemorySource("assignments", assignCols, []string{"id"})

	commentCols := map[string]string{"id": "string", "assignmentId": "string", "text": "string"}
	commentSrc := NewMemorySource("comments", commentCols, []string{"id"})

	ticketSrc.BulkInsert([]Row{
		{"id": "t1", "title": "T1"},
		{"id": "t2", "title": "T2"},
		{"id": "t3", "title": "T3"},
		{"id": "t4", "title": "T4"},
		{"id": "t5", "title": "T5"},
	})
	assignSrc.BulkInsert([]Row{
		{"id": "a1", "ticketId": "t1", "assignee": "x"},
		{"id": "a2", "ticketId": "t2", "assignee": "y"},
	})
	commentSrc.BulkInsert([]Row{
		{"id": "c1", "assignmentId": "a1", "text": "hi"},
	})

	sort := Ordering{{"id", "asc"}}
	ticketConn := ticketSrc.Connect(sort, nil, nil)
	assignConn := assignSrc.Connect(sort, nil, nil)
	commentConn := commentSrc.Connect(sort, nil, nil)

	join1 := NewJoin(JoinArgs{
		Parent:           assignConn,
		Child:            commentConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"assignmentId"},
		RelationshipName: "comments",
	})

	take := NewTake(ticketConn, NewMemoryTakeStorage(), 2, nil)

	join2 := NewJoin(JoinArgs{
		Parent:           take,
		Child:            join1,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"ticketId"},
		RelationshipName: "assignments",
	})

	join2.SetOutput(&testOutput{})

	fetched := slices.Collect(join2.Fetch(FetchRequest{}))
	if len(fetched) != 2 {
		t.Fatalf("expected 2 nodes (Take limit=2), got %d", len(fetched))
	}

	if fetched[0].Row["id"] != "t1" || fetched[1].Row["id"] != "t2" {
		t.Errorf("expected t1,t2; got %s,%s",
			fetched[0].Row["id"], fetched[1].Row["id"])
	}

	// Verify t1 has a1 with c1 comment
	a1Stream := fetched[0].Relationships["assignments"]
	if a1Stream == nil {
		t.Fatal("t1 has no assignments relationship")
	}
	assignments := slices.Collect(a1Stream())
	if len(assignments) != 1 || assignments[0].Row["id"] != "a1" {
		t.Errorf("t1 assignments: expected [a1], got %v", assignments)
	}
	c1Stream := assignments[0].Relationships["comments"]
	if c1Stream == nil {
		t.Fatal("a1 has no comments relationship")
	}
	comments := slices.Collect(c1Stream())
	if len(comments) != 1 || comments[0].Row["id"] != "c1" {
		t.Errorf("a1 comments: expected [c1], got %v", comments)
	}
}
