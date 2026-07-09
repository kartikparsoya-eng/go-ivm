package ivm

import (
	"iter"
	"sync/atomic"
	"testing"
)

func TestJoinInProgressChildOverlayStopsChildFetchOnEarlyStop(t *testing.T) {
	parentSrc := NewMemorySource("parents", map[string]string{"id": "string"}, []string{"id"})
	parentSrc.BulkInsert([]Row{
		{"id": "p1"},
		{"id": "p2"},
	})
	parentConn := parentSrc.Connect(Ordering{{"id", "asc"}}, nil, nil)

	childSrc := NewMemorySource("children", map[string]string{
		"id":       "string",
		"parentId": "string",
	}, []string{"id"})
	childSrc.BulkInsert([]Row{
		{"id": "c1", "parentId": "p2"},
		{"id": "c2", "parentId": "p2"},
		{"id": "c3", "parentId": "p2"},
		{"id": "c4", "parentId": "p2"},
	})
	childConn := childSrc.Connect(Ordering{{"id", "asc"}}, nil, nil)
	childCounter := &countingInput{inner: childConn}

	join := NewJoin(JoinArgs{
		Parent:           parentConn,
		Child:            childCounter,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"parentId"},
		RelationshipName: "children",
	})

	remove := MakeRemoveChange(Node{
		Row: Row{"id": "c0", "parentId": "p2"},
	})
	join.inprogressChildChange = &remove
	join.inprogressChildChangePosition = Row{"id": "p1"}
	defer func() {
		join.inprogressChildChange = nil
		join.inprogressChildChangePosition = nil
	}()

	node := join.processParentNode(Row{"id": "p2"}, map[string]func() iter.Seq[Node]{}, nil)
	stream := node.Relationships["children"]()

	var got []string
	stream(func(n Node) bool {
		got = append(got, n.Row["id"].(string))
		return false
	})

	if len(got) != 1 || got[0] != "c0" {
		t.Fatalf("first child = %v, want [c0]", got)
	}
	if pulled := atomic.LoadInt32(&childCounter.rowsReturned); pulled > 1 {
		t.Fatalf("early-stopped overlay stream pulled %d base children, want <= 1", pulled)
	}
}
