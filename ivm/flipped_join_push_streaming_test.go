package ivm

import (
	"sync/atomic"
	"testing"
)

type panicOnFirstFlippedJoinOutput struct {
	pushes    int32
	panicWith any
}

func (o *panicOnFirstFlippedJoinOutput) Push(Change, InputBase) {
	atomic.AddInt32(&o.pushes, 1)
	panic(o.panicWith)
}

func TestFlippedJoinPushChildStreamsParentFetch(t *testing.T) {
	parent := NewMemorySource("parent",
		map[string]string{"id": "string", "group": "string"},
		[]string{"id"})
	child := NewMemorySource("child",
		map[string]string{"id": "string", "group": "string"},
		[]string{"id"})

	for i := 1; i <= 5; i++ {
		parent.BulkInsert([]Row{{"id": pID(i), "group": "g"}})
	}

	parentCounter := &countingInput{
		inner: parent.Connect(Ordering{{"id", "asc"}}, nil, nil),
	}
	fj := NewFlippedJoin(FlippedJoinArgs{
		Parent:           parentCounter,
		Child:            child.Connect(Ordering{{"id", "asc"}}, nil, nil),
		ParentKey:        CompoundKey{"group"},
		ChildKey:         CompoundKey{"group"},
		RelationshipName: "children",
		System:           "client",
	})

	const sentinel = "stop after first parent"
	out := &panicOnFirstFlippedJoinOutput{panicWith: sentinel}
	fj.SetOutput(out)

	func() {
		defer func() {
			if r := recover(); r != sentinel {
				t.Fatalf("panic = %v, want %q", r, sentinel)
			}
		}()
		fj.pushChild(MakeAddChange(Node{Row: Row{"id": "c-new", "group": "g"}}))
		t.Fatal("pushChild returned without downstream panic")
	}()

	if pushes := atomic.LoadInt32(&out.pushes); pushes != 1 {
		t.Fatalf("output pushes = %d, want 1", pushes)
	}
	if pulled := atomic.LoadInt32(&parentCounter.rowsReturned); pulled != 1 {
		t.Fatalf("parent rows pulled before first downstream panic = %d, want 1", pulled)
	}
}
