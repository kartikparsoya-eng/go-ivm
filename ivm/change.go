package ivm


// ChangeType mirrors change-type-enum.ts
type ChangeType int

const (
	ChangeTypeAdd    ChangeType = 0 // ADD = 0
	ChangeTypeRemove ChangeType = 1 // REMOVE = 1
	ChangeTypeEdit   ChangeType = 2 // EDIT = 2
	ChangeTypeChild  ChangeType = 3 // CHILD = 3
)

// Change is the discriminated union of change types.
// In TS this is a tuple [type, node, extra]. We use a struct with a type tag.
type Change struct {
	Type ChangeType
	Node Node

	// For EditChange: the old node (change.ts line 57)
	OldNode *Node

	// For ChildChange: the child data (change.ts line 7-10)
	Child *ChildData
}

// ChildData is the payload carried by a ChildChange.
type ChildData struct {
	RelationshipName string
	Change           Change
}

func MakeAddChange(node Node) Change {
	return Change{Type: ChangeTypeAdd, Node: node}
}

func MakeRemoveChange(node Node) Change {
	return Change{Type: ChangeTypeRemove, Node: node}
}

func MakeChildChange(node Node, child ChildData) Change {
	return Change{Type: ChangeTypeChild, Node: node, Child: &child}
}

func MakeEditChange(node Node, oldNode Node) Change {
	return Change{Type: ChangeTypeEdit, Node: node, OldNode: &oldNode}
}
