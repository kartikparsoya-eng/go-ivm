package ivm

import (
	"encoding/json"
	"fmt"
)

// The Exists operator filters data based on whether or not a relationship
// is non-empty (EXISTS) or empty (NOT EXISTS).

// ExistsType distinguishes EXISTS from NOT EXISTS.
type ExistsType int

const (
	ExistsTypeExists    ExistsType = 0
	ExistsTypeNotExists ExistsType = 1
)

// Exists implements FilterOperator.
type Exists struct {
	input            FilterInput
	relationshipName string
	not              bool
	parentJoinKey    CompoundKey
	noSizeReuse      bool
	cache            map[string]bool
	output           FilterOutput
	inPush           bool
}

func NewExists(
	input FilterInput,
	relationshipName string,
	parentJoinKey CompoundKey,
	existsType ExistsType,
) *Exists {
	schema := input.GetSchema()
	if schema.Relationships[relationshipName] == nil {
		panic(fmt.Sprintf("Input schema missing %s", relationshipName))
	}

	// If the parentJoinKey is the primary key, no sense in trying to reuse.
	noSizeReuse := compoundKeysEqual(parentJoinKey, schema.PrimaryKey)

	e := &Exists{
		input:            input,
		relationshipName: relationshipName,
		not:              existsType == ExistsTypeNotExists,
		parentJoinKey:    parentJoinKey,
		noSizeReuse:      noSizeReuse,
		cache:            make(map[string]bool),
		output:           ThrowFilterOutput,
		inPush:           false,
	}
	input.SetFilterOutput(e)
	return e
}

func (e *Exists) SetFilterOutput(output FilterOutput) {
	e.output = output
}

func (e *Exists) BeginFilter() {
	e.output.BeginFilter()
}

func (e *Exists) EndFilter() {
	e.cache = make(map[string]bool)
	e.output.EndFilter()
}

// Filter — checks if node's relationship exists/not exists, with caching.
func (e *Exists) Filter(node Node) bool {
	var exists bool
	var hasCached bool

	if !e.noSizeReuse && !e.inPush {
		key := e.getCacheKey(node, e.parentJoinKey)
		cached, ok := e.cache[key]
		if ok {
			exists = cached
			hasCached = true
		} else {
			exists = e.fetchExists(node)
			e.cache[key] = exists
			hasCached = true
		}
	}

	if !hasCached {
		exists = e.fetchExists(node)
	}

	result := e.filterWithExists(node, exists) && e.output.Filter(node)
	return result
}

func (e *Exists) Destroy() {
	e.input.Destroy()
}

func (e *Exists) GetSchema() *SourceSchema {
	return e.input.GetSchema()
}

// Push — handles incremental changes through the exists filter.
func (e *Exists) Push(change Change, pusher InputBase) []Change {
	if e.inPush {
		panic("Unexpected re-entrancy")
	}
	e.inPush = true
	defer func() { e.inPush = false }()

	switch change.Type {
	case ChangeTypeAdd, ChangeTypeEdit, ChangeTypeRemove:
		return e.pushWithFilter(change, nil)
	case ChangeTypeChild:
		// Only add/remove child changes for the specific relationship
		// can change the existence condition
		if change.Child.RelationshipName != e.relationshipName ||
			change.Child.Change.Type == ChangeTypeEdit ||
			change.Child.Change.Type == ChangeTypeChild {
			return e.pushWithFilter(change, nil)
		}

		switch change.Child.Change.Type {
		case ChangeTypeAdd:
			size := e.fetchSize(change.Node)
			if size == 1 {
				if e.not {
					// Push remove with empty relationship
					emptyRels := make(map[string]func() []Node)
					for k, v := range change.Node.Relationships {
						emptyRels[k] = v
					}
					emptyRels[e.relationshipName] = func() []Node { return nil }
					return e.output.Push(MakeRemoveChange(Node{
						Row:           change.Node.Row,
						Relationships: emptyRels,
					}), e)
				}
				return e.output.Push(MakeAddChange(change.Node), e)
			}
			existsVal := size > 0
			return e.pushWithFilter(change, &existsVal)

		case ChangeTypeRemove:
			size := e.fetchSize(change.Node)
			if size == 0 {
				if e.not {
					return e.output.Push(MakeAddChange(change.Node), e)
				}
				// Push remove with the removed child included
				withChildRels := make(map[string]func() []Node)
				for k, v := range change.Node.Relationships {
					withChildRels[k] = v
				}
				removedChild := change.Child.Change.Node
				withChildRels[e.relationshipName] = func() []Node { return []Node{removedChild} }
				return e.output.Push(MakeRemoveChange(Node{
					Row:           change.Node.Row,
					Relationships: withChildRels,
				}), e)
			}
		existsVal := size > 0
		return e.pushWithFilter(change, &existsVal)
		default:
			panic("Exists pushChild: unreachable child change type")
		}
	}
	return nil
}

// filterWithExists — applies the exists/not-exists logic.
func (e *Exists) filterWithExists(node Node, exists bool) bool {
	if e.not {
		return !exists
	}
	return exists
}

// getCacheKey — builds a cache key from the node's join key values.
func (e *Exists) getCacheKey(node Node, key CompoundKey) string {
	values := make([]interface{}, len(key))
	for i, k := range key {
		v := node.Row[k]
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v
		}
	}
	b, _ := json.Marshal(values)
	return string(b)
}

// pushWithFilter — pushes change if it passes the exists filter.
func (e *Exists) pushWithFilter(change Change, exists *bool) []Change {
	var ex bool
	if exists != nil {
		ex = *exists
	} else {
		ex = e.fetchExists(change.Node)
	}
	if e.filterWithExists(change.Node, ex) {
		return e.output.Push(change, e)
	}
	return nil
}

// fetchExists — checks if relationship has any nodes.
func (e *Exists) fetchExists(node Node) bool {
	return e.fetchSize(node) > 0
}

// fetchSize — counts nodes in the relationship.
func (e *Exists) fetchSize(node Node) int {
	relationship := node.Relationships[e.relationshipName]
	if relationship == nil {
		panic(fmt.Sprintf("Exists: relationship %q not found on node", e.relationshipName))
	}
	nodes := relationship()
	return len(nodes)
}

// compoundKeysEqual checks if two compound keys are equal.
func compoundKeysEqual(a, b CompoundKey) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
