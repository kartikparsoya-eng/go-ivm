package ivm

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
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
//
// Concurrency: TS's IVM runs on a single-threaded JS event loop, so its
// equivalent `#inPush` flag (exists.ts:39, asserted at line 110) catches
// only logical re-entry through the operator graph. Go's parallel push
// (parallel.go) fans the same advance out to per-connection goroutines —
// while distinct connections have distinct Exists instances, a single
// connection's Exists can still be re-entered concurrently when the
// caller (e.g. zero-cache's sidecar RPC layer) handles overlapping
// advance frames or when downstream Fetches happen mid-Push. We
// serialize Push with a mutex and expose inPush via atomic so Filter
// (which runs during Fetch on a separate goroutine) sees a coherent
// read without racing.
type Exists struct {
	input            FilterInput
	relationshipName string
	not              bool
	parentJoinKey    CompoundKey
	noSizeReuse      bool
	cache            map[string]bool
	cacheMu          sync.Mutex
	output           FilterOutput
	inPush           atomic.Bool
	pushMu           sync.Mutex
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
	e.cacheMu.Lock()
	e.cache = make(map[string]bool)
	e.cacheMu.Unlock()
	e.output.EndFilter()
}

// Filter — checks if node's relationship exists/not exists, with caching.
// Cache access is guarded by cacheMu; inPush is read atomically because
// it's also written from Push goroutines (see struct doc).
func (e *Exists) Filter(node Node) bool {
	var exists bool
	var hasCached bool

	if !e.noSizeReuse && !e.inPush.Load() {
		key := e.getCacheKey(node, e.parentJoinKey)
		e.cacheMu.Lock()
		cached, ok := e.cache[key]
		e.cacheMu.Unlock()
		if ok {
			exists = cached
			hasCached = true
		} else {
			exists = e.fetchExists(node)
			e.cacheMu.Lock()
			e.cache[key] = exists
			e.cacheMu.Unlock()
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
// pushMu serializes concurrent Push calls on the same Exists (see
// struct doc); the original `if inPush { panic }` re-entrancy guard
// was misfiring under Go's parallel push fan-out even though TS
// (single-threaded) was safe with the same logic. The mutex preserves
// the invariant the guard was meant to enforce — at most one Push in
// progress at a time on a given Exists — without the panic.
func (e *Exists) Push(change Change, pusher InputBase) []Change {
	e.pushMu.Lock()
	defer e.pushMu.Unlock()
	e.inPush.Store(true)
	defer e.inPush.Store(false)

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
	default:
		panic("Exists.Push: unreachable change type")
	}
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
	b, err := json.Marshal(values)
	if err != nil {
		// Unreachable for scalar join-key values; panic to surface the
		// impossible rather than silently collide cache keys.
		panic("getCacheKey: json.Marshal: " + err.Error())
	}
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
//
// Mirrors exists.ts:241-246. While it seems like this should be able to
// fetch just 1 node to check for exists, we can't because Take does not
// support early return during initial fetch — so we drain the full
// relationship via fetchSize (a counting loop, no slice allocation) rather
// than pulling one element.
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
