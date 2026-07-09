package ivm

// The Cap operator is a count-based limiter for EXISTS subqueries that
// does not require ordering. Unlike Take, it tracks membership by primary
// key set rather than by a sorted bound. This means:
//
//   - No comparator needed (no ordering requirement)
//   - No `start` or `reverse` fetch support
//   - No `#rowHiddenFromFetch` complexity (we can defer when adding to the
//     pk set)
//
// Cap is used in EXISTS child pipelines where only the count of matching
// rows matters, not their order. This allows SQLite to skip ORDER BY
// entirely, enabling much faster query plans.
//
// Cap can count rows globally or by unique value of some partition key
// (same as Take).
//
// Line-faithful port of mono/packages/zql/src/ivm/cap.ts.

import (
	"encoding/json"
	"iter"
	"slices"
)

// CapState tracks the size and tracked-PK set for a given partition.
// Source: cap.ts:25-28 (CapState = {size, pks}).
type CapState struct {
	Size int
	Pks  []string
}

// CapStorage provides get/set/del for cap state keyed by string.
// Source: cap.ts:30-34 (CapStorage narrows the generic operator Storage).
type CapStorage interface {
	GetCapState(key string) *CapState
	SetCapState(key string, state CapState)
	Del(key string)
}

// Cap implements Operator. Source: cap.ts:52-298.
type Cap struct {
	input                  Input
	storage                CapStorage
	limit                  int
	partitionKey           PartitionKey
	partitionKeyComparator Comparator
	primaryKey             []string
	output                 Output
}

// NewCap — Source: cap.ts:62-77 (constructor).
func NewCap(input Input, storage CapStorage, limit int, partitionKey PartitionKey) *Cap {
	if limit < 0 {
		panic("Limit must be non-negative")
	}
	c := &Cap{
		input:        input,
		storage:      storage,
		limit:        limit,
		partitionKey: partitionKey,
		primaryKey:   input.GetSchema().PrimaryKey,
		output:       ThrowOutput,
	}
	if len(partitionKey) > 0 {
		c.partitionKeyComparator = MakePartitionKeyComparator(partitionKey)
	}
	input.SetOutput(c)
	return c
}

func (c *Cap) SetOutput(output Output) {
	c.output = output
}

func (c *Cap) GetSchema() *SourceSchema {
	return c.input.GetSchema()
}

func (c *Cap) Destroy() {
	c.input.Destroy()
}

// Fetch — Source: cap.ts:87-123.
//
// TS's *fetch is a generator, so its asserts run lazily at first
// consumption; mirror that by keeping the whole body inside the returned
// seq (same discipline as Take.initialFetch).
func (c *Cap) Fetch(req FetchRequest) iter.Seq[Node] {
	return func(yield func(Node) bool) {
		if req.Start != nil {
			panic("Cap does not support start")
		}
		if req.Reverse {
			panic("Cap does not support reverse")
		}

		// cap.ts:91-100 — Cap is only built for non-flipped EXISTS
		// subqueries, whose only downstream consumer is a Join that always
		// fetches with a constraint built from the correlation's childField
		// — which is Cap's partition key. So either partitionKey is empty,
		// or the constraint matches it.
		if len(c.partitionKey) > 0 &&
			!(req.Constraint != nil && ConstraintMatchesPartitionKey(req.Constraint, c.partitionKey)) {
			panic("Cap fetch: constraint must match partition key when partitioned")
		}

		capStateKey := GetCapStateKey(c.partitionKey, constraintToRow(req.Constraint))
		capState := c.storage.GetCapState(capStateKey)
		if capState == nil {
			c.initialFetch(req, capStateKey)(yield)
			return
		}
		if capState.Size == 0 {
			return
		}
		// cap.ts:111-122 — PK-based point lookups: fetch each tracked row by
		// its PK directly, rather than scanning the partition and filtering.
		for _, pk := range capState.Pks {
			constraint := deserializePKToConstraint(pk, c.primaryKey)
			for inputNode := range c.input.Fetch(FetchRequest{Constraint: &constraint}) {
				if !yield(inputNode) {
					return
				}
			}
		}
	}
}

// initialFetch — Source: cap.ts:125-175.
func (c *Cap) initialFetch(req FetchRequest, capStateKey string) iter.Seq[Node] {
	return func(yield func(Node) bool) {
		// cap.ts:126-128 — the limit-0 early return runs BEFORE the
		// constraint assert (see the "Cap limit 0" TS regression tests):
		// a limit-0 Cap fetched with a non-partition constraint must not
		// trip the assert, and no capState is ever persisted for it.
		if c.limit == 0 {
			return
		}

		if !ConstraintMatchesPartitionKey(req.Constraint, c.partitionKey) {
			panic("Constraint should match partition key")
		}
		if c.storage.GetCapState(capStateKey) != nil {
			panic("Cap state should be undefined")
		}

		size := 0
		var pks []string
		// Source: cap.ts:143-174 (try/catch/finally).
		//   - downstreamEarlyReturn mirrors the TS flag: it stays true until
		//     the input is fully consumed (exhausted or limit reached). A
		//     downstream consumer that breaks early leaves it true.
		//   - On a panic during iteration TS's exceptionThrown branch skips
		//     setCapState so a partial state is never persisted, then
		//     re-raises. The recover()/re-panic below does the same.
		downstreamEarlyReturn := true
		defer func() {
			if r := recover(); r != nil {
				panic(r)
			}
			c.storage.SetCapState(capStateKey, CapState{Size: size, Pks: pks})
			// cap.ts:165-172: capState must be fully hydrated. If downstream
			// early return ever becomes necessary, remove this panic and
			// instead consume the input stream until limit is reached or the
			// input is exhausted so capState is properly hydrated.
			if downstreamEarlyReturn {
				panic("Cap.initialFetch: unexpected early return prevented full hydration")
			}
		}()
		// Match TS ordering exactly: yield BEFORE recording the pk/size, so
		// a downstream break (yield→false) leaves them untouched — identical
		// to TS where .return() at `yield inputNode` skips the following
		// `pks.push(...); size++`.
		for inputNode := range c.input.Fetch(req) {
			if !yield(inputNode) {
				return
			}
			pks = append(pks, serializePK(inputNode.Row, c.primaryKey))
			size++
			if size == c.limit {
				break
			}
		}
		downstreamEarlyReturn = false
	}
}

// Push — Source: cap.ts:177-258.
func (c *Cap) Push(change Change, pusher InputBase) {
	if change.Type == ChangeTypeEdit {
		c.pushEditChange(change)
		return
	}

	capStateKey := GetCapStateKey(c.partitionKey, change.Node.Row)
	capState := c.storage.GetCapState(capStateKey)
	if capState == nil {
		return
	}

	pk := serializePK(change.Node.Row, c.primaryKey)

	switch change.Type {
	case ChangeTypeAdd:
		if capState.Size < c.limit {
			pks := append(slices.Clone(capState.Pks), pk)
			c.storage.SetCapState(capStateKey, CapState{Size: capState.Size + 1, Pks: pks})
			c.output.Push(change, c)
			return
		}
		// Full — drop (cap.ts:201-202).
		return

	case ChangeTypeRemove:
		pkIndex := slices.Index(capState.Pks, pk)
		if pkIndex == -1 {
			// Not in our set — drop (cap.ts:205-208).
			return
		}
		// Remove from set (cap.ts:209-212).
		pks := slices.Delete(slices.Clone(capState.Pks), pkIndex, pkIndex+1)
		newSize := capState.Size - 1

		// Try to refill: fetch from input with partition constraint, find
		// first row NOT in PK set (cap.ts:214-236).
		pkSet := make(map[string]struct{}, len(pks))
		for _, p := range pks {
			pkSet[p] = struct{}{}
		}
		var constraint *Constraint
		if len(c.partitionKey) > 0 {
			cn := make(Constraint, len(c.partitionKey))
			for _, key := range c.partitionKey {
				cn[key] = change.Node.Row[key]
			}
			constraint = &cn
		}

		var replacement *Node
		for node := range c.input.Fetch(FetchRequest{Constraint: constraint}) {
			nodePK := serializePK(node.Row, c.primaryKey)
			if _, ok := pkSet[nodePK]; !ok {
				n := node
				replacement = &n
				break
			}
		}

		if replacement != nil {
			// cap.ts:238-247 — store state WITHOUT replacement during
			// remove forward, matching Take's pattern of hiding in-flight
			// changes from re-fetches. Then add replacement to the set and
			// forward the add.
			c.storage.SetCapState(capStateKey, CapState{Size: newSize, Pks: pks})
			c.output.Push(change, c)
			replacementPK := serializePK(replacement.Row, c.primaryKey)
			pks = append(pks, replacementPK)
			c.storage.SetCapState(capStateKey, CapState{Size: newSize + 1, Pks: pks})
			c.output.Push(MakeAddChange(*replacement), c)
		} else {
			c.storage.SetCapState(capStateKey, CapState{Size: newSize, Pks: pks})
			c.output.Push(change, c)
		}

	case ChangeTypeChild:
		if slices.Contains(capState.Pks, pk) {
			c.output.Push(change, c)
		}
	}
}

// pushEditChange — Source: cap.ts:260-293.
func (c *Cap) pushEditChange(change Change) {
	if c.partitionKeyComparator != nil &&
		c.partitionKeyComparator(change.OldNode.Row, change.Node.Row) != 0 {
		panic("Unexpected change of partition key")
	}
	capStateKey := GetCapStateKey(c.partitionKey, change.OldNode.Row)
	capState := c.storage.GetCapState(capStateKey)
	if capState == nil {
		return
	}

	oldPK := serializePK(change.OldNode.Row, c.primaryKey)
	if slices.Contains(capState.Pks, oldPK) {
		// Update the PK in our set if it changed (cap.ts:284-289).
		newPK := serializePK(change.Node.Row, c.primaryKey)
		if oldPK != newPK {
			pks := make([]string, len(capState.Pks))
			for i, p := range capState.Pks {
				if p == oldPK {
					pks[i] = newPK
				} else {
					pks[i] = p
				}
			}
			c.storage.SetCapState(capStateKey, CapState{Size: capState.Size, Pks: pks})
		}
		c.output.Push(change, c)
	}
	// If not in our set, drop (cap.ts:292).
}

// GetCapStateKey — Source: cap.ts:300-313. JSON array of "cap" followed by
// the partition values pulled from the row/constraint (same shape as
// GetTakeStateKey with the "take" prefix).
func GetCapStateKey(partitionKey PartitionKey, rowOrConstraint Row) string {
	values := []Value{"cap"}
	if len(partitionKey) > 0 && rowOrConstraint != nil {
		for _, key := range partitionKey {
			values = append(values, rowOrConstraint[key])
		}
	}
	// jsonKeyBytes mirrors JSON.stringify (cap.ts:312): NaN/±Inf → null.
	b, err := jsonKeyBytes(values)
	if err != nil {
		// Unreachable for scalar partition-key values; panic to surface the
		// impossible rather than silently collide cache keys.
		panic("GetCapStateKey: json.Marshal: " + err.Error())
	}
	return string(b)
}

// serializePK — Source: cap.ts:315-317: JSON.stringify of the row's PK
// values in primary-key column order.
func serializePK(row Row, primaryKey []string) string {
	values := make([]Value, len(primaryKey))
	for i, k := range primaryKey {
		values[i] = row[k]
	}
	// jsonKeyBytes mirrors JSON.stringify (cap.ts:316): NaN/±Inf → null.
	b, err := jsonKeyBytes(values)
	if err != nil {
		panic("Cap serializePK: json.Marshal: " + err.Error())
	}
	return string(b)
}

// deserializePKToConstraint — Source: cap.ts:319-329: parse the serialized
// PK back into a point-lookup constraint. json.Unmarshal decodes numbers as
// float64 — the same canonical shape normalized source rows carry, so the
// constraint compares correctly against fetched rows.
func deserializePKToConstraint(pk string, primaryKey []string) Constraint {
	var values []Value
	if err := json.Unmarshal([]byte(pk), &values); err != nil {
		panic("Cap deserializePKToConstraint: " + err.Error())
	}
	constraint := make(Constraint, len(primaryKey))
	for i, k := range primaryKey {
		constraint[k] = values[i]
	}
	return constraint
}

// MemoryCapStorage is a simple in-memory implementation of CapStorage
// (mirrors MemoryTakeStorage; used by tests and the test harness).
type MemoryCapStorage struct {
	states map[string]CapState
}

func NewMemoryCapStorage() *MemoryCapStorage {
	return &MemoryCapStorage{states: make(map[string]CapState)}
}

func (s *MemoryCapStorage) GetCapState(key string) *CapState {
	st, ok := s.states[key]
	if !ok {
		return nil
	}
	return &st
}

func (s *MemoryCapStorage) SetCapState(key string, state CapState) {
	s.states[key] = state
}

func (s *MemoryCapStorage) Del(key string) {
	delete(s.states, key)
}

// States exposes the raw state map for test assertions (storage snapshot
// comparisons ported from cap.push.test.ts).
func (s *MemoryCapStorage) States() map[string]CapState {
	return s.states
}
