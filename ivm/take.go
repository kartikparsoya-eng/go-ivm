package ivm

// The Take operator implements LIMIT queries. It takes the first N nodes
// from its input as determined by the input's comparator. It maintains a
// *bound* of the last accepted item to evaluate new incoming pushes.
//
// Take can count rows globally or by unique value of some partition key.

import (
	"encoding/json"
	"fmt"
	"os"
)

const maxBoundKey = "maxBound"

// TakeState tracks the size and bound for a given partition.
type TakeState struct {
	Size  int
	Bound Row // nil means no bound
}

// TakeStorage provides get/set/del for take state keyed by string.
type TakeStorage interface {
	GetTakeState(key string) *TakeState
	SetTakeState(key string, state TakeState)
	GetMaxBound() Row
	SetMaxBound(bound Row)
	Del(key string)
}

// PartitionKey is a list of column names that define partitioning.
type PartitionKey = []string

// Take implements Operator.
type Take struct {
	input                  Input
	storage                TakeStorage
	limit                  int
	partitionKey           PartitionKey
	partitionKeyComparator Comparator
	rowHiddenFromFetch     Row
	output                 Output
	DebugLabel             string // optional: set to queryID for debug tracing

	// flightRecorder: ring buffer of recent Push + setTakeState events.
	// Dumped on stale-bound panics to identify what corrupted state.
	// Always-on, small bound (32 events) to cap memory cost.
	flightRecorder    [takeFlightRecorderSize]takeEvent
	flightRecorderIdx int
}

const takeFlightRecorderSize = 32

// takeEvent is one entry in Take's flight recorder ring buffer.
type takeEvent struct {
	op       string // "push-add", "push-rem", "push-edit", "set-state"
	rowID    interface{}
	rowSort  interface{} // primary sort-key value (e.g., updatedAt)
	oldRowID interface{}
	oldSort  interface{}
	boundID  interface{}
	boundSort interface{}
	size     int
	cmpInfo  string // free-form annotation (oldCmp, newCmp values)
}

func NewTake(input Input, storage TakeStorage, limit int, partitionKey PartitionKey) *Take {
	if limit < 0 {
		panic("Limit must be non-negative")
	}
	schema := input.GetSchema()
	if schema.Sort == nil {
		panic("Take requires sorted input")
	}
	t := &Take{
		input:        input,
		storage:      storage,
		limit:        limit,
		partitionKey: partitionKey,
		output:       ThrowOutput,
	}
	if len(partitionKey) > 0 {
		t.partitionKeyComparator = MakePartitionKeyComparator(partitionKey)
	}
	input.SetOutput(t)
	return t
}

func (t *Take) SetOutput(output Output) {
	t.output = output
}

func (t *Take) GetSchema() *SourceSchema {
	return t.input.GetSchema()
}

func (t *Take) Destroy() {
	t.input.Destroy()
}

// Fetch — Source: take.ts line 93-156
func (t *Take) Fetch(req FetchRequest) []Node {
	if len(t.partitionKey) == 0 ||
		(req.Constraint != nil && ConstraintMatchesPartitionKey(req.Constraint, t.partitionKey)) {
		takeStateKey := GetTakeStateKey(t.partitionKey, constraintToRow(req.Constraint))
		takeState := t.storage.GetTakeState(takeStateKey)
		if takeState == nil {
			return t.initialFetch(req)
		}
		if takeState.Bound == nil {
			return nil
		}
		var result []Node
		for _, inputNode := range t.input.Fetch(req) {
			if t.GetSchema().CompareRows(takeState.Bound, inputNode.Row) < 0 {
				break
			}
			if t.rowHiddenFromFetch != nil &&
				t.GetSchema().CompareRows(t.rowHiddenFromFetch, inputNode.Row) == 0 {
				continue
			}
			result = append(result, inputNode)
		}
		return result
	}

	// Partition key exists but fetch not constrained on it
	maxBound := t.storage.GetMaxBound()
	if maxBound == nil {
		return nil
	}
	var result []Node
	for _, inputNode := range t.input.Fetch(req) {
		if t.GetSchema().CompareRows(inputNode.Row, maxBound) > 0 {
			break
		}
		takeStateKey := GetTakeStateKey(t.partitionKey, inputNode.Row)
		takeState := t.storage.GetTakeState(takeStateKey)
		if takeState != nil && takeState.Bound != nil &&
			t.GetSchema().CompareRows(takeState.Bound, inputNode.Row) >= 0 {
			result = append(result, inputNode)
		}
	}
	return result
}

// initialFetch — Source: take.ts line 158-216
func (t *Take) initialFetch(req FetchRequest) []Node {
	if t.limit == 0 {
		return nil
	}

	takeStateKey := GetTakeStateKey(t.partitionKey, constraintToRow(req.Constraint))

	// Limit pushdown: when our input is a base source (no intervening Filter/
	// Join/Skip that could drop rows after the source truncates), hint it to
	// stop after t.limit rows so it doesn't materialise the whole table. This is
	// transparent — we keep exactly the same first t.limit rows and record the
	// same bound/size — it just avoids building throwaway Row maps. See
	// FetchRequest.Limit / LeafSource. (Non-leaf inputs leave req.Limit at 0.)
	if _, ok := t.input.(LeafSource); ok {
		req.Limit = t.limit
	}

	var result []Node
	var size int
	var bound Row
	for _, inputNode := range t.input.Fetch(req) {
		result = append(result, inputNode)
		bound = inputNode.Row
		size++
		if size == t.limit {
			break
		}
	}

	t.setTakeState(takeStateKey, size, bound, t.storage.GetMaxBound())
	return result
}

// Push — Source: take.ts line 247-430
func (t *Take) Push(change Change, pusher InputBase) []Change {
	t.debugf("Push type=%d row=%v", change.Type, change.Node.Row["id"])
	if change.Type == ChangeTypeEdit {
		t.recordEvent(takeEvent{
			op:       "push-edit",
			rowID:    change.Node.Row["id"],
			rowSort:  change.Node.Row[t.sortColName()],
			oldRowID: change.OldNode.Row["id"],
			oldSort:  change.OldNode.Row[t.sortColName()],
		})
		return t.pushEditChange(change)
	}

	row := change.Node.Row
	switch change.Type {
	case ChangeTypeAdd:
		t.recordEvent(takeEvent{op: "push-add", rowID: row["id"], rowSort: row[t.sortColName()]})
	case ChangeTypeRemove:
		t.recordEvent(takeEvent{op: "push-rem", rowID: row["id"], rowSort: row[t.sortColName()]})
	}
	takeStateKey, takeState, maxBound, constraint := t.getStateAndConstraint(row)
	if takeState == nil {
		t.debugf("Push type=%d row=%v: takeState==nil, dropping", change.Type, row["id"])
		return nil
	}

	compareRows := t.GetSchema().CompareRows

	switch change.Type {
	case ChangeTypeAdd:
		if takeState.Size < t.limit {
			newBound := takeState.Bound
			if newBound == nil || compareRows(newBound, change.Node.Row) < 0 {
				newBound = change.Node.Row
			}
			t.setTakeState(takeStateKey, takeState.Size+1, newBound, maxBound)
			t.debugf("Push ADD row=%v: size<limit, added (size=%d→%d)", row["id"], takeState.Size, takeState.Size+1)
			return t.output.Push(change, t)
		}
		// size === limit
		if takeState.Bound == nil || compareRows(change.Node.Row, takeState.Bound) >= 0 {
			t.debugf("Push ADD row=%v: DROPPED (outside bound=%v, size=%d)", row["id"], takeState.Bound["id"], takeState.Size)
			return nil
		}
		// added row < bound
		var beforeBoundNode, boundNode *Node
		if t.limit == 1 {
			for _, node := range t.input.Fetch(FetchRequest{
				Start:      &Start{Row: takeState.Bound, Basis: "at"},
				Constraint: constraint,
			}) {
				n := node
				boundNode = &n
				break
			}
		} else {
			for _, node := range t.input.Fetch(FetchRequest{
				Start:      &Start{Row: takeState.Bound, Basis: "at"},
				Constraint: constraint,
				Reverse:    true,
			}) {
				n := node
				if boundNode == nil {
					boundNode = &n
				} else {
					beforeBoundNode = &n
					break
				}
			}
		}
		if boundNode == nil {
			panic("Take: boundNode must be found during fetch")
		}
		removeChange := MakeRemoveChange(*boundNode)
		newBound := change.Node.Row
		if beforeBoundNode != nil && compareRows(change.Node.Row, beforeBoundNode.Row) <= 0 {
			newBound = beforeBoundNode.Row
		}
		t.setTakeState(takeStateKey, takeState.Size, newBound, maxBound)
		var results []Change
		results = append(results, t.pushWithRowHiddenFromFetch(change.Node.Row, removeChange)...)
		results = append(results, t.output.Push(change, t)...)
		return results

	case ChangeTypeRemove:
		if takeState.Bound == nil {
			t.debugf("Push REMOVE row=%v: bound==nil, dropping", row["id"])
			return nil
		}
		compToBound := compareRows(change.Node.Row, takeState.Bound)
		if compToBound > 0 {
			t.debugf("Push REMOVE row=%v: outside bound=%v (cmp=%d), dropping", row["id"], takeState.Bound["id"], compToBound)
			return nil
		}
		t.debugf("Push REMOVE row=%v: inside (cmp=%d), bound=%v size=%d", row["id"], compToBound, takeState.Bound["id"], takeState.Size)

		var beforeBoundNode *Node
		for _, node := range t.input.Fetch(FetchRequest{
			Start:      &Start{Row: takeState.Bound, Basis: "after"},
			Constraint: constraint,
			Reverse:    true,
		}) {
			n := node
			beforeBoundNode = &n
			break
		}

		type newBoundInfo struct {
			node *Node
			push bool
		}
		var newBound *newBoundInfo
		if beforeBoundNode != nil {
			push := compareRows(beforeBoundNode.Row, takeState.Bound) > 0
			newBound = &newBoundInfo{node: beforeBoundNode, push: push}
		}
		if newBound == nil || !newBound.push {
			for _, node := range t.input.Fetch(FetchRequest{
				Start:      &Start{Row: takeState.Bound, Basis: "at"},
				Constraint: constraint,
			}) {
				n := node
				push := compareRows(n.Row, takeState.Bound) > 0
				newBound = &newBoundInfo{node: &n, push: push}
				if push {
					break
				}
			}
		}

		if newBound != nil && newBound.push {
			var results []Change
			results = append(results, t.output.Push(change, t)...)
			t.setTakeState(takeStateKey, takeState.Size, newBound.node.Row, maxBound)
			results = append(results, t.output.Push(MakeAddChange(*newBound.node), t)...)
			return results
		}
		// Match TS take.ts:413-418 — unconditional Size-1, Bound = newBound?.node.row
		// (may be nil). The Take invariant "Size > 0 → Bound != nil" is enforced
		// in TS by the assertions inside pushEditChange ("Bound should be set");
		// the Go-only Size=0 reset diverged the partition's state from TS so
		// subsequent pushes were against a different baseline. (Audit fix B.)
		var boundRow Row
		if newBound != nil {
			boundRow = newBound.node.Row
		}
		t.setTakeState(takeStateKey, takeState.Size-1, boundRow, maxBound)
		return t.output.Push(change, t)

	case ChangeTypeChild:
		if takeState.Bound != nil &&
			compareRows(change.Node.Row, takeState.Bound) <= 0 {
			return t.output.Push(change, t)
		}
		return nil
	}
	return nil
}

// pushEditChange — Source: take.ts line 432-675
func (t *Take) pushEditChange(change Change) []Change {
	if t.partitionKeyComparator != nil &&
		t.partitionKeyComparator(change.OldNode.Row, change.Node.Row) != 0 {
		panic("Unexpected change of partition key")
	}

	takeStateKey, takeState, maxBound, constraint := t.getStateAndConstraint(change.OldNode.Row)
	if takeState == nil {
		t.debugf("pushEdit row=%v: takeState==nil, dropping", change.Node.Row["id"])
		return nil
	}
	if takeState.Bound == nil {
		panic("Bound should be set")
	}

	compareRows := t.GetSchema().CompareRows
	oldCmp := compareRows(change.OldNode.Row, takeState.Bound)
	newCmp := compareRows(change.Node.Row, takeState.Bound)
	t.debugf("pushEdit row=%v oldCmp=%d newCmp=%d bound=%v size=%d", change.Node.Row["id"], oldCmp, newCmp, takeState.Bound["id"], takeState.Size)

	replaceBoundAndForwardChange := func() []Change {
		t.setTakeState(takeStateKey, takeState.Size, change.Node.Row, maxBound)
		return t.output.Push(change, t)
	}

	// Bounds row was changed
	if oldCmp == 0 {
		if newCmp == 0 {
			return t.output.Push(change, t)
		}
		if newCmp < 0 {
			if t.limit == 1 {
				return replaceBoundAndForwardChange()
			}
		var beforeBoundNode *Node
		for _, node := range t.input.Fetch(FetchRequest{
			Start:      &Start{Row: takeState.Bound, Basis: "after"},
			Constraint: constraint,
			Reverse:    true,
		}) {
			n := node
			beforeBoundNode = &n
			break
		}
		if beforeBoundNode == nil {
			// Match TS take.ts:502-505 assertion: at this point (oldCmp==0,
			// newCmp<0, size>1) there must be a row before the bound. If we
			// reach here something is wrong with bound tracking — panic loudly
			// rather than silently fall back, so divergence surfaces.
			// (Porting review HIGH-3.)
			panic("Take.pushEditChange: beforeBoundNode must be found when oldCmp==0, newCmp<0, size>1")
		}
		t.setTakeState(takeStateKey, takeState.Size, beforeBoundNode.Row, maxBound)
		return t.output.Push(change, t)
		}

		// newCmp > 0
		var newBoundNode *Node
		for _, node := range t.input.Fetch(FetchRequest{
			Start:      &Start{Row: takeState.Bound, Basis: "at"},
			Constraint: constraint,
		}) {
			n := node
			newBoundNode = &n
			break
		}
		if newBoundNode == nil {
			panic("Take: newBoundNode must be found during fetch")
		}
		if compareRows(newBoundNode.Row, change.Node.Row) == 0 {
			return replaceBoundAndForwardChange()
		}
		t.setTakeState(takeStateKey, takeState.Size, newBoundNode.Row, maxBound)
		var results []Change
		results = append(results, t.pushWithRowHiddenFromFetch(newBoundNode.Row, MakeRemoveChange(*change.OldNode))...)
		results = append(results, t.output.Push(MakeAddChange(*newBoundNode), t)...)
		return results
	}

	if oldCmp > 0 {
		if newCmp == 0 {
			t.dumpFlightRecorder(fmt.Sprintf(
				"stale-bound (oldCmp>0, newCmp==0): edit row.id=%v sort=%v→%v bound.id=%v boundSort=%v size=%d",
				change.Node.Row["id"], change.OldNode.Row[t.sortColName()], change.Node.Row[t.sortColName()],
				takeState.Bound["id"], takeState.Bound[t.sortColName()], takeState.Size))
			// Self-heal: engine.Advance recovers *DriftError, drops the advance,
			// and TS re-inits the engine from current SQLite truth — bound is
			// repopulated by the fresh hydrate. Replaces the prior crash-the-
			// sidecar behavior that turned a recoverable state corruption into
			// a total outage. Flight recorder above carries the leading-up
			// event sequence for offline root-cause work.
			panic(t.staleBoundDriftError(change, "oldCmp>0, newCmp==0"))
		}
		if newCmp > 0 {
			t.debugf("pushEdit DROPPED (both outside) row=%v bound=%v", change.Node.Row["id"], takeState.Bound["id"])
			return nil
		}
		// old outside, new inside
		var oldBoundNode, newBoundNode *Node
		for _, node := range t.input.Fetch(FetchRequest{
			Start:      &Start{Row: takeState.Bound, Basis: "at"},
			Constraint: constraint,
			Reverse:    true,
		}) {
			n := node
			if oldBoundNode == nil {
				oldBoundNode = &n
			} else {
				newBoundNode = &n
				break
			}
		}
		// Match TS take.ts:594-600 — both oldBoundNode and newBoundNode are
		// asserted non-nil. The TS invariant: when old is outside the bound
		// and new is inside, the fetch (reverse from bound 'at') MUST return
		// at least 2 nodes (the bound itself + the row before it that becomes
		// the new bound). If only 1 node is returned, the source state is
		// inconsistent — TS asserts (throws), Go panics with DriftError so
		// the engine recovers via re-init.
		// Pre-fix audit C had a Go-only fallback that silently bumped Size+1
		// while keeping the stale Bound. That diverged the partition's state
		// from TS, causing subsequent pushes to operate on a wrong baseline.
		// Match TS take.ts:594-600 assertions but signal as DriftError so
		// engine.Advance's recover catches it and triggers re-init from
		// SQLite truth (the same pattern as the other stale-bound sites in
		// pushEditChange). A raw panic here would crash the sidecar and
		// every cg on it; DriftError lets just THIS advance be dropped.
		if oldBoundNode == nil {
			t.dumpFlightRecorder("oldBoundNode missing in pushEditChange oldCmp>0,newCmp<0 fetch")
			panic(t.staleBoundDriftError(change, "oldBoundNode nil (oldCmp>0, newCmp<0)"))
		}
		if newBoundNode == nil {
			t.dumpFlightRecorder("newBoundNode missing in pushEditChange oldCmp>0,newCmp<0 fetch")
			panic(t.staleBoundDriftError(change, "newBoundNode nil (oldCmp>0, newCmp<0)"))
		}
		t.setTakeState(takeStateKey, takeState.Size, newBoundNode.Row, maxBound)
		var results []Change
		results = append(results, t.pushWithRowHiddenFromFetch(change.Node.Row, MakeRemoveChange(*oldBoundNode))...)
		results = append(results, t.output.Push(MakeAddChange(change.Node), t)...)
		return results
	}

	// oldCmp < 0
	if newCmp == 0 {
		t.dumpFlightRecorder(fmt.Sprintf(
			"stale-bound (oldCmp<0, newCmp==0): edit row.id=%v sort=%v→%v bound.id=%v boundSort=%v size=%d",
			change.Node.Row["id"], change.OldNode.Row[t.sortColName()], change.Node.Row[t.sortColName()],
			takeState.Bound["id"], takeState.Bound[t.sortColName()], takeState.Size))
		panic(t.staleBoundDriftError(change, "oldCmp<0, newCmp==0"))
	}
	if newCmp < 0 {
		return t.output.Push(change, t)
	}
	// old inside, new > bound
	var afterBoundNode *Node
	for _, node := range t.input.Fetch(FetchRequest{
		Start:      &Start{Row: takeState.Bound, Basis: "after"},
		Constraint: constraint,
	}) {
		n := node
		afterBoundNode = &n
		break
	}
	if afterBoundNode == nil {
		panic("Take: afterBoundNode must be found during fetch")
	}
	if compareRows(afterBoundNode.Row, change.Node.Row) == 0 {
		return replaceBoundAndForwardChange()
	}
	var results []Change
	results = append(results, t.output.Push(MakeRemoveChange(*change.OldNode), t)...)
	t.setTakeState(takeStateKey, takeState.Size, afterBoundNode.Row, maxBound)
	results = append(results, t.output.Push(MakeAddChange(*afterBoundNode), t)...)
	return results
}

// pushWithRowHiddenFromFetch — Source: take.ts line 677-684
func (t *Take) pushWithRowHiddenFromFetch(row Row, change Change) []Change {
	t.rowHiddenFromFetch = row
	defer func() { t.rowHiddenFromFetch = nil }()
	return t.output.Push(change, t)
}

// setTakeState — Source: take.ts line 686-703
func (t *Take) setTakeState(takeStateKey string, size int, bound Row, maxBound Row) {
	if t.DebugLabel != "" {
		var boundID, oldBoundID interface{}
		if bound != nil {
			boundID = bound["id"]
		}
		oldState := t.storage.GetTakeState(takeStateKey)
		if oldState != nil && oldState.Bound != nil {
			oldBoundID = oldState.Bound["id"]
		}
		oldSize := 0
		if oldState != nil {
			oldSize = oldState.Size
		}
		if boundID != oldBoundID || size != oldSize {
			t.debugf("setState bound=%v→%v size=%d→%d", oldBoundID, boundID, oldSize, size)
		}
	}
	// Flight recorder: record the new state for post-mortem on stale-bound panics.
	t.recordEvent(takeEvent{
		op:        "set-state",
		boundID:   ifNotNil(bound, "id"),
		boundSort: ifNotNil(bound, t.sortColName()),
		size:      size,
	})
	t.storage.SetTakeState(takeStateKey, TakeState{Size: size, Bound: bound})
	if bound != nil {
		if maxBound == nil || t.GetSchema().CompareRows(bound, maxBound) > 0 {
			t.storage.SetMaxBound(bound)
		}
	}
}

// recordEvent appends to the per-Take flight recorder ring buffer. Cheap (no
// allocation) so it's safe to run unconditionally on every Push.
func (t *Take) recordEvent(ev takeEvent) {
	t.flightRecorder[t.flightRecorderIdx%takeFlightRecorderSize] = ev
	t.flightRecorderIdx++
}

// dumpFlightRecorder writes the recent event history to stderr, ordered from
// oldest to newest. Called from the stale-bound panic sites so the failure log
// carries enough state to identify the corrupting sequence.
func (t *Take) dumpFlightRecorder(label string) {
	fmt.Fprintf(os.Stderr, "\n[TAKE-FLIGHT-RECORDER] panic-context: %s\n", label)
	fmt.Fprintf(os.Stderr, "  limit=%d partitionKey=%v\n", t.limit, t.partitionKey)
	n := takeFlightRecorderSize
	if t.flightRecorderIdx < n {
		n = t.flightRecorderIdx
	}
	start := t.flightRecorderIdx - n
	for i := 0; i < n; i++ {
		ev := t.flightRecorder[(start+i)%takeFlightRecorderSize]
		fmt.Fprintf(os.Stderr,
			"  [%2d] op=%-10s row=%v sort=%v old=%v oldSort=%v bound=%v boundSort=%v size=%d %s\n",
			i, ev.op, ev.rowID, ev.rowSort, ev.oldRowID, ev.oldSort,
			ev.boundID, ev.boundSort, ev.size, ev.cmpInfo)
	}
	fmt.Fprintf(os.Stderr, "[TAKE-FLIGHT-RECORDER] end\n\n")
}

// sortColName returns the primary sort column for this Take (first column in
// the schema's Sort). Used by the flight recorder to capture sort-key values
// alongside row IDs. Falls back to empty string if Sort is missing.
func (t *Take) sortColName() string {
	if schema := t.input.GetSchema(); schema != nil && len(schema.Sort) > 0 {
		return schema.Sort[0][0]
	}
	return ""
}

// ifNotNil returns row[col] if row != nil, else nil. Helper for the flight
// recorder so it doesn't NPE on nil-bound transitions.
func ifNotNil(row Row, col string) interface{} {
	if row == nil {
		return nil
	}
	return row[col]
}

// staleBoundDriftError constructs a *DriftError that the engine's recover
// will treat as a recoverable signal (drop in-flight advance, re-init from
// SQLite truth) rather than a crash-the-sidecar panic. Op="Edit-stale-bound"
// distinguishes Take's invariant violation from the MemorySource Add/Edit/
// Remove drift cases.
func (t *Take) staleBoundDriftError(change Change, marker string) *DriftError {
	schema := t.input.GetSchema()
	pk := map[string]Value{}
	if schema != nil && change.OldNode != nil {
		for _, col := range schema.PrimaryKey {
			pk[col] = change.OldNode.Row[col]
		}
	}
	table := ""
	if schema != nil {
		table = schema.TableName
	}
	return &DriftError{
		Table:    table,
		Op:       "Edit-stale-bound (" + marker + ")",
		PK:       pk,
		HasCount: -1, // not meaningful for Take-level drift
	}
}

// getStateAndConstraint — Source: take.ts line 218-245
func (t *Take) getStateAndConstraint(row Row) (string, *TakeState, Row, *Constraint) {
	takeStateKey := GetTakeStateKey(t.partitionKey, row)
	takeState := t.storage.GetTakeState(takeStateKey)
	if takeState == nil {
		return takeStateKey, nil, nil, nil
	}
	maxBound := t.storage.GetMaxBound()
	var constraint *Constraint
	if len(t.partitionKey) > 0 {
		c := make(Constraint)
		for _, key := range t.partitionKey {
			c[key] = row[key]
		}
		constraint = &c
	}
	return takeStateKey, takeState, maxBound, constraint
}

// GetTakeStateKey — Source: take.ts line 710-725
func GetTakeStateKey(partitionKey PartitionKey, rowOrConstraint Row) string {
	values := []Value{"take"}
	if len(partitionKey) > 0 && rowOrConstraint != nil {
		for _, key := range partitionKey {
			values = append(values, rowOrConstraint[key])
		}
	}
	b, _ := json.Marshal(values)
	return string(b)
}

// ConstraintMatchesPartitionKey — Source: take.ts line 727-743
//
// Note on duplicate-column compound correlations: a correlation like
// parentField:["id","id"] + childField:["channelId","channelId"] (the
// "compositeKey" alias Zero emits for composite-key tables) reaches Take
// with partitionKey=["channelId","channelId"] — duplicated. The matching
// constraint comes from BuildJoinConstraint, which writes through a
// map[string]Value so it deduplicates to {"channelId": v}. A raw length
// compare (len(partitionKey)=2 vs len(constraint)=1) used to return false
// here, causing Take.Fetch to skip its takeState path and fall through to
// the maxBound branch which returns nil on first hydrate — producing 0
// child rows in the Join, EXISTS false on every parent, and the channels
// MISMATCH observed in the 2026-05-27 shadow-tablesrc soak. The fix is to
// compare against the DISTINCT column set, matching what BuildJoinConstraint
// produces.
func ConstraintMatchesPartitionKey(constraint *Constraint, partitionKey PartitionKey) bool {
	if constraint == nil && len(partitionKey) == 0 {
		return true
	}
	if constraint == nil || len(partitionKey) == 0 {
		return false
	}
	distinct := make(map[string]struct{}, len(partitionKey))
	for _, key := range partitionKey {
		distinct[key] = struct{}{}
	}
	if len(distinct) != len(*constraint) {
		return false
	}
	for key := range distinct {
		if _, ok := (*constraint)[key]; !ok {
			return false
		}
	}
	return true
}

// MakePartitionKeyComparator — Source: take.ts line 745-757
func MakePartitionKeyComparator(partitionKey PartitionKey) Comparator {
	return func(a, b Row) int {
		for _, key := range partitionKey {
			cmp := CompareValues(a[key], b[key])
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	}
}

// constraintToRow converts a Constraint pointer to a Row for key generation.
func constraintToRow(c *Constraint) Row {
	if c == nil {
		return nil
	}
	return Row(*c)
}

// MemoryTakeStorage is a simple in-memory implementation of TakeStorage.
type MemoryTakeStorage struct {
	states   map[string]TakeState
	maxBound Row
}

func NewMemoryTakeStorage() *MemoryTakeStorage {
	return &MemoryTakeStorage{states: make(map[string]TakeState)}
}

func (s *MemoryTakeStorage) GetTakeState(key string) *TakeState {
	st, ok := s.states[key]
	if !ok {
		return nil
	}
	return &st
}

func (s *MemoryTakeStorage) SetTakeState(key string, state TakeState) {
	s.states[key] = state
}

func (s *MemoryTakeStorage) GetMaxBound() Row {
	return s.maxBound
}

func (s *MemoryTakeStorage) SetMaxBound(bound Row) {
	s.maxBound = bound
}

func (s *MemoryTakeStorage) Del(key string) {
	delete(s.states, key)
}

// debugf logs a debug message if DebugLabel is set.
func (t *Take) debugf(format string, args ...interface{}) {
	if t.DebugLabel == "" {
		return
	}
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "[TAKE-DEBUG][%s] limit=%d %s\n", t.DebugLabel, t.limit, msg)
}

// Ensure unused import doesn't cause issues
var _ = fmt.Sprintf
