package ivm

// MemorySource: in-memory root data provider for IVM pipelines. Holds rows
// sorted by primary key in a slice, supports multiple connections (each with
// its own sort + filter), and uses an atomic.Pointer overlay during pushes
// so concurrent fetches see consistent state.

import (
	"encoding/json"
	"fmt"
	"iter"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// SourceChange represents a change at the source level (row-only, no relationships).
// TS uses tuple: [type, row, extra/oldRow]
type SourceChange struct {
	Type   ChangeType
	Row    Row
	OldRow Row // only for Edit
}

// SourceDriftError builds the plain error a source-state divergence panics
// with: an Edit/Remove against a missing row, or a duplicate Add — the direct
// port of TS's genPush asserts (memory-source.ts:529-550) and TableSource's
// push asserts. TS throws an Error here and the view-syncer tears the client
// group down; the Go panic gets the identical disposition (propagates out of
// the engine → RPC error → 'unclassified' → rethrow → teardown). The message
// format is the old *DriftError.Error() text, kept byte-identical so log
// greps survive the type change. Shared by MemorySource (test fixture),
// tablesource.Source (prod leaf), Take's stale-bound asserts, join key-change
// asserts, and operator-storage failures.
func SourceDriftError(table, op string, pk map[string]Value, hasCount int) error {
	return fmt.Errorf(
		"source drift: table=%s op=%s pk=%v has_count=%d",
		table, op, pk, hasCount,
	)
}

func MakeSourceChangeAdd(row Row) SourceChange {
	return SourceChange{Type: ChangeTypeAdd, Row: row}
}

func MakeSourceChangeRemove(row Row) SourceChange {
	return SourceChange{Type: ChangeTypeRemove, Row: row}
}

func MakeSourceChangeEdit(row, oldRow Row) SourceChange {
	return SourceChange{Type: ChangeTypeEdit, Row: row, OldRow: oldRow}
}

// Overlay represents a pending change visible during fetch.
type Overlay struct {
	Epoch  int
	Change SourceChange
}

// Overlays holds the add/remove rows derived from an overlay.
type Overlays struct {
	Add    Row
	Remove Row
}

// Connection represents a downstream consumer of the source.
type Connection struct {
	Input           Input
	Output          Output
	Sort            Ordering
	SplitEditKeys   map[string]bool
	CompareRows     Comparator
	FilterPredicate func(Row) bool
	// LastPushedEpoch is the most recent pushEpoch delivered to this
	// connection — bumped immediately before the connection's FilterPush,
	// per connection, exactly like TS (memory-source.ts:625-629). Atomic
	// because GenPushParallel bumps it on each connection's own fan-out
	// goroutine while another connection's pipeline may concurrently fetch
	// through this connection (computeOverlays reads it) — e.g. a self-join
	// whose parent and child conns land on different fan-out goroutines.
	LastPushedEpoch atomic.Int64
	ConnID          int // unique ID for debug tracing
}

// MemorySource implements the Source interface.
//
// Concurrency model:
//   - `connections` is guarded by `connsMu` (RWMutex). Push paths take RLock
//     to snapshot the slice; Connect/Disconnect take Lock for mutation. This
//     hardens MEDIUM-1 from the parallelism review: the invariant that
//     Connect/Disconnect only run under Engine.mu was load-bearing but
//     fragile.
//   - `overlay` is an atomic.Pointer so cross-goroutine reads during parallel
//     fan-out have a documented happens-before edge (MEDIUM-5 from the
//     parallelism review). Push paths Store; Fetch paths Load.
type MemorySource struct {
	tableName         string
	columns           map[string]string
	primaryKey        []string
	primarySort       Ordering
	data              []Row // sorted by primarySort comparator
	comparator        Comparator
	connsMu           sync.RWMutex
	connections       []*Connection
	overlay           atomic.Pointer[Overlay]
	pushEpoch         int
	parallel          bool
	parallelThreshold int // min connections to trigger parallel (default 2)
	nextConnID        int
	// converter, if set, replaces the default partial-coverage NormalizeRow
	// behavior with a per-column conversion (e.g. sqlite.FromSQLiteType).
	// REVIEW-ts-integration CRITICAL-3 / REVIEW-porting MEDIUM-2.
	converter ValueConverter
	// batchState tracks, per PK written in the current advance batch, the row
	// the source data now holds for that PK (nil = removed/absent). Last
	// write wins: trackAdded overwrites trackRemoved and vice versa, so the
	// entry always mirrors what TS's lazy diff would read from the mutated
	// prev snapshot at this point in the batch (see resolveBatchChange).
	// Allocated lazily; cleared by ClearBatchState at the true batch boundary
	// (engine.signalAdvanceEnd).
	batchState map[string]Row
}

// ValueConverter coerces a raw decoded value to its canonical Value for the
// given column type. Injected at construction so the ivm package doesn't have
// to depend on sqlite-specific decoding logic.
type ValueConverter func(v interface{}, colType string) Value

func NewMemorySource(tableName string, columns map[string]string, primaryKey []string) *MemorySource {
	return NewMemorySourceWithConverter(tableName, columns, primaryKey, nil)
}

// NewMemorySourceWithConverter constructs a MemorySource that normalizes
// rows via the supplied converter on every NormalizeRow call. Pass
// sqlite.FromSQLiteType to get full coverage of string / json / blob /
// number / boolean conversions matching TableSource.
func NewMemorySourceWithConverter(
	tableName string,
	columns map[string]string,
	primaryKey []string,
	converter ValueConverter,
) *MemorySource {
	// V5: an empty primary key is a programmer/config error, not a runtime
	// condition. With no PK, primarySort and the comparator degenerate and
	// every row's extracted key map is empty — all rows silently collide on
	// the empty key, producing corruption that surfaces far downstream as
	// mysterious dedup/order bugs instead of a clear failure at the source.
	// Fail fast at construction (init path, never the hot path) rather than
	// corrupt silently. A table with no PK cannot be an IVM source.
	if len(primaryKey) == 0 {
		panic(fmt.Sprintf("NewMemorySourceWithConverter: table %q has an empty primary key; an IVM source requires a non-empty PK", tableName))
	}
	primarySort := make(Ordering, len(primaryKey))
	for i, k := range primaryKey {
		primarySort[i] = [2]string{k, "asc"}
	}
	return &MemorySource{
		tableName:   tableName,
		columns:     columns,
		primaryKey:  primaryKey,
		primarySort: primarySort,
		comparator:  MakeComparator(primarySort, false),
		converter:   converter,
	}
}

// Connect creates a new connection with the given sort and optional filter.
// Takes connsMu write lock to mutate connections; safe against concurrent push
// (which only takes RLock-style reads of a snapshotted slice).
func (ms *MemorySource) Connect(connSort Ordering, filterPredicate func(Row) bool, splitEditKeys map[string]bool) *SourceInput {
	// memory-source.ts:168 — "unordered" means the caller passed no sort and
	// we default to the primary index sort (which trivially includes the PK).
	unordered := connSort == nil
	if connSort == nil {
		connSort = ms.primarySort
	}
	// memory-source.ts:198-200 — an explicit sort must include every PK
	// column or the connection's comparator is not total.
	if !unordered {
		AssertOrderingIncludesPK(connSort, ms.primaryKey)
	}
	compareRows := MakeComparator(connSort, false)

	ms.connsMu.Lock()
	ms.nextConnID++
	connID := ms.nextConnID
	conn := &Connection{
		Sort:            connSort,
		SplitEditKeys:   splitEditKeys,
		CompareRows:     compareRows,
		FilterPredicate: filterPredicate,
		ConnID:          connID,
	}

	si := &SourceInput{
		source: ms,
		conn:   conn,
		schema: &SourceSchema{
			TableName:     ms.tableName,
			Columns:       ms.columns,
			PrimaryKey:    ms.primaryKey,
			Sort:          connSort,
			System:        "client",
			Relationships: map[string]*SourceSchema{},
			CompareRows:   compareRows,
		},
	}
	conn.Input = si
	ms.connections = append(ms.connections, conn)
	ms.connsMu.Unlock()
	return si
}

// Disconnect removes a connection. Takes connsMu write lock.
func (ms *MemorySource) Disconnect(si *SourceInput) {
	ms.connsMu.Lock()
	defer ms.connsMu.Unlock()
	for i, c := range ms.connections {
		if c == si.conn {
			ms.connections = append(ms.connections[:i], ms.connections[i+1:]...)
			return
		}
	}
	// memory-source.ts:207 — assert(idx !== -1): a double-disconnect or a
	// disconnect of a never-connected input is a pipeline-lifecycle bug;
	// silently ignoring it would mask double-Destroy paths.
	panic("Connection not found")
}

// Push applies a source change: pushes to all connections, then writes.
// If parallel mode is enabled and there are enough connections, uses goroutine fan-out.
//
// The parallel-threshold check below reads len(ms.connections) without
// connsMu. Safe today because all Push callers hold engine.mu, which
// also gates Connect/Disconnect — so connections can't be mutated
// concurrently. We take RLock anyway: the invariant lives in the
// engine layer (not visible here), and one tiny RLock per Push is
// cheaper than a future regression where someone adds a Disconnect
// path outside engine.mu and silently races a slice-header read.
func (ms *MemorySource) Push(change SourceChange) {
	ms.connsMu.RLock()
	useParallel := ms.parallel && len(ms.connections) >= ms.parallelThreshold
	ms.connsMu.RUnlock()
	if useParallel {
		ms.genPushAndWriteParallel(change)
		return
	}
	ms.genPushAndWriteWithSplitEdit(change)
}

// genPushAndWriteWithSplitEdit — Source: memory-source.ts line 452-506
func (ms *MemorySource) genPushAndWriteWithSplitEdit(change SourceChange) {
	// Resolve intra-batch staleness BEFORE the split decision: TS's split
	// logic (memory-source.ts:452-506) receives the Edit with the OldRow the
	// diff lazily read from the mutated prev, so the split-key comparison
	// must run against the batch-current value, not the eager-collected one.
	var skip bool
	if change, skip = ms.resolveBatchChange(change); skip {
		return
	}
	shouldSplit := false
	if change.Type == ChangeTypeEdit {
		// connsMu guards the slice header against Connect/Disconnect
		// torn-reads; even with the engine.mu invariant in production,
		// defensive locking here keeps the function self-correct.
		ms.connsMu.RLock()
		for _, conn := range ms.connections {
			if conn.SplitEditKeys != nil {
				for key := range conn.SplitEditKeys {
					if !ValuesEqual(change.Row[key], change.OldRow[key]) {
						shouldSplit = true
						break
					}
				}
			}
			if shouldSplit {
				break
			}
		}
		ms.connsMu.RUnlock()
	}

	if change.Type == ChangeTypeEdit && shouldSplit {
		ms.genPushAndWrite(MakeSourceChangeRemove(change.OldRow))
		ms.genPushAndWrite(MakeSourceChangeAdd(change.Row))
		return
	}
	ms.genPushAndWrite(change)
}

// resolveBatchChange substitutes the change's prev-side values with the
// batch-current state for any PK already written in this advance batch,
// reproducing TS's diff-layer lazy iteration: TS reads prevValues from the
// prev snapshot AFTER earlier writeChanges in the same batch mutated it
// (snapshotter.ts:519-544), while Go's diff collects all entries eagerly
// from the clean prev. For a PK untouched this batch the collected value IS
// the current value, so the change passes through unchanged — and genuine
// drift (an untouched PK contradicting source state) still reaches
// genPush's asserts and panics.
//
// Cases (skip=true ⇒ drop the change entirely):
//   - Edit, OldRow PK removed in-batch  → Add(Row)        (TS: prevValues=[] → Add)
//   - Edit, OldRow PK written in-batch  → Edit(Row, cur)  (TS: prevValues=[cur] → Edit)
//   - Add, PK holds an in-batch row     → Edit(Row, cur)  (TS: prevValues=[cur] → Edit)
//   - Remove, PK removed in-batch       → skip            (TS no-op filter, snapshotter.ts:540-544)
//   - Remove, PK written in-batch       → Remove(cur)     (TS: prev.getRow → cur)
//
// The first, third, and fourth cases are the original BUG 1b/1c/1 rewrites;
// the Edit→Edit and Remove→Remove(cur) substitutions close the remaining
// gap where a PK EDITED earlier in the batch passed its stale pre-batch
// value through (silently divergent OldRow/Row — observable through filter
// old-row predicates and sort position math downstream).
func (ms *MemorySource) resolveBatchChange(change SourceChange) (SourceChange, bool) {
	if ms.batchState == nil {
		return change, false
	}
	switch change.Type {
	case ChangeTypeEdit:
		if cur, ok := ms.batchState[ms.pkKey(change.OldRow)]; ok {
			if cur == nil {
				return MakeSourceChangeAdd(change.Row), false
			}
			return MakeSourceChangeEdit(change.Row, cur), false
		}
	case ChangeTypeAdd:
		if cur, ok := ms.batchState[ms.pkKey(change.Row)]; ok && cur != nil {
			return MakeSourceChangeEdit(change.Row, cur), false
		}
	case ChangeTypeRemove:
		if cur, ok := ms.batchState[ms.pkKey(change.Row)]; ok {
			if cur == nil {
				return SourceChange{}, true
			}
			return MakeSourceChangeRemove(cur), false
		}
	}
	return change, false
}

// genPushAndWrite — Source: memory-source.ts line 508-520
func (ms *MemorySource) genPushAndWrite(change SourceChange) {
	// Second resolution for the split-edit legs (the Remove leg's writeChange
	// updates batchState before the Add leg arrives); idempotent for changes
	// already resolved at the split-decision entry point.
	var skip bool
	if change, skip = ms.resolveBatchChange(change); skip {
		return
	}
	ms.genPush(change)
	ms.writeChange(change)
}

// genPush — Source: memory-source.ts line 522-578
// THIS IS WHERE PARALLELISM WILL BE INJECTED (goroutine fan-out across connections)
func (ms *MemorySource) genPush(change SourceChange) {
	// Validate — direct port of TS MemorySource genPush's asserts
	// (memory-source.ts:529-550). TS throws; the Go panic propagates out of
	// the engine unrecovered → RPC error → 'unclassified' → CG teardown —
	// the same disposition TS's throw gets from the view-syncer.
	switch change.Type {
	case ChangeTypeAdd:
		if ms.has(change.Row) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.Row[c]
			}
			panic(SourceDriftError(ms.tableName, "Add", pk, len(ms.data)))
		}
	case ChangeTypeRemove:
		if !ms.has(change.Row) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.Row[c]
			}
			panic(SourceDriftError(ms.tableName, "Remove", pk, len(ms.data)))
		}
	case ChangeTypeEdit:
		if !ms.has(change.OldRow) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.OldRow[c]
			}
			panic(SourceDriftError(ms.tableName, "Edit", pk, len(ms.data)))
		}
	}

	ms.pushEpoch++
	epoch := ms.pushEpoch

	// Defer the overlay clear so a panic mid-loop can't leak the overlay.
	defer ms.overlay.Store(nil)

	// Snapshot connections under RLock — Connect/Disconnect may run on other
	// goroutines and would otherwise race with iteration.
	ms.connsMu.RLock()
	conns := make([]*Connection, len(ms.connections))
	copy(conns, ms.connections)
	ms.connsMu.RUnlock()

	for _, conn := range conns {
		if conn.Output == nil {
			continue
		}
		conn.LastPushedEpoch.Store(int64(epoch))
		ms.overlay.Store(&Overlay{Epoch: epoch, Change: change})

		outputChange := ms.sourceChangeToChange(change)
		FilterPush(outputChange, conn.Output, conn.Input, conn.FilterPredicate)
	}
}

// sourceChangeToChange converts SourceChange to pipeline Change.
func (ms *MemorySource) sourceChangeToChange(sc SourceChange) Change {
	switch sc.Type {
	case ChangeTypeEdit:
		return MakeEditChange(
			Node{Row: sc.Row, Relationships: map[string]func() iter.Seq[Node]{}},
			Node{Row: sc.OldRow, Relationships: map[string]func() iter.Seq[Node]{}},
		)
	case ChangeTypeAdd:
		return MakeAddChange(Node{Row: sc.Row, Relationships: map[string]func() iter.Seq[Node]{}})
	case ChangeTypeRemove:
		return MakeRemoveChange(Node{Row: sc.Row, Relationships: map[string]func() iter.Seq[Node]{}})
	}
	panic("unreachable")
}

// writeChange — Source: memory-source.ts line 390-429
func (ms *MemorySource) writeChange(change SourceChange) {
	switch change.Type {
	case ChangeTypeAdd:
		ms.insert(change.Row)
		ms.trackAdded(change.Row)
	case ChangeTypeRemove:
		ms.remove(change.Row)
		ms.trackRemoved(change.Row)
	case ChangeTypeEdit:
		ms.remove(change.OldRow)
		ms.trackRemoved(change.OldRow)
		ms.insert(change.Row)
		ms.trackAdded(change.Row)
	}
}

// has checks if a row exists in the primary data.
func (ms *MemorySource) has(row Row) bool {
	idx := ms.search(row)
	return idx < len(ms.data) && ms.comparator(ms.data[idx], row) == 0
}

// insert adds a row in sorted order.
func (ms *MemorySource) insert(row Row) {
	idx := ms.search(row)
	ms.data = append(ms.data, nil)
	copy(ms.data[idx+1:], ms.data[idx:])
	ms.data[idx] = row
}

// remove deletes a row.
func (ms *MemorySource) remove(row Row) {
	idx := ms.search(row)
	if idx < len(ms.data) && ms.comparator(ms.data[idx], row) == 0 {
		ms.data = append(ms.data[:idx], ms.data[idx+1:]...)
	} else {
		panic("MemorySource.remove: row not found in source data")
	}
}

func (ms *MemorySource) trackRemoved(row Row) {
	if ms.batchState == nil {
		ms.batchState = make(map[string]Row)
	}
	ms.batchState[ms.pkKey(row)] = nil
}

func (ms *MemorySource) trackAdded(row Row) {
	if ms.batchState == nil {
		ms.batchState = make(map[string]Row)
	}
	ms.batchState[ms.pkKey(row)] = row
}

func (ms *MemorySource) pkKey(row Row) string {
	parts := make([]string, len(ms.primaryKey))
	for i, k := range ms.primaryKey {
		parts[i] = fmt.Sprintf("%v", row[k])
	}
	return strings.Join(parts, "\x00")
}

func (ms *MemorySource) ClearBatchState() {
	ms.batchState = nil
}

// search returns the index where row would be inserted (binary search).
func (ms *MemorySource) search(row Row) int {
	return sort.Search(len(ms.data), func(i int) bool {
		return ms.comparator(ms.data[i], row) >= 0
	})
}

// Data returns the source data (for testing).
// TableName returns the table name.
func (ms *MemorySource) TableName() string { return ms.tableName }

// PrimaryKey returns the primary key columns.
func (ms *MemorySource) PrimaryKey() []string { return ms.primaryKey }

// Columns returns the column type map.
func (ms *MemorySource) Columns() map[string]string { return ms.columns }

// NormalizeRow coerces row values to match column types (for JSON-deserialized data).
//
// When a ValueConverter is installed (via NewMemorySourceWithConverter), it is
// applied to EVERY column — matching what TableSource.NormalizeRow does via
// FromSQLiteType. Without a converter we fall back to the legacy partial
// coverage (number / boolean only), which is enough for tests but misses
// json / string / blob — see REVIEW-ts-integration CRITICAL-3.
func (ms *MemorySource) NormalizeRow(row Row) {
	if row == nil {
		return
	}
	if ms.converter != nil {
		for col, colType := range ms.columns {
			v, ok := row[col]
			if !ok {
				continue
			}
			// Skip json — see Source.NormalizeRow (internal/tablesource): the
			// wire value is ALREADY parsed (TS coerce-once model), so re-running
			// the converter's strict JSON.parse on a json scalar string would
			// panic. The converter (FromSQLiteType) stays strict for the
			// SQLite-read boundary. Memory mode is non-prod; this keeps the two
			// NormalizeRow impls behaviorally identical.
			if colType == "json" {
				continue
			}
			row[col] = ms.converter(v, colType)
		}
		return
	}
	for col, colType := range ms.columns {
		v, ok := row[col]
		if !ok || v == nil {
			continue
		}
		switch colType {
		case "number":
			switch val := v.(type) {
			case json.Number:
				if f, err := val.Float64(); err == nil {
					row[col] = f
				}
			case bool:
				if val {
					row[col] = float64(1)
				} else {
					row[col] = float64(0)
				}
			case int64:
				row[col] = float64(val)
			case uint64:
				row[col] = float64(val)
			case int:
				row[col] = float64(val)
			}
		case "boolean":
			switch val := v.(type) {
			case float64:
				row[col] = val != 0
			case json.Number:
				if f, err := val.Float64(); err == nil {
					row[col] = f != 0
				}
			case int64:
				row[col] = val != 0
			case uint64:
				row[col] = val != 0
			case int:
				row[col] = val != 0
			}
		}
	}
}

// BulkInsert inserts multiple rows without triggering push. Used for initial data loading.
//
// Takes connsMu write-lock to keep this safe against any future caller path
// that runs concurrently with a push or Fetch. Today the sidecar serializes
// all source-touching RPCs via group.mu, so this is belt-and-braces — but
// it documents the invariant and survives future API surface changes
// (REVIEW-final LOW-PORT-1).
func (ms *MemorySource) BulkInsert(rows []Row) {
	ms.connsMu.Lock()
	defer ms.connsMu.Unlock()
	for _, row := range rows {
		ms.insert(row)
	}
}

func (ms *MemorySource) Data() []Row {
	return ms.data
}

// FilterPush — port of filter-push.ts
func FilterPush(change Change, output Output, pusher InputBase, predicate func(Row) bool) {
	if predicate == nil {
		output.Push(change, pusher)
		return
	}
	switch change.Type {
	case ChangeTypeAdd, ChangeTypeRemove, ChangeTypeChild:
		if predicate(change.Node.Row) {
			output.Push(change, pusher)
		}
	case ChangeTypeEdit:
		MaybeSplitAndPushEditChange(change, predicate, output, pusher)
	}
}

// SourceInput implements Input for a connection.
type SourceInput struct {
	source *MemorySource
	conn   *Connection
	schema *SourceSchema
	output Output
}

func (si *SourceInput) SetOutput(output Output) {
	si.output = output
	si.conn.Output = output
}

func (si *SourceInput) GetSchema() *SourceSchema {
	return si.schema
}

func (si *SourceInput) Destroy() {
	si.source.Disconnect(si)
}

// Fetch — Source: memory-source.ts line 256-364 (simplified)
// Uses overlay + constraint + start + filter logic.
func (si *SourceInput) Fetch(req FetchRequest) iter.Seq[Node] {
	ms := si.source
	conn := si.conn
	compareRows := conn.CompareRows

	// Get rows in connection sort order
	rows := ms.getSortedRows(conn.Sort)

	// Apply overlay
	overlays := ms.computeOverlays(req.Start, req.Constraint, conn, req.Reverse)

	// Generate nodes with overlay spliced in
	nodes := generateWithOverlayInner(rows, overlays, MakeComparator(conn.Sort, false))

	// Apply start
	if req.Start != nil {
		nodes = applyStart(nodes, req.Start, compareRows, req.Reverse, conn.Sort)
	}

	// Apply constraint
	if req.Constraint != nil {
		nodes = applyConstraint(nodes, req.Constraint)
	}

	// Apply multi-constraints (batched IN clauses — FlippedJoin #5928).
	// TS MemorySource#fetch fans a sub-fetch out per entry of the first
	// non-empty multi and post-filters the rest (memory-source.ts:381-436);
	// with unique entries (the documented MultiConstraint invariant) that is
	// row-for-row equivalent to filtering the ordered scan by "matches every
	// non-empty multi", which is what we do — including the overlay row,
	// which TS gates via applyMultiConstraintsToOverlays and we gate by
	// filtering after the overlay splice above.
	if len(req.MultiConstraints) > 0 {
		nodes = applyMultiConstraintsFilter(nodes, req.MultiConstraints)
	}

	// Apply filter
	if conn.FilterPredicate != nil {
		nodes = applyFilter(nodes, conn.FilterPredicate)
	}

	// Apply reverse
	if req.Reverse {
		reverseNodes(nodes)
	}

	return slices.Values(nodes)
}

// getSortedRows returns data sorted by the given ordering.
func (ms *MemorySource) getSortedRows(ordering Ordering) []Row {
	cmp := MakeComparator(ordering, false)
	sorted := make([]Row, len(ms.data))
	copy(sorted, ms.data)
	sort.Slice(sorted, func(i, j int) bool {
		return cmp(sorted[i], sorted[j]) < 0
	})
	return sorted
}

// computeOverlays — Source: memory-source.ts line 647-692
func (ms *MemorySource) computeOverlays(start *Start, constraint *Constraint, conn *Connection, reverse bool) Overlays {
	o := Overlays{}
	overlay := ms.overlay.Load()
	if overlay == nil || conn.LastPushedEpoch.Load() < int64(overlay.Epoch) {
		return o
	}

	switch overlay.Change.Type {
	case ChangeTypeAdd:
		o.Add = overlay.Change.Row
	case ChangeTypeRemove:
		o.Remove = overlay.Change.Row
	case ChangeTypeEdit:
		o.Add = overlay.Change.Row
		o.Remove = overlay.Change.OldRow
	}

	// overlaysForStartAt — Source: memory-source.ts line 696-707
	// Filter out overlay rows that sort before the start position in the scan
	// direction. TS uses indexComparator (which is reversed for reverse fetches).
	//
	// PARTIAL-bound comparator: start.Row may be a partial pagination cursor
	// (e.g. {createdAt} while the sort is [createdAt, conversationId]). With the
	// plain MakeComparator the missing cursor column hits CompareValues(rowVal,
	// nil) → ±1 (nil < non-nil), so for a FORWARD fetch the gate over-includes a
	// Basis:"after" boundary row (ranked strictly after → kept), and for a
	// REVERSE "at" (inclusive) fetch the reversed comparator returns -1 → the
	// gate DROPS the boundary overlay before applyStart can keep it — a real
	// missing-row bug. MakePartialBoundComparator stops at the first sort column
	// absent from the cursor, ranking the boundary EQUAL so the Basis (applied
	// later by applyStart via CompareWithPartialBound) decides. Identical to
	// MakeComparator when start.Row is a complete row. Mirrors the tablesource
	// gate at internal/tablesource/source.go (MakePartialBoundComparator).
	if start != nil {
		compare := MakePartialBoundComparator(conn.Sort, reverse)
		if o.Add != nil && compare(o.Add, start.Row) < 0 {
			o.Add = nil
		}
		if o.Remove != nil && compare(o.Remove, start.Row) < 0 {
			o.Remove = nil
		}
	}

	if constraint != nil {
		if o.Add != nil && !ConstraintMatchesRow(constraint, o.Add) {
			o.Add = nil
		}
		if o.Remove != nil && !ConstraintMatchesRow(constraint, o.Remove) {
			o.Remove = nil
		}
	}

	if conn.FilterPredicate != nil {
		if o.Add != nil && !conn.FilterPredicate(o.Add) {
			o.Add = nil
		}
		if o.Remove != nil && !conn.FilterPredicate(o.Remove) {
			o.Remove = nil
		}
	}

	return o
}

// generateWithOverlayInner — Source: memory-source.ts line 739-768
func generateWithOverlayInner(rows []Row, overlays Overlays, compare Comparator) []Node {
	var result []Node
	addYielded := false
	removeSkipped := false

	for _, row := range rows {
		if !addYielded && overlays.Add != nil {
			cmp := compare(overlays.Add, row)
			if cmp < 0 {
				addYielded = true
				result = append(result, Node{Row: overlays.Add, Relationships: map[string]func() iter.Seq[Node]{}})
			}
		}
		if !removeSkipped && overlays.Remove != nil {
			cmp := compare(overlays.Remove, row)
			if cmp == 0 {
				removeSkipped = true
				continue
			}
		}
		result = append(result, Node{Row: row, Relationships: map[string]func() iter.Seq[Node]{}})
	}

	if !addYielded && overlays.Add != nil {
		result = append(result, Node{Row: overlays.Add, Relationships: map[string]func() iter.Seq[Node]{}})
	}

	return result
}

// applyStart filters nodes based on start position.
// For forward fetch: skip nodes until we reach/pass the start row.
// For reverse fetch: keep only nodes before (or at) the start row.
// The caller will reverse the result afterward for reverse fetches.
func applyStart(nodes []Node, start *Start, compare Comparator, reverse bool, sort Ordering) []Node {
	// Use partial-cursor compare for cursor-vs-row (cursor may lack some
	// sort columns). Use the schema's full compare for row-vs-cursor
	// inversion-via-negation. See CompareWithPartialBound for rationale.
	cmpCursorVsRow := func(row Row) int {
		// CompareWithPartialBound(bound, row) returns -1 if bound < row.
		// applyStart's existing logic uses compare(node.Row, start.Row),
		// i.e. row-vs-cursor. Negate to convert.
		return -CompareWithPartialBound(start.Row, row, sort)
	}
	if reverse {
		var result []Node
		for _, node := range nodes {
			if start.Basis == "at" {
				if cmpCursorVsRow(node.Row) <= 0 {
					result = append(result, node)
				}
			} else { // "after"
				if cmpCursorVsRow(node.Row) < 0 {
					result = append(result, node)
				}
			}
		}
		return result
	}
	// Forward: skip until we reach start
	var result []Node
	started := false
	for _, node := range nodes {
		if !started {
			if start.Basis == "at" {
				if cmpCursorVsRow(node.Row) >= 0 {
					started = true
				}
			} else { // "after"
				if cmpCursorVsRow(node.Row) > 0 {
					started = true
				}
			}
		}
		if started {
			result = append(result, node)
		}
	}
	return result
}

// applyConstraint filters nodes that match constraint.
func applyConstraint(nodes []Node, constraint *Constraint) []Node {
	var result []Node
	for _, node := range nodes {
		if ConstraintMatchesRow(constraint, node.Row) {
			result = append(result, node)
		}
	}
	return result
}

// applyMultiConstraintsFilter keeps nodes satisfying every non-empty
// MultiConstraint (see RowMatchesMultiConstraints).
func applyMultiConstraintsFilter(nodes []Node, multis []MultiConstraint) []Node {
	var result []Node
	for _, node := range nodes {
		if RowMatchesMultiConstraints(multis, node.Row) {
			result = append(result, node)
		}
	}
	return result
}

// applyFilter filters nodes by predicate.
func applyFilter(nodes []Node, predicate func(Row) bool) []Node {
	var result []Node
	for _, node := range nodes {
		if predicate(node.Row) {
			result = append(result, node)
		}
	}
	return result
}

// reverseNodes reverses a slice of nodes in place.
func reverseNodes(nodes []Node) {
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

// ConstraintMatchesRow checks if a row satisfies a constraint.
//
// Uses ValuesEqual (not Go's interface ==) so cross-type numeric equality works
// (e.g., int64(5) from a constraint built from an unnormalized row matches
// float64(5) from a normalized row). Also: ValuesEqual returns false for
// nil/nil, matching TS join semantics — a null constraint value does NOT match
// a null row value.
func ConstraintMatchesRow(constraint *Constraint, row Row) bool {
	if constraint == nil {
		return true
	}
	for k, v := range *constraint {
		if !ValuesEqual(row[k], v) {
			return false
		}
	}
	return true
}

// MaybeSplitAndPushEditChange — port of maybe-split-and-push-edit-change.ts
func MaybeSplitAndPushEditChange(change Change, predicate func(Row) bool, output Output, pusher InputBase) {
	oldMatches := predicate(change.OldNode.Row)
	newMatches := predicate(change.Node.Row)

	if oldMatches && newMatches {
		output.Push(change, pusher)
		return
	}
	if !oldMatches && newMatches {
		output.Push(MakeAddChange(change.Node), pusher)
		return
	}
	if oldMatches && !newMatches {
		output.Push(MakeRemoveChange(*change.OldNode), pusher)
	}
	// neither matches: no-op
}

// Ensure json import is used
var _ = json.Marshal
