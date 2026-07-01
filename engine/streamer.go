package engine

// Flattens nested IVM Change trees (CHILD changes can contain sub-changes
// for relationships) into one RowChange per affected row.

import (
	"fmt"
	"sort"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// pkValue reads a primary-key column from a row, panicking if it is missing
// or nil. Primary keys are NOT NULL by definition, so a nil PK means a
// corrupted replica or a join mis-constructing the node — TS fails loud here
// via must() (pipeline-driver.ts:3184). Go previously wrote nil silently,
// shipping a {pk: nil} rowKey the CVR records but no future advance matches →
// permanent client-view soft-leak (HIGH-6). Panicking matches TS's
// fail-fast contract; engine.Advance's recover surfaces it for re-init.
func pkValue(row ivm.Row, pk, table string) interface{} {
	v, ok := row[pk]
	if !ok || v == nil {
		panic(fmt.Sprintf(
			"streamer: row for table %q missing primary-key column %q (nil PK)",
			table, pk))
	}
	return v
}

// RowChangeType mirrors the output change type enum.
const (
	RowChangeAdd    = 0
	RowChangeRemove = 1
	RowChangeEdit   = 2
)

// RowChange is the flat output format — one per affected row per query.
type RowChange struct {
	Type    int                    `json:"type"` // 0=add, 1=remove, 2=edit
	QueryID string                 `json:"queryID"`
	Table   string                 `json:"table"`
	RowKey  map[string]interface{} `json:"rowKey"`
	Row     ivm.Row                `json:"row,omitempty"` // nil for remove
}

// Streamer accumulates IVM changes and flattens them to RowChanges.
// Thread-safe: Accumulate may be called concurrently from parallel pipelines.
//
// Flatten-in-push (DESIGN-streaming-advance.md): Accumulate flattens each
// change tree to RowChanges SYNCHRONOUSLY, while the caller's Output.Push is
// still on the stack and the mutation overlay + join in-progress state are
// live (§3). This deletes the eager materializeChange deep-copy and the
// Nodes-then-RowChanges double-hold, matching TS's #streamNodes generator
// (pipeline-driver.ts:6022-6024) which flattens inside the push loop.
//
// Ordering contract (D8):
//   - Changes within a single Accumulate call preserve their input slice
//     order in the Stream() output.
//   - Order ACROSS Accumulate calls follows the order Accumulate acquired
//     the mutex. Under sequential genPush this is deterministic (pipelines
//     emit in registration order). Under GenPushParallel — where many
//     pipelines push concurrently — cross-queryID order is determined by
//     the lock-acquisition race and is therefore non-deterministic.
//   - Callers requiring deterministic global order (e.g., wire-shadow
//     comparators) MUST sort by a stable key (queryID, table, rowKey, type
//     in TS's #shadowCompare).
type Streamer struct {
	mu   sync.Mutex
	rows []RowChange
	// chunkSink, when non-nil, switches Accumulate into operator-level streaming:
	// full chunkRows/chunkBytes chunks of a single source-change's fan-out are
	// handed off DURING the flatten (a 50k-child ADD ships as N frames, not one),
	// and only the sub-threshold residual coalesces into `rows`. Set by
	// AdvanceStream for the advance window; nil elsewhere so hydrate/companion
	// keep slice mode. chunkSink MUST consume its argument synchronously (the
	// AdvanceStream sink encodes it into a wire frame before returning).
	chunkSink  func([]RowChange)
	chunkRows  int
	chunkBytes int
}

// NewStreamer creates a new Streamer.
func NewStreamer() *Streamer {
	return &Streamer{}
}

// SetChunkSink switches Accumulate into operator-level streaming mode for the
// advance window: full chunks (rowLimit rows OR byteLimit estimated bytes) flush
// via sink mid-flatten. Pass (nil,0,0) to restore slice mode. Call only while the
// engine lock is held and no push is active (AdvanceStream setup/teardown), so it
// never races an in-flight Accumulate.
func (s *Streamer) SetChunkSink(sink func([]RowChange), rowLimit, byteLimit int) {
	s.mu.Lock()
	s.chunkSink = sink
	s.chunkRows = rowLimit
	s.chunkBytes = byteLimit
	s.mu.Unlock()
}

// Accumulate flattens IVM changes to RowChanges immediately — while the
// overlay and join push-scoped state are still live (DESIGN-streaming-advance
// §3). Called by each pipeline's output handler during push; the flatten
// runs on the caller's goroutine, parallelizing across push pipelines.
func (s *Streamer) Accumulate(queryID string, schema *ivm.SourceSchema, changes []ivm.Change) {
	s.mu.Lock()
	sink := s.chunkSink
	rowLimit := s.chunkRows
	byteLimit := s.chunkBytes
	s.mu.Unlock()

	if sink == nil {
		// Slice mode (hydrate/companion): flatten lock-free, append once.
		flat := streamChanges(queryID, schema, changes)
		if len(flat) == 0 {
			return
		}
		s.mu.Lock()
		s.rows = append(s.rows, flat...)
		s.mu.Unlock()
		return
	}

	// Streaming mode (advance): flatten lock-free into a per-goroutine local
	// buffer; hand full chunks to sink mid-walk so a single source-change's huge
	// fan-out streams incrementally (peak buffer bounded to a chunk, matching TS
	// #streamNodes' yield* + poke-every-CURSOR_PAGE_SIZE). The sub-threshold
	// residual coalesces into rows, drained per source-change by Stream().
	cap0 := rowLimit
	if cap0 > 1024 {
		cap0 = 1024
	}
	local := make([]RowChange, 0, cap0)
	localBytes := 0
	emit := func(rc RowChange) {
		local = append(local, rc)
		localBytes += estimateRowChangeBytes(rc)
		if len(local) >= rowLimit || localBytes >= byteLimit {
			sink(local) // consumed synchronously (encoded to a wire frame)
			local = make([]RowChange, 0, cap0)
			localBytes = 0
		}
	}
	streamChangesInto(emit, queryID, schema, changes)
	if len(local) > 0 {
		s.mu.Lock()
		s.rows = append(s.rows, local...)
		s.mu.Unlock()
	}
}

// Stream drains all accumulated RowChanges. After this call the Streamer
// is empty; the next Accumulate re-allocates. Hand-off: ownership of the
// backing slice transfers to the caller.
func (s *Streamer) Stream() []RowChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.rows
	s.rows = nil
	return out
}

// streamChanges recursively flattens IVM changes into a slice. Compat wrapper
// over streamChangesInto (emit = append) for callers that want the whole result
// in memory (hydrate, the streamer's residual path).
func streamChanges(queryID string, schema *ivm.SourceSchema, changes []ivm.Change) []RowChange {
	var result []RowChange
	streamChangesInto(func(rc RowChange) { result = append(result, rc) }, queryID, schema, changes)
	return result
}

// streamChangesInto recursively flattens IVM changes, emitting each RowChange to
// `emit` the moment it is produced instead of accumulating a slice. This is the
// streaming core: callers wanting a slice pass emit = append (streamChanges);
// the advance path passes an emit that flushes a partial frame at a row/byte
// threshold, so a single source-change's huge relationship fan-out streams out
// incrementally — matching TS #streamNodes' yield* (never holds the whole delta).
func streamChangesInto(emit func(RowChange), queryID string, schema *ivm.SourceSchema, changes []ivm.Change) {
	for _, change := range changes {
		// Skip permissions system schemas
		if schema.System == "permissions" {
			continue
		}

		switch change.Type {
		case ivm.ChangeTypeAdd:
			streamNodesInto(emit, queryID, schema, RowChangeAdd, change.Node)
		case ivm.ChangeTypeRemove:
			streamNodesInto(emit, queryID, schema, RowChangeRemove, change.Node)
		case ivm.ChangeTypeEdit:
			// IsScalar guard — mirrors streamNodesInto's check (:180). Without
			// this, an Edit on a pre-resolved scalar CSQ row is emitted while
			// Add/Remove for the same schema are correctly suppressed.
			if schema.IsScalar {
				continue
			}
			// Edit: emit the new row only (no relationship recursion for edits)
			rowKey := make(map[string]interface{}, len(schema.PrimaryKey))
			for _, pk := range schema.PrimaryKey {
				rowKey[pk] = pkValue(change.Node.Row, pk, schema.TableName)
			}
			emit(RowChange{
				Type:    RowChangeEdit,
				QueryID: queryID,
				Table:   schema.TableName,
				RowKey:  rowKey,
				Row:     change.Node.Row,
			})
		case ivm.ChangeTypeChild:
			// Child: recurse into the relationship
			if change.Child != nil && schema.Relationships != nil {
				childSchema := schema.Relationships[change.Child.RelationshipName]
				if childSchema != nil {
					streamChangesInto(emit, queryID, childSchema, []ivm.Change{change.Child.Change})
				}
			}
		}
	}
}

// streamNodes produces RowChanges for a node and its relationships.
//
// Thin wrapper over streamNodesInto (T1-3): pre-sizes a single accumulator and
// lets the whole relationship subtree append into it. The previous version
// allocated a fresh []RowChange at every recursion level and re-appended it
// into the parent's slice — O(N log N) allocator work for an O(N) result.
func streamNodes(queryID string, schema *ivm.SourceSchema, op int, node ivm.Node) []RowChange {
	// Estimate: this node + its immediate relationships. Append amortises any
	// deeper growth. We deliberately DON'T tree-walk to count exactly — the
	// relationship values are closures that materialise child nodes, so a
	// counting pre-walk would fetch every relationship twice.
	out := make([]RowChange, 0, 1+len(node.Relationships))
	streamNodesInto(func(rc RowChange) { out = append(out, rc) }, queryID, schema, op, node)
	return out
}

// streamNodesInto appends the RowChanges for a node and its relationships into
// *out, recursing in place so the entire subtree shares one backing slice.
func streamNodesInto(emit func(RowChange), queryID string, schema *ivm.SourceSchema, op int, node ivm.Node) {
	// Skip permission-system rows — mirrors TS pipeline-driver.ts:2101.
	// streamChanges already guards permissions for top-level Add/Remove, but
	// the recursive descent into child relationships re-enters here with the
	// child schema. If that child is a permission CSQ (e.g. the
	// channel_participants EXISTS on conversation read policy), without
	// this guard Go emits those rows to the client while TS correctly
	// suppresses them.
	if schema.System == "permissions" {
		return
	}
	// IsScalar skip — when a CSQ was pre-resolved by the scalar resolver as
	// a companion subquery, the join's relationship is still built (the
	// EXISTS check is needed) but its row emissions are suppressed at the
	// wire so they don't double-count with the companion's own emissions.
	// Mirrors TS's pattern where pre-resolved scalar CSQs don't add a
	// relationship to the streamed node.
	if schema.IsScalar {
		return
	}

	rowKey := make(map[string]interface{}, len(schema.PrimaryKey))
	for _, pk := range schema.PrimaryKey {
		rowKey[pk] = pkValue(node.Row, pk, schema.TableName)
	}

	rc := RowChange{
		Type:    op,
		QueryID: queryID,
		Table:   schema.TableName,
		RowKey:  rowKey,
	}
	if op != RowChangeRemove {
		rc.Row = node.Row
	}
	emit(rc)

	if node.Relationships != nil && schema.Relationships != nil {
		// Iterate relationships in a STABLE order. TS streams them in
		// Object.entries insertion order (pipeline-driver.ts:3189), which is
		// deterministic; Go's map range is randomized per iteration, so the
		// wire order of sibling-relationship child rows varied run-to-run.
		// Sorting by relationship name restores determinism. (Exact parity
		// with TS's insertion order would require threading an ordered
		// relationship list through every Join/Exists/FlippedJoin schema
		// merge — high cost; sibling order is semantically irrelevant since
		// the client buckets child rows per relationship, and the shadow
		// comparator is order-tolerant across relationships.)
		relNames := make([]string, 0, len(node.Relationships))
		for relName := range node.Relationships {
			relNames = append(relNames, relName)
		}
		sort.Strings(relNames)
		for _, relName := range relNames {
			childSchema := schema.Relationships[relName]
			if childSchema == nil {
				continue
			}
			if seq := node.Relationships[relName](); seq != nil {
				for childNode := range seq {
					streamNodesInto(emit, queryID, childSchema, op, childNode)
				}
			}
		}
	}
}
