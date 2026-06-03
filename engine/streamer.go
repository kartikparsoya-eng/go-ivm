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
	Type    int            `json:"type"`    // 0=add, 1=remove, 2=edit
	QueryID string         `json:"queryID"`
	Table   string         `json:"table"`
	RowKey  map[string]interface{} `json:"rowKey"`
	Row     ivm.Row        `json:"row,omitempty"` // nil for remove
}

// Streamer accumulates IVM changes and flattens them to RowChanges.
// Thread-safe: Accumulate may be called concurrently from parallel pipelines.
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
	mu          sync.Mutex
	accumulated []accEntry
}

type accEntry struct {
	queryID string
	schema  *ivm.SourceSchema
	changes []ivm.Change
}

// NewStreamer creates a new Streamer.
func NewStreamer() *Streamer {
	return &Streamer{}
}

// Accumulate stores IVM changes for later flattening.
// Called by each pipeline's output handler during push.
func (s *Streamer) Accumulate(queryID string, schema *ivm.SourceSchema, changes []ivm.Change) {
	s.mu.Lock()
	s.accumulated = append(s.accumulated, accEntry{
		queryID: queryID,
		schema:  schema,
		changes: changes,
	})
	s.mu.Unlock()
}

// Stream flattens all accumulated changes into RowChanges.
func (s *Streamer) Stream() []RowChange {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []RowChange
	for _, entry := range s.accumulated {
		result = append(result, streamChanges(entry.queryID, entry.schema, entry.changes)...)
	}
	// Parallelism review LOW-3: drop the backing array so a one-time large
	// advance doesn't permanently retain memory. The next Accumulate will
	// allocate a fresh small slice.
	s.accumulated = nil
	return result
}

// streamChanges recursively flattens IVM changes.
func streamChanges(queryID string, schema *ivm.SourceSchema, changes []ivm.Change) []RowChange {
	var result []RowChange
	for _, change := range changes {
		// Skip permissions system schemas
		if schema.System == "permissions" {
			continue
		}

		switch change.Type {
		case ivm.ChangeTypeAdd:
			streamNodesInto(&result, queryID, schema, RowChangeAdd, change.Node)
		case ivm.ChangeTypeRemove:
			streamNodesInto(&result, queryID, schema, RowChangeRemove, change.Node)
		case ivm.ChangeTypeEdit:
			// Edit: emit the new row only (no relationship recursion for edits)
			rowKey := make(map[string]interface{}, len(schema.PrimaryKey))
			for _, pk := range schema.PrimaryKey {
				rowKey[pk] = pkValue(change.Node.Row, pk, schema.TableName)
			}
			result = append(result, RowChange{
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
					result = append(result, streamChanges(queryID, childSchema, []ivm.Change{change.Child.Change})...)
				}
			}
		}
	}
	return result
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
	streamNodesInto(&out, queryID, schema, op, node)
	return out
}

// streamNodesInto appends the RowChanges for a node and its relationships into
// *out, recursing in place so the entire subtree shares one backing slice.
func streamNodesInto(out *[]RowChange, queryID string, schema *ivm.SourceSchema, op int, node ivm.Node) {
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
	*out = append(*out, rc)

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
			for _, childNode := range node.Relationships[relName]() {
				streamNodesInto(out, queryID, childSchema, op, childNode)
			}
		}
	}
}
