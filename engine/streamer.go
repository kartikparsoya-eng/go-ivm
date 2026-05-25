package engine

// Flattens nested IVM Change trees (CHILD changes can contain sub-changes
// for relationships) into one RowChange per affected row.

import (
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

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
			result = append(result, streamNodes(queryID, schema, RowChangeAdd, change.Node)...)
		case ivm.ChangeTypeRemove:
			result = append(result, streamNodes(queryID, schema, RowChangeRemove, change.Node)...)
		case ivm.ChangeTypeEdit:
			// Edit: emit the new row only (no relationship recursion for edits)
			rowKey := make(map[string]interface{}, len(schema.PrimaryKey))
			for _, pk := range schema.PrimaryKey {
				rowKey[pk] = change.Node.Row[pk]
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
func streamNodes(queryID string, schema *ivm.SourceSchema, op int, node ivm.Node) []RowChange {
	var result []RowChange

	// Build rowKey from primary key
	rowKey := make(map[string]interface{}, len(schema.PrimaryKey))
	for _, pk := range schema.PrimaryKey {
		rowKey[pk] = node.Row[pk]
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
	result = append(result, rc)

	// Recursively stream child relationships
	if node.Relationships != nil && schema.Relationships != nil {
		for relName, childFn := range node.Relationships {
			childSchema := schema.Relationships[relName]
			if childSchema == nil {
				continue
			}
			childNodes := childFn()
			for _, childNode := range childNodes {
				result = append(result, streamNodes(queryID, childSchema, op, childNode)...)
			}
		}
	}

	return result
}
