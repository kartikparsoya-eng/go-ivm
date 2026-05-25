package testharness

// Same protocol as ts-harness.ts:
//   Input:  { schema, initialData, ast, mutations }
//   Output: { hydration: CaughtNode[], steps: CaughtChange[][] }
//
// Uses MemorySource (same as TS harness) for exact behavioral parity.

import (
	"encoding/json"
	"fmt"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// --- Test Case Protocol Types ---

type TestCase struct {
	Schema      map[string]TableSchema `json:"schema"`
	InitialData map[string][]ivm.Row   `json:"initialData"`
	AST         builder.AST            `json:"ast"`
	Mutations   []Mutation             `json:"mutations"`
}

type TableSchema struct {
	Columns    map[string]ColumnDef `json:"columns"`
	PrimaryKey []string             `json:"primaryKey"`
}

type ColumnDef struct {
	Type string `json:"type"`
}

type Mutation struct {
	Type   string  `json:"type"` // "add", "remove", "edit"
	Table  string  `json:"table"`
	Row    ivm.Row `json:"row"`
	OldRow ivm.Row `json:"oldRow,omitempty"`
}

// --- Output Types (matching TS Catch output format) ---

type CaughtNode struct {
	Row           ivm.Row                   `json:"row"`
	Relationships map[string][]*CaughtNode  `json:"relationships"`
}

type CaughtChildData struct {
	RelationshipName string       `json:"relationshipName"`
	Change           CaughtChange `json:"change"`
}

type CaughtChange struct {
	Type   string          `json:"type"`            // "add", "remove", "edit", "child"
	Node   *CaughtNode     `json:"node,omitempty"`  // for add/remove
	Row    ivm.Row         `json:"row,omitempty"`   // for edit and child (the parent row)
	OldRow ivm.Row         `json:"oldRow,omitempty"` // for edit (old row)
	Child  *CaughtChildData `json:"child,omitempty"` // for child
}

// --- Harness Execution ---

type HarnessResult struct {
	Hydration []*CaughtNode    `json:"hydration"`
	Steps     [][]CaughtChange `json:"steps"`
}

// RunTestCase executes a test case through the Go IVM and returns the result.
func RunTestCase(tc TestCase) (*HarnessResult, error) {
	// Create sources
	sources := make(map[string]*ivm.MemorySource)
	for tableName, schema := range tc.Schema {
		columns := make(map[string]string)
		for colName, colDef := range schema.Columns {
			columns[colName] = colDef.Type
		}
		// Inject FromSQLiteType so the harness's MemorySource normalizes
		// every column type the same way production does (REVIEW-final
		// MED-PORT-1). Without this, tests masked json/string/blob bugs
		// because the legacy NormalizeRow only covered number/boolean.
		sources[tableName] = ivm.NewMemorySourceWithConverter(
			tableName, columns, schema.PrimaryKey,
			func(v interface{}, colType string) ivm.Value {
				return sqlite.FromSQLiteType(v, colType)
			},
		)
	}

	// Populate initial data
	for tableName, rows := range tc.InitialData {
		source := sources[tableName]
		for _, row := range rows {
			source.Push(ivm.MakeSourceChangeAdd(row))
		}
	}

	// Build pipeline using the builder
	delegate := &memoryDelegate{sources: sources}
	pipeline := builder.BuildPipeline(tc.AST, delegate)

	// Attach collector output
	collector := &changeCollector{}
	pipeline.Input.SetOutput(collector)

	// Hydration: fetch initial state
	nodes := pipeline.Input.Fetch(ivm.FetchRequest{})
	hydration := make([]*CaughtNode, 0, len(nodes))
	for _, node := range nodes {
		hydration = append(hydration, expandNode(node))
	}

	// Process mutations
	steps := make([][]CaughtChange, 0, len(tc.Mutations))
	for _, mutation := range tc.Mutations {
		collector.Reset()
		source := sources[mutation.Table]
		if source == nil {
			return nil, fmt.Errorf("unknown table: %s", mutation.Table)
		}
		switch mutation.Type {
		case "add":
			source.Push(ivm.MakeSourceChangeAdd(mutation.Row))
		case "remove":
			source.Push(ivm.MakeSourceChangeRemove(mutation.Row))
		case "edit":
			source.Push(ivm.MakeSourceChangeEdit(mutation.Row, mutation.OldRow))
		}
		steps = append(steps, collector.Changes())
	}

	return &HarnessResult{Hydration: hydration, Steps: steps}, nil
}

// RunTestCaseJSON runs a test case from JSON input and returns JSON output.
func RunTestCaseJSON(input []byte) ([]byte, error) {
	var tc TestCase
	if err := json.Unmarshal(input, &tc); err != nil {
		return nil, fmt.Errorf("parse test case: %w", err)
	}
	result, err := RunTestCase(tc)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(result, "", "  ")
}

// --- changeCollector implements ivm.Output ---

type changeCollector struct {
	changes []CaughtChange
}

func (c *changeCollector) Push(change ivm.Change, pusher ivm.InputBase) []ivm.Change {
	c.changes = append(c.changes, expandChange(change))
	return nil
}

func (c *changeCollector) Reset() {
	c.changes = c.changes[:0]
}

func (c *changeCollector) Changes() []CaughtChange {
	result := make([]CaughtChange, len(c.changes))
	copy(result, c.changes)
	return result
}

// --- memoryDelegate implements builder.Delegate ---

type memoryDelegate struct {
	sources  map[string]*ivm.MemorySource
	storages map[string]*memStorage
}

func (d *memoryDelegate) GetSource(tableName string) builder.Source {
	source, ok := d.sources[tableName]
	if !ok {
		return nil
	}
	return &memSourceAdapter{source: source}
}

func (d *memoryDelegate) CreateStorage(name string) ivm.TakeStorage {
	if d.storages == nil {
		d.storages = make(map[string]*memStorage)
	}
	s := &memStorage{}
	d.storages[name] = s
	return s
}

// --- memSourceAdapter wraps MemorySource as builder.Source ---

type memSourceAdapter struct {
	source *ivm.MemorySource
}

func (a *memSourceAdapter) Connect(opts builder.ConnectOptions) ivm.Input {
	return a.source.Connect(opts.Sort, opts.FilterPredicate, opts.SplitEditKeys)
}

func (a *memSourceAdapter) PrimaryKey() []string {
	return a.source.PrimaryKey()
}

func (a *memSourceAdapter) NormalizeRow(row ivm.Row) {
	a.source.NormalizeRow(row)
}

// --- memStorage implements ivm.TakeStorage ---

type memStorage struct {
	states   map[string]ivm.TakeState
	maxBound ivm.Row
}

func (s *memStorage) GetTakeState(key string) *ivm.TakeState {
	state, ok := s.states[key]
	if !ok {
		return nil
	}
	return &state
}

func (s *memStorage) SetTakeState(key string, state ivm.TakeState) {
	if s.states == nil {
		s.states = make(map[string]ivm.TakeState)
	}
	s.states[key] = state
}

func (s *memStorage) GetMaxBound() ivm.Row {
	return s.maxBound
}

func (s *memStorage) SetMaxBound(bound ivm.Row) {
	s.maxBound = bound
}

func (s *memStorage) Del(key string) {
	delete(s.states, key)
}

// --- Node/Change expansion (matching TS Catch output) ---

func expandNode(node ivm.Node) *CaughtNode {
	rels := make(map[string][]*CaughtNode)
	if node.Relationships != nil {
		for relName, relFn := range node.Relationships {
			children := relFn()
			expanded := make([]*CaughtNode, 0, len(children))
			for _, child := range children {
				expanded = append(expanded, expandNode(child))
			}
			rels[relName] = expanded
		}
	}
	return &CaughtNode{
		Row:           node.Row,
		Relationships: rels,
	}
}

func expandChange(change ivm.Change) CaughtChange {
	switch change.Type {
	case ivm.ChangeTypeAdd:
		return CaughtChange{Type: "add", Node: expandNode(change.Node)}
	case ivm.ChangeTypeRemove:
		return CaughtChange{Type: "remove", Node: expandNode(change.Node)}
	case ivm.ChangeTypeEdit:
		// TS Catch outputs {type: "edit", oldRow, row} (no node wrapper)
		oldRow := ivm.Row{}
		if change.OldNode != nil {
			oldRow = change.OldNode.Row
		}
		return CaughtChange{Type: "edit", Row: change.Node.Row, OldRow: oldRow}
	case ivm.ChangeTypeChild:
		result := CaughtChange{Type: "child", Row: change.Node.Row}
		if change.Child != nil {
			result.Child = &CaughtChildData{
				RelationshipName: change.Child.RelationshipName,
				Change:           expandChange(change.Child.Change),
			}
		}
		return result
	default:
		return CaughtChange{Type: "unknown"}
	}
}
