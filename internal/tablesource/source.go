package tablesource

// Source: read-only TableSource implementing engine.Source against the TS
// replica's SQLite file.
//
// Phase 2a (this file): construct, expose schema, accept Connect()s,
// implement Fetch() for the simple "ORDER BY + filter, no constraint, no
// cursor, no overlay" case. Push() panics — callers must not wire this
// leaf into the engine until Phase 2d lands the push fanout.
//
// Schema: takes (columns, primaryKey) at construction time. We considered
// inferring from PRAGMA table_info, but FromSQLiteType needs TS-level
// types ("number" vs "boolean" vs "json") that SQLite affinity can't
// reliably express. The sidecar already receives this info from TS via
// the `tables` RPC payload, so reusing that data flow keeps Phase 2 a
// pure source-substitution rather than dragging schema discovery in too.

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// Source is the read-only TableSource leaf. One instance per (CG, table).
type Source struct {
	db         *sql.DB
	tableName  string
	primaryKey []string
	columns    map[string]sqlite.ColumnSchema

	// Connection list + per-source push state. Mirrors MemorySource's
	// connection management — Push fans out to every connection and
	// each tracks its own LastPushedEpoch for overlay visibility.
	mu          sync.Mutex
	connections []*connection
	pushEpoch   int
}

// connection is one downstream pipeline subscribed to this source.
type connection struct {
	sort            ivm.Ordering
	splitEditKeys   map[string]bool
	compareRows     ivm.Comparator
	filterPredicate func(ivm.Row) bool

	// Set by SetOutput once the operator above us wires its receiver.
	output ivm.Output
	input  ivm.Input

	// LastPushedEpoch is the most recent pushEpoch this connection has
	// already observed via its Output.Push. Read during Fetch to gate
	// overlay application. (Push fanout will use this in Phase 2d.)
	lastPushedEpoch int
}

// New constructs a Source for tableName. Validates that the table exists
// (a SELECT against an unknown table at first Fetch would surface the
// error far from construction; failing-closed here is friendlier).
//
// columns must include every column the source ever returns — anything
// missing gets dropped silently from fetched rows (TS-MemorySource has
// the same behavior). primaryKey must be non-empty and every entry must
// be present in columns.
func New(db *sql.DB, tableName string, columns map[string]sqlite.ColumnSchema, primaryKey []string) (*Source, error) {
	if db == nil {
		return nil, fmt.Errorf("tablesource.New: db is nil")
	}
	if tableName == "" {
		return nil, fmt.Errorf("tablesource.New: tableName is empty")
	}
	if len(primaryKey) == 0 {
		return nil, fmt.Errorf("tablesource.New %s: primaryKey is empty", tableName)
	}
	for _, k := range primaryKey {
		if _, ok := columns[k]; !ok {
			return nil, fmt.Errorf(
				"tablesource.New %s: primary key %q is not in columns",
				tableName, k)
		}
	}
	// Validate table presence — `SELECT 1 FROM "x" LIMIT 0` errors if the
	// table doesn't exist, costs nothing if it does.
	if _, err := db.Exec(`SELECT 1 FROM ` + quoteIdent(tableName) + ` LIMIT 0`); err != nil {
		return nil, fmt.Errorf("tablesource.New %s: table not found: %w", tableName, err)
	}
	return &Source{
		db:         db,
		tableName:  tableName,
		primaryKey: primaryKey,
		columns:    columns,
	}, nil
}

// TableName / PrimaryKey / NormalizeRow satisfy engine.Source.

func (s *Source) TableName() string { return s.tableName }

func (s *Source) PrimaryKey() []string { return s.primaryKey }

// NormalizeRow coerces every value in row through FromSQLiteType so a row
// arriving from msgpack (numbers as float64, no type fidelity) ends up
// shaped the same as a row read from this Source's Fetch. Unknown
// columns are left as-is — TS MemorySource has the same behavior.
func (s *Source) NormalizeRow(row ivm.Row) {
	if row == nil {
		return
	}
	for col, val := range row {
		cs, ok := s.columns[col]
		if !ok {
			continue
		}
		row[col] = sqlite.FromSQLiteType(val, cs.Type)
	}
}

// Connect registers a new pipeline connection. sort may be nil (defaults
// to primary-key ascending). filterPredicate is applied to every row
// post-fetch; nil disables it. splitEditKeys is the TS partition-key
// set for edit-split correctness during Push (used in Phase 2d).
func (s *Source) Connect(
	sort ivm.Ordering,
	filterPredicate func(ivm.Row) bool,
	splitEditKeys map[string]bool,
) ivm.Input {
	pkSort := make(ivm.Ordering, len(s.primaryKey))
	for i, k := range s.primaryKey {
		pkSort[i] = [2]string{k, "asc"}
	}

	cmp := ivm.MakeComparator(sort, false)
	if sort == nil {
		cmp = ivm.MakeComparator(pkSort, false)
	}

	conn := &connection{
		sort:            sort,
		splitEditKeys:   splitEditKeys,
		compareRows:     cmp,
		filterPredicate: filterPredicate,
	}
	schema := &ivm.SourceSchema{
		TableName:     s.tableName,
		Columns:       s.columnsAsTypeMap(),
		PrimaryKey:    s.primaryKey,
		Relationships: map[string]*ivm.SourceSchema{},
		System:        "client",
		CompareRows:   cmp,
		Sort:          sort,
	}
	in := &sourceInput{src: s, conn: conn, schema: schema}
	conn.input = in

	s.mu.Lock()
	s.connections = append(s.connections, conn)
	s.mu.Unlock()
	return in
}

// Push fans the change through every connection and rolls the source's
// snapshot epoch. NOT yet implemented — Phase 2d. Panics fast so an
// accidental wiring of this source into the engine surfaces at the
// first SourceChange instead of producing silently-wrong IVM output.
func (s *Source) Push(change ivm.SourceChange) []ivm.Change {
	panic("tablesource.Source.Push: not implemented in Phase 2a " +
		"(see DESIGN-tablesource-port.md — Phase 2d adds push fanout)")
}

// columnsAsTypeMap returns the column → type-name map used by the
// SourceSchema. Keeps the schema's Columns shape identical to what the
// existing MemorySource path produces, so downstream builders don't see
// a behavior difference per source variant.
func (s *Source) columnsAsTypeMap() map[string]string {
	m := make(map[string]string, len(s.columns))
	for k, v := range s.columns {
		m[k] = v.Type
	}
	return m
}

// sourceInput is the Source's ivm.Input — what Connect returns. Holds the
// schema + connection record + back-pointer so Fetch can dispatch
// without a map lookup per call.
type sourceInput struct {
	src    *Source
	conn   *connection
	schema *ivm.SourceSchema
}

func (i *sourceInput) GetSchema() *ivm.SourceSchema { return i.schema }

func (i *sourceInput) SetOutput(o ivm.Output) {
	i.conn.output = o
}

func (i *sourceInput) Destroy() {
	i.src.disconnect(i.conn)
}

func (i *sourceInput) Fetch(req ivm.FetchRequest) []ivm.Node {
	return i.src.fetchForConn(req, i.conn)
}

// disconnect removes conn from the source's connection list. O(n) over
// the conns slice — fine because each CG typically has a handful of
// connections per source, not thousands.
func (s *Source) disconnect(c *connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, cc := range s.connections {
		if cc == c {
			s.connections = append(s.connections[:i], s.connections[i+1:]...)
			return
		}
	}
}

// fetchForConn runs the SELECT for conn and returns the resulting nodes.
//
// Coverage:
//   - ORDER BY from conn.sort (or PK ascending if nil)
//   - Reverse → DESC order
//   - Filter predicate applied post-scan
//   - Constraint → WHERE eq pushdown (Phase 2b)
//
// Phase 2c will add: Start cursor (WHERE lexicographic compare).
func (s *Source) fetchForConn(req ivm.FetchRequest, conn *connection) []ivm.Node {
	if req.Start != nil {
		panic("tablesource.Source.Fetch: Start cursor not implemented in Phase 2b")
	}

	// ORDER BY clause from connection sort (PK ascending if unset).
	order := conn.sort
	if order == nil {
		order = make(ivm.Ordering, len(s.primaryKey))
		for i, k := range s.primaryKey {
			order[i] = [2]string{k, "asc"}
		}
	}

	cols := s.columnList()
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, c := range cols {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(quoteIdent(c))
	}
	sb.WriteString(" FROM ")
	sb.WriteString(quoteIdent(s.tableName))

	// WHERE from constraint. Keys are sorted so the SQL is deterministic
	// (matters for any future statement-cache keying and for log diffing).
	var params []interface{}
	if req.Constraint != nil && len(*req.Constraint) > 0 {
		keys := make([]string, 0, len(*req.Constraint))
		for k := range *req.Constraint {
			keys = append(keys, k)
		}
		for i := 1; i < len(keys); i++ {
			for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
				keys[j-1], keys[j] = keys[j], keys[j-1]
			}
		}
		sb.WriteString(" WHERE ")
		for i, k := range keys {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(quoteIdent(k))
			sb.WriteString(" = ?")
			cs, ok := s.columns[k]
			colType := ""
			if ok {
				colType = cs.Type
			}
			params = append(params, sqlite.ToSQLiteType((*req.Constraint)[k], colType))
		}
	}

	sb.WriteString(" ORDER BY ")
	for i, ord := range order {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(quoteIdent(ord[0]))
		dir := strings.ToUpper(ord[1])
		if req.Reverse {
			if dir == "ASC" {
				dir = "DESC"
			} else {
				dir = "ASC"
			}
		}
		sb.WriteByte(' ')
		sb.WriteString(dir)
	}

	rows, err := s.db.Query(sb.String(), params...)
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: query: %v\nSQL: %s",
			s.tableName, err, sb.String()))
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: columns: %v",
			s.tableName, err))
	}

	var out []ivm.Node
	for rows.Next() {
		raw := make([]interface{}, len(colNames))
		ptrs := make([]interface{}, len(colNames))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: scan: %v",
				s.tableName, err))
		}
		row := make(ivm.Row, len(colNames))
		for i, c := range colNames {
			cs, ok := s.columns[c]
			if !ok {
				continue
			}
			row[c] = sqlite.FromSQLiteType(raw[i], cs.Type)
		}
		if conn.filterPredicate != nil && !conn.filterPredicate(row) {
			continue
		}
		out = append(out, ivm.Node{Row: row})
	}
	if err := rows.Err(); err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: rows: %v",
			s.tableName, err))
	}
	return out
}

// columnList returns column names in a stable order so generated SQL is
// deterministic (helps with logging + future query-plan caching).
func (s *Source) columnList() []string {
	names := make([]string, 0, len(s.columns))
	for k := range s.columns {
		names = append(names, k)
	}
	// Sort lexicographically — stable + lets test fixtures predict the
	// returned column order.
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j-1] > names[j]; j-- {
			names[j-1], names[j] = names[j], names[j-1]
		}
	}
	return names
}

// quoteIdent escapes a SQL identifier (table or column name) by doubling
// any embedded quotes. The result is double-quoted, which both SQLite
// and the SQL standard accept for identifiers.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
