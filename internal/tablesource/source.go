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
	"context"
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

	// overlay is the pending Push that hasn't finished fanning out yet.
	// Concurrent Fetches whose lastPushedEpoch < overlay.Epoch splice
	// this in so they see the post-push state — same contract as
	// MemorySource's overlay field. nil when no Push is in flight.
	overlay *ivm.Overlay

	// snapshotEpoch keys the per-Source TxCache. It is bumped AFTER
	// Push fanout completes (in the deferred cleanup), NOT before. The
	// two-epoch dance is what makes read-from-replica correct:
	//
	//   pushEpoch  — bumped before fanout. Drives overlay visibility
	//                via conn.lastPushedEpoch < overlay.Epoch gate.
	//   snapshotEpoch — bumped after fanout. Drives TxCache.Acquire —
	//                a bump here is what tells the cache to roll the
	//                pinned read tx to a fresh WAL frame.
	//
	// During Push fanout, a downstream Fetch sees snapshotEpoch
	// UNCHANGED → cache returns the still-pinned (pre-replicator-
	// commit) tx → query yields the pre-commit state → overlay
	// splices in the change → result is the post-commit view. After
	// fanout, snapshotEpoch++ → next Fetch's Acquire rolls the tx →
	// new tx pins to the now-current WAL frame.
	//
	// See DESIGN-tablesource-port.md "Phase 4" for the full rationale.
	snapshotEpoch uint64

	// txCache holds at most one pinned read tx for this Source. Per-
	// Source (not shared) — each table is its own cache entry under a
	// stable key (the table name). The TxCache's internal mutex
	// serializes concurrent Fetches on the same Source, which is
	// required because *sql.Tx is not goroutine-safe.
	txCache *TxCache
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
		txCache:    NewTxCache(db),
	}, nil
}

// Close releases the pinned read transaction (if any). The underlying
// *sql.DB pool is NOT closed — that's owned by the sidecar's process-
// level lifecycle, not by individual sources. Idempotent.
func (s *Source) Close() error {
	if s.txCache == nil {
		return nil
	}
	return s.txCache.Close()
}

// RefreshSnapshot bumps snapshotEpoch so the next Fetch rolls the pinned
// read tx to the current WAL frame. Called externally (e.g., by the
// drift audit) when the caller needs Go's reads to compare against the
// replica's latest committed state rather than the snapshot held since
// the last Push.
//
// No-op while a Push is in progress (overlay != nil) — rolling the tx
// mid-fanout would invalidate the snapshot-consistency guarantee that
// downstream Fetches inside Output.Push depend on. Once the Push
// completes, its own deferred snapshotEpoch++ takes care of the roll
// and a subsequent RefreshSnapshot can move us forward further.
func (s *Source) RefreshSnapshot() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.overlay != nil {
		return
	}
	s.snapshotEpoch++
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

// Push fans the change through every subscribed connection. Unlike the
// legacy in-process sqlite.TableSource, we do NOT write to the database
// here — the TS replicator owns the file and has already written by
// the time advance() reaches us. This source is a pure read-side
// adapter; Push is fanout + overlay only.
//
// Edit / split-edit / filter-transition handling mirrors the existing
// sqlite.TableSource.Push contract so downstream operators see
// identical change sequences regardless of which leaf is in use.
func (s *Source) Push(change ivm.SourceChange) []ivm.Change {
	s.mu.Lock()
	s.pushEpoch++
	epoch := s.pushEpoch
	s.overlay = &ivm.Overlay{Epoch: epoch, Change: change}
	conns := make([]*connection, len(s.connections))
	copy(conns, s.connections)
	s.mu.Unlock()

	// Defer overlay clear AND snapshotEpoch bump to the end of fan-out.
	// Bump order matters: snapshotEpoch++ is what tells the next
	// Fetch's TxCache.Acquire to roll the pinned read tx to a fresh
	// WAL frame. Doing this BEFORE fanout would roll the tx mid-Push,
	// then Fetches inside the fanout would see post-replicator-commit
	// state AND the overlay → double-application. Doing it AFTER means
	// the tx stays pinned through the whole fanout, so Fetch sees pre-
	// commit + overlay = correct post-commit view.
	defer func() {
		s.mu.Lock()
		s.overlay = nil
		s.snapshotEpoch++
		s.mu.Unlock()
	}()

	var out []ivm.Change

	for _, conn := range conns {
		if conn.output == nil {
			continue
		}

		// Filter-transition handling:
		//  - row fails filter → either remove (if Edit and old passed)
		//    or skip
		//  - Edit where only old passed filter → emit Remove
		//  - Edit where only new passes filter → emit Add
		if conn.filterPredicate != nil {
			if !conn.filterPredicate(change.Row) {
				if change.Type == ivm.ChangeTypeEdit && conn.filterPredicate(change.OldRow) {
					rm := ivm.MakeRemoveChange(ivm.Node{Row: change.OldRow})
					out = append(out, conn.output.Push(rm, conn.input)...)
				}
				conn.lastPushedEpoch = epoch
				continue
			}
			if change.Type == ivm.ChangeTypeEdit && !conn.filterPredicate(change.OldRow) {
				ad := ivm.MakeAddChange(ivm.Node{Row: change.Row})
				out = append(out, conn.output.Push(ad, conn.input)...)
				conn.lastPushedEpoch = epoch
				continue
			}
		}

		// Split-edit: when an Edit changes a partition key the connection
		// cares about (per splitEditKeys), the downstream needs to see
		// the edit as a Remove + Add so its IVM state stays consistent
		// with the partitioning.
		if change.Type == ivm.ChangeTypeEdit && conn.splitEditKeys != nil &&
			editChangesSplitKeys(change, conn.splitEditKeys) {
			rm := ivm.MakeRemoveChange(ivm.Node{Row: change.OldRow})
			out = append(out, conn.output.Push(rm, conn.input)...)
			ad := ivm.MakeAddChange(ivm.Node{Row: change.Row})
			out = append(out, conn.output.Push(ad, conn.input)...)
			conn.lastPushedEpoch = epoch
			continue
		}

		ivmChange := sourceChangeToChange(change)
		if ivmChange == nil {
			continue
		}
		out = append(out, conn.output.Push(*ivmChange, conn.input)...)
		conn.lastPushedEpoch = epoch
	}

	return out
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
// SQL generation is delegated to sqlite.BuildSelectQuery — the same
// builder the legacy in-process sqlite.TableSource uses, so cursor
// lexicographic semantics, NULL-aware comparisons, and constraint
// emission are byte-identical between the two source variants.
//
// Coverage:
//   - ORDER BY from conn.sort (or PK ascending if nil)
//   - Reverse flips ASC↔DESC
//   - Constraint → WHERE eq pushdown
//   - Start cursor → WHERE lexicographic compare
//   - Filter predicate applied post-scan (per-connection Go closure;
//     can't be pushed to SQL because it's an opaque func)
//
// Phase 2d adds Push fanout + overlay handling on top of this.
func (s *Source) fetchForConn(req ivm.FetchRequest, conn *connection) []ivm.Node {
	// ORDER BY clause from connection sort (PK ascending if unset).
	// Always non-nil so cursor / orderBy paths in BuildSelectQuery work.
	order := conn.sort
	if order == nil {
		order = make(ivm.Ordering, len(s.primaryKey))
		for i, k := range s.primaryKey {
			order[i] = [2]string{k, "asc"}
		}
	}

	q := sqlite.BuildSelectQuery(
		s.tableName,
		s.columns,
		req.Constraint,
		nil, // filter pushdown via *Condition unused — we only have the Go predicate
		order,
		req.Reverse,
		req.Start,
	)

	// Snapshot epoch read under s.mu — bumped only by Push's deferred
	// cleanup, so during a Push fanout it's stable and the cache hands
	// back the same pre-commit tx. Between advances it bumps once per
	// Push, so the cache rolls the tx to a fresh WAL frame for the
	// next round.
	s.mu.Lock()
	epoch := s.snapshotEpoch
	s.mu.Unlock()

	tx, release, err := s.txCache.Acquire(context.Background(), s.tableName, epoch)
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: tx acquire: %v",
			s.tableName, err))
	}
	defer release()

	rows, err := tx.Query(q.SQL, q.Params...)
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: query: %v\nSQL: %s",
			s.tableName, err, q.SQL))
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

	// Apply pending Push overlay so a downstream Fetch fired mid-Push
	// sees the change as if it had landed. Gate is the same as
	// MemorySource: conn.lastPushedEpoch < overlay.Epoch means this
	// connection hasn't yet observed the overlay's change via its own
	// Output.Push, so we splice it in. Overlay rows are also subjected
	// to the connection's filter predicate — they're indistinguishable
	// from SQL-sourced rows downstream.
	s.mu.Lock()
	overlay := s.overlay
	s.mu.Unlock()
	if overlay != nil && conn.lastPushedEpoch < overlay.Epoch {
		out = applyOverlay(out, overlay.Change, conn.compareRows, req.Constraint, s.primaryKey)
		if conn.filterPredicate != nil {
			filtered := out[:0]
			for _, n := range out {
				if conn.filterPredicate(n.Row) {
					filtered = append(filtered, n)
				}
			}
			out = filtered
		}
	}
	return out
}

// quoteIdent escapes a SQL identifier (table or column name) by doubling
// any embedded quotes. The result is double-quoted, which both SQLite
// and the SQL standard accept for identifiers.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
