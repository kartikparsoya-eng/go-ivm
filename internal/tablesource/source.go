package tablesource

// Source: TableSource leaf implementing engine.Source against the TS
// replica's SQLite file. The architecture mirrors TS's Snapshotter +
// TableSource pair (`mono/packages/zero-cache/.../snapshotter.ts`,
// `mono/packages/zqlite/.../table-source.ts`):
//
//   - Per Source: one dedicated *sql.Conn from a writable pool.
//   - On first use: `BEGIN CONCURRENT` on that conn — the "prev snapshot".
//     Every read in this Source goes through this conn (so it sees the
//     pinned WAL frame plus any in-flight writes from this batch).
//   - On Push: `writeChange` applies INSERT/UPDATE/DELETE on the prev
//     conn within the same tx. Subsequent Fetches in the same batch
//     observe those writes (read-your-own-writes within the tx). This
//     is what TS's TableSource.#writeChange does (table-source.ts:416).
//   - On OnAdvanceEnd (called by engine.signalAdvanceEnd at end of
//     batch): ROLLBACK + new BEGIN CONCURRENT, repinning at the
//     post-batch WAL frame. That's TS's `Snapshot.resetToHead()`
//     (snapshotter.ts:392).
//
// Why this matters: the previous architecture used sqlite3_snapshot_get
// + per-Fetch sqlite3_snapshot_open to pin reads at a WAL frame, with
// an in-memory `batchDelta` overlaying not-yet-applied changes during a
// batch. That overlay couldn't influence SQL ordering — so a tied
// sort-key REMOVE-then-ADD inside a batch returned rows in a different
// order than TS, which made Take pick a different bound row. The TS
// approach writes the change into the SQL tx, so subsequent SELECTs
// produce the correct ordering on their own.

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// Source is the read-only TableSource leaf. One instance per (CG, table).
type Source struct {
	// ctx is derived from the CG's lifetime context. All SQLite calls use
	// this rather than context.Background() so that CG teardown / sidecar
	// shutdown can cancel blocked queries (e.g. WAL contention busy-wait)
	// instead of leaking goroutines. cancel is called from Close().
	ctx    context.Context
	cancel context.CancelFunc

	// db is the original read-only pool from `tablesource.Open`. Kept
	// only for the initial `SELECT 1 FROM <table> LIMIT 0` table-presence
	// probe in New() — the prev-tx path below uses writableDB exclusively.
	db *sql.DB

	// writableDB is the writable pool from `tablesource.OpenWritable`.
	// Each Source acquires ONE dedicated *sql.Conn from it on first use,
	// holds it until Close. Writes never commit (we always rollback) so
	// the underlying file is never mutated — matches TS Snapshotter's
	// "Applied changes are ephemeral" contract (snapshotter.ts:283).
	writableDB *sql.DB

	tableName  string
	primaryKey []string
	columns    map[string]sqlite.ColumnSchema

	// Cached ordering of columns + cached SQL for writeChange. Computed
	// once at New() so the Push hot path doesn't re-derive them per
	// change. Mirrors TS's `#getStatementsFor` cache (table-source.ts:136).
	columnOrder    []string // stable iteration order for INSERT VALUES (...)
	nonPKCols      []string // cols not in the primary key, for UPDATE SET ...
	insertSQL      string
	deleteSQL      string
	updateSQL      string // "" if all columns are part of the primary key
	checkExistsSQL string // SELECT 1 FROM t WHERE pk=? ... LIMIT 1

	// mu serializes all Source state: connection list, pushEpoch,
	// overlay, AND the prev-tx (prevConn + prevTxStarted). Acquired
	// across the full Push and Fetch hot paths because the prev-tx
	// conn is single-flight (one writer at a time per SQLite tx). TS
	// has implicit serialization via its single-threaded JS event loop;
	// we recreate it explicitly. Hot-path contention is bounded because
	// in practice all calls on a given Source come from one CG worker.
	mu          sync.Mutex
	connections []*connection
	pushEpoch   int

	// overlay is the pending Push that hasn't finished fanning out yet.
	// A Fetch fired DURING the current Push's output.Push (e.g. a
	// child-side fetch inside an EXISTS join) sees the change spliced
	// in if its connection hasn't observed the push yet. After the
	// current Push's writeChange lands in the prev tx, overlay clears
	// and subsequent Fetches see the change via the SQL read directly.
	// Mirrors TS MemorySource's `#overlay` field (memory-source.ts).
	overlay *ivm.Overlay

	// Prev-tx state.
	//
	// prevConn is the *sql.Conn dedicated to this Source's prev snapshot.
	// Acquired lazily on first ensurePrevTx call. Released on Close.
	//
	// prevTxStarted is true once a `BEGIN CONCURRENT` has succeeded on
	// prevConn (or plain `BEGIN` if the build doesn't have rocicorp's
	// wal2 patch). False after OnAdvanceEnd has rolled it back but
	// before the next ensurePrevTx restarts it.
	//
	// beginStmt is the BEGIN variant that worked the first time
	// (`BEGIN CONCURRENT` or `BEGIN`). Cached so OnAdvanceEnd doesn't
	// retry CONCURRENT each cycle when the build doesn't support it.
	prevConn      *sql.Conn
	prevTxStarted bool
	beginStmt     string

	// externalConn, when non-nil, redirects ALL prev-tx reads/writes to a
	// connection owned by something else (the Snapshotter's `prev` Snapshot —
	// P2 frame-coordination). While bound, ensurePrevTx/OnAdvanceEnd are
	// no-ops: the Snapshotter owns the pinned BEGIN CONCURRENT frame the diff
	// was derived against, so applying that diff's changes here lands them in
	// the SAME frame (no independent re-pin = no frame-timing drift). Set/
	// cleared via BindConn/UnbindConn around a driven advance or hydrate.
	externalConn *sql.Conn

	// removedInBatch tracks PKs removed by writeChangeLocked in the
	// current advance batch. Used to distinguish intra-batch duplicate
	// Removes (skipped, matching TS's no-op filter) from genuine drift
	// (panics with DriftError). Allocated lazily in trackRemoved and
	// cleared by ClearBatchState at the end of each advance batch.
	//
	// IMPORTANT: it is cleared ONLY via ClearBatchState (engine
	// signalAdvanceEnd), NOT via OnAdvanceEnd. OnAdvanceEnd is also reached
	// by the drift audit's RefreshSnapshot, which runs lock-free and
	// CONCURRENTLY with an in-flight advance batch (engine.RefreshAllSources).
	// Clearing here would let a concurrent audit wipe the set mid-batch —
	// between two pushes, when overlay is nil — and resurface the very
	// false-drift this set exists to suppress.
	removedInBatch map[string]bool
	addedInBatch   map[string]ivm.Row
	// stmtCache memoizes prepared SELECT statements for fetchForConn, keyed by
	// (active conn, SQL text). database/sql's one-shot QueryContext re-runs
	// sqlite3_prepare_v2 on every call (14.6% of cgo time in the live read-path
	// profile); a Conn-bound *sql.Stmt amortizes that to a cheap sqlite3_reset.
	//
	// Keyed by conn pointer because a Conn-bound Stmt is valid ONLY on the conn
	// it was prepared on. The conn set is tiny and stable — this Source's own
	// prevConn plus the Snapshotter's two leapfrog frame conns (bound via
	// BindConn), all re-pinned in place via ROLLBACK+BEGIN and never reopened
	// mid-life — so cardinality stays at a few entries and a cached stmt stays
	// valid across advances (a prepare_v2 stmt is frame-independent and SQLite
	// auto-recompiles it on the rare replica-schema change). Invalidated
	// whenever a conn is torn down. Guarded by s.mu (held across the fetch).
	stmtCache map[*sql.Conn]map[string]*sql.Stmt

	// readerPool, when non-nil, is a CG-shared pool of read connections ALL
	// pinned to the same WAL frame (one stateVersion). It is bound ONLY during
	// the advance-free cold-start hydrate window (GO_IVM_HYDRATE_READERS>1),
	// where it lets the per-query hydrate goroutines read this Source in
	// parallel instead of serializing on the single bound externalConn. While
	// bound, fetchForConn takes the lock-free fetchViaPool path: the pool is
	// bound only when no Push is in flight, so s.overlay is always nil and the
	// per-connection fields read during a fetch are immutable post-Connect.
	// atomic so fetchForConn can read it without s.mu. Set/cleared via
	// BindReaderPool / UnbindReaderPool (engine.BindTableSourcesToReaderPool).
	readerPool atomic.Pointer[ReaderPool]
}

// connection is one downstream pipeline subscribed to this source.
type connection struct {
	sort            ivm.Ordering
	splitEditKeys   map[string]bool
	compareRows     ivm.Comparator
	filterPredicate func(ivm.Row) bool
	filterCondition *sqlite.Condition

	// Set by SetOutput once the operator above us wires its receiver.
	output ivm.Output
	input  ivm.Input

	// LastPushedEpoch is the most recent pushEpoch this connection has
	// already observed via its Output.Push. Read during Fetch to gate
	// overlay application.
	lastPushedEpoch int
}

// New constructs a Source for tableName.
//
// db is the read-only pool used for the table-presence probe; the live
// read+write path uses writableDB instead.
//
// columns must include every column the source ever returns — anything
// missing gets dropped silently from fetched rows. primaryKey must be
// non-empty and every entry must be present in columns.
func New(db *sql.DB, writableDB *sql.DB, tableName string, columns map[string]sqlite.ColumnSchema, primaryKey []string) (*Source, error) {
	return NewWithContext(context.Background(), db, writableDB, tableName, columns, primaryKey)
}

// NewWithContext constructs a Source whose SQLite operations are cancellable
// via the provided context. Use this when the caller owns a CG-scoped context
// that should abort in-flight queries on teardown.
func NewWithContext(parent context.Context, db *sql.DB, writableDB *sql.DB, tableName string, columns map[string]sqlite.ColumnSchema, primaryKey []string) (*Source, error) {
	if db == nil {
		return nil, fmt.Errorf("tablesource.New: db is nil")
	}
	if writableDB == nil {
		return nil, fmt.Errorf("tablesource.New: writableDB is nil")
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

	// Pre-compute the column ordering used for INSERT VALUES (...) and
	// derived SQL strings. Sorted for stable iteration; matches the
	// behavior TS gets implicitly from Object.keys insertion order.
	colOrder := make([]string, 0, len(columns))
	for c := range columns {
		colOrder = append(colOrder, c)
	}
	// Sort for determinism so the generated SQL is identical across runs.
	for i := 1; i < len(colOrder); i++ {
		for j := i; j > 0 && colOrder[j] < colOrder[j-1]; j-- {
			colOrder[j], colOrder[j-1] = colOrder[j-1], colOrder[j]
		}
	}
	pkSet := make(map[string]bool, len(primaryKey))
	for _, k := range primaryKey {
		pkSet[k] = true
	}
	nonPK := make([]string, 0, len(colOrder))
	for _, c := range colOrder {
		if !pkSet[c] {
			nonPK = append(nonPK, c)
		}
	}

	insertCols := make([]string, len(colOrder))
	for i, c := range colOrder {
		insertCols[i] = quoteIdent(c)
	}
	placeholders := make([]string, len(colOrder))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(tableName),
		strings.Join(insertCols, ","),
		strings.Join(placeholders, ","),
	)

	pkConds := make([]string, len(primaryKey))
	for i, k := range primaryKey {
		pkConds[i] = quoteIdent(k) + "=?"
	}
	deleteSQL := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		quoteIdent(tableName),
		strings.Join(pkConds, " AND "),
	)

	updateSQL := ""
	if len(nonPK) > 0 {
		setClauses := make([]string, len(nonPK))
		for i, c := range nonPK {
			setClauses[i] = quoteIdent(c) + "=?"
		}
		updateSQL = fmt.Sprintf(
			"UPDATE %s SET %s WHERE %s",
			quoteIdent(tableName),
			strings.Join(setClauses, ","),
			strings.Join(pkConds, " AND "),
		)
	}

	checkExistsSQL := fmt.Sprintf(
		"SELECT 1 FROM %s WHERE %s LIMIT 1",
		quoteIdent(tableName),
		strings.Join(pkConds, " AND "),
	)

	ctx, cancel := context.WithCancel(parent)
	return &Source{
		ctx:            ctx,
		cancel:         cancel,
		db:             db,
		writableDB:     writableDB,
		tableName:      tableName,
		primaryKey:     primaryKey,
		columns:        columns,
		columnOrder:    colOrder,
		nonPKCols:      nonPK,
		insertSQL:      insertSQL,
		deleteSQL:      deleteSQL,
		updateSQL:      updateSQL,
		checkExistsSQL: checkExistsSQL,
	}, nil
}

// Close rolls back the prev tx (if started) and releases the dedicated
// conn back to the writable pool. Idempotent. Also cancels the Source's
// context so any in-flight SQLite calls unblock.
func (s *Source) Close() error {
	s.cancel()
	s.UnbindReaderPool()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeAllCachedStmtsLocked()
	if s.prevConn != nil {
		if s.prevTxStarted {
			_, _ = s.prevConn.ExecContext(context.Background(), "ROLLBACK")
			s.prevTxStarted = false
		}
		_ = s.prevConn.Close()
		s.prevConn = nil
	}
	return nil
}

// BindConn binds this Source's prev-tx reads/writes to an externally-owned,
// already-pinned connection (the Snapshotter's `prev` Snapshot conn). While
// bound, the Source does NOT manage its own prev tx — the snapshotter owns the
// frame. P2 frame-coordination: the engine applies a Snapshotter-derived diff
// into the very frame it was derived against, eliminating the residual drift
// that an independently-re-pinned per-Source tx caused.
func (s *Source) BindConn(conn *sql.Conn) {
	s.mu.Lock()
	s.externalConn = conn
	s.mu.Unlock()
}

// UnbindConn detaches the external connection; the Source reverts to its own
// prev tx (re-acquired lazily on the next ensurePrevTx).
func (s *Source) UnbindConn() {
	s.mu.Lock()
	s.externalConn = nil
	s.mu.Unlock()
}

// activeConn returns the connection reads/writes should use: the externally
// bound (Snapshotter-owned) conn when frame-coordinated, else this Source's own
// prev-tx conn. MUST be called with s.mu held.
func (s *Source) activeConn() *sql.Conn {
	if s.externalConn != nil {
		return s.externalConn
	}
	return s.prevConn
}

// preparedSelectLocked returns a prepared statement for query bound to conn,
// preparing and caching it on first use. MUST be called with s.mu held. The
// returned *sql.Stmt executes on conn's current transaction (the pinned frame
// plus any in-flight writeChange writes), so it observes read-your-own-writes
// exactly like the previous one-shot QueryContext did. The caller resolved
// conn from activeConn() under the same lock, and the cache is invalidated
// whenever a conn is torn down, so a stale entry can never be executed.
func (s *Source) preparedSelectLocked(conn *sql.Conn, query string) (*sql.Stmt, error) {
	bySQL := s.stmtCache[conn]
	if bySQL == nil {
		if s.stmtCache == nil {
			s.stmtCache = make(map[*sql.Conn]map[string]*sql.Stmt, 3)
		}
		bySQL = make(map[string]*sql.Stmt, 4)
		s.stmtCache[conn] = bySQL
	}
	if st, ok := bySQL[query]; ok {
		return st, nil
	}
	st, err := conn.PrepareContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	bySQL[query] = st
	return st, nil
}

// closeCachedStmtsForConnLocked finalizes and drops every prepared statement
// cached against conn. Called immediately before conn is closed so the
// compiled sqlite3_stmts are released and a later conn that happens to reuse
// the same pointer can't hit a stale entry. MUST be called with s.mu held.
func (s *Source) closeCachedStmtsForConnLocked(conn *sql.Conn) {
	bySQL := s.stmtCache[conn]
	if bySQL == nil {
		return
	}
	for _, st := range bySQL {
		_ = st.Close()
	}
	delete(s.stmtCache, conn)
}

// closeAllCachedStmtsLocked finalizes every cached statement across all conns.
// Used at Source teardown. MUST be called with s.mu held.
func (s *Source) closeAllCachedStmtsLocked() {
	for _, bySQL := range s.stmtCache {
		for _, st := range bySQL {
			_ = st.Close()
		}
	}
	s.stmtCache = nil
}

// ensurePrevTxLocked makes sure prevConn is acquired and a tx is open.
// MUST be called with s.mu held.
//
// First-time path:
//  1. Acquire a *sql.Conn from writableDB.
//  2. Try `BEGIN CONCURRENT` (rocicorp's wal2 patch). On error, fall
//     back to plain `BEGIN`.
//  3. Cache which BEGIN variant worked in s.beginStmt.
//  4. Issue a `SELECT 1` to actually acquire the read lock — `BEGIN
//     CONCURRENT` alone is deferred until first read (snapshotter.ts:308).
//
// Subsequent calls (after OnAdvanceEnd rolled the tx back) just re-issue
// the cached BEGIN variant + the warm-up SELECT.
func (s *Source) ensurePrevTxLocked() error {
	// Frame-coordinated: the Snapshotter owns the pinned frame on externalConn,
	// already BEGIN-and-read. Nothing for this Source to acquire.
	if s.externalConn != nil {
		return nil
	}
	ctx := context.Background()
	if s.prevConn == nil {
		// Use a bounded timeout so pool exhaustion surfaces as a fast error
		// instead of blocking indefinitely (which previously caused the TS-side
		// 120s RPC timeout to fire with no diagnostic). 30s is long enough to
		// ride out a transient checkpoint stall but short enough to fail before
		// the TS timeout.
		acquireCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		conn, err := s.writableDB.Conn(acquireCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("ensurePrevTx %s: acquire conn (30s timeout): %w", s.tableName, err)
		}
		s.prevConn = conn
	}
	if s.prevTxStarted {
		return nil
	}
	if s.beginStmt == "" {
		// First-ever tx: try CONCURRENT, fall back to plain BEGIN.
		// CONCURRENT requires rocicorp's wal2 patch — present in the
		// libsqlite3-tag build, absent in mattn-bundled test builds.
		if _, err := s.prevConn.ExecContext(ctx, "BEGIN CONCURRENT"); err == nil {
			s.beginStmt = "BEGIN CONCURRENT"
		} else if _, err2 := s.prevConn.ExecContext(ctx, "BEGIN"); err2 == nil {
			s.beginStmt = "BEGIN"
		} else {
			return fmt.Errorf("ensurePrevTx %s: BEGIN: %w", s.tableName, err2)
		}
	} else {
		if _, err := s.prevConn.ExecContext(ctx, s.beginStmt); err != nil {
			return fmt.Errorf("ensurePrevTx %s: %s: %w", s.tableName, s.beginStmt, err)
		}
	}
	// Warm-up read to actually acquire the snapshot. BEGIN CONCURRENT
	// is deferred-style: no lock is taken until first access. Without
	// this, sqlite3_snapshot_get-equivalent semantics don't kick in
	// until the first Fetch — which would race with a concurrent
	// writer commit. Matches snapshotter.ts:308-311.
	if _, err := s.prevConn.ExecContext(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("ensurePrevTx %s: warm-up: %w", s.tableName, err)
	}
	s.prevTxStarted = true
	return nil
}

// OnAdvanceEnd is called by the engine.signalAdvanceEnd at the end of
// each advance batch (success OR drift). Rolls back the prev tx
// (discarding all writeChange-applied mutations) and starts a fresh
// `BEGIN CONCURRENT` pinned at the current WAL frame.
//
// Matches TS Snapshotter.advanceWithoutDiff (snapshotter.ts:183-196)
// which does: prev.resetToHead() → rollback() + new BEGIN CONCURRENT.
func (s *Source) OnAdvanceEnd() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Frame-coordinated: the Snapshotter drives the leapfrog (resetToHead on its
	// own prev conn). This Source must NOT roll back or re-pin externalConn.
	if s.externalConn != nil {
		return
	}
	// Guard against TOCTOU race with RefreshSnapshot: overlay may have been set
	// between RefreshSnapshot's unlock and this lock acquisition. Rolling back
	// mid-push would violate the snapshot-consistency guarantee that downstream
	// Fetches inside Output.Push depend on.
	if s.overlay != nil {
		return
	}
	if s.prevConn == nil || !s.prevTxStarted {
		return
	}
	ctx := context.Background()
	if _, err := s.prevConn.ExecContext(ctx, "ROLLBACK"); err != nil {
		// If ROLLBACK fails the conn is in an unknown state; close it
		// so the next ensurePrevTx will reacquire a fresh one. Drop the
		// statements cached against it first — they die with the conn.
		s.closeCachedStmtsForConnLocked(s.prevConn)
		_ = s.prevConn.Close()
		s.prevConn = nil
		s.prevTxStarted = false
		return
	}
	s.prevTxStarted = false

	// EAGERLY re-pin the prev snapshot here — do NOT defer to the next
	// Push/Fetch. This is the crux of TS Snapshotter.resetToHead's timing
	// (snapshotter.ts:183-196): it pins the new snapshot at the END of the
	// current advance, BEFORE the replicator commits the next batch. The
	// IVM then reads the next batch from a frame that does NOT yet contain
	// that batch's rows.
	//
	// Deferring the re-BEGIN to the next batch's first Push pins the prev tx
	// AFTER the replicator has already committed that batch (the replicator
	// writes the replica file and only then sends the advance RPC to the
	// sidecar). checkExists would then see the batch's own freshly-committed
	// rows and false-drift every ADD: the batch's own freshly-committed
	// rows look like unexpected additions, forcing needless re-hydrates
	// while TS produces the changes normally.
	if err := s.ensurePrevTxLocked(); err != nil {
		// Re-pin failed; leave prevTxStarted=false so the next ensurePrevTx
		// retries. The next batch reads from a later frame (possibly
		// including its own commits) — a missed re-pin degrades to the old
		// lazy behavior for one batch rather than wedging.
		_ = err
	}
}

// RefreshSnapshot is the legacy API name kept for callers (e.g. the
// drift audit) that want the prev tx repinned to the current WAL frame
// outside the engine's normal batch lifecycle. Semantically equivalent
// to OnAdvanceEnd: rollback + lazy re-BEGIN on next use.
//
// No-op while a Push is in progress (overlay != nil) — rolling the tx
// mid-fanout would invalidate the snapshot-consistency guarantee that
// downstream Fetches inside Output.Push depend on.
func (s *Source) RefreshSnapshot() {
	s.mu.Lock()
	if s.overlay != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.OnAdvanceEnd()
}

// TableName / PrimaryKey / NormalizeRow satisfy engine.Source.

func (s *Source) TableName() string { return s.tableName }

func (s *Source) PrimaryKey() []string { return s.primaryKey }

// NormalizeRow coerces every value in row through FromSQLiteType so a row
// arriving from msgpack (numbers as float64, no type fidelity) ends up
// shaped the same as a row read from this Source's Fetch. Unknown
// columns are left as-is — TS MemorySource has the same behavior.
//
// json columns are deliberately skipped. This mirrors TS's coerce-once model:
// fromSQLiteType (the strict JSON.parse) runs ONLY at the SQLite-read boundary
// (table-source.ts #fetch/#pull → fromSQLiteTypes), and the snapshotter runs it
// before shipping over the wire (snapshotter.ts:577-581). TS's push path then
// consumes the incoming change row AS-IS — genPush/#writeChange never re-coerce
// with fromSQLiteType. So by the time a json value reaches this Go wire-normalize
// path it is ALREADY parsed: a json scalar string like "Payment Failures" arrives
// as a bare Go string, not as JSON text. Re-running FromSQLiteType's strict parse
// on it would panic (W1). Passing it through unchanged is both panic-free and
// behavior-identical to the old code for every other json shape (objects, arrays,
// numbers, bools, null already hit FromSQLiteType's json default: return v). The
// strict parse stays where it belongs — the SQLite-read boundary in scanRows.
func (s *Source) NormalizeRow(row ivm.Row) {
	if row == nil {
		return
	}
	for col, val := range row {
		cs, ok := s.columns[col]
		if !ok {
			continue
		}
		if cs.Type == "json" {
			continue
		}
		row[col] = sqlite.FromSQLiteType(val, cs.Type)
	}
}

// Connect registers a new pipeline connection.
func (s *Source) Connect(
	sort ivm.Ordering,
	filter *builder.Condition,
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
		filterCondition: s.convertFilter(filter),
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

// Push fans the change through every subscribed connection AND applies
// the change to the prev-snapshot tx via writeChange so subsequent
// Fetches in this batch observe it.
//
// Order matches TS MemorySource.genPushAndWriteWithSplitEdit
// (memory-source.ts:452-506):
//  1. Decide shouldSplitEdit GLOBALLY by scanning every connection's
//     splitEditKeys against the Edit's old/new.
//  2. For each source-level change (1 or 2), per change:
//     a. Set overlay (epoch++).
//     b. Fan out to every connection through filterPush.
//     c. After fanout: writeChange applies the change to prev tx.
//     d. Clear overlay.
func (s *Source) Push(change ivm.SourceChange) []ivm.Change {
	// Acquire mu just long enough to set up the in-flight work:
	// ensure prev tx, snapshot connections, decide shouldSplitEdit.
	// Release before fanout — downstream output.Push callbacks may
	// recursively call back into this Source (Fetch / RefreshSnapshot)
	// and would deadlock against a held mu.
	s.mu.Lock()
	if err := s.ensurePrevTxLocked(); err != nil {
		s.mu.Unlock()
		panic(fmt.Sprintf("tablesource.Source.Push %s: ensurePrevTx: %v", s.tableName, err))
	}
	shouldSplitEdit := false
	if change.Type == ivm.ChangeTypeEdit {
		for _, conn := range s.connections {
			if conn.splitEditKeys != nil && editChangesSplitKeys(change, conn.splitEditKeys) {
				shouldSplitEdit = true
				break
			}
		}
	}
	conns := make([]*connection, len(s.connections))
	copy(conns, s.connections)
	s.mu.Unlock()

	var out []ivm.Change
	if change.Type == ivm.ChangeTypeEdit && shouldSplitEdit {
		out = append(out, s.genPushAndWrite(ivm.MakeSourceChangeRemove(change.OldRow), conns)...)
		out = append(out, s.genPushAndWrite(ivm.MakeSourceChangeAdd(change.Row), conns)...)
	} else {
		out = append(out, s.genPushAndWrite(change, conns)...)
	}
	return out
}

// genPushAndWrite runs ONE source-level change through every connection's
// filterPush, then applies the change to the prev tx via writeChange.
//
// Locking discipline mirrors TS's MemorySource.genPushAndWriteWithSplitEdit:
//   - Overlay is set under mu, then mu released. Fanout runs WITHOUT mu
//     so downstream callbacks can Fetch / RefreshSnapshot recursively.
//   - After fanout: re-acquire mu, run writeChange against prev tx, clear
//     overlay, release.
//
// Direct port of TS's genPushAndWrite (memory-source.ts).
func (s *Source) genPushAndWrite(change ivm.SourceChange, conns []*connection) []ivm.Change {
	s.mu.Lock()
	// BUG 1b: if this Edit's OldRow was removed earlier in this batch,
	// convert to Add (matching TS's lazy iteration: the prev row was
	// already deleted by a previous writeChange, so TS's prev.getRows
	// returns empty and TS emits an Add, not an Edit).
	if change.Type == ivm.ChangeTypeEdit && !s.existsLocked(change.OldRow) {
		if s.removedInBatch != nil && s.removedInBatch[s.pkKey(change.OldRow)] {
			change = ivm.MakeSourceChangeAdd(change.Row)
		}
	}
	// BUG 1c: if this Add duplicates a row added earlier in this batch,
	// convert to Edit (matching TS's lazy iteration: TS's prev.getRows sees
	// the just-INSERTed row and produces an Edit, not a duplicate Add).
	if change.Type == ivm.ChangeTypeAdd && s.existsLocked(change.Row) {
		if s.addedInBatch != nil {
			if prevRow, ok := s.addedInBatch[s.pkKey(change.Row)]; ok {
				change = ivm.MakeSourceChangeEdit(change.Row, prevRow)
			}
		}
	}
	// Drift validation BEFORE any state mutation or fanout — matches TS
	// MemorySource.genPush (memory-source.ts:529-550) and the in-memory
	// MemorySource port (ivm/source.go genPush). Raising here, before
	// pushEpoch++/overlay/writeChange and before any Output.Push, keeps the
	// DriftError recoverability invariant: engine.Advance recovers the panic
	// and drops the in-flight advance with no half-applied state. This also
	// replaces the previous raw "UNIQUE constraint failed" panic from
	// writeChange (a dup-Add) which was NOT a *DriftError and would crash the
	// cg/sidecar instead of triggering a clean re-hydrate.
	if d := s.driftCheckLocked(change); d != nil {
		// BUG 1: skip Remove against a row removed earlier in this batch
		if change.Type == ivm.ChangeTypeRemove && s.removedInBatch != nil && s.removedInBatch[s.pkKey(change.Row)] {
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()
		panic(d)
	}
	s.pushEpoch++
	epoch := s.pushEpoch
	s.overlay = &ivm.Overlay{Epoch: epoch, Change: change}
	s.mu.Unlock()

	var out []ivm.Change
	for _, conn := range conns {
		if conn.output == nil {
			continue
		}
		// Bump lastPushedEpoch BEFORE filterPush so a downstream Fetch
		// inside output.Push sees the gate match TS's genPush ordering
		// (memory-source.ts:555). lastPushedEpoch is a single int field
		// touched only from Push fanout + Fetch overlay-gate read; the
		// engine serializes both per-source so no data race.
		conn.lastPushedEpoch = epoch
		outputChange := sourceChangeToChange(change)
		if outputChange == nil {
			continue
		}
		out = append(out, filterPush(*outputChange, conn)...)
	}

	// Re-acquire mu for writeChange + overlay clear.
	s.mu.Lock()
	err := s.writeChangeLocked(change)
	s.overlay = nil
	s.mu.Unlock()
	if err != nil {
		// Stale prev tx is unrecoverable from inside Push; surface
		// as panic and let the engine's drift-recovery handle it
		// (engine.Advance catches and re-inits).
		panic(fmt.Sprintf("tablesource.Source.Push %s: writeChange: %v", s.tableName, err))
	}

	return out
}

// driftCheckLocked validates the change against current source state,
// returning a *ivm.DriftError if it diverged (so the caller raises it
// BEFORE any fanout/mutation). Direct port of TS genPush's pre-fanout
// asserts (memory-source.ts:529-550) using TS TableSource's `exists`
// closure (table-source.ts:399-413, backed by the checkExists stmt).
// Mirrors the in-memory MemorySource Go port (ivm/source.go genPush)
// switch + DriftError construction exactly.
//
//   - ADD:    assert !exists(row)      → dup-Add drift
//   - REMOVE: assert  exists(row)      → missing-row drift
//   - EDIT:   assert  exists(oldRow)   → missing-row drift
//
// MUST be called with s.mu held (queries the prev tx via prevConn).
func (s *Source) driftCheckLocked(change ivm.SourceChange) *ivm.DriftError {
	switch change.Type {
	case ivm.ChangeTypeAdd:
		if s.existsLocked(change.Row) {
			return &ivm.DriftError{Table: s.tableName, Op: "Add", PK: s.pkOf(change.Row), HasCount: s.countLocked()}
		}
	case ivm.ChangeTypeRemove:
		if !s.existsLocked(change.Row) {
			return &ivm.DriftError{Table: s.tableName, Op: "Remove", PK: s.pkOf(change.Row), HasCount: s.countLocked()}
		}
	case ivm.ChangeTypeEdit:
		if !s.existsLocked(change.OldRow) {
			return &ivm.DriftError{Table: s.tableName, Op: "Edit", PK: s.pkOf(change.OldRow), HasCount: s.countLocked()}
		}
	}
	return nil
}

// existsLocked reports whether a row with row's primary key exists in the
// prev tx. Faithful port of TS TableSource's `exists` closure
// (table-source.ts:399-402): checkExists SELECT 1 ... LIMIT 1.
//
// MUST be called with s.mu held (queries prevConn). ensurePrevTxLocked has
// already run by the time genPushAndWrite reaches the drift check.
func (s *Source) existsLocked(row ivm.Row) bool {
	args := s.rowToPKArgs(row)
	var one int
	err := s.activeConn().QueryRowContext(context.Background(), s.checkExistsSQL, args...).Scan(&one)
	return err == nil && one == 1
}

// pkOf extracts the primary-key columns of row into a fresh map, matching
// the MemorySource port's DriftError.PK construction (ivm/source.go).
func (s *Source) pkOf(row ivm.Row) map[string]ivm.Value {
	pk := make(map[string]ivm.Value, len(s.primaryKey))
	for _, c := range s.primaryKey {
		pk[c] = row[c]
	}
	return pk
}

// countLocked returns the current source row count for DriftError
// diagnostics (analogous to len(ms.data) in the MemorySource port). Only
// called on the rare drift path, so the COUNT(*) cost is irrelevant.
func (s *Source) countLocked() int {
	var n int
	if err := s.activeConn().QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM "+quoteIdent(s.tableName)).Scan(&n); err != nil {
		return -1
	}
	return n
}

// writeChangeLocked applies the SourceChange to the prev-snapshot tx as
// SQL writes. Direct port of TS TableSource.#writeChange
// (table-source.ts:416-478).
//
// MUST be called with s.mu held (so prevConn is single-flight).
func (s *Source) writeChangeLocked(change ivm.SourceChange) error {
	ctx := context.Background()
	conn := s.activeConn()
	switch change.Type {
	case ivm.ChangeTypeAdd:
		args := s.rowToInsertArgs(change.Row)
		if _, err := conn.ExecContext(ctx, s.insertSQL, args...); err != nil {
			return fmt.Errorf("INSERT: %w", err)
		}
		s.trackAdded(change.Row)
		return nil

	case ivm.ChangeTypeRemove:
		args := s.rowToPKArgs(change.Row)
		if _, err := conn.ExecContext(ctx, s.deleteSQL, args...); err != nil {
			return fmt.Errorf("DELETE: %w", err)
		}
		s.trackRemoved(change.Row)
		return nil

	case ivm.ChangeTypeEdit:
		if s.canUseUpdate(change.OldRow, change.Row) {
			merged := mergeRow(change.OldRow, change.Row)
			args := append(s.rowToNonPKArgs(merged), s.rowToPKArgs(merged)...)
			if _, err := conn.ExecContext(ctx, s.updateSQL, args...); err != nil {
				return fmt.Errorf("UPDATE: %w", err)
			}
			s.trackAdded(change.Row)
			return nil
		}
		// PK changed: DELETE + INSERT.
		delArgs := s.rowToPKArgs(change.OldRow)
		if _, err := conn.ExecContext(ctx, s.deleteSQL, delArgs...); err != nil {
			return fmt.Errorf("EDIT.DELETE: %w", err)
		}
		s.trackRemoved(change.OldRow)
		insArgs := s.rowToInsertArgs(change.Row)
		if _, err := conn.ExecContext(ctx, s.insertSQL, insArgs...); err != nil {
			return fmt.Errorf("EDIT.INSERT: %w", err)
		}
		s.trackAdded(change.Row)
		return nil
	}
	return fmt.Errorf("writeChange: unknown change type %v", change.Type)
}

func (s *Source) trackRemoved(row ivm.Row) {
	if s.removedInBatch == nil {
		s.removedInBatch = make(map[string]bool)
	}
	s.removedInBatch[s.pkKey(row)] = true
}

func (s *Source) trackAdded(row ivm.Row) {
	if s.addedInBatch == nil {
		s.addedInBatch = make(map[string]ivm.Row)
	}
	s.addedInBatch[s.pkKey(row)] = row
}

// ClearBatchState drops the intra-batch removed-PK set. Called by
// engine.signalAdvanceEnd at the end of every advance batch (success OR
// drift), so the set never outlives the batch that built it and can never
// mask genuine cross-batch drift.
//
// Deliberately NOT folded into OnAdvanceEnd: OnAdvanceEnd is also reached by
// the drift audit's RefreshSnapshot (engine.RefreshAllSources), which runs
// lock-free and concurrently with an in-flight advance. Keeping the clear on
// the signalAdvanceEnd-only path guarantees it fires at true batch
// boundaries and never mid-batch. Mirrors ivm.MemorySource.ClearBatchState.
func (s *Source) ClearBatchState() {
	s.mu.Lock()
	s.removedInBatch = nil
	s.addedInBatch = nil
	s.mu.Unlock()
}

func (s *Source) pkKey(row ivm.Row) string {
	parts := make([]string, len(s.primaryKey))
	for i, k := range s.primaryKey {
		parts[i] = fmt.Sprintf("%v", row[k])
	}
	return strings.Join(parts, "\x00")
}

// canUseUpdate returns true if the edit can use UPDATE (PK unchanged
// AND there are non-PK columns to set). Matches TS's canUseUpdate
// (table-source.ts:647-659).
func (s *Source) canUseUpdate(oldRow, newRow ivm.Row) bool {
	for _, pk := range s.primaryKey {
		if ivm.CompareValues(oldRow[pk], newRow[pk]) != 0 {
			return false
		}
	}
	return s.updateSQL != ""
}

// mergeRow returns oldRow overlaid with newRow's fields. TS spreads
// {...oldRow, ...newRow} — newRow wins for any overlapping key.
func mergeRow(oldRow, newRow ivm.Row) ivm.Row {
	out := make(ivm.Row, len(oldRow)+len(newRow))
	for k, v := range oldRow {
		out[k] = v
	}
	for k, v := range newRow {
		out[k] = v
	}
	return out
}

// rowToInsertArgs returns SQLite-typed values in s.columnOrder order.
func (s *Source) rowToInsertArgs(row ivm.Row) []any {
	args := make([]any, len(s.columnOrder))
	for i, c := range s.columnOrder {
		args[i] = sqlite.ToSQLiteType(row[c], s.columns[c].Type)
	}
	return args
}

// rowToPKArgs returns SQLite-typed PK values in s.primaryKey order.
func (s *Source) rowToPKArgs(row ivm.Row) []any {
	args := make([]any, len(s.primaryKey))
	for i, k := range s.primaryKey {
		args[i] = sqlite.ToSQLiteType(row[k], s.columns[k].Type)
	}
	return args
}

// rowToNonPKArgs returns SQLite-typed non-PK values in s.nonPKCols order.
func (s *Source) rowToNonPKArgs(row ivm.Row) []any {
	args := make([]any, len(s.nonPKCols))
	for i, c := range s.nonPKCols {
		args[i] = sqlite.ToSQLiteType(row[c], s.columns[c].Type)
	}
	return args
}

// filterPush is the per-connection filter-aware push.
// Direct port of mono/packages/zql/src/ivm/filter-push.ts:10-38.
func filterPush(change ivm.Change, conn *connection) []ivm.Change {
	if conn.filterPredicate == nil {
		return conn.output.Push(change, conn.input)
	}
	switch change.Type {
	case ivm.ChangeTypeAdd, ivm.ChangeTypeRemove, ivm.ChangeTypeChild:
		if conn.filterPredicate(change.Node.Row) {
			return conn.output.Push(change, conn.input)
		}
		return nil
	case ivm.ChangeTypeEdit:
		return maybeSplitAndPushEditChange(change, conn)
	}
	return nil
}

// maybeSplitAndPushEditChange handles the EDIT-with-filter-transition
// case. Direct port of mono/packages/zql/src/ivm/maybe-split-and-push-edit-change.ts.
func maybeSplitAndPushEditChange(change ivm.Change, conn *connection) []ivm.Change {
	oldWasPresent := conn.filterPredicate(change.OldNode.Row)
	newIsPresent := conn.filterPredicate(change.Node.Row)
	if oldWasPresent && newIsPresent {
		return conn.output.Push(change, conn.input)
	}
	if oldWasPresent && !newIsPresent {
		return conn.output.Push(ivm.MakeRemoveChange(*change.OldNode), conn.input)
	}
	if !oldWasPresent && newIsPresent {
		return conn.output.Push(ivm.MakeAddChange(change.Node), conn.input)
	}
	return nil
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

// LeafSourceMarker marks sourceInput as a base source for Take's limit pushdown.
func (i *sourceInput) LeafSourceMarker() {}

func (i *sourceInput) SetOutput(o ivm.Output) {
	i.conn.output = o
}

func (i *sourceInput) Destroy() {
	i.src.disconnect(i.conn)
}

func (i *sourceInput) Fetch(req ivm.FetchRequest) iter.Seq[ivm.Node] {
	if pool := i.src.readerPool.Load(); pool != nil {
		return i.src.fetchViaPoolStream(req, i.conn, pool)
	}
	return slices.Values(i.src.fetchForConn(req, i.conn))
}

// disconnect removes conn from the source's connection list.
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
// Reads go through s.prevConn — the dedicated conn for this Source's
// prev-snapshot tx. The tx already contains any writeChanges from
// previous Pushes in the current batch, so SQL ordering is correct
// without any in-memory overlay/delta machinery.
//
// The one exception is the IN-FLIGHT current push: s.overlay holds the
// change being fanned out RIGHT NOW. writeChange hasn't run yet (it
// runs after fanout). A Fetch fired during that fanout (e.g., from a
// downstream Output.Push) splices the overlay in so the downstream
// sees the post-push view. Connection lastPushedEpoch gates this:
// connections that have already received the push via Output.Push
// don't re-see it via overlay (their lastPushedEpoch matches/exceeds).
func (s *Source) fetchForConn(req ivm.FetchRequest, conn *connection) []ivm.Node {
	// Cold-start parallel-hydrate fast path: when a frame-pinned reader pool is
	// bound (GO_IVM_HYDRATE_READERS>1), read via a borrowed pool conn WITHOUT
	// holding s.mu. Every pool conn is pinned at the same stateVersion as this
	// Source's bound frame, so the read is byte-identical to the single-conn
	// path — it just runs concurrently with sibling queries' fetches. The pool
	// is bound only during the advance-free hydrate window, so there is no
	// in-flight Push (s.overlay is nil) and no prev-tx writeChange to observe.
	// When a reader pool is bound, sourceInput.Fetch dispatches directly to
	// fetchViaPoolStream (lazy, row-at-a-time) before reaching here. This
	// pool check remains as a defensive fallback for any direct fetchForConn
	// caller that might execute with a pool still bound.
	if pool := s.readerPool.Load(); pool != nil {
		return s.fetchViaPool(req, conn, pool)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensurePrevTxLocked(); err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: ensurePrevTx: %v", s.tableName, err))
	}

	// ORDER BY clause from connection sort (PK ascending if unset).
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
		conn.filterCondition,
		order,
		req.Reverse,
		req.Start,
	)
	ctx := context.Background()
	// Reuse a prepared statement for this (conn, SQL) instead of letting
	// database/sql re-compile via sqlite3_prepare_v2 on every QueryContext
	// (14.6% of cgo time in the live read-path profile). activeConn is a
	// stable, single-flight conn (this Source's prevConn, or a Snapshotter
	// frame conn bound via BindConn), so the cached Conn-bound stmt stays valid
	// across advances and re-reads the new frame after each leapfrog.
	dbConn := s.activeConn()
	stmt, err := s.preparedSelectLocked(dbConn, q.SQL)
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: prepare: %v\nSQL: %s",
			s.tableName, err, q.SQL))
	}
	rows, err := stmt.QueryContext(ctx, q.Params...)
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

	out := s.scanRows(rows, colNames, conn, req, s.overlay != nil)

	// Apply in-flight overlay (the push currently fanning out, whose
	// writeChange hasn't run yet against the prev tx).
	//
	// Gate matches TS generateWithOverlay (memory-source.ts:634):
	//   apply iff lastPushedEpoch >= overlay.epoch
	//
	// i.e. the overlay is visible to the connection CURRENTLY being pushed
	// (its lastPushedEpoch was just set == overlay.epoch in genPushAndWrite),
	// NOT to sibling connections that haven't observed the push yet. This is
	// the path that makes an EXISTS/Join child re-fetch — which runs through
	// the very connection the push came in on — see the in-flight row, so
	// the existence condition flips and the parent is (re-)emitted. An
	// inverted gate (`<`) would hide the in-flight row from exactly that
	// re-fetch, causing advance to emit far fewer changes than TS. The
	// old snapshot+batchDelta
	// arch masked the inversion by applying every batch push to every fetch
	// unconditionally; the prev-tx arch relies on this gate being correct.
	//
	// Once writeChange runs (after fanout) and overlay clears, the SQL query
	// above naturally reflects the change — no overlay needed.
	//
	// The overlay must be spliced into `out` at the position the fetch's
	// effective order would place it — which for a reverse fetch is the
	// REVERSE comparator, NOT conn.compareRows (the forward one). TS's
	// #fetch passes makeComparator(sort, req.reverse) to generateWithOverlay
	// (table-source.ts:298-312); generateWithStart/overlaysForStartAt also
	// drops overlay rows that fall before req.start in that order. Using the
	// forward comparator here would put the in-flight Add at the wrong index
	// in a reverse fetch — exactly the fetch Take issues for its displaced-
	// bound lookup (start:bound, basis:'at', reverse:true) — so Take would
	// pick a different boundNode/beforeBoundNode and emit a different
	// displaced row than TS.
	if s.overlay != nil && conn.lastPushedEpoch >= s.overlay.Epoch {
		// PARTIAL-bound comparator: req.Start may be a partial pagination cursor
		// (e.g. {createdAt} while the sort is [createdAt, conversationId]). The
		// overlay start-gate (overlayRowAtOrAfterStart) compares the in-flight
		// row against req.Start.Row; with the plain MakeComparator the missing
		// cursor column hits CompareValues(rowVal, nil) → +1 and the gate keeps a
		// Basis:"after" boundary row it should drop (Go-vs-TS over-include at the
		// exclusive cursor boundary). MakePartialBoundComparator stops at the
		// first sort column absent from the cursor, matching SQL `col > NULL` and
		// the Skip operator's CompareWithPartialBound. It is identical to
		// MakeComparator for insertSorted (which compares two COMPLETE rows), so
		// overlay placement order is unchanged.
		effCmp := ivm.MakePartialBoundComparator(order, req.Reverse)
		out = applyOverlay(out, s.overlay.Change, effCmp, req.Constraint, req.Start, s.primaryKey)
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

// scanRows materialises rows into Nodes, applying the connection's residual
// (non-pushed-down) filterPredicate and the Take limit-pushdown early-stop.
// Shared by the locked single-conn path and the lock-free pool path — it reads
// only immutable Source state (s.columns) plus per-connection fields fixed at
// Connect, so it is safe to run from multiple goroutines on distinct rows/conns.
//
// overlayActive disables the limit-pushdown break: when an in-flight overlay is
// present the caller may splice a row into the top-N below, so the full
// candidate set is needed. The pool path always passes false (it is bound only
// in the advance-free window, where no overlay can exist).
func (s *Source) scanRows(
	rows *sql.Rows,
	colNames []string,
	conn *connection,
	req ivm.FetchRequest,
	overlayActive bool,
) []ivm.Node {
	var out []ivm.Node
	// Reuse the scan buffers across rows. `raw` holds one row's column values and
	// `ptrs` the &raw[i] pointers Scan writes through; both are fixed-shape for
	// the query, so allocating them per-row (this was ~588MB / 13% of all sidecar
	// allocations in the 20-user profile — the #2/#3 alloc lines after the row
	// map) is pure waste. Safe to reuse: each rows.Scan OVERWRITES raw[i] with a
	// freshly-materialised value, and FromSQLiteType copies that value into the
	// per-row `row` map (line below) — it never retains a reference to `raw` or
	// its slots — so the previous row's data, already handed to its own map, is
	// untouched by the next iteration.
	raw := make([]any, len(colNames))
	ptrs := make([]any, len(colNames))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	for rows.Next() {
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
		// Limit pushdown (Take.initialFetch): once we have req.Limit
		// post-predicate rows we can stop scanning. The SQLite cursor yields
		// lazily via rows.Next(), so breaking here avoids materialising a Row
		// map for every remaining replica row (a Limit:50 dashboard query over
		// 1k rows built ~950 throwaway maps).
		if req.Limit > 0 && !overlayActive && len(out) >= req.Limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: rows: %v",
			s.tableName, err))
	}
	return out
}

// fetchViaPool is the lock-free hydrate read: it borrows a frame-pinned reader
// from the bound pool, runs the SELECT on it via the reader's own prepared-stmt
// cache, and returns the materialised Nodes. No s.mu is taken — every field
// touched (s.tableName/columns/primaryKey, conn.sort/filterCondition/
// filterPredicate) is immutable for the lifetime of the bound pool, and the
// borrowed reader is exclusive to this call. The overlay path is intentionally
// absent: the pool is bound only in the advance-free window, so no Push is in
// flight (invariant asserted by the engine's bind/unbind discipline).
// fetchViaPoolStream is the LAZY hydrate read: it borrows a frame-pinned
// reader from the bound pool, runs the SELECT, and yields each row one at a
// time via iter.Seq — the reader and sql.Rows stay open across the entire
// iteration, so a parent Join can hold this cursor while fetching a child.
// This is the actual memory win: no []Node is materialised. The pool is
// sized K = P × Cmax to guarantee enough readers for every lane's concurrent
// cursors (§3d).
//
// No s.mu is taken — every field touched is immutable for the lifetime of
// the bound pool (same invariant as fetchViaPool). The overlay path is
// intentionally absent: the pool is bound only in the advance-free window.
func (s *Source) fetchViaPoolStream(req ivm.FetchRequest, conn *connection, pool *ReaderPool) iter.Seq[ivm.Node] {
	return func(yield func(ivm.Node) bool) {
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
			conn.filterCondition,
			order,
			req.Reverse,
			req.Start,
		)
		r, err := pool.acquire(s.ctx)
		if err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: pool acquire: %v", s.tableName, err))
		}
		defer pool.release(r)
		stmt, err := r.prepared(s.ctx, q.SQL)
		if err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: pool prepare: %v\nSQL: %s",
				s.tableName, err, q.SQL))
		}
		rows, err := stmt.QueryContext(s.ctx, q.Params...)
		if err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: pool query: %v\nSQL: %s",
				s.tableName, err, q.SQL))
		}
		defer rows.Close()
		colNames, err := rows.Columns()
		if err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: pool columns: %v", s.tableName, err))
		}
		raw := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		count := 0
		for rows.Next() {
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
			if !yield(ivm.Node{Row: row}) {
				return
			}
			count++
			if req.Limit > 0 && count >= req.Limit {
				return
			}
		}
		if err := rows.Err(); err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: rows: %v",
				s.tableName, err))
		}
	}
}

// fetchViaPool is the EAGER pool read (returns []ivm.Node). It is the
// defensive fallback for callers that invoke fetchForConn directly with a
// pool still bound; sourceInput.Fetch normally takes the streaming
// fetchViaPoolStream path instead. Kept for correctness — no caller should
// reach it via sourceInput.Fetch, but a direct fetchForConn caller might.
func (s *Source) fetchViaPool(req ivm.FetchRequest, conn *connection, pool *ReaderPool) []ivm.Node {
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
		conn.filterCondition,
		order,
		req.Reverse,
		req.Start,
	)
	r, err := pool.acquire(s.ctx)
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: pool acquire: %v", s.tableName, err))
	}
	defer pool.release(r)
	stmt, err := r.prepared(s.ctx, q.SQL)
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: pool prepare: %v\nSQL: %s",
			s.tableName, err, q.SQL))
	}
	rows, err := stmt.QueryContext(s.ctx, q.Params...)
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: pool query: %v\nSQL: %s",
			s.tableName, err, q.SQL))
	}
	defer rows.Close()
	colNames, err := rows.Columns()
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: pool columns: %v", s.tableName, err))
	}
	return s.scanRows(rows, colNames, conn, req, false)
}

// BindReaderPool binds a CG-shared frame-pinned reader pool for the cold-start
// parallel-hydrate window. pool is passed as `any` so engine/ (which must not
// import this package — that would cycle) can fan it out across its leaf
// sources via BindTableSourcesToReaderPool. A nil or wrong-typed value is
// ignored. MUST be paired with UnbindReaderPool before the first advance (after
// which the pool's pinned frame is stale).
func (s *Source) BindReaderPool(pool any) {
	if p, ok := pool.(*ReaderPool); ok && p != nil {
		s.readerPool.Store(p)
	}
}

// UnbindReaderPool detaches the reader pool; subsequent fetches revert to the
// single-conn locked path. Does not Close the pool — the owner (sidecar
// ClientGroup) owns its lifecycle.
func (s *Source) UnbindReaderPool() {
	s.readerPool.Store(nil)
}

// convertFilter converts a builder.Condition (AST-level) to a sqlite.Condition
// for SQL pushdown. Sets ColType on literal sides from the column side's schema
// so ToSQLiteType can handle booleans (true→1, false→0) and JSON marshalling.
func (s *Source) convertFilter(cond *builder.Condition) *sqlite.Condition {
	if cond == nil {
		return nil
	}
	switch cond.Type {
	case "simple":
		return s.convertSimpleCondition(cond)
	case "and":
		if len(cond.Conditions) == 0 {
			return nil
		}
		out := &sqlite.Condition{Type: "and"}
		for i := range cond.Conditions {
			child := s.convertFilter(&cond.Conditions[i])
			if child != nil {
				out.Conditions = append(out.Conditions, child)
			}
		}
		if len(out.Conditions) == 0 {
			return nil
		}
		return out
	case "or":
		if len(cond.Conditions) == 0 {
			return nil
		}
		out := &sqlite.Condition{Type: "or"}
		for i := range cond.Conditions {
			child := s.convertFilter(&cond.Conditions[i])
			if child != nil {
				out.Conditions = append(out.Conditions, child)
			}
		}
		if len(out.Conditions) == 0 {
			return nil
		}
		return out
	}
	return nil
}

func (s *Source) convertSimpleCondition(cond *builder.Condition) *sqlite.Condition {
	out := &sqlite.Condition{
		Type: "simple",
		Op:   cond.Op,
	}
	out.Left = s.convertValuePos(cond.Left)
	out.Right = s.convertValuePos(cond.Right)
	if cond.Left != nil && cond.Left.Type == "column" {
		if cs, ok := s.columns[cond.Left.Name]; ok {
			if out.Right.Type == "literal" {
				out.Right.ColType = cs.Type
			}
		}
	}
	if cond.Right != nil && cond.Right.Type == "column" {
		if cs, ok := s.columns[cond.Right.Name]; ok {
			if out.Left.Type == "literal" {
				out.Left.ColType = cs.Type
			}
		}
	}
	return out
}

func (s *Source) convertValuePos(vp *builder.ValuePos) sqlite.ValuePos {
	if vp == nil {
		return sqlite.ValuePos{}
	}
	return sqlite.ValuePos{
		Type:  vp.Type,
		Name:  vp.Name,
		Value: vp.Value,
	}
}

// quoteIdent escapes a SQL identifier (table or column name) by doubling
// any embedded quotes. The result is double-quoted, which both SQLite
// and the SQL standard accept for identifiers.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
