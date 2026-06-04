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
	"strings"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// Source is the read-only TableSource leaf. One instance per (CG, table).
type Source struct {
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

	return &Source{
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
// conn back to the writable pool. Idempotent.
func (s *Source) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
		conn, err := s.writableDB.Conn(ctx)
		if err != nil {
			return fmt.Errorf("ensurePrevTx %s: acquire conn: %w", s.tableName, err)
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
	if s.prevConn == nil || !s.prevTxStarted {
		return
	}
	ctx := context.Background()
	if _, err := s.prevConn.ExecContext(ctx, "ROLLBACK"); err != nil {
		// If ROLLBACK fails the conn is in an unknown state; close it
		// so the next ensurePrevTx will reacquire a fresh one.
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
	// rows and false-drift every ADD ("source drift table=messages op=Add
	// ... has_count=N" while TS produces the changes normally) — observed in
	// the shadow soak as TS-produced-8 / Go-produced-0 re-hydrates.
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

// Connect registers a new pipeline connection.
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
		return nil

	case ivm.ChangeTypeRemove:
		args := s.rowToPKArgs(change.Row)
		if _, err := conn.ExecContext(ctx, s.deleteSQL, args...); err != nil {
			return fmt.Errorf("DELETE: %w", err)
		}
		return nil

	case ivm.ChangeTypeEdit:
		if s.canUseUpdate(change.OldRow, change.Row) {
			merged := mergeRow(change.OldRow, change.Row)
			args := append(s.rowToNonPKArgs(merged), s.rowToPKArgs(merged)...)
			if _, err := conn.ExecContext(ctx, s.updateSQL, args...); err != nil {
				return fmt.Errorf("UPDATE: %w", err)
			}
			return nil
		}
		// PK changed: DELETE + INSERT.
		delArgs := s.rowToPKArgs(change.OldRow)
		if _, err := conn.ExecContext(ctx, s.deleteSQL, delArgs...); err != nil {
			return fmt.Errorf("EDIT.DELETE: %w", err)
		}
		insArgs := s.rowToInsertArgs(change.Row)
		if _, err := conn.ExecContext(ctx, s.insertSQL, insArgs...); err != nil {
			return fmt.Errorf("EDIT.INSERT: %w", err)
		}
		return nil
	}
	return fmt.Errorf("writeChange: unknown change type %v", change.Type)
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

func (i *sourceInput) SetOutput(o ivm.Output) {
	i.conn.output = o
}

func (i *sourceInput) Destroy() {
	i.src.disconnect(i.conn)
}

func (i *sourceInput) Fetch(req ivm.FetchRequest) []ivm.Node {
	return i.src.fetchForConn(req, i.conn)
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
		nil, // filter pushdown unused — we have only the Go predicate
		order,
		req.Reverse,
		req.Start,
	)
	ctx := context.Background()
	rows, err := s.activeConn().QueryContext(ctx, q.SQL, q.Params...)
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
	}
	if err := rows.Err(); err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: rows: %v",
			s.tableName, err))
	}

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
	// the existence condition flips and the parent is (re-)emitted. A prior
	// inverted gate (`<`) hid the in-flight row from exactly that re-fetch,
	// causing advance to emit far fewer changes than TS (shadow soak:
	// "TS produced 21 changes, Go produced 2"). The old snapshot+batchDelta
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
	// forward comparator here put the in-flight Add at the wrong index in a
	// reverse fetch — exactly the fetch Take issues for its displaced-bound
	// lookup (start:bound, basis:'at', reverse:true) — so Take picked a
	// different boundNode/beforeBoundNode and emitted a different displaced
	// row than TS (shadow soak: TS removes msg-X / Go removes msg-Y).
	if s.overlay != nil && conn.lastPushedEpoch >= s.overlay.Epoch {
		effCmp := ivm.MakeComparator(order, req.Reverse)
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

// quoteIdent escapes a SQL identifier (table or column name) by doubling
// any embedded quotes. The result is double-quoted, which both SQLite
// and the SQL standard accept for identifiers.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
