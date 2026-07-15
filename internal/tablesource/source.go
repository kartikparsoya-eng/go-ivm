// Package tablesource is the read-only TableSource port from TS: the
// SQLite-backed leaf Source reading the zero-cache replica directly,
// constructed per (cg, table) in cmd/sidecar. It is the ONLY leaf source;
// ivm.MemorySource survives as an engine-test fixture.
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
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/procclock"
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
	nonPKCols      []string // cols not in the primary key, for UPDATE SET clause
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

	// advanceClock, when non-nil, is the processing-clock accumulator of
	// the advance in flight: fanOut's parallel worker goroutines bracket
	// their pushGroup CPU into it so the sidecar's economic
	// advancement-abort budget sees their work (see
	// cmd/sidecar/advance_abort.go MEASUREMENT). Installed/cleared by the
	// engine around each clocked advance (engine.mu serializes advances;
	// atomic.Pointer lets fanOut read it without s.mu).
	advanceClock atomic.Pointer[procclock.Accumulator]

	// advanceCtx, when non-nil, is a context with the advance's wall-clock
	// budget deadline. SQL queries on the advance path (fetchSerial,
	// fetchDuringPushStream) use this instead of s.ctx so that a query
	// blocking on WAL contention is interrupted by the budget deadline via
	// sqlite3_interrupt (the go-sqlite3 driver honors context cancellation).
	// Installed/cleared by the engine alongside advanceClock. nil during
	// hydrate (falls back to s.ctx, the CG lifetime context).
	advanceCtx atomic.Pointer[context.Context]

	// advanceAbortCheck, when non-nil, is the advance's per-fetch abort
	// checkpoint (TS parity: #shouldAdvanceYieldMaybeAbortAdvance runs on
	// every row fetched during push processing). Called at the top of every
	// advance-path fetch and every scanned-row batch; panics a typed abort
	// (recognized by the sidecar's recover → rpcCodeAdvanceAborted → TS
	// reset) when the economic or wall budget is exceeded. Installed/cleared
	// by the engine alongside advanceClock/advanceCtx; nil during hydrate.
	advanceAbortCheck atomic.Pointer[func()]

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
	// frame coordination). While bound, ensurePrevTx/OnAdvanceEnd are
	// no-ops: the Snapshotter owns the pinned BEGIN CONCURRENT frame the diff
	// was derived against, so applying that diff's changes here lands them in
	// the SAME frame (no independent re-pin = no frame-timing drift). Set/
	// cleared via BindConn/UnbindConn around a driven advance or hydrate.
	externalConn *sql.Conn

	// batchState tracks, per PK written by writeChangeLocked in the current
	// advance batch, the row the prev tx now holds for that PK (nil =
	// removed/absent). Last write wins, so the entry always mirrors what
	// TS's lazy diff would read from the mutated prev snapshot at this point
	// in the batch (see resolveBatchChangeLocked). Genuine drift — an
	// untouched PK contradicting prev state — is left to driftCheckLocked,
	// which panics. Allocated lazily in trackAdded/trackRemoved and cleared
	// by ClearBatchState at the end of each advance batch (the true batch
	// boundary — engine.signalAdvanceEnd).
	batchState map[string]ivm.Row
	// stmtCache memoizes prepared SELECT statements for fetchForConn, keyed by
	// (active conn, SQL text). database/sql's one-shot QueryContext re-runs
	// sqlite3_prepare_v2 on every call (14.6% of cgo time in the live read-path
	// profile); a Conn-bound *sql.Stmt amortizes that to a cheap sqlite3_reset.
	//
	// Keyed by conn pointer because a Conn-bound Stmt is valid ONLY on the conn
	// it was prepared on. The conn set is tiny and stable — this Source's own
	// prevConn plus the Snapshotter's two leapfrog frame conns (bound via
	// BindConn), all re-pinned in place via ROLLBACK+BEGIN and never reopened
	// mid-life — so the OUTER map stays at a few entries and a cached stmt
	// stays valid across advances (a prepare_v2 stmt is frame-independent and
	// SQLite auto-recompiles it on the rare replica-schema change).
	//
	// The INNER per-SQL map is the unbounded axis: conn
	// teardown — the only full invalidation — never happens mid-life for
	// exactly those long-lived conns, and nothing maps a removed query back
	// to its SQL shapes, so a long-lived CG with query churn accumulated one
	// compiled sqlite3_stmt (C heap, invisible to Go's allocator) per
	// distinct SQL text FOREVER — an RSS ratchet. Bounded now: each conn
	// bucket holds at most stmtCachePerConnCap entries; overflow evicts the
	// least-recently-returned quarter (see returnSelectStmtLocked). Guarded
	// by s.mu. Entries are CHECKED OUT (removed from the map) while their
	// cursor is open and handed back after — see checkoutSelectLocked for
	// why sharing a live stmt corrupts.
	stmtCache map[*sql.Conn]map[string]*cachedStmt
	// stmtCacheTick is a monotonic recency counter stamped on every stmt
	// return; the eviction scan sorts on it. Guarded by s.mu.
	stmtCacheTick uint64

	// pushStmtCache memoizes the fixed drift/write statements used by Push:
	// checkExists, insert, delete, update. Unlike SELECT fetch statements these
	// never hold open cursors across callbacks and Source.Push serializes their
	// use under s.mu, so they can stay checked in and be reused directly.
	pushStmtCache map[*sql.Conn]map[string]*sql.Stmt

	// readerPool, when non-nil, is a CG-shared pool of read connections ALL
	// pinned to the same WAL frame (one stateVersion). It is bound ONLY during
	// advance-free hydrate windows (cold-start first hydrate, warm adds).
	// Option B resource model: each hydrate pipeline holds ONE exclusive
	// reader (pool.AcquireForPipeline, keyed by the engine's queryID group
	// tag) and every fetch of that pipeline — nested included — rides it with
	// interleaved cursors (fetchViaBoundReaderStream). Fetches outside a
	// bound pipeline (build-phase scalar-resolver executor, legacy AddQuery)
	// read the serial bound conn instead — the SAME pinned frame. While
	// bound, no Push is in flight, so s.overlay is always nil and the
	// per-connection fields read during a fetch are immutable post-Connect.
	// atomic so fetches can read it without s.mu. Set/cleared via
	// BindReaderPool / UnbindReaderPool (engine.BindTableSourcesToReaderPool).
	readerPool atomic.Pointer[ReaderPool]

	// nextConnectGroup is the pipeline-group tag the NEXT Connect call
	// stamps on its connection (SetNextConnectGroup — the engine sets it to
	// the owning queryID right before each Connect during a pipeline
	// build). Guarded by s.mu. See parallel_fanout.go.
	nextConnectGroup string
}

// cachedStmt pairs a prepared statement with the recency tick of its last
// return, for the bounded stmt-cache's eviction scan.
type cachedStmt struct {
	st       *sql.Stmt
	lastTick uint64
}

// stmtCachePerConnCap bounds each conn's stmt-cache bucket. A long-lived
// CG's active query set is a few hundred shapes at most; abandoned shapes
// (removed queries, one-off bound permutations) past the cap are evicted
// coldest-first instead of pinning C-heap sqlite3_stmts for the CG's whole
// life. Sized so the hot working set never thrashes: at the cap, a
// checkout+return cycle of an EXISTING shape does not trigger eviction —
// only genuinely new shapes do.
const stmtCachePerConnCap = 512

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

	// group is the pipeline group this connection belongs to (the engine's
	// queryID, via SetNextConnectGroup) — the parallel-advance
	// serialization unit. Connections sharing a group push sequentially;
	// distinct groups may push concurrently (see parallel_fanout.go).
	// "" for non-engine callers → one shared serial group.
	group string

	// LastPushedEpoch is the most recent pushEpoch this connection has
	// already observed via its Output.Push. Read during Fetch to gate
	// overlay application.
	lastPushedEpoch int
}

// presenceKey identifies one table on one read pool for the presence-probe
// cache. The pool pointer (not the file path) is deliberate: a different
// pool — different process wiring, potentially a different replica file —
// must re-validate from scratch.
type presenceKey struct {
	db    *sql.DB
	table string
}

// probedTables caches SUCCESSFUL presence probes per (read pool, table) so
// CG init only touches the read pool for the first init of each table.
// See the probe block in NewWithContext for the full rationale.
// Negative results are never stored.
var probedTables sync.Map // presenceKey → struct{}

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

func Validate(db *sql.DB, tableName string, columns map[string]sqlite.ColumnSchema, primaryKey []string) error {
	return ValidateWithContext(context.Background(), db, tableName, columns, primaryKey)
}

func ValidateWithContext(parent context.Context, db *sql.DB, tableName string, columns map[string]sqlite.ColumnSchema, primaryKey []string) error {
	if db == nil {
		return fmt.Errorf("tablesource.New: db is nil")
	}
	if tableName == "" {
		return fmt.Errorf("tablesource.New: tableName is empty")
	}
	if len(primaryKey) == 0 {
		return fmt.Errorf("tablesource.New %s: primaryKey is empty", tableName)
	}
	for _, k := range primaryKey {
		if _, ok := columns[k]; !ok {
			return fmt.Errorf(
				"tablesource.New %s: primary key %q is not in columns",
				tableName, k)
		}
	}
	// Validate table presence — `SELECT 1 FROM "x" LIMIT 0` errors if the
	// table doesn't exist, costs nothing if it does. Bounded: this probe
	// acquires a read-pool conn, and under pool exhaustion an unbounded
	// acquire wedged handleInit for the full 120s TS RPC timeout and then
	// leaked the goroutine forever — the probe
	// sat behind deadlocked pool builders (see PoolAcquireTimeout). The
	// deadline is on the PROBE only — the Source's stored lifetime ctx
	// below stays bound to parent, not to this timeout.
	//
	// CACHED per (pool, table): this probe is the only hard-fail
	// read-pool acquisition on the CG-init path, and it
	// ran once per Source per CG init. Under sustained CG churn the warm-
	// hydrate reader pools (K=8 conns each, held for the full hydrate —
	// observed up to 10s) legitimately saturate the read pool (128/128 for
	// 93 consecutive 10s windows), so init probes queued behind hydrates
	// timed out (34 'presence probe timed out' failures in 40 min) → Go
	// backend init failed → the TS view-syncer run-loop died → every
	// subsequent client message hit the dead-but-lingering VS and got a
	// Rehome storm (view-syncer.ts:460). Table presence is a property of
	// the REPLICA FILE, not of the CG: once a table has been seen on a
	// given pool, re-probing it per init buys nothing. TS never probes at
	// all — its TableSource constructor (table-source.ts:96-119) builds
	// from the replica's own introspected schema, so a missing table is
	// impossible there; the Go probe guards wire-schema-vs-replica drift
	// (the init schema arrives over RPC) and one probe per (pool, table)
	// preserves that guard. With the cache, CG init touches the read pool
	// only for the FIRST init of each table after process start; reader-
	// pool builds (the remaining read-pool users) already degrade to
	// serial hydrate on acquire timeout, so read-pool saturation no longer
	// has any hard-failure path.
	//
	// Policy:
	//   - Only SUCCESS is cached. A negative result is never stored: a
	//     table created by a later schema migration must become probeable,
	//     and a timed-out probe must retry on the next init.
	//   - Keyed by the *sql.DB pointer, not the path: a hypothetical new
	//     pool (e.g. after a replica swap) starts cold and re-validates
	//     everything. A live cache entry pins its pool's struct; entries
	//     for a closed pool are a few bytes each and bounded by table
	//     count, which is accepted.
	pkey := presenceKey{db: db, table: tableName}
	if _, probed := probedTables.Load(pkey); !probed {
		probeCtx, cancelProbe := context.WithTimeout(parent, PoolAcquireTimeout)
		_, probeErr := db.ExecContext(probeCtx, `SELECT 1 FROM `+quoteIdent(tableName)+` LIMIT 0`)
		cancelProbe()
		if probeErr != nil {
			// Classify on DeadlineExceeded SPECIFICALLY: cancelProbe() has
			// already run, so probeCtx.Err() is never nil here — the old
			// `probeCtx.Err() != nil` check made the "table not found"
			// branch was unreachable and reported genuinely missing tables as
			// pool exhaustion. Err() sticks at DeadlineExceeded once the
			// deadline fires (a later cancel() does not overwrite it), so
			// this cleanly separates "queued too long on the pool" from
			// "the table is not in the replica".
			if probeCtx.Err() == context.DeadlineExceeded {
				return fmt.Errorf(
					"tablesource.New %s: presence probe timed out after %v — replica read pool exhausted?: %w",
					tableName, PoolAcquireTimeout, probeErr)
			}
			return fmt.Errorf("tablesource.New %s: table not found: %w", tableName, probeErr)
		}
		probedTables.Store(pkey, struct{}{})
	}
	return nil
}

// NewWithContext constructs a Source whose SQLite operations are cancellable
// via the provided context. Use this when the caller owns a CG-scoped context
// that should abort in-flight queries on teardown.
func NewWithContext(parent context.Context, db *sql.DB, writableDB *sql.DB, tableName string, columns map[string]sqlite.ColumnSchema, primaryKey []string) (*Source, error) {
	if writableDB == nil {
		return nil, fmt.Errorf("tablesource.New: writableDB is nil")
	}
	if err := ValidateWithContext(parent, db, tableName, columns, primaryKey); err != nil {
		return nil, err
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
	s.closePrevConnLocked()
	return nil
}

// BindConn binds this Source's prev-tx reads/writes to an externally-owned,
// already-pinned connection (the Snapshotter's `prev` Snapshot conn). While
// bound, the Source does NOT manage its own prev tx — the snapshotter owns the
// frame. Frame coordination: the engine applies a Snapshotter-derived diff
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

// checkoutSelectLocked returns a prepared statement for query bound to conn,
// preparing and caching it on first use. The returned *sql.Stmt executes on
// conn's current transaction (the pinned frame plus any in-flight writeChange
// writes), so it observes read-your-own-writes exactly like a one-shot
// QueryContext would. The caller resolved conn from activeConn() under the
// same lock, and the cache is invalidated whenever a conn is torn down, so a
// stale entry can never be executed.
//
// CHECKOUT semantics (mirrors TS zqlite's StatementCache, whose `get`
// removes the stmt from the cache until `return`): the stmt is REMOVED from
// the cache while checked out and MUST be handed back via
// returnSelectStmt(Locked) once its rows are fully consumed. A *sql.Stmt
// wraps a single sqlite3_stmt — re-running Query on it while a previous
// *sql.Rows is still open silently RESETS the live cursor, which then
// iterates the second query's result set (verified experimentally: silent
// row corruption, no error). The eager fetch drains its cursor before any
// same-SQL re-entry could run, so a shared map was safe there; the lazy
// advance fetch (fetchDuringPushStream) holds cursors open across nested
// child fetches, where a same-SQL sibling (e.g. a self-join's two legs)
// would corrupt. A concurrent checkout of the same (conn, SQL) simply
// prepares a fresh transient stmt; whichever returns second is closed.
// MUST be called with s.mu held.
func (s *Source) checkoutSelectLocked(conn *sql.Conn, query string) (*sql.Stmt, error) {
	bySQL := s.stmtCache[conn]
	if bySQL == nil {
		if s.stmtCache == nil {
			s.stmtCache = make(map[*sql.Conn]map[string]*cachedStmt, 3)
		}
		bySQL = make(map[string]*cachedStmt, 4)
		s.stmtCache[conn] = bySQL
	}
	if e, ok := bySQL[query]; ok {
		delete(bySQL, query)
		return e.st, nil
	}
	st, err := conn.PrepareContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// returnSelectStmtLocked hands a checked-out stmt back to the cache. The stmt
// is CLOSED instead of cached when (a) the conn's cache bucket vanished while
// it was out — the conn was torn down via closeCachedStmtsForConnLocked /
// closeAllCachedStmtsLocked, so the stmt is dead weight — or (b) another
// checkout of the same (conn, SQL) returned first — at most one stmt is
// cached per key. In practice a checkout never straddles conn teardown (lazy
// cursors live only inside a push fanout, during which OnAdvanceEnd's
// overlay guard blocks the rollback path); the close is defensive.
//
// Inserting past stmtCachePerConnCap evicts the least-recently-returned
// quarter of the bucket (without a bound, one C-heap
// sqlite3_stmt per distinct SQL shape accrued for the CG's whole life —
// conn teardown, the only invalidation, never fires for the long-lived
// prevConn/externalConn).
// MUST be called with s.mu held.
func (s *Source) returnSelectStmtLocked(conn *sql.Conn, query string, st *sql.Stmt) {
	bySQL := s.stmtCache[conn]
	if bySQL == nil {
		_ = st.Close()
		return
	}
	if _, occupied := bySQL[query]; occupied {
		_ = st.Close()
		return
	}
	s.stmtCacheTick++
	bySQL[query] = &cachedStmt{st: st, lastTick: s.stmtCacheTick}
	if len(bySQL) > stmtCachePerConnCap {
		evictColdestStmtsLocked(bySQL)
	}
}

// evictColdestStmtsLocked closes and drops the least-recently-returned
// quarter of bucket. O(n log n) on n≤cap+1, and it runs only when a NEW
// shape lands on a full bucket — steady-state checkout/return of existing
// shapes never triggers it. MUST be called with s.mu held.
func evictColdestStmtsLocked(bySQL map[string]*cachedStmt) {
	type kv struct {
		sql  string
		tick uint64
	}
	entries := make([]kv, 0, len(bySQL))
	for q, e := range bySQL {
		entries = append(entries, kv{q, e.lastTick})
	}
	slices.SortFunc(entries, func(a, b kv) int {
		switch {
		case a.tick < b.tick:
			return -1
		case a.tick > b.tick:
			return 1
		}
		return 0
	})
	evict := len(entries) / 4
	if evict < 1 {
		evict = 1
	}
	for _, e := range entries[:evict] {
		_ = bySQL[e.sql].st.Close()
		delete(bySQL, e.sql)
	}
}

// returnSelectStmt is returnSelectStmtLocked for callers not holding s.mu
// (the lazy fetch returns its stmt after releasing the lock).
func (s *Source) returnSelectStmt(conn *sql.Conn, query string, st *sql.Stmt) {
	s.mu.Lock()
	s.returnSelectStmtLocked(conn, query, st)
	s.mu.Unlock()
}

func (s *Source) pushStmtLocked(conn *sql.Conn, query string) (*sql.Stmt, error) {
	bySQL := s.pushStmtCache[conn]
	if bySQL == nil {
		if s.pushStmtCache == nil {
			s.pushStmtCache = make(map[*sql.Conn]map[string]*sql.Stmt, 3)
		}
		bySQL = make(map[string]*sql.Stmt, 4)
		s.pushStmtCache[conn] = bySQL
	}
	if st := bySQL[query]; st != nil {
		return st, nil
	}
	st, err := conn.PrepareContext(s.ctx, query)
	if err != nil {
		return nil, err
	}
	bySQL[query] = st
	return st, nil
}

func (s *Source) execPushStmtLocked(conn *sql.Conn, query string, args ...interface{}) error {
	st, err := s.pushStmtLocked(conn, query)
	if err != nil {
		return err
	}
	_, err = st.ExecContext(s.ctx, args...)
	return err
}

// closeCachedStmtsForConnLocked finalizes and drops every prepared statement
// cached against conn. Called immediately before conn is closed so the
// compiled sqlite3_stmts are released and a later conn that happens to reuse
// the same pointer can't hit a stale entry. MUST be called with s.mu held.
func (s *Source) closeCachedStmtsForConnLocked(conn *sql.Conn) {
	bySQL := s.stmtCache[conn]
	if bySQL != nil {
		for _, e := range bySQL {
			_ = e.st.Close()
		}
		delete(s.stmtCache, conn)
	}
	if pushBySQL := s.pushStmtCache[conn]; pushBySQL != nil {
		for _, st := range pushBySQL {
			_ = st.Close()
		}
		delete(s.pushStmtCache, conn)
	}
}

func (s *Source) closePrevConnLocked() {
	if s.prevConn == nil {
		s.prevTxStarted = false
		return
	}
	s.closeCachedStmtsForConnLocked(s.prevConn)
	if s.prevTxStarted {
		_, _ = s.prevConn.ExecContext(context.Background(), "ROLLBACK")
	}
	_ = s.prevConn.Close()
	s.prevConn = nil
	s.prevTxStarted = false
}

// closeAllCachedStmtsLocked finalizes every cached statement across all conns.
// Used at Source teardown. MUST be called with s.mu held.
func (s *Source) closeAllCachedStmtsLocked() {
	for _, bySQL := range s.stmtCache {
		for _, e := range bySQL {
			_ = e.st.Close()
		}
	}
	s.stmtCache = nil
	for _, bySQL := range s.pushStmtCache {
		for _, st := range bySQL {
			_ = st.Close()
		}
	}
	s.pushStmtCache = nil
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
//
// When s.prevConn is nil, s.mu is RELEASED during the conn pool
// acquisition (up to 30s) and re-acquired after. Without this, a pool
// stall would block all Push/Fetch/Close on this Source — and since Push
// runs under e.mu, the entire engine would freeze. After re-acquiring
// s.mu, a double-check handles the race where another goroutine also
// acquired a conn while s.mu was released.
func (s *Source) ensurePrevTxLocked() error {
	// Frame-coordinated: the Snapshotter owns the pinned frame on externalConn,
	// already BEGIN-and-read. Nothing for this Source to acquire.
	if s.externalConn != nil {
		return nil
	}
	ctx := s.ctx
	if s.prevConn == nil {
		// Release s.mu during conn pool acquisition so a 30s pool stall
		// doesn't block all Push/Fetch/Close on this Source.
		s.mu.Unlock()
		// Use a bounded timeout so pool exhaustion surfaces as a fast error
		// instead of blocking indefinitely (which previously caused the TS-side
		// 120s RPC timeout to fire with no diagnostic). 30s is long enough to
		// ride out a transient checkpoint stall but short enough to fail before
		// the TS timeout.
		acquireCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		conn, err := s.writableDB.Conn(acquireCtx)
		cancel()
		s.mu.Lock()
		if err != nil {
			return fmt.Errorf("ensurePrevTx %s: acquire conn (30s timeout): %w", s.tableName, err)
		}
		// Double-check: another goroutine may have acquired a conn
		// while s.mu was released. If so, close the extra conn.
		if s.prevConn != nil {
			_ = conn.Close()
		} else {
			s.prevConn = conn
		}
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
	// has already succeeded; if this read fails the conn has an open
	// transaction that must be rolled back so the next ensurePrevTx
	// can retry cleanly (otherwise BEGIN-within-BEGIN loops forever).
	if _, err := s.prevConn.ExecContext(ctx, "SELECT 1"); err != nil {
		_, _ = s.prevConn.ExecContext(context.Background(), "ROLLBACK")
		s.beginStmt = "" // force re-probe on next call
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
	// Guard against a TOCTOU race: overlay may have been set between an
	// earlier unlock and this lock acquisition. Rolling back
	// mid-push would violate the snapshot-consistency guarantee that downstream
	// Fetches inside Output.Push depend on.
	if s.overlay != nil {
		return
	}
	if s.prevConn == nil || !s.prevTxStarted {
		return
	}
	ctx := s.ctx
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
	if len(s.connections) == 0 {
		s.closePrevConnLocked()
		return
	}

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
		// lazy behavior for one batch rather than wedging. Log loudly so
		// the error is visible instead of silently swallowed.
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][TABLESOURCE] OnAdvanceEnd %s: ensurePrevTx re-pin failed: %v\n",
			s.tableName, err)
	}
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
// on it would panic. Passing it through unchanged is both panic-free and
// behavior-identical to the current code for every other json shape (objects, arrays,
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

	// table-source.ts:266-268 — an explicit sort must include every PK column
	// or the connection's comparator is not total ("unordered" connections
	// fall back to the PK comparator, which trivially qualifies).
	if sort != nil {
		ivm.AssertOrderingIncludesPK(sort, s.primaryKey)
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
	// Consume the pending connect-group tag (engine sets it immediately
	// before Connect; builds are serialized under Engine.mu so set→consume
	// cannot interleave across queries).
	conn.group = s.nextConnectGroup
	s.connections = append(s.connections, conn)
	s.mu.Unlock()
	return in
}

// HasPushObservers reports whether an advance push can reach any registered
// query pipeline. Engine uses this to skip eager all-table maintenance for
// tables TS would not have a TableSource for at all.
func (s *Source) HasPushObservers() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasPushObserversLocked()
}

func (s *Source) hasPushObserversLocked() bool {
	return len(s.connections) > 0
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
func (s *Source) Push(change ivm.SourceChange) {
	// Acquire mu just long enough to set up the in-flight work:
	// ensure prev tx, snapshot connections, decide shouldSplitEdit.
	// Release before fanout — downstream output.Push callbacks may
	// recursively call back into this Source (Fetch)
	// and would deadlock against a held mu.
	s.mu.Lock()
	if err := s.ensurePrevTxLocked(); err != nil {
		s.mu.Unlock()
		panic(fmt.Sprintf("tablesource.Source.Push %s: ensurePrevTx: %v", s.tableName, err))
	}
	// Resolve intra-batch staleness BEFORE the split decision: TS's split
	// logic receives the Edit with the OldRow the diff lazily read from the
	// mutated prev, so the split-key comparison must run against the
	// batch-current value. See resolveBatchChangeLocked for the case table.
	var skip bool
	if change, skip = s.resolveBatchChangeLocked(change); skip {
		s.mu.Unlock()
		return
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

	if change.Type == ivm.ChangeTypeEdit && shouldSplitEdit {
		s.genPushAndWrite(ivm.MakeSourceChangeRemove(change.OldRow), conns)
		s.genPushAndWrite(ivm.MakeSourceChangeAdd(change.Row), conns)
	} else {
		s.genPushAndWrite(change, conns)
	}
}

// genPushAndWrite runs ONE source-level change through every connection's
// filterPush, then applies the change to the prev tx via writeChange.
//
// Locking discipline mirrors TS's MemorySource.genPushAndWriteWithSplitEdit:
//   - Overlay is set under mu, then mu released. Fanout runs WITHOUT mu
//     so downstream callbacks can Fetch recursively.
//   - After fanout: re-acquire mu, run writeChange against prev tx, clear
//     overlay, release.
//
// Direct port of TS's genPushAndWrite (memory-source.ts).
func (s *Source) genPushAndWrite(change ivm.SourceChange, conns []*connection) {
	s.mu.Lock()
	// Second resolution for the split-edit legs (the Remove leg's
	// writeChangeLocked updates batchState before the Add leg arrives);
	// idempotent for changes already resolved in Push. This replaces the
	// legacy existsLocked probes — batchState is authoritative for
	// any PK touched this batch, so no SQL probe is needed for the rewrite
	// decision (untouched PKs pass through to driftCheckLocked unchanged).
	var skip bool
	if change, skip = s.resolveBatchChangeLocked(change); skip {
		s.mu.Unlock()
		return
	}
	// Drift validation BEFORE any state mutation or fanout — matches TS
	// MemorySource.genPush (memory-source.ts:529-550) and the in-memory
	// MemorySource port (ivm/source.go genPush). Raising here, before
	// pushEpoch++/overlay/writeChange and before any Output.Push, keeps the
	// failure clean: no half-applied state when the panic unwinds. TS
	// asserts (throws) at the same point and the view-syncer tears the
	// client group down; the Go panic propagates to the same disposition.
	// This also replaces the previous raw "UNIQUE constraint failed" panic
	// from writeChange (a dup-Add), which surfaced far from the cause.
	d, derr := s.driftCheckLocked(change)
	if derr != nil {
		// A REAL DB error from the exists probe (I/O, SQLITE_BUSY, closed
		// conn) is NOT drift — treating it as "absent" (the prior
		// behavior) fabricated missing-row drift for every Remove/Edit
		// under I/O pressure. Panic with the plain error, matching TS where
		// better-sqlite3 THROWS from the exists closure
		// (table-source.ts:399-413).
		s.mu.Unlock()
		panic(fmt.Sprintf("tablesource.Source.Push %s: %v", s.tableName, derr))
	}
	if d != nil {
		s.mu.Unlock()
		panic(d)
	}
	s.pushEpoch++
	epoch := s.pushEpoch
	s.overlay = &ivm.Overlay{Epoch: epoch, Change: change}
	s.mu.Unlock()

	// Panic-safety: clear the overlay on UNWIND too. A panic raised inside
	// the fanout (an operator assert like a Take stale-bound, or a DataError
	// from a poison value hit during a downstream fetch) is recovered
	// per-goroutine and re-raised on this goroutine (parallel_fanout.go),
	// unwinding past the success-path clear below. A stuck overlay is
	// silent staleness, not a crash:
	//   - OnAdvanceEnd early-returns while overlay != nil, so the engine's
	//     "signalAdvanceEnd fires on the panic path too" invariant is
	//     defeated for this source — the prev tx (holding this batch's
	//     earlier writeChanges) is never rolled back or re-pinned;
	//   - every fetch on an epoch-current connection splices the FAILED
	//     change into results (fetchForConn applyOverlay,
	//     fetchDuringPushStream) — a phantom row delivered into any
	//     hydrate that lands before the teardown completes.
	// Epoch-guarded so this can only clear ITS OWN overlay (defense —
	// pushes on one source are serialized, so a mismatch implies a bug).
	// On the success path the primary clear below already ran and this is
	// a no-op.
	defer func() {
		s.mu.Lock()
		if s.overlay != nil && s.overlay.Epoch == epoch {
			s.overlay = nil
		}
		s.mu.Unlock()
	}()

	// Fan out to every connection — in parallel across pipeline groups when
	// enabled (see parallel_fanout.go), serially otherwise.
	s.fanOut(change, epoch, conns)

	// Re-acquire mu for writeChange + overlay clear. Use a closure with
	// defer Unlock so the mutex is released even if writeChangeLocked
	// panics — the outer defer's re-lock would deadlock otherwise.
	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		err := s.writeChangeLocked(change)
		s.overlay = nil
		return err
	}()
	if err != nil {
		// Stale prev tx is unrecoverable from inside Push; surface as a
		// panic — it propagates out of the engine and the client group is
		// torn down (TS's disposition for a failed write).
		panic(fmt.Sprintf("tablesource.Source.Push %s: writeChange: %v", s.tableName, err))
	}
}

// driftCheckLocked validates the change against current source state,
// returning a plain drift error if it diverged (so the caller raises it
// BEFORE any fanout/mutation). Direct port of TS genPush's pre-fanout
// asserts (memory-source.ts:529-550) using TS TableSource's `exists`
// closure (table-source.ts:399-413, backed by the checkExists stmt).
// Mirrors the in-memory MemorySource Go port (ivm/source.go genPush)
// switch + error construction exactly.
//
//   - ADD:    assert !exists(row)      → dup-Add drift
//   - REMOVE: assert  exists(row)      → missing-row drift
//   - EDIT:   assert  exists(oldRow)   → missing-row drift
//
// MUST be called with s.mu held (queries the prev tx via prevConn).
// A non-nil probeErr means the exists probe itself FAILED (real DB error) —
// the caller must abort the push (unlock, panic), never interpret it.
func (s *Source) driftCheckLocked(change ivm.SourceChange) (driftErr error, probeErr error) {
	switch change.Type {
	case ivm.ChangeTypeAdd:
		exists, err := s.existsLocked(change.Row)
		if err != nil {
			return nil, err
		}
		if exists {
			return ivm.SourceDriftError(s.tableName, "Add", s.pkOf(change.Row), s.countLocked()), nil
		}
	case ivm.ChangeTypeRemove:
		exists, err := s.existsLocked(change.Row)
		if err != nil {
			return nil, err
		}
		if !exists {
			return ivm.SourceDriftError(s.tableName, "Remove", s.pkOf(change.Row), s.countLocked()), nil
		}
	case ivm.ChangeTypeEdit:
		exists, err := s.existsLocked(change.OldRow)
		if err != nil {
			return nil, err
		}
		if !exists {
			return ivm.SourceDriftError(s.tableName, "Edit", s.pkOf(change.OldRow), s.countLocked()), nil
		}
	}
	return nil, nil
}

// existsLocked reports whether a row with row's primary key exists in the
// prev tx. Faithful port of TS TableSource's `exists` closure
// (table-source.ts:399-402): checkExists SELECT 1 ... LIMIT 1.
//
// A REAL statement failure (I/O error, SQLITE_BUSY, closed conn — anything
// but ErrNoRows) returns a non-nil error and MUST NOT be read as "row
// absent": at the drift check that fabricated missing-row DriftErrors
// for every Remove/Edit under I/O pressure (drift storm → reset loop),
// and at the Edit-to-Add conversion it silently turned Edits
// into Adds against live state. TS's exists closure THROWS on statement
// error; callers here panic AFTER releasing s.mu.
//
// MUST be called with s.mu held (queries prevConn). ensurePrevTxLocked has
// already run by the time genPushAndWrite reaches the drift check.
func (s *Source) existsLocked(row ivm.Row) (bool, error) {
	args := s.rowToPKArgs(row)
	conn := s.activeConn()
	st, err := s.pushStmtLocked(conn, s.checkExistsSQL)
	if err != nil {
		return false, fmt.Errorf("checkExists prepare: %w", err)
	}
	var one int
	err = st.QueryRowContext(s.ctx, args...).Scan(&one)
	switch {
	case err == nil:
		return one == 1, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("checkExists: %w", err)
	}
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
	conn := s.activeConn()
	switch change.Type {
	case ivm.ChangeTypeAdd:
		args := s.rowToInsertArgs(change.Row)
		if err := s.execPushStmtLocked(conn, s.insertSQL, args...); err != nil {
			return fmt.Errorf("INSERT: %w", err)
		}
		s.trackAdded(change.Row)
		return nil

	case ivm.ChangeTypeRemove:
		args := s.rowToPKArgs(change.Row)
		if err := s.execPushStmtLocked(conn, s.deleteSQL, args...); err != nil {
			return fmt.Errorf("DELETE: %w", err)
		}
		s.trackRemoved(change.Row)
		return nil

	case ivm.ChangeTypeEdit:
		if s.canUseUpdate(change.OldRow, change.Row) {
			merged := mergeRow(change.OldRow, change.Row)
			args := append(s.rowToNonPKArgs(merged), s.rowToPKArgs(merged)...)
			if err := s.execPushStmtLocked(conn, s.updateSQL, args...); err != nil {
				return fmt.Errorf("UPDATE: %w", err)
			}
			s.trackAdded(merged)
			return nil
		}
		// PK changed: DELETE + INSERT.
		delArgs := s.rowToPKArgs(change.OldRow)
		if err := s.execPushStmtLocked(conn, s.deleteSQL, delArgs...); err != nil {
			return fmt.Errorf("EDIT.DELETE: %w", err)
		}
		s.trackRemoved(change.OldRow)
		insArgs := s.rowToInsertArgs(change.Row)
		if err := s.execPushStmtLocked(conn, s.insertSQL, insArgs...); err != nil {
			return fmt.Errorf("EDIT.INSERT: %w", err)
		}
		s.trackAdded(change.Row)
		return nil
	}
	return fmt.Errorf("writeChange: unknown change type %v", change.Type)
}

func (s *Source) trackRemoved(row ivm.Row) {
	if s.batchState == nil {
		s.batchState = make(map[string]ivm.Row)
	}
	s.batchState[s.pkKey(row)] = nil
}

func (s *Source) trackAdded(row ivm.Row) {
	if s.batchState == nil {
		s.batchState = make(map[string]ivm.Row)
	}
	s.batchState[s.pkKey(row)] = row
}

// resolveBatchChangeLocked substitutes the change's prev-side values with
// the batch-current state for any PK already written in this advance batch,
// reproducing TS's diff-layer lazy iteration: TS reads prevValues from the
// prev snapshot AFTER earlier writeChanges in the same batch mutated it
// (snapshotter.ts:519-544), while Go's diff collects all entries eagerly
// from the clean prev. Mirrors ivm.MemorySource.resolveBatchChange — see
// its doc comment for the full case table (Edit→Add / Edit→Edit(cur) /
// Add→Edit(cur) / Remove→skip / Remove→Remove(cur)).
//
// MUST be called with s.mu held (batchState is mu-guarded here).
func (s *Source) resolveBatchChangeLocked(change ivm.SourceChange) (ivm.SourceChange, bool) {
	if s.batchState == nil {
		return change, false
	}
	switch change.Type {
	case ivm.ChangeTypeEdit:
		if cur, ok := s.batchState[s.pkKey(change.OldRow)]; ok {
			if cur == nil {
				return ivm.MakeSourceChangeAdd(change.Row), false
			}
			return ivm.MakeSourceChangeEdit(change.Row, cur), false
		}
	case ivm.ChangeTypeAdd:
		if cur, ok := s.batchState[s.pkKey(change.Row)]; ok && cur != nil {
			return ivm.MakeSourceChangeEdit(change.Row, cur), false
		}
	case ivm.ChangeTypeRemove:
		if cur, ok := s.batchState[s.pkKey(change.Row)]; ok {
			if cur == nil {
				return ivm.SourceChange{}, true
			}
			return ivm.MakeSourceChangeRemove(cur), false
		}
	}
	return change, false
}

// ClearBatchState drops the intra-batch PK→row state. Called by
// engine.signalAdvanceEnd at the end of every advance batch (success OR
// panic), so the map never outlives the batch that built it and can never
// mask genuine cross-batch drift. Kept separate from OnAdvanceEnd so it
// fires at true batch boundaries only. Mirrors
// ivm.MemorySource.ClearBatchState.
func (s *Source) ClearBatchState() {
	s.mu.Lock()
	s.batchState = nil
	s.mu.Unlock()
}

func (s *Source) pkKey(row ivm.Row) string {
	var b strings.Builder
	for _, k := range s.primaryKey {
		appendPKKeyPart(&b, row[k])
	}
	return b.String()
}

func appendPKKeyPart(b *strings.Builder, v ivm.Value) {
	switch x := v.(type) {
	case nil:
		b.WriteString("n;")
	case string:
		b.WriteByte('s')
		b.WriteString(strconv.Itoa(len(x)))
		b.WriteByte(':')
		b.WriteString(x)
	case bool:
		if x {
			b.WriteString("b1;")
		} else {
			b.WriteString("b0;")
		}
	case float64:
		b.WriteByte('d')
		b.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
		b.WriteByte(';')
	case int:
		appendPKKeyPart(b, float64(x))
	case int8:
		appendPKKeyPart(b, float64(x))
	case int16:
		appendPKKeyPart(b, float64(x))
	case int32:
		appendPKKeyPart(b, float64(x))
	case int64:
		appendPKKeyPart(b, float64(x))
	case uint:
		appendPKKeyPart(b, float64(x))
	case uint8:
		appendPKKeyPart(b, float64(x))
	case uint16:
		appendPKKeyPart(b, float64(x))
	case uint32:
		appendPKKeyPart(b, float64(x))
	case uint64:
		appendPKKeyPart(b, float64(x))
	case []byte:
		b.WriteByte('B')
		b.WriteString(strconv.Itoa(len(x)))
		b.WriteByte(':')
		b.Write(x)
	default:
		repr := fmt.Sprintf("%T:%#v", v, v)
		b.WriteByte('x')
		b.WriteString(strconv.Itoa(len(repr)))
		b.WriteByte(':')
		b.WriteString(repr)
	}
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
func filterPush(change ivm.Change, conn *connection) {
	if conn.filterPredicate == nil {
		conn.output.Push(change, conn.input)
		return
	}
	switch change.Type {
	case ivm.ChangeTypeAdd, ivm.ChangeTypeRemove, ivm.ChangeTypeChild:
		if conn.filterPredicate(change.Node.Row) {
			conn.output.Push(change, conn.input)
		}
	case ivm.ChangeTypeEdit:
		maybeSplitAndPushEditChange(change, conn)
	}
}

// maybeSplitAndPushEditChange handles the EDIT-with-filter-transition
// case. Direct port of mono/packages/zql/src/ivm/maybe-split-and-push-edit-change.ts.
func maybeSplitAndPushEditChange(change ivm.Change, conn *connection) {
	oldWasPresent := conn.filterPredicate(change.OldNode.Row)
	newIsPresent := conn.filterPredicate(change.Node.Row)
	if oldWasPresent && newIsPresent {
		conn.output.Push(change, conn.input)
		return
	}
	if oldWasPresent && !newIsPresent {
		conn.output.Push(ivm.MakeRemoveChange(*change.OldNode), conn.input)
		return
	}
	if !oldWasPresent && newIsPresent {
		conn.output.Push(ivm.MakeAddChange(change.Node), conn.input)
	}
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

func (i *sourceInput) Fetch(req ivm.FetchRequest) iter.Seq[ivm.Node] {
	if pool := i.src.readerPool.Load(); pool != nil {
		if r := pool.readerFor(i.conn.group); r != nil {
			// Option B hydrate leaf: this connection's pipeline holds an
			// exclusive frame-pinned reader — every fetch of the pipeline
			// (nested child fetches included) rides it with interleaved
			// cursors, exactly TS's one-conn better-sqlite3 model.
			return i.src.fetchViaBoundReaderStream(req, i.conn, r)
		}
		// Pool bound but this fetch is outside any bound pipeline — the
		// build-phase scalar-resolver executor (runs under e.mu before any
		// hydrate goroutine exists) or a legacy AddQuery hydrate. Fall
		// through to the serial bound-conn read: the SAME pinned frame
		// (pool.Version() == the bound curr's version — verified at bind),
		// serialized under s.mu, deadlock-free (this goroutine holds no
		// reader while it waits on s.mu; the eager read borrows nothing).
	}
	// Streaming advance-time leaf fetch: yields rows from a live SQLite
	// cursor on the prev-tx conn instead of materializing the whole result
	// set first — matching TS's lazy statement.iterate() leaf (zqlite
	// table-source.ts #fetch), whose cursor nesting semantics SQLite
	// natively supports on one connection. The eager fetchForConn survives
	// only as the overlay-splice oracle fetchDuringPushStream delegates to.
	return i.src.fetchDuringPushStream(req, i.conn)
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
	// table-source.ts:243-244 — assert(idx !== -1): a double-disconnect or a
	// disconnect of a never-connected input is a pipeline-lifecycle bug;
	// silently ignoring it would mask double-Destroy paths.
	panic("Connection not found")
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
	// Hydrate-window dispatch, mirroring sourceInput.Fetch: when this
	// connection's pipeline holds an exclusive bound reader (Option B), the
	// eager read runs on that reader; otherwise — pool bound but fetch
	// outside any bound pipeline (build-phase executor, legacy AddQuery), or
	// no pool at all — it runs on the serial bound conn. Both sit on the
	// same pinned frame.
	if pool := s.readerPool.Load(); pool != nil {
		if r := pool.readerFor(conn.group); r != nil {
			return s.fetchViaBoundReader(req, conn, r)
		}
	}
	return s.fetchSerial(req, conn)
}

// fetchSerial is the single-conn locked read — fetchForConn minus the pool
// dispatch. Split out so the pool paths' exhaustion FALLBACK can reach the
// serial read directly: routing the fallback through fetchForConn would
// re-enter the pool branch and recurse. It is also what the pool-exhaustion
// fallback relies on for deadlock-freedom: the read borrows the conn, drains
// the cursor EAGERLY under s.mu, and releases before yielding anything — no
// cursor is ever held across a consumer yield, so this fetch can never
// participate in a hold-and-wait cycle.
func (s *Source) fetchSerial(req ivm.FetchRequest, conn *connection) []ivm.Node {
	// Per-fetch abort checkpoint (TS parity — see SetAdvanceAbortCheck).
	// Checked BEFORE taking s.mu: an advance whose budget is already blown
	// must not queue another query. No-op outside a clocked advance.
	s.checkAdvanceAbort()

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensurePrevTxLocked(); err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: ensurePrevTx: %v", s.tableName, err))
	}

	// ORDER BY clause from the connection sort. An UNORDERED connection
	// (sort == nil — the Cap/EXISTS-child path) issues NO ORDER BY: TS
	// #requestToSQL passes the connection's (undefined) sort straight
	// through (table-source.ts:283-286, query-builder.ts:63-66) so SQLite
	// is free to pick any plan and never builds a temp b-tree.
	order := conn.sort

	q := sqlite.BuildSelectQuery(
		s.tableName,
		s.columns,
		req.Constraint,
		conn.filterCondition,
		order,
		req.Reverse,
		req.Start,
		req.MultiConstraints,
	)
	ctx := s.advanceQueryCtx()
	// Reuse a prepared statement for this (conn, SQL) instead of letting
	// database/sql re-compile via sqlite3_prepare_v2 on every QueryContext
	// (14.6% of cgo time in the live read-path profile). activeConn is a
	// stable, single-flight conn (this Source's prevConn, or a Snapshotter
	// frame conn bound via BindConn), so the cached Conn-bound stmt stays valid
	// across advances and re-reads the new frame after each leapfrog.
	dbConn := s.activeConn()
	stmt, err := s.checkoutSelectLocked(dbConn, q.SQL)
	if err != nil {
		panic(fmt.Sprintf("tablesource.Source.Fetch %s: prepare: %v\nSQL: %s",
			s.tableName, err, q.SQL))
	}
	// Hand the stmt back before s.mu releases (defers run LIFO; the Unlock
	// defer above runs after this). The eager scan below fully drains the
	// cursor under the lock, so the stmt is idle again by then.
	defer s.returnSelectStmtLocked(dbConn, q.SQL, stmt)
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
		if order == nil {
			// UNORDERED overlay — TS generateWithOverlayUnordered
			// (memory-source.ts:885-951): no comparator and no start gate
			// (BuildSelectQuery panics on start-without-ordering, so an
			// unordered fetch can never carry a cursor). The remove overlay
			// suppresses the first PK-matching row; the add overlay is
			// injected eagerly at the START of the stream.
			add, remove := unorderedOverlayPlan(s.overlay.Change, req.Constraint, req.MultiConstraints)
			if remove != nil {
				// TS unordered convention: rowMatchesPK/valuesEqual
				// (memory-source.ts:940-948) — a NULL PK never matches.
				out = removeByPKUnordered(out, remove, s.primaryKey)
			}
			if add != nil && (conn.filterPredicate == nil || conn.filterPredicate(add)) {
				out = append([]ivm.Node{{Row: add}}, out...)
			}
			return out
		}
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
		out = applyOverlay(out, s.overlay.Change, effCmp, req.Constraint, req.MultiConstraints, req.Start, s.primaryKey)
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

// fetchDuringPushStream is the LAZY advance-time leaf read — the
// unconditional non-pooled dispatch: it yields rows one at a time from a live SQLite
// cursor on the prev-tx conn, splicing the in-flight overlay per row, instead
// of materializing the whole result set the way fetchForConn does. This is
// the Go analog of TS's leaf during push processing — statement.iterate()
// wrapped by generateWithOverlay (zqlite table-source.ts #fetch) — and the
// advance-side counterpart of fetchViaBoundReaderStream: a parent Join can hold this
// cursor open while it fetches a child, so a large fan-out streams cursor →
// operator → flatten → wire chunk with nothing fully co-resident.
//
// The yielded sequence is element-for-element identical to
// slices.Values(fetchForConn(req, conn)) — fetchForConn is the oracle the
// parity tests compare against. Behavioral mirrors, in order:
//   - overlay nil ⇒ delegate to the eager path (hydrate-without-pool etc.);
//   - epoch gate (lastPushedEpoch >= overlay.Epoch) decides splicing, with
//     the same PartialBoundComparator;
//   - the overlay-add row passes conn.filterPredicate or is dropped
//     (fetchForConn refilters the spliced slice; SQL rows are refiltered
//     idempotently there, so predicate-gating just the add is equivalent);
//   - NO limit-pushdown early stop — fetchForConn disables the break
//     whenever an overlay is live (scanRows overlayActive), because a
//     spliced row may land inside the top-N. Consumers (Take) stop pulling
//     when satisfied, which is the real limit.
//
// Locking: setup (prev tx, SQL build, stmt checkout, overlay snapshot) runs
// under s.mu; the cursor is then iterated WITHOUT the lock so nested child
// fetches on this or sibling sources can run mid-iteration (they re-take
// s.mu per-fetch; SQLite interleaves cursors on one conn natively — the
// single-threaded-JS-equivalent discipline TS gets for free). Releasing the
// lock is safe because every field the iteration touches is stable for the
// cursor's lifetime: the cursor exists only inside a push fanout, during
// which s.overlay is set exactly once (genPushAndWrite sets it before any
// Output.Push and clears it only after the fanout returns), writeChangeLocked
// runs strictly after fanout, OnAdvanceEnd refuses to roll
// back while overlay is non-nil, and conn.lastPushedEpoch for THIS conn was
// bumped before its filterPush. The checked-out stmt makes the cursor
// private (see checkoutSelectLocked).
//
// Uses s.ctx (CG lifetime) per the Source ctx contract — a teardown mid-
// cursor surfaces as a panic that the engine's advance recovery converts to
// a clean terminal frame.
func (s *Source) fetchDuringPushStream(req ivm.FetchRequest, conn *connection) iter.Seq[ivm.Node] {
	return func(yield func(ivm.Node) bool) {
		// Per-fetch abort checkpoint (TS parity — see SetAdvanceAbortCheck).
		// The eager branch below re-checks via fetchForConn → fetchSerial;
		// this covers the streaming branch's query. No-op outside a clocked
		// advance.
		s.checkAdvanceAbort()
		// Locked setup, PANIC-SAFE : the splice
		// plan runs user-value-sensitive code inside the lock —
		// overlaySplicePlan invokes the effective comparator against
		// req.Start (CompareValues panics with a DataError on non-scalar /
		// mismatched sort keys) and the connection's filterPredicate
		// (ported TS closures; type asserts). The previous manual
		// Lock/Unlock pairs let such a panic escape WITH s.mu held: every
		// later Push/Fetch on this source blocked forever — a silent CG
		// wedge with no error frame and no restart trigger, strictly worse
		// than the panic itself (which the engine recovers into a DataError
		// teardown). The closure's deferred unlock guarantees release on
		// every exit; the stmt checkout runs LAST so no panic can fire
		// while a stmt is checked out of the cache (an orphaned checkout
		// would leak until conn teardown).
		var (
			eager                     bool
			unordered                 bool
			dbConn                    *sql.Conn
			stmt                      *sql.Stmt
			qSQL                      string
			qParams                   []any
			effCmp                    ivm.Comparator
			pendingAdd, pendingRemove ivm.Row
		)
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.overlay == nil {
				// Not inside a push fanout (e.g. hydrate without a reader pool,
				// or a companion re-check between batches). The eager path is
				// the reference behavior there; it re-takes s.mu itself (after
				// this closure's deferred unlock).
				eager = true
				return
			}

			if err := s.ensurePrevTxLocked(); err != nil {
				panic(fmt.Sprintf("tablesource.Source.Fetch %s: ensurePrevTx: %v", s.tableName, err))
			}
			// Unordered connections (sort == nil) issue NO ORDER BY and use
			// the unordered overlay plan — see fetchForConn.
			order := conn.sort
			unordered = order == nil
			q := sqlite.BuildSelectQuery(
				s.tableName,
				s.columns,
				req.Constraint,
				conn.filterCondition,
				order,
				req.Reverse,
				req.Start,
				req.MultiConstraints,
			)
			qSQL, qParams = q.SQL, q.Params
			// Snapshot the splice plan under the lock. Same comparator + gate as
			// fetchForConn's applyOverlay block; see overlaySplicePlan for the
			// positional contract that makes the per-row merge equivalent to
			// insertSorted + removeByPK on the materialized slice. CAN PANIC on
			// poison values — deliberately placed BEFORE the stmt checkout.
			if conn.lastPushedEpoch >= s.overlay.Epoch {
				if unordered {
					// TS generateWithOverlayUnordered — no comparator/start
					// gate; the add is yielded eagerly before the first SQL
					// row (see the pre-loop inject below).
					pendingAdd, pendingRemove = unorderedOverlayPlan(s.overlay.Change, req.Constraint, req.MultiConstraints)
				} else {
					effCmp = ivm.MakePartialBoundComparator(order, req.Reverse)
					pendingAdd, pendingRemove = overlaySplicePlan(s.overlay.Change, effCmp, req.Constraint, req.MultiConstraints, req.Start)
				}
				if pendingAdd != nil && conn.filterPredicate != nil && !conn.filterPredicate(pendingAdd) {
					pendingAdd = nil
				}
			}
			dbConn = s.activeConn()
			var err error
			stmt, err = s.checkoutSelectLocked(dbConn, qSQL)
			if err != nil {
				panic(fmt.Sprintf("tablesource.Source.Fetch %s: prepare: %v\nSQL: %s",
					s.tableName, err, qSQL))
			}
		}()
		if eager {
			for _, n := range s.fetchForConn(req, conn) {
				if !yield(n) {
					return
				}
			}
			return
		}

		defer s.returnSelectStmt(dbConn, qSQL, stmt)
		rows, err := stmt.QueryContext(s.advanceQueryCtx(), qParams...)
		if err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: query: %v\nSQL: %s",
				s.tableName, err, qSQL))
		}
		defer rows.Close()
		colNames, err := rows.Columns()
		if err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: columns: %v",
				s.tableName, err))
		}

		// Per-row scan mirrors scanRows (reused buffers; each row's values are
		// copied into its own map, so buffer reuse never aliases yielded rows).
		raw := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		// Unordered overlay-add: TS generateWithOverlayInnerUnordered yields
		// the add FIRST, before any SQL row (memory-source.ts:934-937).
		if unordered && pendingAdd != nil {
			add := pendingAdd
			pendingAdd = nil
			if !yield(ivm.Node{Row: add}) {
				return
			}
		}
		scanned := 0
		for rows.Next() {
			// Per-fetch abort checkpoint every scanned-row batch — same
			// amortization as scanRows.
			if scanned++; scanned&1023 == 0 {
				s.checkAdvanceAbort()
			}
			if err := rows.Scan(ptrs...); err != nil {
				panic(fmt.Sprintf("tablesource.Source.Fetch %s: scan: %v",
					s.tableName, err))
			}
			row := make(ivm.Row, len(colNames))
			for i, c := range colNames {
				cs, ok := s.columns[c]
				if !ok {
					s.invalidColumnPanic(c)
				}
				row[c] = sqlite.FromSQLiteType(raw[i], cs.Type)
			}
			if conn.filterPredicate != nil && !conn.filterPredicate(row) {
				continue
			}
			if pendingRemove != nil && overlayRemoveMatches(row, pendingRemove, s.primaryKey, unordered) {
				pendingRemove = nil
				continue
			}
			// TS generateWithOverlayInner yields the add before the first row
			// it sorts STRICTLY before (`cmp < 0`, memory-source.ts:858-862);
			// equal keys are unreachable (sort includes PK; the add row is
			// not in the streamed set) — see overlaySplicePlan's contract.
			if pendingAdd != nil && effCmp(pendingAdd, row) < 0 {
				add := pendingAdd
				pendingAdd = nil
				if !yield(ivm.Node{Row: add}) {
					return
				}
			}
			if !yield(ivm.Node{Row: row}) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: rows: %v",
				s.tableName, err))
		}
		// Overlay add that sorts after every SQL row (or empty result).
		if pendingAdd != nil {
			if !yield(ivm.Node{Row: pendingAdd}) {
				return
			}
		}
	}
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
	scanned := 0
	for rows.Next() {
		// Per-fetch abort checkpoint every scanned-row batch: bounds how
		// far a single huge result scan can outrun the advance budget
		// (TS checks on every fetched row; 1024 amortizes the clock read).
		if scanned++; scanned&1023 == 0 {
			s.checkAdvanceAbort()
		}
		if err := rows.Scan(ptrs...); err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: scan: %v",
				s.tableName, err))
		}
		row := make(ivm.Row, len(colNames))
		for i, c := range colNames {
			cs, ok := s.columns[c]
			if !ok {
				s.invalidColumnPanic(c)
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

// invalidColumnPanic ports TS fromSQLiteTypes' unknown-column throw
// (table-source.ts:608-614): a SELECTed column absent from the synced
// schema is schema drift (e.g. a replica column added after the spec was
// loaded) — TS fails loud and the view-syncer resets pipelines; silently
// dropping the column would ship rows missing data with no signal.
func (s *Source) invalidColumnPanic(col string) {
	names := make([]string, 0, len(s.columns))
	for c := range s.columns {
		names = append(names, c)
	}
	sort.Strings(names)
	panic(fmt.Sprintf("Invalid column %q for table %q. Synced columns include %s",
		col, s.tableName, strings.Join(names, ", ")))
}

// driverRowToIVM converts one raw driver.Value row into an ivm.Row,
// applying the same per-column coercion (sqlite.FromSQLiteType) and
// unknown-column tripwire as scanRows. []byte values are cloned first —
// database/sql clones driver []byte before handing it to Scan (drivers may
// reuse buffers); mattn's are fresh GoBytes copies, but the clone keeps the
// raw path byte-identical to the pooled one.
func (s *Source) driverRowToIVM(dest []driver.Value, colNames []string) ivm.Row {
	row := make(ivm.Row, len(colNames))
	for i, c := range colNames {
		cs, ok := s.columns[c]
		if !ok {
			s.invalidColumnPanic(c)
		}
		v := dest[i]
		if b, isBytes := v.([]byte); isBytes {
			v = bytes.Clone(b)
		}
		row[c] = sqlite.FromSQLiteType(v, cs.Type)
	}
	return row
}

// fetchViaBoundReaderStream is the LAZY Option B hydrate read: it runs the
// SELECT on the pipeline's exclusively-held raw reader and yields each row
// one at a time via iter.Seq — the driver cursor stays open across the
// entire iteration, so a parent Join holds this cursor while fetching a
// child ON THE SAME READER (SQLite interleaves live statements on one
// connection natively; database/sql couldn't express this, which is why the
// reader is a raw driver.Conn). This is TS's resource model verbatim: one
// connection per view-syncer, nested statement.iterate() cursors.
//
// NO acquire happens here — the reader was bound at hydrate start
// (AcquireForPipeline, wait-while-holding-nothing), so this fetch can never
// participate in a hold-and-wait cycle. Same-SQL nesting is safe via the
// reader's checkout stmt cache (one sqlite3_stmt is one cursor; a nested
// same-shape fetch prepares a transient duplicate — see poolReader).
//
// No s.mu is taken — every field touched (s.tableName/columns/primaryKey,
// conn.sort/filterCondition/filterPredicate/group) is immutable for the
// lifetime of the bound pool, and the reader is exclusive to this pipeline's
// single drain goroutine. The overlay path is intentionally absent: the pool
// is bound only in the advance-free window, so no Push is in flight
// (invariant asserted by the engine's bind/unbind discipline).
//
// Uses s.ctx (CG lifetime) per the Source ctx contract — a teardown mid-
// cursor surfaces as a panic that the engine's hydrate recovery converts to
// a clean error frame.
func (s *Source) fetchViaBoundReaderStream(req ivm.FetchRequest, conn *connection, r *poolReader) iter.Seq[ivm.Node] {
	return func(yield func(ivm.Node) bool) {
		// Unordered connections issue NO ORDER BY — see fetchForConn.
		order := conn.sort
		q := sqlite.BuildSelectQuery(
			s.tableName,
			s.columns,
			req.Constraint,
			conn.filterCondition,
			order,
			req.Reverse,
			req.Start,
			req.MultiConstraints,
		)
		stmt, err := r.checkoutStmt(s.ctx, q.SQL)
		if err != nil {
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: reader prepare: %v\nSQL: %s",
				s.tableName, err, q.SQL))
		}
		// healthy flips false on any cursor-level error so returnStmt closes
		// the suspect stmt instead of caching it. Defers run LIFO: rows.Close
		// (registered below) resets the stmt BEFORE returnStmt caches it.
		healthy := true
		defer func() { r.returnStmt(q.SQL, stmt, healthy) }()
		rows, err := queryStmt(s.ctx, stmt, q.Params)
		if err != nil {
			healthy = false
			panic(fmt.Sprintf("tablesource.Source.Fetch %s: reader query: %v\nSQL: %s",
				s.tableName, err, q.SQL))
		}
		defer rows.Close()
		colNames := rows.Columns()
		dest := make([]driver.Value, len(colNames))
		for {
			err := rows.Next(dest)
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				healthy = false
				panic(fmt.Sprintf("tablesource.Source.Fetch %s: reader rows: %v",
					s.tableName, err))
			}
			row := s.driverRowToIVM(dest, colNames)
			if conn.filterPredicate != nil && !conn.filterPredicate(row) {
				continue
			}
			if !yield(ivm.Node{Row: row}) {
				return
			}
		}
	}
}

// fetchViaBoundReader is the EAGER Option B read (returns []ivm.Node) — the
// fetchForConn dispatch for direct eager callers running inside a bound
// pipeline. Semantically identical to draining fetchViaBoundReaderStream
// into a slice; kept separate so the eager path needs no iter plumbing.
func (s *Source) fetchViaBoundReader(req ivm.FetchRequest, conn *connection, r *poolReader) []ivm.Node {
	var out []ivm.Node
	for n := range s.fetchViaBoundReaderStream(req, conn, r) {
		out = append(out, n)
	}
	return out
}

// BindReaderPool binds a CG-shared frame-pinned reader pool for the
// parallel-hydrate windows (cold start, warm adds). pool is passed as `any`
// so engine/ (which must not import this package — that would cycle) can fan
// it out across its leaf sources via BindTableSourcesToReaderPool. A nil or
// wrong-typed value is ignored. MUST be paired with UnbindReaderPool before
// the first advance (after which the pool's pinned frame is stale).
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
// for SQL pushdown. Literal typing happens in the sqlite layer by the
// LITERAL's own JS-type (query_builder.go jsValueType — mirrors TS
// query-builder.ts getJsType); the old column-schema ColType stamping made a
// string literal on a json column bind as its JSON encoding, diverging from
// TS.
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
