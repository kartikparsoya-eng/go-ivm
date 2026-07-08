package tablesource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// stateVersionSQL reads the replica's monotonic replication version — TS's
// `_zero.replicationState.stateVersion` (replication-state.ts). Reading it
// inside a freshly-BEGUN read tx both PINS the WAL frame (a WAL snapshot is
// taken on the tx's first read) AND identifies which frame was pinned. This is
// the same load-bearing read the Snapshotter uses (internal/snapshotter
// selectStateVersion).
//
// The pool relies on it to prove every reader is on the SAME frame: equal
// stateVersion ⟹ identical committed content (snapshot isolation + monotonic
// marker). That equality is what lets N independent connections stand in for
// one shared C-handle snapshot — sqlite3_snapshot_open, which is dead on wal2
// (returns SQLITE_ERROR) — without any cross-reader frame skew.
const stateVersionSQL = `SELECT stateVersion FROM "_zero.replicationState"`

// maxConvergeAttempts bounds the re-pin retries when readers land on different
// frames (the replicator committed between opening reader N and reader N+1).
// Each retry ROLLBACKs the laggards and re-BEGINs them — a fresh BEGIN always
// lands on the latest (or same) frame, so convergence is guaranteed under a
// quiescent replica and bounded under sustained writes.
const maxConvergeAttempts = 10

// PoolAcquireTimeout bounds a reader-pool BUILD (raw opens + BEGINs +
// converge reads) and the Source constructor's presence probe against the
// shared replica read pool. Var (not const) so tests can shrink the window.
//
// History: this deadline was introduced when pool builds acquired K conns
// from the SHARED database/sql read pool one at a time while holding the
// ones already acquired — hold-and-wait across concurrent builders under
// read-pool exhaustion was a permanent deadlock (2026-07-06 ART incident).
// Option B's raw driver opens don't queue on any pool, so the build-time
// hold-and-wait class is structurally gone; the deadline is kept because a
// BEGIN/converge read can still stall on WAL-lock contention, and a bounded
// build failure degrades cleanly to serial hydrate.
var PoolAcquireTimeout = 5 * time.Second

// PipelineAcquireTripwire documentation lives with the acquire LOOP in
// engine/ (engine.PipelineReaderTripwire): under Option B a hydrate pipeline
// acquires its ONE reader while holding nothing, so queueing at
// AcquireForPipeline is the normal admission behavior when a batch is wider
// than K (TS's model is K=1 — every query queues behind the single conn).
// The engine's tripwire firing therefore means something is genuinely stuck
// (a parked producer never released, a leaked reader) — a BUG, not load:
// the engine PANICS loudly. Never a silent fallback — mid-flight acquires no
// longer exist, so there is nothing to fall back to.

// poolReader is one frame-pinned RAW read connection (driver.Conn — outside
// database/sql, see rawOpenReaderConn) plus its own prepared-statement cache.
// It is bound exclusively to ONE hydrate pipeline at a time via
// AcquireForPipeline, and every fetch of that pipeline — nested child
// fetches included — runs on this single conn with INTERLEAVED cursors:
// SQLite natively supports many live statements on one connection inside one
// read tx (TS's better-sqlite3 nested iterate() model). NOTE (F3,
// parallelism audit 2026-07-10): database/sql's serialization was
// empirically OVERSTATED as the raw-conn motivation — conn-prepared
// statements (conn.PrepareContext → stmt.QueryContext) DO interleave live
// cursors on one *sql.Conn (verified against mattn). The raw-conn design
// stands on its real pillars: stmt busy-checkout control for same-SQL
// nesting (checkoutStmt — database/sql's stmt layer cannot express it),
// pool-accounting bypass (builds invisible to MaxOpenConns — the
// 2026-07-06 builder-starves-probe class), driver-level scan (no
// database/sql convert layer), and shell reuse across pool generations
// (reader_cache.go).
//
// Single-goroutine discipline: a pipeline's drain is one goroutine (iter.Seq
// is synchronous), so reader state needs no locking. mattn's own internal
// mutexes cover its C-level bookkeeping.
//
// The stmt cache is BOUNDED (napi review M4): IN-clause SQL shapes vary by
// list LENGTH — `IN (?,?)` vs `IN (?,?,?)` are distinct texts — so a batched
// flipped-join hydrate with varying key-set sizes mints unbounded distinct
// shapes, each pinning a compiled sqlite3_stmt on the C heap (invisible to
// Go's allocator and GOMEMLIMIT) for the reader's lifetime. Same bound and
// eviction policy as Source.stmtCache (stmtCachePerConnCap, evict the
// least-recently-USED quarter).
//
// CHECKOUT semantics (the Option B sine-qua-non — mirrors TS zqlite's
// StatementCache and Source.checkoutSelectLocked): one sqlite3_stmt is ONE
// cursor. Interleaving works at the CONNECTION level, but nested fetches
// with the SAME SQL and different binds (self-referential joins, a repeated
// table+shape in one tree) would collide on a shared stmt — re-binding a
// live stmt silently RESETS its open cursor (verified experimentally on the
// Source cache: silent row corruption, no error; better-sqlite3 throws
// "statement is busy" for the same reason). checkoutStmt REMOVES the stmt
// from the cache while its cursor is open; a same-SQL nested checkout simply
// prepares a fresh duplicate. returnStmt hands it back (or closes it when
// the slot was re-filled first). Eviction can never close a checked-out
// stmt: checked-out stmts are not in the map.
type poolReader struct {
	dc    driver.Conn
	stmts map[string]*poolStmt
	tick  uint64
}

// poolStmt pairs a prepared statement with its last-use tick for the
// eviction scan (mirrors Source.cachedStmt).
type poolStmt struct {
	st       driver.Stmt
	lastTick uint64
}

// rawExec runs a no-result statement (BEGIN/ROLLBACK) on the raw conn.
func (r *poolReader) rawExec(ctx context.Context, query string) error {
	ec, ok := r.dc.(driver.ExecerContext)
	if !ok {
		return fmt.Errorf("reader conn does not implement driver.ExecerContext")
	}
	_, err := ec.ExecContext(ctx, query, nil)
	return err
}

// readStateVersion reads the replica's stateVersion on the raw conn (the
// frame-identifying read — see stateVersionSQL).
func (r *poolReader) readStateVersion(ctx context.Context) (string, error) {
	qc, ok := r.dc.(driver.QueryerContext)
	if !ok {
		return "", fmt.Errorf("reader conn does not implement driver.QueryerContext")
	}
	rows, err := qc.QueryContext(ctx, stateVersionSQL, nil)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return "", fmt.Errorf("stateVersion row: %w", err)
	}
	switch v := dest[0].(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("stateVersion has unexpected type %T", dest[0])
	}
}

// checkoutStmt returns a prepared statement for query, preparing a fresh one
// when the cache has none (first use, or the cached one is checked out by an
// enclosing same-SQL cursor). The stmt is REMOVED from the cache until
// returnStmt — see the poolReader doc for why sharing a live stmt corrupts.
func (r *poolReader) checkoutStmt(ctx context.Context, query string) (driver.Stmt, error) {
	r.tick++
	if e, ok := r.stmts[query]; ok {
		delete(r.stmts, query)
		return e.st, nil
	}
	pc, ok := r.dc.(driver.ConnPrepareContext)
	if !ok {
		return nil, fmt.Errorf("reader conn does not implement driver.ConnPrepareContext")
	}
	return pc.PrepareContext(ctx, query)
}

// returnStmt hands a checked-out stmt back to the cache. Closed instead of
// cached when (a) healthy=false — the caller saw a cursor error and the
// stmt's state is suspect — or (b) another checkout of the same SQL returned
// first (at most one cached stmt per shape; the transient duplicate from a
// same-SQL nested fetch dies here). Inserting past stmtCachePerConnCap
// evicts the least-recently-used quarter.
func (r *poolReader) returnStmt(query string, st driver.Stmt, healthy bool) {
	if !healthy {
		_ = st.Close()
		return
	}
	if _, occupied := r.stmts[query]; occupied {
		_ = st.Close()
		return
	}
	r.tick++
	r.stmts[query] = &poolStmt{st: st, lastTick: r.tick}
	if len(r.stmts) > stmtCachePerConnCap {
		r.evictColdest()
	}
}

// evictColdest closes and drops the least-recently-used quarter of the
// cache. Runs only when a NEW shape lands on a full cache; steady-state
// reuse of existing shapes never triggers it. The just-inserted entry has
// the highest tick, so it always survives. Checked-out stmts are not in the
// map and can never be evicted mid-cursor.
func (r *poolReader) evictColdest() {
	type kv struct {
		sql  string
		tick uint64
	}
	entries := make([]kv, 0, len(r.stmts))
	for q, e := range r.stmts {
		entries = append(entries, kv{q, e.lastTick})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].tick < entries[j].tick })
	drop := len(entries) / 4
	if drop < 1 {
		drop = 1
	}
	for _, e := range entries[:drop] {
		_ = r.stmts[e.sql].st.Close()
		delete(r.stmts, e.sql)
	}
}

// closeConn finalizes every cached stmt and closes the raw conn. The
// tx-release half lives in close(); the shell cache calls this directly on
// shells whose tx was already rolled back at cache time.
func (r *poolReader) closeConn() {
	for _, e := range r.stmts {
		_ = e.st.Close()
	}
	r.stmts = nil
	_ = r.dc.Close()
}

func (r *poolReader) close(ctx context.Context) {
	_ = r.rawExec(ctx, "ROLLBACK")
	r.closeConn()
}

// ReaderPool is a set of K raw read connections ALL pinned to the same WAL
// frame — the frame whose stateVersion == Version(). It is the wal2-viable
// replacement for the dead sqlite3_snapshot_open shared-handle pool: instead
// of a C handle, every connection independently BEGINs and is validated
// (re-pinned if needed) to the one target stateVersion.
//
// Resource model (Option B — TS parity): each hydrate pipeline acquires ONE
// reader at its start via AcquireForPipeline (wait-while-holding-nothing —
// structurally deadlock-free) and runs EVERY fetch of that pipeline, nested
// included, on that single reader with interleaved cursors. K therefore
// bounds concurrent-hydrate WIDTH, not cursor demand: a batch wider than K
// simply queues at admission (TS's model is the K=1 degenerate case — one
// conn per view-syncer, queries hydrate sequentially). Mid-flight reader
// acquires no longer exist, which eliminates the hold-and-wait deadlock
// class (readers held while waiting for more readers) outright.
type ReaderPool struct {
	free    chan *poolReader
	all     []*poolReader
	version string
	// db is the replica read pool the readers were provisioned against —
	// Close uses it to return idle shells to the reader-shell cache
	// (reader_cache.go) instead of closing them.
	db *sql.DB
	// bound maps a pipeline group (the engine's queryID) to the reader that
	// pipeline exclusively holds, from AcquireForPipeline to its release.
	// Read lock-free by every leaf fetch (readerFor) on the hydrate path.
	bound sync.Map // string → *poolReader
	// releases counts every reader RETURN to the pool — the pool-wide
	// PROGRESS signal the engine's admission tripwire keys on (F2,
	// parallelism audit 2026-07-10): a waiter resets its deadline whenever
	// this moves, so only a pool with ZERO movement for the whole tripwire
	// window (a genuine wedge — leaked reader, never-released parked
	// producer) trips; a busy-but-moving admission queue never false-fires.
	releases atomic.Uint64
}

// provisionReader returns one reader with an open (or armed) read tx plus
// the stateVersion it landed on: a cached shell when the reader-shell cache
// has one (the conn + prepared-stmt cache survive pool generations — see
// reader_cache.go), a fresh raw open otherwise. begin performs the
// pin-establishing sequence (converge: BEGIN+read; coread: BEGIN+arm+read).
//
// Self-healing: a cached shell that fails begin — the conn died while idle,
// or a leaked tx made BEGIN fail closed ("cannot start a transaction within
// a transaction" / the coread TXN_NONE guard) — is closed and replaced with
// ONE fresh open. A fresh conn's failure is systemic (expired build budget,
// non-wal2 arm, anchor gone) and is returned to fail the build.
func provisionReader(
	ctx, closeCtx context.Context,
	db *sql.DB,
	begin func(context.Context, *poolReader) (string, error),
) (*poolReader, string, error) {
	if r := popCachedShell(db); r != nil {
		if ver, err := begin(ctx, r); err == nil {
			return r, ver, nil
		}
		r.close(closeCtx)
	}
	dc, err := rawOpenReaderConn(db)
	if err != nil {
		return nil, "", err
	}
	r := &poolReader{dc: dc, stmts: map[string]*poolStmt{}}
	ver, err := begin(ctx, r)
	if err != nil {
		r.close(closeCtx)
		return nil, "", err
	}
	return r, ver, nil
}

// NewReaderPool opens k RAW read connections (rawOpenReaderConn — same DSN,
// outside database/sql), all converged onto the same WAL frame. Instead of
// requiring a pre-determined wantVersion (the old strategy that lost the
// race when the replicator advanced past it), it uses a "converge-to-latest"
// strategy:
//
//  1. Open all K readers — each BEGINs and reads whatever stateVersion it lands on.
//  2. Find the max version V_max among all readers.
//  3. ROLLBACK + re-BEGIN any reader behind V_max — they'll land on V_max or newer.
//  4. Repeat until all agree (or give up after maxConvergeAttempts).
//
// This always succeeds under a quiescent replica (all readers land on the same
// frame on the first try) and converges quickly under sustained writes (each
// iteration ratchets forward — a fresh BEGIN never lands on an older frame).
// The returned pool's Version() is whatever the readers converged to; the caller
// should refresh curr to match.
func NewReaderPool(ctx context.Context, db *sql.DB, _ string, k int) (*ReaderPool, error) {
	if db == nil {
		return nil, fmt.Errorf("tablesource.NewReaderPool: db is nil")
	}
	if k < 1 {
		k = 1
	}
	// Bound the whole build (opens + BEGINs + converge reads): see
	// PoolAcquireTimeout. On expiry the error paths below unwind every
	// already-opened reader and the caller falls back to serial hydrate.
	//
	// closeCtx: the unwind paths MUST NOT reuse the (possibly just-expired)
	// build ctx — close(expiredCtx) fails the ROLLBACK instantly without
	// executing it. Raw conns are closed outright here (not returned to any
	// pool), so a skipped ROLLBACK would only matter for the instant before
	// dc.Close — but WithoutCancel keeps the unwind semantics identical to
	// the Source paths (see NewCoReadReaderPool's note).
	ctx, cancel := context.WithTimeout(ctx, PoolAcquireTimeout)
	defer cancel()
	closeCtx := context.WithoutCancel(ctx)

	readers := make([]*poolReader, k)
	versions := make([]string, k)

	// The pin-establishing sequence for a converge reader: a fresh BEGIN
	// lands on the latest frame; the stateVersion read identifies it.
	beginConverge := func(bctx context.Context, r *poolReader) (string, error) {
		if err := r.rawExec(bctx, "BEGIN"); err != nil {
			return "", fmt.Errorf("BEGIN: %w", err)
		}
		ver, err := r.readStateVersion(bctx)
		if err != nil {
			return "", fmt.Errorf("read stateVersion: %w", err)
		}
		return ver, nil
	}

	for i := 0; i < k; i++ {
		r, ver, err := provisionReader(ctx, closeCtx, db, beginConverge)
		if err != nil {
			for j := 0; j < i; j++ {
				readers[j].close(closeCtx)
			}
			return nil, fmt.Errorf("reader pool: conn %d: %w", i, err)
		}
		readers[i] = r
		versions[i] = ver
	}

	for attempt := 0; attempt < maxConvergeAttempts; attempt++ {
		maxVer := versions[0]
		for _, v := range versions {
			if v > maxVer {
				maxVer = v
			}
		}
		allMatch := true
		for i, r := range readers {
			if versions[i] != maxVer {
				if err := r.rawExec(ctx, "ROLLBACK"); err != nil {
					for _, r2 := range readers {
						r2.close(closeCtx)
					}
					return nil, fmt.Errorf("reader pool: converge ROLLBACK: %w", err)
				}
				if err := r.rawExec(ctx, "BEGIN"); err != nil {
					for _, r2 := range readers {
						r2.close(closeCtx)
					}
					return nil, fmt.Errorf("reader pool: converge BEGIN: %w", err)
				}
				ver, err := r.readStateVersion(ctx)
				if err != nil {
					for _, r2 := range readers {
						r2.close(closeCtx)
					}
					return nil, fmt.Errorf("reader pool: converge read: %w", err)
				}
				versions[i] = ver
				if ver != maxVer {
					allMatch = false
				}
			}
		}
		if allMatch {
			p := &ReaderPool{
				free:    make(chan *poolReader, k),
				all:     readers,
				version: maxVer,
				db:      db,
			}
			for _, r := range readers {
				p.free <- r
			}
			return p, nil
		}
	}

	for _, r := range readers {
		r.close(closeCtx)
	}
	return nil, fmt.Errorf("reader pool: could not converge %d readers after %d attempts", k, maxConvergeAttempts)
}

// AcquireForPipeline borrows one exclusive frame-pinned reader for the
// pipeline identified by queryID (the engine's per-query group tag), waiting
// at most wait for one to free. On success the reader is registered in
// p.bound so every leaf fetch carrying that group (sourceInput.Fetch →
// conn.group) rides it; the returned release unbinds and returns the reader.
// ok=false means the wait elapsed with no reader free — the caller decides
// whether to keep waiting (admission queueing is NORMAL when a batch is
// wider than K) or to trip the wedge alarm (PipelineAcquireTripwire).
//
// The waiter holds NOTHING while blocked — this is the whole deadlock-freedom
// argument (wait-while-holding-nothing; see the ReaderPool doc).
func (p *ReaderPool) AcquireForPipeline(queryID string, wait time.Duration) (release func(), ok bool) {
	var r *poolReader
	select {
	case r = <-p.free:
	default:
		t := time.NewTimer(wait)
		select {
		case r = <-p.free:
			t.Stop()
		case <-t.C:
			return nil, false
		}
	}
	if prev, loaded := p.bound.Load(queryID); loaded && prev != nil {
		// A pipeline group may hold at most one reader — a double acquire
		// for the same queryID means the engine's acquire/release pairing
		// broke. Fail loud: silently replacing the binding would strand the
		// previous reader.
		p.free <- r
		panic(fmt.Sprintf("ReaderPool.AcquireForPipeline: group %q already holds a reader", queryID))
	}
	p.bound.Store(queryID, r)
	return func() {
		p.bound.Delete(queryID)
		// Bump BEFORE returning the reader so a waiter woken by the free
		// send observes the moved counter (progress — see releases).
		p.releases.Add(1)
		p.free <- r
	}, true
}

// Releases reports the cumulative reader-return count — the engine's
// pipelineReaderPool progress signal (see the releases field).
func (p *ReaderPool) Releases() uint64 { return p.releases.Load() }

// readerFor returns the reader bound to a pipeline group, or nil when the
// group holds none (build-phase fetches, legacy AddQuery hydrates, engine
// callers outside a bound pipeline) — the caller then reads through the
// serial bound conn, which sits on the SAME pinned frame.
func (p *ReaderPool) readerFor(group string) *poolReader {
	if group == "" {
		return nil
	}
	v, ok := p.bound.Load(group)
	if !ok {
		return nil
	}
	r, _ := v.(*poolReader)
	return r
}

// Version is the stateVersion every reader in the pool is pinned at.
func (p *ReaderPool) Version() string { return p.version }

// Size is the number of readers (K) the pool was built with — the
// concurrent-hydrate admission width under Option B (batches wider than K
// queue at AcquireForPipeline).
func (p *ReaderPool) Size() int { return len(p.all) }

// Close releases every reader. Healthy teardown (no reader borrowed — the
// lifecycle invariant below) returns each shell to the reader-shell cache
// when one is enabled for p.db: the read tx is ROLLED BACK first (a cached
// shell pins NO WAL frame and satisfies the coread arm's TXN_NONE
// precondition), the conn + prepared-stmt cache survive for the next pool
// generation. Without a cache — or on the BUG path — readers close outright.
// Safe to call on a partially-built pool (NewReaderPool's error path closes
// readers directly, never through here).
//
// Lifecycle invariant: the owner (sidecar ClientGroup) tears the pool down
// only under group.mu, which every hydrate RPC holds for its whole duration —
// so no reader can be borrowed when Close runs. Violations are a lifecycle
// bug upstream; Close reports them loudly and closes everything outright
// rather than caching conns with live cursors (a cached shell must be
// provably idle).
func (p *ReaderPool) Close() {
	ctx := context.Background()
	if n := len(p.free); p.all != nil && n != len(p.all) {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][POOL] BUG: ReaderPool.Close with %d/%d readers still borrowed — closing anyway; borrowers hold dead conns\n",
			len(p.all)-n, len(p.all))
		for _, r := range p.all {
			r.close(ctx)
		}
		p.all = nil
		return
	}
	cache := shellCacheFor(p.db)
	for _, r := range p.all {
		if cache == nil {
			r.close(ctx)
			continue
		}
		_ = r.rawExec(ctx, "ROLLBACK")
		cache.put(r)
	}
	p.all = nil
}

// namedDriverArgs converts BuildSelectQuery params to positional
// driver.NamedValues, applying the same DefaultParameterConverter
// database/sql applies for drivers without a NamedValueChecker (mattn has
// none) — so raw driver-level binds are byte-identical to the pooled path.
func namedDriverArgs(params []any) ([]driver.NamedValue, error) {
	if len(params) == 0 {
		return nil, nil
	}
	out := make([]driver.NamedValue, len(params))
	for i, p := range params {
		v, err := driver.DefaultParameterConverter.ConvertValue(p)
		if err != nil {
			return nil, fmt.Errorf("arg %d (%T): %w", i, p, err)
		}
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out, nil
}

// queryStmt runs a checked-out driver.Stmt with params. mattn implements
// driver.StmtQueryContext, so ctx cancellation (CG teardown) interrupts a
// blocked step exactly as the database/sql path did.
func queryStmt(ctx context.Context, st driver.Stmt, params []any) (driver.Rows, error) {
	args, err := namedDriverArgs(params)
	if err != nil {
		return nil, err
	}
	qc, ok := st.(driver.StmtQueryContext)
	if !ok {
		return nil, errors.New("reader stmt does not implement driver.StmtQueryContext")
	}
	return qc.QueryContext(ctx, args)
}
