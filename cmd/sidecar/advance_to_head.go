package main

// advanceToHeadStream: the Go-derived-diff advance.
//
// Instead of TS computing the snapshot Diff and shipping a SnapshotChange[]
// over the wire, the Go sidecar derives its OWN diff from the replica's
// changeLog2 via internal/snapshotter and applies it to its engine — a
// fully self-consistent advance, frame-coordinated against the Snapshotter's
// prev frame. The resulting RowChanges stream back to TS per row.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/internal/snapshotter"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
)

// errSeqConsumerStopped aborts diff.Each when the engine stops consuming
// the lazy change seq: the engine broke its range (panic unwind /
// budget abort), so the cursor must stop reading — it is NOT a cursor
// failure and is swallowed by the seq adapter.
var errSeqConsumerStopped = errors.New("advance seq consumer stopped")

type advanceToHeadParams struct {
	ClientGroupID string `json:"clientGroupID"`
	InitEpoch     uint64 `json:"initEpoch"`
	// RowMode opts the stream into the NAPI row plane: the engine's
	// RowChanges cross the Go↔JS boundary as per-row flat records
	// (kind 2/3 deliveries) instead of msgpack partial frames. This is
	// mandatory for production advance streaming.
	RowMode bool `json:"rowMode,omitempty"`
	// PullMode opts row-mode advance into the ABI v3 credit gate. Row-bearing
	// deliveries consume one credit; header/final/error frames ride free.
	// This is mandatory with RowMode.
	PullMode bool `json:"pullMode,omitempty"`
	// PullWindow is the opening credit count for PullMode. 0 parks before the
	// first row until the client grants explicit credit.
	PullWindow int `json:"pullWindow,omitempty"`
	// TotalHydrationTimeMs arms TS's economic advancement-abort for this
	// call (see advance_abort.go): the measured cost of re-hydrating every
	// pipeline in the CG, i.e. the price of the reset an abort triggers.
	// TS computes it (PipelineDriver.totalHydrationTimeMs() — for Go-owned
	// pipelines those entries ARE Go's own hydrate timingMs, stored back by
	// TS at registration) and ships it per request so the decision inputs
	// are identical to TS's own #shouldAdvanceYieldMaybeAbortAdvance.
	// Absent (nil — old TS, shadow paths, tests) → abort disarmed; the
	// legacy GO_IVM_ADVANCE_BUDGET_MS env deadline still applies if set.
	TotalHydrationTimeMs *float64 `json:"totalHydrationTimeMs,omitempty"`
	// SuppressAbort mirrors TS #advance's suppressAbort flag (:5863):
	// evaluate nothing even when TotalHydrationTimeMs is present.
	SuppressAbort bool `json:"suppressAbort,omitempty"`
}

// resetWire reports a ResetPipelinesSignal-equivalent: the diff aborted and the
// caller must re-hydrate all pipelines at `version`.
type resetWire struct {
	Reason string `json:"reason"`
	Msg    string `json:"msg"`
}

// advanceDeadline computes this call's budget deadline from
// advanceBudgetMs. ok=false when the budget is non-positive (disabled).
func advanceDeadline() (time.Time, bool) {
	if advanceBudgetMs <= 0 {
		return time.Time{}, false
	}
	return time.Now().Add(time.Duration(advanceBudgetMs) * time.Millisecond), true
}

// checkAdvanceBudget panics a TYPED *advanceAbortedError (recovered by
// handleStreamWithRecover → rpcCodeAdvanceAborted) when the budget deadline
// has passed. TS maps that code to ResetPipelinesSignal
// ('advancement-timeout') — reset + re-hydrate, the same recovery as the
// TS-economic abort. Deliberately NOT a DataError (→ 'data-error' teardown,
// never reset) and NOT a plain string (→ -32000 'unclassified', which since
// the follow-TS failure model RETHROWS into a CG teardown — a time-bound
// overrun is an economics decision, not a bug).
func checkAdvanceBudget(deadline time.Time, on bool, phase, cgID string) {
	if on && time.Now().After(deadline) {
		panic(&advanceAbortedError{msg: fmt.Sprintf(
			"advance exceeded GO_IVM_ADVANCE_BUDGET_MS=%d during %s (cg=%s) — "+
				"caller should reset/re-hydrate; a slow advance pins the WAL frame "+
				"the diff was derived against", advanceBudgetMs, phase, cgID)})
	}
}

type initSnapshotterState struct {
	snap     *snapshotter.Snapshotter
	specs    map[string]*snapshotter.TableSpec
	allNames map[string]bool
	current  *snapshotter.Snapshot
}

func (st *initSnapshotterState) destroy() {
	if st != nil && st.snap != nil {
		st.snap.Destroy()
		st.snap = nil
	}
}

// buildSnapshotterState constructs and pins a Snapshotter without publishing it
// to the ClientGroup. handleInit swaps it in only after the full init generation
// has succeeded.
func (s *Server) buildSnapshotterState(p *initParams) (*initSnapshotterState, error) {
	db, err := s.getReplicaDB()
	if err != nil {
		return nil, fmt.Errorf("replica not ready: %w", err)
	}
	writableDB := s.getReplicaWritableDB()
	if writableDB == nil {
		return nil, fmt.Errorf("writable replica pool not ready")
	}

	appID := p.AppID
	if appID == "" {
		appID = s.appID
	}

	snap, err := snapshotter.New(writableDB, appID)
	if err != nil {
		return nil, err
	}
	if err := snap.Init(); err != nil {
		snap.Destroy()
		return nil, fmt.Errorf("snapshotter init: %w", err)
	}

	// allTableNames is the full replicated table set (from sqlite_master), so
	// the Diff can SKIP change-log entries for replicated-but-non-syncable
	// tables instead of erroring on them. The syncable set is exactly the
	// tables TS registered in this init.
	allNames, err := readAllTableNames(db)
	if err != nil {
		snap.Destroy()
		return nil, fmt.Errorf("read table names: %w", err)
	}
	cur, err := snap.Current()
	if err != nil {
		snap.Destroy()
		return nil, fmt.Errorf("snapshotter current: %w", err)
	}

	return &initSnapshotterState{
		snap:     snap,
		specs:    buildSnapshotterSpecs(p.Tables),
		allNames: allNames,
		current:  cur,
	}, nil
}

// poolSerialLogW is the sink for the [GO-IVM][POOL-SERIAL] serial-hydrate marker
// (test-swappable, like wedgeLogW; production is always os.Stderr).
var poolSerialLogW io.Writer = os.Stderr

// logPoolSerial emits the serial-hydrate marker: a pool
// build FAILED and the hydrate is running single-conn. With the reader-shell
// cache (reader_cache.go) making builds nearly free, the 7addd28 build-slot
// gate — whose skip-to-serial was the last ROUTINE serial path — is deleted;
// what remains serial is failure-only (coread capture error, frame
// mismatch, build error), which deserves the same greppable marker status
// as [GO-IVM][WEDGE]: any nonzero count in a soak is a bug to chase, never
// "the design working". Deliberate configuration serial (feature off, K≤1)
// stays silent.
func logPoolSerial(cgID, path, reason string, err error) {
	fmt.Fprintf(poolSerialLogW, "[GO-IVM][POOL-SERIAL] cg=%s path=%s reason=%s err=%v\n",
		cgID, path, reason, err)
}

// buildReaderPoolLocked builds a reader pool whose K connections are all
// converged onto the same WAL frame (the replica head at build time).
// NewReaderPool handles the internal convergence (converge-upward) so all K
// readers agree on one frame without requiring a pre-computed target version.
// Returns (pool, nil) on success, (nil, nil) when the feature is off
// (k<=1), or (nil, err) on a hard failure. The caller MUST verify
// pool.Version() matches the Snapshotter's curr and retry if not. MUST hold
// group.mu.
func (s *Server) buildReaderPoolLocked(cur *snapshotter.Snapshot) (*tablesource.ReaderPool, *tablesource.CoRead, error) {
	// Pool size K = max(hydrateReaders, hydrateLanes) — the concurrent-
	// hydrate ADMISSION width. Option B resource model: each hydrate
	// pipeline holds exactly ONE reader for its whole drain (nested fetches
	// ride the same conn with interleaved cursors), so K bounds how many
	// pipelines hydrate in parallel; wider batches queue at
	// AcquireForPipeline while holding nothing (TS's model is the K=1
	// degenerate case — one conn per view-syncer). The old K = P × Cmax
	// concurrent-cursor sizing — and the whole Cmax AST walk — dissolved
	// with the per-fetch acquires.
	k := s.hydrateReaders
	if s.hydrateLanes > k {
		k = s.hydrateLanes
	}
	if k <= 1 {
		return nil, nil, nil // feature off: serial by design, not a failure
	}
	// No build-concurrency gate: builds provision conns from the worker-wide
	// reader-shell cache (reader_cache.go) — a churn burst costs cache pops,
	// not K fresh SQLite opens — so the 7addd28 build-slot cap (and its
	// skip-to-serial, the last ROUTINE serial-hydrate path) is deleted with
	// the provisioning cost that justified it.
	rdb, derr := s.getReplicaDB()
	if derr != nil {
		return nil, nil, derr
	}

	// Coread-fast path: capture from the anchor conn, arm K readers onto
	// the same wal2 frame. Falls through to converge-upward on any error
	// (not wal2 mode, SQLITE_ERROR, etc.).
	if cur != nil {
		if cr, cerr := tablesource.CaptureCoReadFromConn(cur.Conn()); cerr == nil {
			pool, perr := tablesource.NewCoReadReaderPool(context.Background(), rdb, cr, k)
			if perr == nil {
				return pool, cr, nil
			}
			// Coread pool failed; free the handle and fall through.
			cr.Free()
		}
	}

	// Converge-fallback path (shipped pool: K readers converge-upward).
	pool, err := tablesource.NewReaderPool(context.Background(), rdb, "", k)
	return pool, nil, err
}

// tearDownReaderPool unbinds and closes the group's cold-start reader pool, if
// any. Called at the first advance (curr is about to rotate off the pinned
// frame, making the pool stale) and on group teardown/re-init. MUST hold
// group.mu.
func (s *Server) tearDownReaderPool(group *ClientGroup) {
	if group.readerPool == nil {
		return
	}
	if group.eng != nil {
		group.eng.UnbindTableSourcesReaderPool()
	}
	group.readerPool.Close()
	group.readerPool = nil
	group.readerPoolBoundAt = time.Time{}
	if group.coread != nil {
		group.coread.Free()
		group.coread = nil
	}
}

// buildWarmReaderPoolLocked builds an EPHEMERAL co-read reader pool for a WARM
// hydrate — an addQueriesStream on a CG that already has live pipelines. Unlike
// the cold pool, it pins to curr's CURRENT frame (no RefreshCurrentToHead): the
// new queries must hydrate at exactly the frame the existing pipelines sit on,
// or they desync. That constraint forces co-read-ONLY: a converge-upward
// fallback would land on a NEWER head than the live pipelines, so on any co-read
// failure we return (nil,nil) and the warm add reads the bound curr.Conn()
// serially — correct, just not parallel.
//
// Safe because handleAddQueriesStream holds group.mu for the whole call and
// advances also take group.mu, so curr cannot rotate mid-hydrate: the co-read
// frame stays identical to the Source's bound frame (the bound-reader read's
// invariant — see fetchViaBoundReaderStream).
//
// Returns (pool, coread) bound and ready, or (nil, nil) when warm pooling is
// off / not applicable / co-read unavailable. The caller MUST pair a non-nil
// return with tearDownWarmReaderPool after AddQueriesStream. The pool is NOT
// stored on the group (it is per-call); group.readerPool is the cold pool's
// slot and is left untouched. MUST hold group.mu.
func (s *Server) buildWarmReaderPoolLocked(group *ClientGroup, cgID string) (*tablesource.ReaderPool, *tablesource.CoRead) {
	// K = admission width (see buildReaderPoolLocked — Option B: one reader
	// per hydrate pipeline, wider batches queue while holding nothing).
	k := s.hydrateReaders
	if s.hydrateLanes > k {
		k = s.hydrateLanes
	}
	// GO_IVM_WARM_HYDRATE_POOL=false is the operator KILL SWITCH for this
	// production default. The gate must stay wired even though the default
	// is ON: 0df0f63 dropped this check when streaming went default-on, and
	// the 2026-07-02 flag consolidation then documented the env knob while
	// it was consumed nowhere — a dead kill switch
	// B1; TestBuildWarmReaderPool_Guards/feature_off pins it now).
	// k<=1 is unreachable with the default lanes but kept as a defensive
	// floor.
	if !s.warmHydratePoolEnabled || k <= 1 {
		return nil, nil
	}
	if group.snap == nil || group.eng == nil {
		return nil, nil
	}
	// Cold path's job: the FIRST hydrate (no live pipelines) is handled by
	// refreshSnapForInitialHydrateLocked, which also gets to refresh curr.
	if group.eng.PipelineCount() == 0 {
		return nil, nil
	}
	// A cold pool is still bound (queries hydrated but no advance yet). It is
	// already pinned to curr's frame and bound on the sources, so this warm
	// add's pipelines acquire their readers from it directly. Under Option B
	// ANY bound pool suffices for any batch — a pipeline needs exactly ONE
	// reader regardless of join depth (nested fetches interleave cursors on
	// it), and a batch wider than the pool queues at admission while holding
	// nothing. The previous K ≥ P × Cmax(new) resize dance —
	// rebuildColdReaderPoolLocked) dissolved with the per-fetch acquires it
	// existed to keep deadlock-free.
	if group.readerPool != nil {
		return nil, nil
	}

	rdb, derr := s.getReplicaDB()
	if derr != nil {
		logPoolSerial(cgID, "warm", "replica-db", derr)
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}
	cur, cerr := group.snap.Current()
	if cerr != nil {
		logPoolSerial(cgID, "warm", "snapshot-current", cerr)
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}

	// Co-read-ONLY capture of curr's existing frame. No converge fallback.
	cr, capErr := tablesource.CaptureCoReadFromConn(cur.Conn())
	if capErr != nil {
		// Non-wal2 or capture error → serial. On a wal2 production replica
		// this is an INCIDENT (co-read should always capture from a pinned
		// anchor); on plain-wal builds it is the expected mode — the marker's
		// reason string keeps the two distinguishable in one grep.
		logPoolSerial(cgID, "warm", "coread-capture", capErr)
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}
	pool, perr := tablesource.NewCoReadReaderPool(context.Background(), rdb, cr, k)
	if perr != nil {
		cr.Free()
		logPoolSerial(cgID, "warm", "coread-pool-build", perr)
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}
	// Defensive: the co-read latches K readers to curr's frame, so versions must
	// agree. If they somehow don't (curr moved under us despite group.mu), drop
	// the pool rather than hydrate the new query at a frame the live pipelines
	// aren't on.
	if pool.Version() != cur.Version() {
		poolVer := pool.Version()
		pool.Close()
		cr.Free()
		logPoolSerial(cgID, "warm", "frame-mismatch",
			fmt.Errorf("pool pinned %s, curr at %s", poolVer, cur.Version()))
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}

	group.eng.BindTableSourcesToReaderPool(pool)
	metrics.recordWarmReaderPoolBind(true)
	return pool, cr
}

// tearDownWarmReaderPool unbinds and closes a pool built by
// buildWarmReaderPoolLocked. Unbinding reverts the sources to their still-set
// single-conn binding (curr.Conn(), via BindConn) — no rebind needed. The pool
// is ephemeral and not tracked on the group. MUST hold group.mu.
func (s *Server) tearDownWarmReaderPool(group *ClientGroup, pool *tablesource.ReaderPool, cr *tablesource.CoRead) {
	if pool == nil {
		return
	}
	if group.eng != nil {
		group.eng.UnbindTableSourcesReaderPool()
	}
	pool.Close()
	if cr != nil {
		cr.Free()
	}
}

// buildSnapshotterSpecs maps the init table schemas to snapshotter.TableSpecs.
// UniqueKeys falls back to the primary key when TS sent none (the Diff needs at
// least the PK to find unique-conflict prevValues on a set).
func buildSnapshotterSpecs(tables map[string]tableSchemaParams) map[string]*snapshotter.TableSpec {
	out := make(map[string]*snapshotter.TableSpec, len(tables))
	for name, sc := range tables {
		uk := sc.UniqueKeys
		if len(uk) == 0 && len(sc.PrimaryKey) > 0 {
			uk = [][]string{sc.PrimaryKey}
		}
		out[name] = &snapshotter.TableSpec{
			Name:          name,
			Columns:       sc.Columns,
			UniqueKeys:    uk,
			MinRowVersion: sc.MinRowVersion,
		}
	}
	return out
}

// readAllTableNames returns every base table in the replica (sqlite_master),
// used as the Diff's allTableNames set.
func readAllTableNames(db *sql.DB) (map[string]bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), tablesource.PoolAcquireTimeout)
	defer cancel()
	rows, err := db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf(
				"presence probe timed out after %v while reading table names — replica read pool exhausted?: %w",
				tablesource.PoolAcquireTimeout, err)
		}
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

// refreshSnapForInitialHydrateLocked prepares the Snapshotter's curr and the
// cold-start reader pool for the FIRST hydrate in drive mode.
//
// Go's Snapshotter was pinned at the current replica head during handleInit
// (buildSnapshotterLocked → snap.Init), which is the same window TS's own
// Snapshotter pins during its init — both happen during connection setup,
// before any replicator advance. Hydrating at this init-time frame matches TS.
//
// We deliberately do NOT call RefreshCurrentToHead here. The previous
// implementation refreshed curr to head immediately before the first hydrate,
// but the replicator can advance between TS's hydrate pin and Go's refresh,
// causing Go to hydrate at a LATER version than TS. Every row's _0_version
// (and any server-side updatedAt) then differs, producing false [shadow]
// MISMATCH events in the batch-hydrate comparison (44 false mismatches in a
// 10-user 120s soak). The refresh also caused the first advanceToHead to
// produce go-out=0 (Go was already at the destination version). Removing the
// refresh keeps Go at the init-time pin, which closely matches TS's pin.
//
// No-op once any pipeline exists: re-pinning curr would desync hydrated
// pipelines. MUST hold group.mu.
func (s *Server) refreshSnapForInitialHydrateLocked(cgID string, group *ClientGroup) {
	if group.snap == nil || group.eng == nil {
		return
	}
	if group.eng.PipelineCount() > 0 {
		return
	}

	cur, cerr := group.snap.Current()
	if cerr != nil {
		return
	}

	// Bind sources to curr's conn so the serial fallback (and the hydrate
	// itself) reads the init-time frame.
	group.eng.BindTableSourcesToConn(cur.Conn())

	// Streaming-by-default (this branch): ALWAYS build the frame-pinned reader
	// pool for cold hydrate so every pipeline streams row-at-a-time on its own
	// exclusive reader (fetchViaBoundReaderStream) instead of materializing
	// through fetchForConn. K = max(hydrateReaders, hydrateLanes) is the
	// concurrent-hydrate admission width (buildReaderPoolLocked — Option B:
	// one reader per pipeline, nested fetches interleave cursors on it); with
	// the default hydrateLanes=4, K ≥ 4 even when GO_IVM_HYDRATE_READERS is
	// unset, so there is NO readers<=1 eager fallback on the hydrate path. The
	// only eager (materializing) reader left is fetchForConn on the
	// ADVANCE/overlay path, where the pool is deliberately torn down
	// (tearDownReaderPool) — it must stay eager there to splice in-flight
	// pushes (s.overlay). GO_IVM_HYDRATE_READERS now only RAISES K; it can no
	// longer disable streaming.

	// Build pool at curr's current (init-time) frame. The coread-fast path
	// latches K readers to this frame; the converge-fallback path would
	// ratchet to head (a different frame), so on misalignment we stay serial
	// rather than hydrating at a frame that doesn't match TS.
	pool, cr, perr := s.buildReaderPoolLocked(cur)
	if perr != nil || pool == nil {
		if perr != nil {
			logPoolSerial(cgID, "cold", "build-error", perr)
		}
		if s.hydrateReaders > 1 {
			metrics.recordReaderPoolBind(poolBindSerial, 1)
		}
		return
	}

	if pool.Version() == cur.Version() {
		group.readerPool = pool
		group.readerPoolBoundAt = time.Now()
		group.coread = cr
		group.eng.BindTableSourcesToReaderPool(pool)
		outcome, via := poolBindConverge, "converge"
		if cr != nil {
			outcome, via = poolBindCoread, "coread"
		}
		metrics.recordReaderPoolBind(outcome, 1)
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][POOL] cg=%s cold-hydrate pin via %s: readers=%d frame=%s\n",
			cgID, via, s.hydrateReaders, pool.Version())
		return
	}

	// Pool converged to head while curr stayed at the init-time pin. Stay
	// serial — hydrating at head would mismatch TS's version.
	poolVer := pool.Version()
	pool.Close()
	if cr != nil {
		cr.Free()
	}
	logPoolSerial(cgID, "cold", "frame-mismatch",
		fmt.Errorf("pool converged to %s, curr pinned %s", poolVer, cur.Version()))
	metrics.recordReaderPoolBind(poolBindSerial, 1)
}

// advanceToHeadStreamPartial is the on-wire partial frame for
// advanceToHeadStream: an optional metadata header, chunked RowChanges +
// chunkIndex + final + per-final timings. Version + NumChanges ride the header
// so TS can stamp the CVR updater before row streaming starts; they also ride
// the Final frame as the authoritative completion metadata.
//
// Reset is mutually exclusive with streamed changes: when the derived diff
// aborts on a reset/truncate/permissions-change there are no RowChanges, so the
// handler emits a single Final frame carrying Reset + Version and the caller
// re-hydrates at Version.
type advanceToHeadStreamPartial struct {
	// Positional (rev 9) RowChange encoding — see positional.go.
	Dict       []dictEntry          `json:"d,omitempty"`
	Rows       [][]interface{}      `json:"r,omitempty"`
	ChunkIndex int                  `json:"chunkIndex"`
	Final      bool                 `json:"final"`
	Header     bool                 `json:"header,omitempty"`
	Timings    []engine.TableTiming `json:"timings,omitempty"`
	// Header + Final metadata (omitted on row-bearing non-final partials):
	Version    string            `json:"version,omitempty"`
	NumChanges int               `json:"numChanges,omitempty"`
	Reset      *resetWire        `json:"reset,omitempty"`
	SigDeltas  map[string]string `json:"sigDeltas,omitempty"`
}

// handleAdvanceToHeadStream is THE advance (Go-primary drive). It derives
// Go's own diff from the changelog cursor, then applies it to the engine —
// emitting the resulting RowChanges as chunked partial frames instead of one
// 64MB-capped msgpack frame. This removes the single-frame cap that a bulk
// backfill / mass UPDATE would otherwise blow on the deployed Go-primary path
// (finding F5): a frame over the cap is SKIPPED by the TS receive loop,
// orphaning the RPC until it times out.
//
// Wire shape: one OR MORE partial frames with the same
// id in monotonic chunkIndex order, exactly one Final=true (the last). Only the
// Final frame carries Timings, Version, NumChanges. After the Final
// partial, a terminal frame whose Result is the literal "done" resolves the
// call promise on the TS client.
func (s *Server) handleAdvanceToHeadStream(req RPCRequest, streamW streamWriter) RPCResponse {
	var p advanceToHeadParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		return rpcError(req.ID, -32602, err.Error())
	}

	cgID := p.ClientGroupID
	if cgID == "" {
		cgID = "default"
	}

	group := s.getGroup(cgID, false)
	if group == nil {
		return rpcError(req.ID, -32000, "engine not initialized (call init first)")
	}
	group.mu.Lock()
	defer group.mu.Unlock()

	if group.eng == nil {
		return rpcError(req.ID, -32000, "engine not initialized")
	}
	if group.snap == nil {
		// Defensive: handleInit fails loudly when the snapshotter can't
		// build, so a live group always has one — unless this advance raced
		// a re-init teardown.
		return rpcError(req.ID, -32000,
			"advanceToHeadStream unavailable (snapshotter not armed)")
	}
	if resp, stale := checkInitEpoch(group, req.ID, p.InitEpoch); stale {
		return resp
	}

	// Advance-time budget (item): one deadline covers derive +
	// Collect + engine apply + emit — the whole window during which the diff
	// pins prev's WAL frame. Checked between phases and per streamed partial
	// (checkAdvanceBudget panics; handleStreamWithRecover → rpcError → the
	// TS classifier's reset bucket).
	budgetDeadline, budgetOn := advanceDeadline()

	// Wall-clock budget context: when the advance budget is enabled, create
	// a context with the same deadline and thread it through to the sources'
	// SQL queries. The go-sqlite3 driver calls sqlite3_interrupt() when this
	// context fires, unblocking a query stuck on WAL contention that the
	// per-entry/per-partial Go-side budget checks can't reach (they only
	// run between cgo calls, not inside one).
	var advCtx context.Context
	if budgetOn {
		var cancel context.CancelFunc
		advCtx, cancel = context.WithDeadline(context.Background(), budgetDeadline)
		defer cancel()
	}
	// TS economic abort (advance_abort.go): armed only when the request
	// carries totalHydrationTimeMs — the production drive path. This only
	// captures the formula params; the processing clock arms AFTER the
	// leapfrog below (TS parity — its advance timer starts once the diff
	// exists, view-syncer.ts:2544, and measures processing laps, not wall).
	abort := newAdvanceAbort(p.TotalHydrationTimeMs, p.SuppressAbort)

	// Per-fetch abort checkpoint, threaded to the sources' advance-path
	// fetch loops (engine → SetAdvanceAbortCheck). TS runs its abort check
	// on EVERY row fetched during push processing; without this a push
	// re-fetch that emits nothing (e.g. a chained-FlippedJoin drain) never
	// crosses the per-change or per-partial check sites and outruns both
	// budgets — the 2026-07-13 advance wedge. Both panics are TYPED
	// (advanceAbortedError → rpcCodeAdvanceAborted → TS's
	// 'advancement-timeout' reset), never a plain string (which would
	// classify 'unclassified' → CG teardown).
	var fetchAbortCheck func()
	if abort.armed || budgetOn {
		fetchAbortCheck = func() {
			if aerr := abort.check(); aerr != nil {
				panic(aerr)
			}
			checkAdvanceBudget(budgetDeadline, budgetOn, "fetch", cgID)
		}
	}

	// First advance ends the cold-start hydrate window; drop the reader pool
	// before curr rotates off its pinned frame.
	s.tearDownReaderPool(group)

	diff, err := group.snap.Advance(group.snapSpecs, group.snapAllNames)
	if err != nil {
		// CLEAN failure: snapshotter.Advance is failure-atomic (the prev/curr
		// swap commits only after the diff exists) and the engine applied
		// nothing — this call is idempotent to retry in place. The retryable
		// code lets TS retry with bounded backoff instead of resetting (the
		// TS-native path has no transient-advance-failure class at all; the
		// retry keeps that divergence invisible).
		return rpcError(req.ID, rpcCodeAdvanceCleanRetryable,
			"advanceToHeadStream advance: "+err.Error())
	}
	// Arm the processing clock now that the diff exists: registers this
	// goroutine's OS thread with the abort's CPU accumulator. The WAL
	// leapfrog's lock/IO waits above are NOT processing — TS's timer
	// starts after its snapshotter advanced, and counting wall waits was
	// exactly what turned pod load into false-abort reset storms (see the
	// MEASUREMENT note in advance_abort.go). Fanout worker threads bracket
	// themselves via the engine plumbing (AdvanceStreamChunkedSeqClocked →
	// tablesource.SetAdvanceClock).
	stopProcessingClock := abort.beginProcessing()
	defer stopProcessingClock()

	version := diff.Curr().Version()

	// After the leapfrog the sources (sticky-bound to the old curr) ARE bound to
	// diff.Prev(). Make it explicit for the apply, and rebind to the new curr on
	// every exit so the next hydrate/fetch reads head.
	group.eng.BindTableSourcesToConn(diff.Prev().Conn())
	rebindCurr := func() {
		group.eng.BindTableSourcesToConn(diff.Curr().Conn())
	}
	// Rebind on EVERY exit including a panic
	// unwind — the engine re-raises panics after its terminal
	// flush, which would otherwise skip the success-path rebindCurr() and
	// strand the sources on diff.Prev() (one-frame-behind staleness).
	// Idempotent, so the explicit reset/error-path calls below keep their
	// rebind-before-Final-frame ordering.
	defer rebindCurr()

	// The changelog cursor feeds the engine LAZILY — no diff.Collect
	// materialization, no GO_IVM_MAX_DIFF_CHANGES cap (and no cap-induced
	// reset). Peak memory is O(chunk); the time budget (checked per emitted
	// partial) is the bound. diff.Each runs INSIDE the engine's range on this
	// goroutine: one changelog entry is read, pushed, flattened, and emitted
	// before the next is read — TS's lazy-cursor #advance shape.
	//
	// Cursor errors surface IN-BAND through the seq's error slot. The
	// engine stops, skips its terminal Final flush, and returns the error
	// (a half-applied diff must never settle as a clean Final). Cursor
	// failures split on whether anything already reached the wire:
	//   - nothing emitted (common: reset detected at the first entries) →
	//     today's clean single-Final reset frame / plain rpcError;
	//   - partials already emitted → rpcError ONLY (a reset-Final after
	//     row partials would double-terminate the accumulator); the TS
	//     advance classifier routes it to reset/re-hydrate (a1), which
	//     discards the half-advanced engine.
	changesSeq := func(yield func(engine.SnapshotChange, error) bool) {
		err := diff.Each(func(c snapshotter.Change) error {
			// TS checkpoint 1 (pipeline-driver.ts:2484-2490): "Check progress
			// here before processing the next change." The abort error rides
			// the seq's error slot — the same in-band path as cursor errors —
			// so the engine stops, skips its Final flush, and unwinds its
			// cursors cleanly.
			if aerr := abort.check(); aerr != nil {
				return aerr
			}
			// Wall-clock budget check per changelog entry: a pathologically
			// large diff (e.g. the mutation matrix's 220 mutations → thousands
			// of changelog entries) can spend minutes in GetRow/GetRows SQL
			// queries without ever reaching the between-phases or per-partial
			// checks. This ensures the 60s budget fires inside the loop.
			checkAdvanceBudget(budgetDeadline, budgetOn, "collect", cgID)
			if !yield(engine.SnapshotChange{
				Table:      c.Table,
				PrevValues: c.PrevValues,
				NextValue:  c.NextValue,
			}, nil) {
				return errSeqConsumerStopped
			}
			// TS increments pos in the per-change finally (:2257): by the
			// time yield returns, the engine has fully processed this change
			// (the lazy feed runs inside the engine's range). Atomic: sink-
			// site checks read pos from parallel fanout goroutines.
			abort.pos.Add(1)
			return nil
		})
		if err != nil && !errors.Is(err, errSeqConsumerStopped) {
			yield(engine.SnapshotChange{}, err)
		}
	}
	// Budget cut between the leapfrog and the engine apply (the apply
	// itself is checked per emitted partial below).
	checkAdvanceBudget(budgetDeadline, budgetOn, "collect", cgID)

	// Version + NumChanges ride the Final frame only.
	// diff.Changes is the changelog COUNT (known before any
	// row values are read), so it still rides the Final frame.
	numChanges := diff.Changes
	abort.setNumChanges(numChanges) // same count TS's formula uses (SnapshotDiff.changes)
	var emittedPartial bool

	headerPartial := advanceToHeadStreamPartial{
		ChunkIndex: 0,
		Final:      false,
		Header:     true,
		Version:    version,
		NumChanges: numChanges,
	}

	var advanceRP *rowPlane

	// finishStream maps the engine's returned error to the wire per the
	// cursor-error split above.
	finishStream := func(streamErr error) RPCResponse {
		if streamErr == nil {
			return RPCResponse{JSONRPC: "2.0", Result: "done", ID: req.ID}
		}
		var aerr *advanceAbortedError
		if errors.As(streamErr, &aerr) {
			// TS 'advancement-timeout' twin. State is half-advanced (like
			// TS's own mid-apply abort); the caller maps this code to
			// ResetPipelinesSignal('advancement-timeout') — reset+re-hydrate,
			// identical recovery to TS's own abort.
			fmt.Fprintf(os.Stderr, "[GO-IVM] advanceToHeadStream cg=%s economic abort: %s\n",
				cgID, aerr.Error())
			return rpcError(req.ID, rpcCodeAdvanceAborted, aerr.Error())
		}
		if rs, ok := snapshotter.IsReset(streamErr); ok {
			if !emittedPartial {
				// Clean pre-stream reset: single Final frame carrying reset +
				// version; the caller re-hydrates at version. It rides the row
				// plane so the header and final share the production queue.
				part := advanceToHeadStreamPartial{
					ChunkIndex: 0,
					Final:      true,
					Version:    version,
					Reset:      &resetWire{Reason: rs.Reason, Msg: rs.Msg},
				}
				if advanceRP == nil || !advanceRP.deliverFrame(part) {
					return rpcError(req.ID, -32000,
						"advanceToHeadStream: row-plane delivery dead before reset final")
				}
				return RPCResponse{JSONRPC: "2.0", Result: "done", ID: req.ID}
			}
			// Mid-stream reset: partials already emitted, so we can't send
			// a reset Final frame (the accumulator would reject a second
			// final). Use rpcCodeScalarReset so TS classifies it as a
			// reset (ResetPipelinesSignal) instead of 'unclassified' →
			// CG teardown. The recovery (reset + re-hydrate) is identical
			// regardless of the reset reason.
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] advanceToHeadStream cg=%s mid-stream reset: %s: %s\n",
				cgID, rs.Reason, rs.Msg)
			return rpcError(req.ID, rpcCodeScalarReset,
				"advanceToHeadStream reset: "+rs.Reason+": "+rs.Msg)
		}
		fmt.Fprintf(os.Stderr, "[GO-IVM] advanceToHeadStream ERROR cg=%s: %v\n", cgID, streamErr)
		return rpcError(req.ID, -32000, "advanceToHeadStream: "+streamErr.Error())
	}

	// Production stream contract: per-row records via the NAPI row plane with
	// pull-mode credit gating. Fallback rows and the terminal Final (carrying
	// Version/NumChanges/Timings) ship as kind-1 frames on the same ordered
	// queue; "done" follows via the pipe (see rowplane.go's ordering invariant).
	rp := newRowPlane(s, req.ID, p.RowMode, cgID, group.done)
	if !p.RowMode || !p.PullMode || rp == nil {
		return rpcError(req.ID, -32000,
			"advanceToHeadStream: requires row-mode pull NAPI transport")
	}
	advanceRP = rp
	rid, _ := numericReqID(req.ID) // non-numeric already refused by newRowPlane
	gate := s.streamGates.register(rid, group, int64(p.PullWindow), func() {
		group.lastUsedNs.Store(time.Now().UnixNano())
	})
	if gate == nil {
		return rpcError(req.ID, -32000,
			"advanceToHeadStream: pull gate registration failed")
	}
	rp.setPullGate(gate)
	defer s.streamGates.unregister(rid)
	if !rp.deliverFrame(headerPartial) {
		return rpcError(req.ID, -32000,
			"advanceToHeadStream: row-plane delivery dead before header")
	}
	streamErr := group.eng.AdvanceStreamChunkedSeqClocked(changesSeq, 1, abort.clock(), advCtx, fetchAbortCheck, func(r engine.AdvanceStreamPartial) {
		// Per-partial budget checkpoint: a panic here escapes
		// AdvanceStreamChunkedSeq cleanly (engine stays reusable — see
		// TestAdvanceStream_PanickingSink_NoDeadlockAndEngineReusable)
		// and handleStreamWithRecover converts it to an RPC error.
		checkAdvanceBudget(budgetDeadline, budgetOn, "apply", cgID)
		// TS checkpoint 2 ("whenever a row is fetched during push"):
		// rowMode emits per RowChange, so this is per-row granularity.
		// The typed panic maps to rpcCodeAdvanceAborted with the exact
		// message (panicErrorCode/panicErrorMessage).
		if aerr := abort.check(); aerr != nil {
			panic(aerr)
		}
		if len(r.Changes) > 0 && !acquirePullCredit(gate, rp) {
			panic(fmt.Errorf("advanceToHeadStream cg=%s: stream cancelled while waiting for pull credit", cgID))
		}
		emittedPartial = true
		if !rp.emitAdvanceToHeadPartial(r, version, numChanges) {
			// Row-plane delivery dead (TSFN closed / group teardown /
			// GO_IVM_DELIVER_TIMEOUT — the bounded successor of the G13
			// blocking-deliver wedge). The engine's sink has no error
			// return — panic into handleStreamWithRecover exactly like
			// the economic abort; the -32000 classification tears the
			// CG down on the TS side, which is right: the client is
			// gone or its loop is starved beyond recovery, and state is
			// half-advanced.
			panic(fmt.Errorf("advanceToHeadStream cg=%s: row-plane delivery dead (stream cancelled or deliver timeout) — aborting advance", cgID))
		}
		if r.Final {
			// rowMode: chunkSize=1, so ChunkIndex+1 is the per-row
			// DELIVERY count, not a chunk count — record it as rows so the
			// advance-chunks histogram isn't polluted.
			metrics.recordAdvanceRows(r.ChunkIndex + 1)
		}
	})
	rebindCurr()
	return finishStream(streamErr)
}
