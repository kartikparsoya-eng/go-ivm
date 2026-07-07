package main

// advanceToHeadStream: the Go-derived-diff advance (design §4, P2 drive).
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
	"os"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/internal/snapshotter"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
)

// errSeqConsumerStopped aborts diff.Each when the engine stops consuming
// the lazy change seq (D9): the engine broke its range (panic unwind /
// budget abort), so the cursor must stop reading — it is NOT a cursor
// failure and is swallowed by the seq adapter.
var errSeqConsumerStopped = errors.New("advance seq consumer stopped")

type advanceToHeadParams struct {
	ClientGroupID string `json:"clientGroupID"`
	InitEpoch     uint64 `json:"initEpoch"`
	// RowMode opts the stream into the NAPI row plane: the engine's
	// RowChanges cross the Go↔JS boundary as per-row flat records
	// (kind 2/3 deliveries) instead of msgpack partial frames. Honored only
	// when the in-process transport is active AND the request ID is numeric;
	// otherwise silently degrades to the ordinary frame path.
	RowMode bool `json:"rowMode,omitempty"`
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

// buildSnapshotterLocked constructs and pins this group's Snapshotter. MUST be
// called with group.mu held (it is, from handleInit). Pins curr at the current
// replica head — the same frame the hydrate reads from.
func (s *Server) buildSnapshotterLocked(group *ClientGroup, p *initParams) error {
	// Re-init: drop any reader pool left over from a prior snapshotter (its
	// frame is about to be replaced).
	s.tearDownReaderPool(group)

	db, err := s.getReplicaDB()
	if err != nil {
		return fmt.Errorf("replica not ready: %w", err)
	}
	writableDB := s.getReplicaWritableDB()
	if writableDB == nil {
		return fmt.Errorf("writable replica pool not ready")
	}

	appID := p.AppID
	if appID == "" {
		appID = s.appID
	}

	snap, err := snapshotter.New(writableDB, appID)
	if err != nil {
		return err
	}
	if err := snap.Init(); err != nil {
		snap.Destroy()
		return fmt.Errorf("snapshotter init: %w", err)
	}

	// allTableNames is the full replicated table set (from sqlite_master), so
	// the Diff can SKIP change-log entries for replicated-but-non-syncable
	// tables instead of erroring on them. The syncable set is exactly the
	// tables TS registered in this init.
	allNames, err := readAllTableNames(db)
	if err != nil {
		snap.Destroy()
		return fmt.Errorf("read table names: %w", err)
	}

	group.snap = snap
	group.snapSpecs = buildSnapshotterSpecs(p.Tables)
	group.snapAllNames = allNames

	// Drive: the engine's tablesource leaves read from the
	// Snapshotter's frame, not their own per-Source tx. Sticky-bind them to
	// curr now so the initial hydrate (addQuery) reads the same frame the
	// Snapshotter is pinned at. Each advance flips the binding to prev for
	// the apply, then back to curr.
	if cur, cerr := snap.Current(); cerr == nil {
		group.eng.BindTableSourcesToConn(cur.Conn())
		// NOTE: the cold-start reader pool is built later, at the first-hydrate
		// refresh seam (refreshSnapForInitialHydrateLocked), NOT here. Building
		// it at init pinned a stateVersion the drive-mode replicator advanced
		// past before hydrate arrived, so the pin failed and every cold batch
		// fell back to serial single-conn reads. Building it together with the
		// curr-refresh, on the same fresh frame, is what lets the pin land.
	}
	return nil
}

// tryAcquireBuildSlot claims a reader-pool build slot, waiting at most
// maxWait (0 = non-blocking). Returns (release, true) on success. On
// failure the caller MUST fall back to serial hydrate — that is the whole
// point: bounded build concurrency, never a queue.
func (s *Server) tryAcquireBuildSlot(maxWait time.Duration) (func(), bool) {
	if maxWait <= 0 {
		select {
		case s.readerPoolBuildSlots <- struct{}{}:
			return func() { <-s.readerPoolBuildSlots }, true
		default:
			metrics.readerPoolBuildSlotSkips.Add(1)
			return nil, false
		}
	}
	t := time.NewTimer(maxWait)
	defer t.Stop()
	select {
	case s.readerPoolBuildSlots <- struct{}{}:
		return func() { <-s.readerPoolBuildSlots }, true
	case <-t.C:
		metrics.readerPoolBuildSlotSkips.Add(1)
		return nil, false
	}
}

// coldBuildSlotWait is how long a COLD pool build waits for a slot before
// degrading to serial hydrate. Cold pools front the CG's first hydrate
// (biggest payoff), so they wait briefly; warm builds never wait (0).
const coldBuildSlotWait = time.Second

// readerPoolBuildSlotCap is the per-Server bound on CONCURRENT reader-pool
// builds (see Server.readerPoolBuildSlots). Two keeps worst-case in-flight
// build demand at 2×(K+1) conns — small enough that concurrent builders
// can't mutually starve inside PoolAcquireTimeout, large enough that a slow
// build doesn't serialize every other CG behind it.
const readerPoolBuildSlotCap = 2

// buildReaderPoolLocked builds a reader pool whose K connections are all
// converged onto the same WAL frame (the replica head at build time).
// NewReaderPool handles the internal convergence (converge-upward) so all K
// readers agree on one frame without requiring a pre-computed target version.
// Returns (pool, nil) on success, (nil, nil) when the feature is off
// (hydrateReaders<=1), or (nil, err) on a hard failure. The caller MUST verify
// pool.Version() matches the Snapshotter's curr and retry if not. MUST hold
// group.mu.
func (s *Server) buildReaderPoolLocked(cur *snapshotter.Snapshot, cmax int) (*tablesource.ReaderPool, *tablesource.CoRead, error) {
	// Pool size K = max(hydrateReaders, hydrateLanes × Cmax). Each of the P
	// worker lanes may need up to Cmax concurrent readers (e.g. a 2-deep
	// join holds a parent cursor while fetching the child → 2 readers per
	// lane). K = P × Cmax guarantees every lane can acquire all the readers
	// it needs without blocking (deadlock-freedom: §3d). When both
	// hydrateReaders and hydrateLanes×cmax are ≤1, the feature is off.
	if cmax < 1 {
		cmax = 1
	}
	k := s.hydrateReaders
	if lanesCmax := s.hydrateLanes * cmax; lanesCmax > k {
		k = lanesCmax
	}
	if k <= 1 {
		return nil, nil, nil // feature off: serial by design, not a failure
	}
	// Bound concurrent builds (see Server.readerPoolBuildSlots): under a
	// churn burst, unbounded builders hold-and-wait each other into the
	// PoolAcquireTimeout; two at a time complete in milliseconds each.
	release, ok := s.tryAcquireBuildSlot(coldBuildSlotWait)
	if !ok {
		return nil, nil, nil // slots busy → serial hydrate (bounded degrade)
	}
	defer release()
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
// frame stays identical to the Source's bound frame (fetchViaPool's invariant).
//
// Returns (pool, coread) bound and ready, or (nil, nil) when warm pooling is
// off / not applicable / co-read unavailable. The caller MUST pair a non-nil
// return with tearDownWarmReaderPool after AddQueriesStream. The pool is NOT
// stored on the group (it is per-call); group.readerPool is the cold pool's
// slot and is left untouched. MUST hold group.mu.
func (s *Server) buildWarmReaderPoolLocked(group *ClientGroup, cmax int) (*tablesource.ReaderPool, *tablesource.CoRead) {
	if cmax < 1 {
		cmax = 1
	}
	k := s.hydrateReaders
	if lanesCmax := s.hydrateLanes * cmax; lanesCmax > k {
		k = lanesCmax
	}
	// GO_IVM_WARM_HYDRATE_POOL=false is the operator KILL SWITCH for this
	// production default. The gate must stay wired even though the default
	// is ON: 0df0f63 dropped this check when streaming went default-on, and
	// the 2026-07-02 flag consolidation then documented the env knob while
	// it was consumed nowhere — a dead kill switch (REVIEW-napi-transport
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
	// already pinned to curr's frame and bound on the sources, so this warm add's
	// fetches ALREADY run through it — but only reuse it if it is big enough
	// for THIS batch's concurrent-cursor demand.
	//
	// C2 (scale review): the cold pool's K was sized for the FIRST batch's
	// Cmax. A second addQueriesStream arriving before the first advance
	// hydrates through that pool; if this batch's joins are deeper
	// (P × Cmax(new) > K), every hydrate lane can end up holding parent
	// readers while blocked in acquire for a child — a resource deadlock
	// with group.mu and the engine lock held. acquire's only unblock is the
	// Source's CG-lifetime ctx, and CG teardown itself needs group.mu, so
	// the wedge is unkillable and queues destroys + the reaper behind it.
	// Rebuild the cold pool at the same frame with the larger K; on any
	// rebuild failure tear the undersized pool down and hydrate serially —
	// slower, never deadlocked.
	if group.readerPool != nil {
		if group.readerPool.Size() >= k {
			return nil, nil // cold pool covers this batch's demand
		}
		s.rebuildColdReaderPoolLocked(group, cmax)
		return nil, nil
	}

	rdb, derr := s.getReplicaDB()
	if derr != nil {
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}
	cur, cerr := group.snap.Current()
	if cerr != nil {
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}

	// Bound concurrent builds — NON-blocking for warm adds: the pool is an
	// optimization, and this runs with group.mu held (queueing the CG's
	// worker behind a build slot inverts the win — the 2026-07-07 convoy).
	// Slots busy → serial hydrate through the bound curr.Conn(), instantly.
	release, ok := s.tryAcquireBuildSlot(0)
	if !ok {
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}
	defer release()

	// Co-read-ONLY capture of curr's existing frame. No converge fallback.
	cr, capErr := tablesource.CaptureCoReadFromConn(cur.Conn())
	if capErr != nil {
		metrics.recordWarmReaderPoolBind(false) // non-wal2 or capture error → serial
		return nil, nil
	}
	pool, perr := tablesource.NewCoReadReaderPool(context.Background(), rdb, cr, k)
	if perr != nil {
		cr.Free()
		metrics.recordWarmReaderPoolBind(false)
		return nil, nil
	}
	// Defensive: the co-read latches K readers to curr's frame, so versions must
	// agree. If they somehow don't (curr moved under us despite group.mu), drop
	// the pool rather than hydrate the new query at a frame the live pipelines
	// aren't on.
	if pool.Version() != cur.Version() {
		pool.Close()
		cr.Free()
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

// rebuildColdReaderPoolLocked replaces a still-bound cold pool with one sized
// for a LARGER concurrent-cursor demand (scale-review C2), pinned to the SAME
// frame — pre-first-advance curr has not rotated, and group.mu (held) blocks
// advances for the duration. Uses the cold-build recipe (co-read fast path,
// converge fallback) + the same version-match verification as
// refreshSnapForInitialHydrateLocked: a converge pool that ratcheted past
// curr's frame is discarded rather than bound (it would desync the new
// queries from the live pipelines' frame).
//
// Every failure path tears the undersized pool down: serial reads on the
// bound conn are slow but deadlock-free, whereas leaving the small pool
// bound reproduces the acquire wedge this exists to prevent. MUST hold
// group.mu.
func (s *Server) rebuildColdReaderPoolLocked(group *ClientGroup, cmax int) {
	oldK := group.readerPool.Size()
	cur, cerr := group.snap.Current()
	if cerr != nil {
		s.tearDownReaderPool(group)
		metrics.recordReaderPoolBind(poolBindSerial, 1)
		return
	}
	pool, cr, perr := s.buildReaderPoolLocked(cur, cmax)
	if perr != nil || pool == nil {
		s.tearDownReaderPool(group)
		metrics.recordReaderPoolBind(poolBindSerial, 1)
		return
	}
	if pool.Version() != cur.Version() {
		// Converge fallback landed on a newer head than the live pipelines'
		// frame — binding it would hydrate the new queries at the wrong
		// frame. Serial instead.
		pool.Close()
		if cr != nil {
			cr.Free()
		}
		s.tearDownReaderPool(group)
		metrics.recordReaderPoolBind(poolBindSerial, 1)
		return
	}
	// Swap: unbind + close the undersized pool, then bind the bigger one.
	s.tearDownReaderPool(group)
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
		"[GO-IVM][POOL] cold-pool resize via %s: readers %d→%d (cmax=%d) frame=%s\n",
		via, oldK, pool.Size(), cmax, pool.Version())
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
	rows, err := db.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
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
func (s *Server) refreshSnapForInitialHydrateLocked(cgID string, group *ClientGroup, specs []engine.QuerySpec) {
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
	// pool for cold hydrate so every leaf streams row-at-a-time via
	// fetchViaPoolStream instead of materializing through fetchForConn. K = P ×
	// Cmax (buildReaderPoolLocked) sizes it for parallel streaming; with the
	// default hydrateLanes=4, K ≥ 4 even when GO_IVM_HYDRATE_READERS is unset, so
	// there is NO readers<=1 eager fallback on the hydrate path. The only eager
	// (materializing) reader left is fetchForConn on the ADVANCE/overlay path,
	// where the pool is deliberately torn down (tearDownReaderPool) — it must stay
	// eager there to splice in-flight pushes (s.overlay). GO_IVM_HYDRATE_READERS
	// now only RAISES K above P×Cmax; it can no longer disable streaming.

	// Build pool at curr's current (init-time) frame. The coread-fast path
	// latches K readers to this frame; the converge-fallback path would
	// ratchet to head (a different frame), so on misalignment we stay serial
	// rather than hydrating at a frame that doesn't match TS.
	cmax := engine.ConservativeHydrateCmaxForSpecs(specs)
	pool, cr, perr := s.buildReaderPoolLocked(cur, cmax)
	if perr != nil || pool == nil {
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
	pool.Close()
	if cr != nil {
		cr.Free()
	}
	metrics.recordReaderPoolBind(poolBindSerial, 1)
}

// advanceToHeadStreamPartial is the on-wire partial frame for
// advanceToHeadStream: chunked RowChanges + chunkIndex + final + per-final
// timings, plus the Version + NumChanges the engine's stream doesn't know
// about — those ride the Final frame only. The TS accumulator reassembles the
// frames into one AdvanceToHeadResult.
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
	Timings    []engine.TableTiming `json:"timings,omitempty"`
	// Final-frame-only metadata (omitted on non-final partials):
	Version    string     `json:"version,omitempty"`
	NumChanges int        `json:"numChanges,omitempty"`
	Reset      *resetWire `json:"reset,omitempty"`
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

	// Advance-time budget (user's-audit item): one deadline covers derive +
	// Collect + engine apply + emit — the whole window during which the diff
	// pins prev's WAL frame. Checked between phases and per streamed partial
	// (checkAdvanceBudget panics; handleStreamWithRecover → rpcError → the
	// TS classifier's reset bucket).
	budgetDeadline, budgetOn := advanceDeadline()

	// TS economic abort (advance_abort.go): armed only when the request
	// carries totalHydrationTimeMs — the production drive path. This only
	// captures the formula params; the processing clock arms AFTER the
	// leapfrog below (TS parity — its advance timer starts once the diff
	// exists, view-syncer.ts:2544, and measures processing laps, not wall).
	abort := newAdvanceAbort(p.TotalHydrationTimeMs, p.SuppressAbort)

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
	// P1 (REVIEW-napi-transport): rebind on EVERY exit including a panic
	// unwind — the engine re-raises panics after its terminal
	// flush, which would otherwise skip the success-path rebindCurr() and
	// strand the sources on diff.Prev() (one-frame-behind staleness).
	// Idempotent, so the explicit reset/error-path calls below keep their
	// rebind-before-Final-frame ordering.
	defer rebindCurr()

	// D9 (DESIGN-duplex-streaming): the changelog cursor feeds the engine
	// LAZILY — no diff.Collect materialization, no GO_IVM_MAX_DIFF_CHANGES
	// cap (and no cap-induced reset). Peak memory is O(chunk); the a3 time
	// budget (checked per emitted partial) is the bound. diff.Each runs
	// INSIDE the engine's range on this goroutine: one changelog entry is
	// read, pushed, flattened, and emitted before the next is read — TS's
	// lazy-cursor #advance shape.
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
			// TS checkpoint 1 (pipeline-driver.ts:5883-5890): "Check progress
			// here before processing the next change." The abort error rides
			// the seq's error slot — the same in-band path as cursor errors —
			// so the engine stops, skips its Final flush, and unwinds its
			// cursors cleanly.
			if aerr := abort.check(); aerr != nil {
				return aerr
			}
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

	// finishStream maps the engine's returned error to the wire per the
	// cursor-error split above. Shared by the rowMode and frame branches.
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
		if rs, ok := snapshotter.IsReset(streamErr); ok && !emittedPartial {
			// Clean pre-stream reset: single Final frame carrying reset +
			// version; the caller re-hydrates at version. streamW is safe
			// in rowMode too — no records exist, so ordering is trivially
			// preserved (see rowplane.go's emitAdvanceToHeadPartial note).
			streamW(req.ID, advanceToHeadStreamPartial{
				ChunkIndex: 0,
				Final:      true,
				Version:    version,
				Reset:      &resetWire{Reason: rs.Reason, Msg: rs.Msg},
			})
			return RPCResponse{JSONRPC: "2.0", Result: "done", ID: req.ID}
		}
		fmt.Fprintf(os.Stderr, "[GO-IVM] advanceToHeadStream ERROR cg=%s: %v\n", cgID, streamErr)
		return rpcError(req.ID, -32000, "advanceToHeadStream: "+streamErr.Error())
	}

	// Row mode (NAPI transport only): per-row records via abiDeliver with
	// chunkSize=1 so each RowChange crosses the boundary as the engine
	// produces it — the deployed Go-primary trigger path gets
	// row-by-row delivery. Fallback rows and
	// the terminal Final (carrying Version/NumChanges/Timings) ship
	// as kind-1 frames on the same ordered queue; "done" follows via the
	// pipe (see rowplane.go's ordering invariant).
	if rp := newRowPlane(s, req.ID, p.RowMode); rp != nil {
		streamErr := group.eng.AdvanceStreamChunkedSeqClocked(changesSeq, 1, abort.clock(), func(r engine.AdvanceStreamPartial) {
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
			emittedPartial = true
			rp.emitAdvanceToHeadPartial(r, version, numChanges)
			if r.Final {
				// rowMode: chunkSize=1, so ChunkIndex+1 is the per-row
				// DELIVERY count, not a chunk count — record it as rows so the
				// advance-chunks histogram isn't polluted (P2).
				metrics.recordAdvanceRows(r.ChunkIndex + 1)
			}
		})
		rebindCurr()
		return finishStream(streamErr)
	}

	streamErr := group.eng.AdvanceStreamChunkedSeqClocked(changesSeq, 0, abort.clock(), func(r engine.AdvanceStreamPartial) {
		checkAdvanceBudget(budgetDeadline, budgetOn, "apply", cgID)
		if aerr := abort.check(); aerr != nil {
			panic(aerr) // TS checkpoint 2 — see the rowMode branch
		}
		emittedPartial = true
		pc := toPositional(r.Changes)
		part := advanceToHeadStreamPartial{
			Dict:       pc.Dict,
			Rows:       pc.Rows,
			ChunkIndex: r.ChunkIndex,
			Final:      r.Final,
			Timings:    r.Timings,
		}
		if r.Final {
			part.Version = version
			part.NumChanges = numChanges
			// One advanceToHeadStream call → one record on the terminal frame;
			// Final's ChunkIndex+1 is the total chunk count for this call.
			metrics.recordAdvanceChunks(r.ChunkIndex + 1)
		}
		streamW(req.ID, part)
	})
	rebindCurr()
	return finishStream(streamErr)
}
