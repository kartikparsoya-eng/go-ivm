package main

// advanceToHead: the Go-derived-diff advance trigger (design §4, P1).
//
// Instead of TS computing the snapshot Diff and shipping a SnapshotChange[]
// over the wire (the legacy `advance`/`advanceStream` path), the Go sidecar
// derives its OWN diff from the replica's changeLog2 via internal/snapshotter.
// In P1 this is a pure derivation: advanceToHead leapfrogs the Snapshotter and
// returns the derived changes + Go's new stateVersion WITHOUT driving the
// engine — so the TS side can compare Go's diff against the one it computed for
// the same advance (a Go-vs-TS diff shadow that proves §3.1 fidelity before any
// CVR version authority moves to Go in P2).

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/internal/snapshotter"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

type advanceToHeadParams struct {
	ClientGroupID string `json:"clientGroupID"`
	InitEpoch     uint64 `json:"initEpoch"`
}

// snapshotChangeWire is the on-wire form of a snapshotter.Change. It carries
// the same {prevValues, nextValue} as engine.SnapshotChange plus the rowKey TS
// uses to align Go's derived changes with its own.
type snapshotChangeWire struct {
	Table      string    `json:"table"`
	PrevValues []ivm.Row `json:"prevValues"`
	NextValue  ivm.Row   `json:"nextValue,omitempty"`
	RowKey     ivm.Row   `json:"rowKey"`
}

// resetWire reports a ResetPipelinesSignal-equivalent: the diff aborted and the
// caller must re-hydrate all pipelines at `version`.
type resetWire struct {
	Reason string `json:"reason"`
	Msg    string `json:"msg"`
}

type advanceToHeadResult struct {
	// Changes is the Go-derived diff (P1 pure-derivation / shadow compare).
	// Populated when NOT in drive mode; omitted in drive mode.
	Changes    []snapshotChangeWire `json:"changes,omitempty"`
	Version    string               `json:"version"`
	NumChanges int                  `json:"numChanges"`
	// RowChanges + Timings are the engine output, populated only in drive mode
	// (P2): the deltas the view-syncer emits to clients, stamped at Version,
	// produced by applying Go's OWN derived diff to Go's engine — frame-
	// coordinated against the Snapshotter's prev frame (no drift).
	RowChanges []engine.RowChange   `json:"rowChanges,omitempty"`
	Timings    []engine.TableTiming `json:"timings,omitempty"`
	// Reset is non-nil when the diff hit a reset/truncate/permissions-change;
	// Changes is then empty and the caller re-hydrates at Version.
	Reset *resetWire `json:"reset,omitempty"`
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

	// Drive mode (P2): the engine's tablesource leaves read from the
	// Snapshotter's frame, not their own per-Source tx. Sticky-bind them to
	// curr now so the initial hydrate (addQuery) reads the same frame the
	// Snapshotter is pinned at. Each advanceToHead flips the binding to prev for
	// the apply, then back to curr. (Non-drive P1 leaves binding untouched.)
	if s.advanceDriveEnabled {
		if cur, cerr := snap.Current(); cerr == nil {
			group.eng.BindTableSourcesToConn(cur.Conn())
			// NOTE: the cold-start reader pool is built later, at the first-hydrate
			// refresh seam (refreshSnapForInitialHydrateLocked), NOT here. Building
			// it at init pinned a stateVersion the drive-mode replicator advanced
			// past before hydrate arrived, so the pin failed and every cold batch
			// fell back to serial single-conn reads. Building it together with the
			// curr-refresh, on the same fresh frame, is what lets the pin land.
		}
	}
	return nil
}

// buildReaderPoolLocked builds a reader pool whose K connections are all
// converged onto the same WAL frame (the replica head at build time).
// NewReaderPool handles the internal convergence (converge-upward) so all K
// readers agree on one frame without requiring a pre-computed target version.
// Returns (pool, nil) on success, (nil, nil) when the feature is off
// (hydrateReaders<=1), or (nil, err) on a hard failure. The caller MUST verify
// pool.Version() matches the Snapshotter's curr and retry if not. MUST hold
// group.mu.
func (s *Server) buildReaderPoolLocked(cur *snapshotter.Snapshot) (*tablesource.ReaderPool, *tablesource.CoRead, error) {
	// Pool size K = max(hydrateReaders, hydrateLanes). hydrateLanes (P worker
	// lanes) needs at least P readers so every lane can acquire one
	// (deadlock-freedom: K = P × Cmax, Cmax=1 while eager → K=P). When both
	// are ≤1, the feature is off: serial by design.
	k := s.hydrateReaders
	if s.hydrateLanes > k {
		k = s.hydrateLanes
	}
	if k <= 1 {
		return nil, nil, nil // feature off: serial by design, not a failure
	}
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
func (s *Server) buildWarmReaderPoolLocked(group *ClientGroup) (*tablesource.ReaderPool, *tablesource.CoRead) {
	k := s.hydrateReaders
	if s.hydrateLanes > k {
		k = s.hydrateLanes
	}
	if !s.warmHydratePoolEnabled || !s.advanceDriveEnabled || k <= 1 {
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
	// fetches ALREADY run through it — don't build a second pool.
	if group.readerPool != nil {
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

// refreshSnapForInitialHydrateLocked re-pins the Snapshotter's curr at head
// before the FIRST hydrate in drive mode, so Go reads the same version TS does.
// Init() pins curr at handleInit time, but the replicator may advance before the
// initial addQueries arrives — leaving Go's hydrate frame slightly behind TS's
// (rare "Go produced 0" shadow misses for rows that landed in that window).
// No-op once any pipeline exists: re-pinning curr would desync hydrated
// pipelines. MUST hold group.mu.
//
// maxReaderPoolConvergeAttempts bounds the converge loop. Each attempt refreshes
// curr to head, builds the pool (which converges K readers to the same frame
// internally via converge-upward), and checks that pool.Version() == curr's
// version. If the replicator advanced between the curr refresh and the pool
// build, the versions disagree and we retry. Capped at 10 so the rare
// unconvergeable case (sustained writes) degrades to the serial path after
// ~10×(K+1) ms — acceptable on the cold-hydrate critical path under group.mu.
const maxReaderPoolConvergeAttempts = 10

func (s *Server) refreshSnapForInitialHydrateLocked(cgID string, group *ClientGroup) {
	if !s.advanceDriveEnabled || group.snap == nil || group.eng == nil {
		return
	}
	if group.eng.PipelineCount() > 0 {
		return
	}

	// Converge curr AND the cold-start reader pool onto ONE fresh frame, right
	// before the first hydrate. The pool's K readers converge to the same frame
	// internally (NewReaderPool, converge-upward); we then check that pool and
	// curr agree. If the replicator advanced between the curr refresh and the
	// pool build, the pool lands on a newer frame — we detect the mismatch and
	// retry, ratcheting both forward. This eliminates the old TOCTOU where a
	// pre-computed wantVersion was already stale by the time the pool tried to
	// pin it (the old inner loop retried 5x against an impossible-to-rewind
	// target, all futile, wasting ~50ms per outer attempt).
	var lastErr error
	for attempt := 0; attempt < maxReaderPoolConvergeAttempts; attempt++ {
		if err := group.snap.RefreshCurrentToHead(); err != nil {
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] refresh curr before initial hydrate failed: %v\n", err)
			if s.hydrateReaders > 1 {
				metrics.recordReaderPoolBind(poolBindSerial, attempt+1)
			}
			return // can't refresh curr at all — serial path, nothing more to try
		}
		cur, cerr := group.snap.Current()
		if cerr != nil {
			lastErr = cerr
			continue
		}
		// Keep the single-conn binding on the just-refreshed frame so the serial
		// fallback (if the pool never binds) still reads the correct version.
		group.eng.BindTableSourcesToConn(cur.Conn())
		if s.hydrateReaders <= 1 {
			return // feature off: curr refreshed, serial by design
		}

		// Build pool — coread-fast (K readers latched to anchor's frame) or
		// converge-fallback (K readers converge-upward).
		pool, cr, perr := s.buildReaderPoolLocked(cur)
		if perr != nil {
			lastErr = perr
			continue // pool build failed; retry with a fresh curr
		}
		if pool == nil {
			return // feature off (guarded above, defensive)
		}

		// Check alignment: pool and curr must be on the same frame.
		if pool.Version() == cur.Version() {
			group.readerPool = pool
			group.coread = cr
			group.eng.BindTableSourcesToReaderPool(pool)
			// cr is non-nil ONLY on the coread-fast success path
			// (buildReaderPoolLocked:153) — every converge/serial path leaves it
			// nil. So this is the one site that can distinguish a co-read pin (all K
			// readers latched to the anchor's wal2 frame) from a converge-upward pin
			// (K independent BEGINs ratcheted to the same head). Record + log it here.
			outcome, via := poolBindConverge, "converge"
			if cr != nil {
				outcome, via = poolBindCoread, "coread"
			}
			metrics.recordReaderPoolBind(outcome, attempt+1)
			fmt.Fprintf(os.Stderr,
				"[GO-IVM][POOL] cg=%s cold-hydrate pin via %s: readers=%d frame=%s attempt=%d/%d\n",
				cgID, via, s.hydrateReaders, pool.Version(), attempt+1, maxReaderPoolConvergeAttempts)
			return // pool + curr aligned — parallel hydrate ready
		}

		// Misaligned: replicator advanced between curr refresh and pool build.
		// Close pool and retry — next iteration re-pins curr at the new head.
		poolVer := pool.Version()
		pool.Close()
		if cr != nil {
			cr.Free()
		}
		lastErr = fmt.Errorf("pool at %q, curr at %q (attempt %d/%d)",
			poolVer, cur.Version(), attempt+1, maxReaderPoolConvergeAttempts)
	}
	metrics.recordReaderPoolBind(poolBindSerial, maxReaderPoolConvergeAttempts)
	fmt.Fprintf(os.Stderr,
		"[GO-IVM] hydrate reader pool not built after %d converge attempts (serial fallback): %v\n",
		maxReaderPoolConvergeAttempts, lastErr)
}

func (s *Server) handleAdvanceToHead(req RPCRequest) RPCResponse {
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
		return rpcError(req.ID, -32000,
			"advanceToHead unavailable (snapshotter not armed; needs GO_IVM_ADVANCE_TO_HEAD=true + table mode)")
	}
	if resp, stale := checkInitEpoch(group, req.ID, p.InitEpoch); stale {
		return resp
	}

	// The cold-start hydrate window ends at the first advance: curr is about to
	// rotate off the pinned frame, so the reader pool's frame goes stale. Drop
	// it (reads revert to the single bound-conn path).
	s.tearDownReaderPool(group)

	diff, err := group.snap.Advance(group.snapSpecs, group.snapAllNames)
	if err != nil {
		return rpcError(req.ID, -32000, "advanceToHead advance: "+err.Error())
	}
	version := diff.Curr().Version()
	drive := s.advanceDriveEnabled

	// After the leapfrog, the sources (sticky-bound to the old curr) ARE bound
	// to diff.Prev() — the exact frame the diff was derived against. Make it
	// explicit, and arrange to rebind to the new curr on every exit so the next
	// hydrate/fetch reads head.
	if drive {
		group.eng.BindTableSourcesToConn(diff.Prev().Conn())
	}
	rebindCurr := func() {
		if drive {
			group.eng.BindTableSourcesToConn(diff.Curr().Conn())
		}
	}

	// Memory guard: diff.Collect materializes the FULL catch-up diff —
	// every change-log entry WITH row values — before the engine applies
	// it. TS iterates its diff lazily, so Go is strictly more exposed: a
	// bulk backfill against a behind CG can be 100k+ fat rows = GBs,
	// multiplied by CGs advancing concurrently. Refuse oversized diffs and
	// surface an error instead — the TS side's documented handling for an
	// advanceToHead error is its own fallback/reset path, which re-hydrates
	// with bounded memory.
	if diff.Changes > maxDiffChanges {
		rebindCurr()
		return rpcError(req.ID, -32000, fmt.Sprintf(
			"advanceToHead diff: %d changes exceeds GO_IVM_MAX_DIFF_CHANGES=%d — "+
				"caller should reset/re-hydrate instead of replaying this diff",
			diff.Changes, maxDiffChanges))
	}

	changes, err := diff.Collect()
	if err != nil {
		rebindCurr()
		// A reset/truncate/permissions-change is a normal outcome: report it so
		// the caller re-hydrates at `version`.
		if rs, ok := snapshotter.IsReset(err); ok {
			return RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: advanceToHeadResult{
				Version: version,
				Reset:   &resetWire{Reason: rs.Reason, Msg: rs.Msg},
			}}
		}
		// InvalidDiffError or a hard error (unknown table / SQL failure):
		// surface to TS, which falls back to its own advance path.
		return rpcError(req.ID, -32000, "advanceToHead diff: "+err.Error())
	}

	// P2 drive mode: apply Go's own derived diff to Go's engine (frame-
	// coordinated against diff.Prev()) and return the resulting RowChanges +
	// version — a fully self-consistent advance with no TS-shipped diff.
	if drive {
		snapChanges := make([]engine.SnapshotChange, len(changes))
		for i, c := range changes {
			snapChanges[i] = engine.SnapshotChange{
				Table:      c.Table,
				PrevValues: c.PrevValues,
				NextValue:  c.NextValue,
			}
		}
		result := group.eng.Advance(snapChanges)
		rebindCurr()
		if result.Drift != nil {
			fmt.Fprintf(os.Stderr, "[GO-IVM][drift] advanceToHead(drive) cg=%s %s\n",
				cgID, result.Drift.Error())
			return RPCResponse{JSONRPC: "2.0", ID: req.ID, Error: &RPCError{
				Code:    rpcCodeDrift,
				Message: result.Drift.Error(),
				Data: driftRPCData{
					Table:          result.Drift.Table,
					Op:             result.Drift.Op,
					PK:             result.Drift.PK,
					HasCount:       result.Drift.HasCount,
					PartialChanges: result.Changes,
					PartialTimings: result.Timings,
				},
			}}
		}
		return RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: advanceToHeadResult{
			Version:    version,
			NumChanges: diff.Changes,
			RowChanges: result.Changes,
			Timings:    result.Timings,
		}}
	}

	// P1 pure derivation: return the derived diff (no engine mutation).
	wire := make([]snapshotChangeWire, len(changes))
	for i, c := range changes {
		wire[i] = snapshotChangeWire{
			Table:      c.Table,
			PrevValues: c.PrevValues,
			NextValue:  c.NextValue,
			RowKey:     c.RowKey,
		}
	}
	return RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: advanceToHeadResult{
		Changes:    wire,
		Version:    version,
		NumChanges: diff.Changes,
	}}
}

// advanceToHeadStreamPartial is the on-wire partial frame for
// advanceToHeadStream. It mirrors advanceStreamPartial (chunked RowChanges +
// chunkIndex + final + per-final timings/drift) and adds the advanceToHead-
// specific Version + NumChanges, which the engine's AdvanceStream doesn't know
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
	Drift      *ivm.DriftError      `json:"drift,omitempty"`
	// Final-frame-only metadata (omitted on non-final partials):
	Version    string     `json:"version,omitempty"`
	NumChanges int        `json:"numChanges,omitempty"`
	Reset      *resetWire `json:"reset,omitempty"`
}

// handleAdvanceToHeadStream is the streaming variant of handleAdvanceToHead for
// DRIVE mode (P2 / Go-primary trigger). It derives Go's own diff exactly like
// handleAdvanceToHead, then applies it to the engine via Engine.AdvanceStream —
// emitting the resulting RowChanges as chunked partial frames instead of one
// 64MB-capped msgpack frame. This removes the single-frame cap that a bulk
// backfill / mass UPDATE would otherwise blow on the deployed Go-primary path
// (finding F5): a frame over the cap is SKIPPED by the TS receive loop,
// orphaning the RPC until it times out.
//
// Drive-mode only: the streamed payload is the engine's RowChanges, which exist
// only when driving. In non-drive (P1 derive-only) mode there are no RowChanges
// to stream, so the handler errors loudly rather than silently dropping the
// derived diff — the derive-only shadow path uses the non-streaming
// advanceToHead.
//
// Wire shape (mirrors advanceStream): one OR MORE partial frames with the same
// id in monotonic chunkIndex order, exactly one Final=true (the last). Only the
// Final frame carries Timings, Drift, Version, NumChanges. After the Final
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
		return rpcError(req.ID, -32000,
			"advanceToHeadStream unavailable (snapshotter not armed; needs GO_IVM_ADVANCE_TO_HEAD=true + table mode)")
	}
	if !s.advanceDriveEnabled {
		// The streamed payload carries engine RowChanges, which only exist in
		// drive mode. Refuse rather than silently drop the derived diff — the
		// derive-only shadow path uses the non-streaming advanceToHead.
		return rpcError(req.ID, -32000,
			"advanceToHeadStream requires drive mode (GO_IVM_ADVANCE_DRIVE=true)")
	}
	if resp, stale := checkInitEpoch(group, req.ID, p.InitEpoch); stale {
		return resp
	}

	// First advance ends the cold-start hydrate window; drop the reader pool
	// before curr rotates off its pinned frame.
	s.tearDownReaderPool(group)

	diff, err := group.snap.Advance(group.snapSpecs, group.snapAllNames)
	if err != nil {
		return rpcError(req.ID, -32000, "advanceToHeadStream advance: "+err.Error())
	}
	version := diff.Curr().Version()

	// After the leapfrog the sources (sticky-bound to the old curr) ARE bound to
	// diff.Prev(). Make it explicit for the apply, and rebind to the new curr on
	// every exit so the next hydrate/fetch reads head. (Same discipline as
	// handleAdvanceToHead's drive branch.)
	group.eng.BindTableSourcesToConn(diff.Prev().Conn())
	rebindCurr := func() {
		group.eng.BindTableSourcesToConn(diff.Curr().Conn())
	}

	// Memory guard — same rationale as handleAdvanceToHead: Collect
	// materializes the full diff with row values; refuse oversized diffs so
	// the caller resets/re-hydrates with bounded memory instead.
	if diff.Changes > maxDiffChanges {
		rebindCurr()
		return rpcError(req.ID, -32000, fmt.Sprintf(
			"advanceToHeadStream diff: %d changes exceeds GO_IVM_MAX_DIFF_CHANGES=%d — "+
				"caller should reset/re-hydrate instead of replaying this diff",
			diff.Changes, maxDiffChanges))
	}

	changes, err := diff.Collect()
	if err != nil {
		rebindCurr()
		// A reset/truncate/permissions-change is a normal outcome: emit a single
		// Final frame carrying the reset + version so the TS accumulator yields
		// AdvanceToHeadResult{reset, version} and the caller re-hydrates at
		// version. There are no RowChanges in this case.
		if rs, ok := snapshotter.IsReset(err); ok {
			streamW(req.ID, advanceToHeadStreamPartial{
				ChunkIndex: 0,
				Final:      true,
				Version:    version,
				Reset:      &resetWire{Reason: rs.Reason, Msg: rs.Msg},
			})
			return RPCResponse{JSONRPC: "2.0", Result: "done", ID: req.ID}
		}
		// InvalidDiffError or a hard error (unknown table / SQL failure): surface
		// to TS, which falls back to its own advance path.
		return rpcError(req.ID, -32000, "advanceToHeadStream diff: "+err.Error())
	}

	snapChanges := make([]engine.SnapshotChange, len(changes))
	for i, c := range changes {
		snapChanges[i] = engine.SnapshotChange{
			Table:      c.Table,
			PrevValues: c.PrevValues,
			NextValue:  c.NextValue,
		}
	}

	// Apply Go's own derived diff to Go's engine, frame-coordinated against
	// diff.Prev(), streaming the resulting RowChanges in advanceChunkSize-sized
	// frames. Version + NumChanges ride the Final frame only. Drift (if any) is
	// carried on the Final frame by AdvanceStream itself (it recovers the drift
	// panic and returns nil) — the TS accumulator re-throws it as a DriftError.
	numChanges := diff.Changes
	streamErr := group.eng.AdvanceStream(snapChanges, func(r engine.AdvanceStreamPartial) {
		pc := toPositional(r.Changes)
		part := advanceToHeadStreamPartial{
			Dict:       pc.Dict,
			Rows:       pc.Rows,
			ChunkIndex: r.ChunkIndex,
			Final:      r.Final,
			Timings:    r.Timings,
			Drift:      r.Drift,
		}
		if r.Final {
			part.Version = version
			part.NumChanges = numChanges
			// One advanceToHeadStream call → one record on the terminal frame;
			// Final's ChunkIndex+1 is the total chunk count for this call.
			metrics.recordAdvanceChunks(r.ChunkIndex + 1)
		}
		streamW(req.ID, part)
		if r.Drift != nil {
			fmt.Fprintf(os.Stderr, "[GO-IVM][drift] advanceToHeadStream(drive) cg=%s %s\n",
				cgID, r.Drift.Error())
		}
	})
	rebindCurr()
	if streamErr != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] advanceToHeadStream ERROR cg=%s: %v\n", cgID, streamErr)
		return rpcError(req.ID, -32000, "advanceToHeadStream: "+streamErr.Error())
	}

	// "done" sentinel — TS client uses this to resolve the call promise.
	return RPCResponse{JSONRPC: "2.0", Result: "done", ID: req.ID}
}
