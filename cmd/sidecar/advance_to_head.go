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
			s.buildReaderPoolLocked(group, cur.Version())
		}
	}
	return nil
}

// buildReaderPoolLocked builds and binds a CG-shared frame-pinned reader pool
// for the cold-start parallel hydrate, when GO_IVM_HYDRATE_READERS>1. The pool's
// K connections are all pinned at wantVersion (curr's stateVersion), so hydrate
// reads run in parallel while staying byte-identical to the single bound-conn
// path. If the pool can't be built — feature off, replica not ready, or the
// replicator already advanced past wantVersion so K conns can't all pin it —
// we silently keep the single-conn path (correctness over speedup). MUST hold
// group.mu (called from buildSnapshotterLocked / handleInit).
func (s *Server) buildReaderPoolLocked(group *ClientGroup, wantVersion string) {
	if s.hydrateReaders <= 1 || wantVersion == "" {
		return
	}
	rdb, derr := s.getReplicaDB()
	if derr != nil {
		return
	}
	pool, perr := tablesource.NewReaderPool(context.Background(), rdb, wantVersion, s.hydrateReaders)
	if perr != nil {
		// Expected under sustained write load (head moved past wantVersion);
		// not an error — just no parallel hydrate this cold start.
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] hydrate reader pool not built (falling back to serial): %v\n", perr)
		return
	}
	group.readerPool = pool
	group.eng.BindTableSourcesToReaderPool(pool)
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
func (s *Server) refreshSnapForInitialHydrateLocked(group *ClientGroup) {
	if !s.advanceDriveEnabled || group.snap == nil || group.eng == nil {
		return
	}
	if group.eng.PipelineCount() > 0 {
		return
	}
	if err := group.snap.RefreshCurrentToHead(); err != nil {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] refresh curr before initial hydrate failed: %v\n", err)
	}
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
