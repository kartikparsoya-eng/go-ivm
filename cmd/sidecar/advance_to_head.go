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

	"github.com/kartikparsoya-eng/go-ivm/internal/snapshotter"
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
	Changes    []snapshotChangeWire `json:"changes"`
	Version    string               `json:"version"`
	NumChanges int                  `json:"numChanges"`
	// Reset is non-nil when the diff hit a reset/truncate/permissions-change;
	// Changes is then empty and the caller re-hydrates at Version.
	Reset *resetWire `json:"reset,omitempty"`
}

// buildSnapshotterLocked constructs and pins this group's Snapshotter. MUST be
// called with group.mu held (it is, from handleInit). Pins curr at the current
// replica head — the same frame the hydrate reads from.
func (s *Server) buildSnapshotterLocked(group *ClientGroup, p *initParams) error {
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
	return nil
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

	diff, err := group.snap.Advance(group.snapSpecs, group.snapAllNames)
	if err != nil {
		return rpcError(req.ID, -32000, "advanceToHead advance: "+err.Error())
	}
	version := diff.Curr().Version()

	changes, err := diff.Collect()
	if err != nil {
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
