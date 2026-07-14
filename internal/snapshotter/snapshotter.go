// Package snapshotter is a line-by-line Go port of TS's Snapshotter
// (mono/packages/zero-cache/src/services/view-syncer/snapshotter.ts).
//
// The Replicator is the sole writer to the wal2 replica file. A ViewSyncer
// never commits; instead it holds TWO BEGIN CONCURRENT snapshots on the same
// file and "leapfrogs" them to replay the timeline in isolation:
//
//		Replicator:  t1 --------------> t2 --------------> t3 -------------->
//		ViewSyncer:       [snapshot_a] ----> [snapshot_b] ----> [snapshot_c]
//		                    (conn_1)           (conn_2)           (conn_1)   ← reused
//
//	  - curr: the snapshot IVM is currently consistent at.
//	  - prev: the previous snapshot, re-pinned at head and reused as the next curr.
//
// The diff between two snapshots is derived NOT from a racing "head" pointer
// but from the version-stamped, append-only `_zero.changeLog2` table (see
// diff.go). Because the log is version-addressable, "changes in (prev, curr]"
// is deterministic regardless of who reads or when — which is exactly what
// makes Go self-consistent (zero frame-timing drift) and is the same diff for
// shadow and Go-primary modes (the mode only selects which output the
// view-syncer commits; the Diff itself is identical).
//
// This package reuses tablesource's connection / BEGIN CONCURRENT plumbing
// model. It depends only on `ivm` and `sqlite` (not `engine`) to keep the
// import graph acyclic — engine consumes this package, not the reverse.
package snapshotter

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

const (
	// zeroVersionColumn is TS's ZERO_VERSION_COLUMN_NAME (`_0_version`).
	// Every synced replica row carries it; the diff-validity checks read it.
	zeroVersionColumn = "_0_version"
)

// Snapshotter manages the progression of database snapshots for one CG's
// pipelines. Mirrors snapshotter.ts class Snapshotter.
type Snapshotter struct {
	// db is the writable pool (tablesource.OpenWritable) on the replica file.
	// Each Snapshot acquires one dedicated *sql.Conn from it and holds a
	// BEGIN CONCURRENT tx for the life of the snapshot. Writes never commit.
	db    *sql.DB
	appID string

	// mu serializes Init/Advance/Destroy against any in-flight Diff
	// iteration. TS relies on the single-threaded JS event loop; Go must
	// serialize advance vs. iteration explicitly.
	mu sync.Mutex

	curr *Snapshot
	prev *Snapshot

	// destroyed is set by Destroy. Callers that can race teardown
	// (e.g. a fire-and-forget reset re-read) check Destroyed() to bail
	// early instead of reading a closed connection (L13, mirrors TS
	// snapshotter.ts #destroyed).
	destroyed bool

	// beginStmt is the BEGIN variant that worked the first time
	// ("BEGIN CONCURRENT" with rocicorp's wal2 patch, else plain "BEGIN").
	// Cached so resetToHead doesn't retry CONCURRENT each cycle on builds
	// that lack the patch. Mirrors tablesource.Source.beginStmt.
	beginStmt string
}

// New constructs an (uninitialized) Snapshotter. Call Init before Advance.
// db must be a writable pool aimed at the wal2 replica file
// (tablesource.OpenWritable). appID derives the permissions table name.
func New(db *sql.DB, appID string) (*Snapshotter, error) {
	if db == nil {
		return nil, fmt.Errorf("snapshotter.New: db is nil")
	}
	return &Snapshotter{db: db, appID: appID}, nil
}

// Init pins the initial snapshot at the current head. Must be called exactly
// once. Mirrors snapshotter.ts init() (116).
func (s *Snapshotter) Init() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curr != nil {
		return fmt.Errorf("snapshotter: already initialized")
	}
	snap, err := s.newSnapshot()
	if err != nil {
		return err
	}
	s.curr = snap
	return nil
}

// Initialized reports whether Init has run. Mirrors initialized() (128).
func (s *Snapshotter) Initialized() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.curr != nil
}

// Destroyed reports whether Destroy has been called. Mirrors TS
// snapshotter.ts get destroyed() (146-148). Callers that can race
// teardown check this to distinguish "snapshot is gone" from "snapshot
// is live but empty" (L13).
func (s *Snapshotter) Destroyed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.destroyed
}

// Current returns the current snapshot. Errors if not initialized.
// Mirrors current() (133).
func (s *Snapshotter) Current() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.destroyed {
		return nil, fmt.Errorf("snapshotter: destroyed")
	}
	if s.curr == nil {
		return nil, fmt.Errorf("snapshotter: not initialized")
	}
	return s.curr, nil
}

// Advance advances to head and returns a lazy Diff between the previously
// current snapshot and a new snapshot at head. Mirrors advance() (175).
//
// syncable maps table name → its spec (the tables Go serves). allNames is the
// full set of replicated table names (used to distinguish "non-syncable, skip"
// from "unknown table, error"). The returned Diff is only valid until the next
// Advance (the prev connection is reused).
//
// FAILURE-ATOMIC: the prev/curr swap commits only after BOTH the new head pin
// and the diff construction succeed. On any error s.curr — the snapshot the
// engine's sources are bound to and whose version matches the engine's applied
// content — is untouched, so the caller may retry Advance in place. Without
// this ordering a newDiff failure (e.g. a transient changelog read error)
// stranded the swap: the next Advance diffed (old-curr → head] while the
// engine still held old-prev content, silently skipping the
// (old-prev → old-curr] window — drift. TS never needed the guarantee (its
// snapshotter.advance is infallible in practice and a failed advance resets
// the world); Go surfaces the retry contract via rpcCodeAdvanceCleanRetryable.
func (s *Snapshotter) Advance(
	syncable map[string]*TableSpec,
	allNames map[string]bool,
) (*Diff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.destroyed {
		return nil, fmt.Errorf("snapshotter: destroyed")
	}
	if s.curr == nil {
		return nil, fmt.Errorf("snapshotter: not initialized")
	}
	// Prepare the head pin WITHOUT touching s.prev/s.curr (leapfrog core,
	// snapshotter.ts:183-196). Connection reuse is load-bearing — avoids a
	// per-advance conn + statement-prep cost (snapshotter.ts:77-89). Note
	// resetToHead re-pins the reused prev conn even if we later fail — safe:
	// a Diff is only valid until the next Advance, so nothing reads prev's
	// old frame after this point, and a retry simply re-pins again.
	var next *Snapshot
	freshConn := s.prev == nil
	if !freshConn {
		if err := s.prev.resetToHead(s.beginStmt); err != nil {
			return nil, err
		}
		next = s.prev
	} else {
		// First advance: no prev yet, so open a fresh second connection.
		var err error
		next, err = s.newSnapshot()
		if err != nil {
			return nil, err
		}
	}
	// Build the diff BEFORE committing the swap: its NumChangesSince read is
	// the only remaining fallible step.
	diff, err := newDiff(s.appID, syncable, allNames, s.curr, next)
	if err != nil {
		if freshConn {
			next.close() // don't leak the just-acquired pool conn
		}
		return nil, err
	}
	s.prev = s.curr
	s.curr = next
	return diff, nil
}

// RefreshCurrentToHead re-pins the current snapshot at the latest replica head
// on its existing connection. Drive mode calls this immediately before the
// FIRST hydrate: Init() pins curr at handleInit time, but the replicator may
// have advanced by the time the initial addQueries arrives, so without this the
// hydrate reads a frame slightly behind the one TS hydrates at (observed as rare
// "Go produced 0" shadow misses for a query whose matching rows landed in that
// window). Because curr carries no IVM writes (only prev does), the rollback is
// a clean re-pin. The connection object is unchanged, so any sources already
// bound to curr.Conn() automatically observe the new frame.
//
// ONLY safe when no pipeline depends on curr yet (initial hydrate): re-pinning
// shifts curr's version, which would desync already-hydrated pipelines.
func (s *Snapshotter) RefreshCurrentToHead() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.destroyed {
		return fmt.Errorf("snapshotter: destroyed")
	}
	if s.curr == nil {
		return fmt.Errorf("snapshotter: not initialized")
	}
	return s.curr.resetToHead(s.beginStmt)
}

// Destroy closes both snapshot connections. Mirrors destroy() (202).
// Sets destroyed so concurrent readers can bail early (L13).
func (s *Snapshotter) Destroy() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.destroyed = true
	if s.curr != nil {
		s.curr.close()
		s.curr = nil
	}
	if s.prev != nil {
		s.prev.close()
		s.prev = nil
	}
}

// newSnapshot opens a fresh connection and pins it. Mirrors
// Snapshot.create (276) + the Snapshot constructor (306).
func (s *Snapshotter) newSnapshot() (*Snapshot, error) {
	acquireCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := s.db.Conn(acquireCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("snapshotter: acquire conn (30s timeout): %w", err)
	}
	snap := &Snapshot{conn: conn}
	if err := s.beginAndPin(snap); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return snap, nil
}

// beginAndPin runs BEGIN [CONCURRENT] then a read of the replication state.
//
// The READ is mandatory and load-bearing: BEGIN CONCURRENT alone does NOT
// acquire the read lock (snapshotter.ts:308-311). The read of
// `_zero.replicationState` is what logically creates the snapshot AND yields
// its version. Skipping it would let a concurrent replicator commit slip into
// the frame (fidelity invariant §3.1.8).
func (s *Snapshotter) beginAndPin(snap *Snapshot) error {
	ctx := context.Background()
	if s.beginStmt == "" {
		if _, err := snap.conn.ExecContext(ctx, "BEGIN CONCURRENT"); err == nil {
			s.beginStmt = "BEGIN CONCURRENT"
		} else if _, err2 := snap.conn.ExecContext(ctx, "BEGIN"); err2 == nil {
			s.beginStmt = "BEGIN"
		} else {
			return fmt.Errorf("snapshotter: BEGIN: %w", err2)
		}
	} else {
		if _, err := snap.conn.ExecContext(ctx, s.beginStmt); err != nil {
			return fmt.Errorf("snapshotter: %s: %w", s.beginStmt, err)
		}
	}
	version, err := selectStateVersion(ctx, snap.conn)
	if err != nil {
		return err
	}
	snap.version = version
	return nil
}

// Snapshot is a single pinned frame plus its stateVersion. Mirrors the TS
// Snapshot class (275-396). It owns ONE *sql.Conn holding an open BEGIN
// [CONCURRENT] read tx. The stmt cache eliminates sqlite3_prepare_v2
// overhead (10% of advance CPU in pprof) by reusing prepared statements
// across GetRow/GetRows calls — matching TS's better-sqlite3 which caches
// prepared statements natively.
type Snapshot struct {
	conn    *sql.Conn
	stmts   map[string]*snapshotStmt
	version string
}

// Version returns the stateVersion this frame is pinned at.
func (s *Snapshot) Version() string { return s.version }

// Conn returns the connection holding this snapshot's pinned BEGIN CONCURRENT
// frame. Frame coordination binds the engine's tablesource leaves to this
// conn so a Snapshotter-derived diff is applied into the exact frame it was
// derived against. The caller must not close it (the Snapshotter owns it).
func (s *Snapshot) Conn() *sql.Conn { return s.conn }

// resetToHead ends this snapshot's tx and re-pins the same connection at the
// new head. Mirrors resetToHead() (392-395). The Snapshot object is mutated in
// place (same conn, new version) and returned to the caller as the next curr.
func (s *Snapshot) resetToHead(beginStmt string) error {
	ctx := context.Background()
	if _, err := s.conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return fmt.Errorf("snapshotter: resetToHead ROLLBACK: %w", err)
	}
	if _, err := s.conn.ExecContext(ctx, beginStmt); err != nil {
		return fmt.Errorf("snapshotter: resetToHead %s: %w", beginStmt, err)
	}
	version, err := selectStateVersion(ctx, s.conn)
	if err != nil {
		return err
	}
	s.version = version
	return nil
}

// close rolls back the open tx and releases the connection. Idempotent:
// safe to call once per Snapshot.
func (s *Snapshot) close() {
	if s.conn != nil {
		ctx := context.Background()
		_, _ = s.conn.ExecContext(ctx, "ROLLBACK")
		s.finalizeStmts()
		_ = s.conn.Close()
		s.conn = nil
	}
}

// selectStateVersion reads `_zero.replicationState.stateVersion` — TS's
// getReplicationState (replication-state.ts). This read is what acquires the
// read lock under BEGIN CONCURRENT.
func selectStateVersion(ctx context.Context, conn *sql.Conn) (string, error) {
	var version string
	err := conn.QueryRowContext(ctx,
		`SELECT stateVersion FROM "_zero.replicationState"`).Scan(&version)
	if err != nil {
		return "", fmt.Errorf("snapshotter: read replicationState: %w", err)
	}
	return version, nil
}
