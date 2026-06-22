package tablesource

import (
	"context"
	"database/sql"
	"fmt"
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

// maxPinAttempts bounds the re-pin retries when a reader's BEGIN lands on a
// newer frame than the target (the replicator committed between the target pin
// and this reader's BEGIN). Under a quiet replica the first attempt matches;
// once the replica has advanced PAST the target version a fresh BEGIN can never
// reach it (you cannot rewind to a past frame without snapshot_open), so the
// pool build fails and the caller falls back to the single-conn serial path.
const maxPinAttempts = 5

// poolReader is one frame-pinned read connection plus its own prepared-statement
// cache. It is borrowed exclusively (one goroutine at a time) via ReaderPool,
// so it needs no internal locking — the per-reader stmt cache replaces the
// s.mu-guarded Source.stmtCache used on the single-conn path.
type poolReader struct {
	conn  *sql.Conn
	stmts map[string]*sql.Stmt
}

func (r *poolReader) prepared(ctx context.Context, query string) (*sql.Stmt, error) {
	if st, ok := r.stmts[query]; ok {
		return st, nil
	}
	st, err := r.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	r.stmts[query] = st
	return st, nil
}

func (r *poolReader) close(ctx context.Context) {
	for _, st := range r.stmts {
		_ = st.Close()
	}
	_, _ = r.conn.ExecContext(ctx, "ROLLBACK")
	_ = r.conn.Close()
}

// ReaderPool is a set of K read connections ALL pinned to the same WAL frame —
// the frame whose stateVersion == Version(). It is the wal2-viable replacement
// for the dead sqlite3_snapshot_open shared-handle pool: instead of a C handle,
// every connection independently BEGINs and is validated (re-pinned if needed)
// to the one target stateVersion.
//
// Cold-start hydrate routes its per-query fetches across the pool so they run
// in parallel while staying on one consistent frame. Borrow with acquire,
// return with release; each borrow is exclusive, so the SQL read runs lock-free.
// Because hydrate fetch is eager (read all rows, then return), a single goroutine
// holds at most one reader at a time, so pool size = #concurrent hydrate
// goroutines is deadlock-free.
type ReaderPool struct {
	free    chan *poolReader
	all     []*poolReader
	version string
}

// NewReaderPool opens k read connections from db, each pinned (via BEGIN + a
// validating stateVersion read) to wantVersion. It fails if any reader cannot be
// pinned to wantVersion within maxPinAttempts (the replica advanced past it) —
// the caller should then keep the single-conn path. db must be a WAL-family read
// pool (e.g. tablesource.Open); wantVersion is the Snapshotter's current frame
// version so the pool reads exactly what the single bound-conn path would.
func NewReaderPool(ctx context.Context, db *sql.DB, wantVersion string, k int) (*ReaderPool, error) {
	if db == nil {
		return nil, fmt.Errorf("tablesource.NewReaderPool: db is nil")
	}
	if wantVersion == "" {
		return nil, fmt.Errorf("tablesource.NewReaderPool: wantVersion is empty")
	}
	if k < 1 {
		k = 1
	}
	p := &ReaderPool{free: make(chan *poolReader, k), version: wantVersion}
	for i := 0; i < k; i++ {
		r, err := openPinnedReader(ctx, db, wantVersion)
		if err != nil {
			p.Close()
			return nil, err
		}
		p.all = append(p.all, r)
		p.free <- r
	}
	return p, nil
}

func openPinnedReader(ctx context.Context, db *sql.DB, wantVersion string) (*poolReader, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reader pin: acquire conn: %w", err)
	}
	var lastVer string
	for attempt := 0; attempt < maxPinAttempts; attempt++ {
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("reader pin: BEGIN: %w", err)
		}
		var ver string
		if err := conn.QueryRowContext(ctx, stateVersionSQL).Scan(&ver); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("reader pin: read stateVersion: %w", err)
		}
		if ver == wantVersion {
			return &poolReader{conn: conn, stmts: map[string]*sql.Stmt{}}, nil
		}
		lastVer = ver
		if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("reader pin: ROLLBACK: %w", err)
		}
	}
	_ = conn.Close()
	return nil, fmt.Errorf("reader pin: want stateVersion %q, replica at %q after %d attempts",
		wantVersion, lastVer, maxPinAttempts)
}

// acquire borrows an exclusive frame-pinned reader, blocking until one frees or
// ctx is cancelled.
func (p *ReaderPool) acquire(ctx context.Context) (*poolReader, error) {
	select {
	case r := <-p.free:
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// release returns a reader to the pool.
func (p *ReaderPool) release(r *poolReader) {
	p.free <- r
}

// Version is the stateVersion every reader in the pool is pinned at.
func (p *ReaderPool) Version() string { return p.version }

// Close rolls back and closes every reader. Safe to call on a partially-built
// pool (NewReaderPool calls it on the error path).
func (p *ReaderPool) Close() {
	ctx := context.Background()
	for _, r := range p.all {
		r.close(ctx)
	}
	p.all = nil
}
