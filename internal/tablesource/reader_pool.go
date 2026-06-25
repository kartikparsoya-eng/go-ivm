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

// maxConvergeAttempts bounds the re-pin retries when readers land on different
// frames (the replicator committed between opening reader N and reader N+1).
// Each retry ROLLBACKs the laggards and re-BEGINs them — a fresh BEGIN always
// lands on the latest (or same) frame, so convergence is guaranteed under a
// quiescent replica and bounded under sustained writes.
const maxConvergeAttempts = 10

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

// NewReaderPool opens k read connections from db, all converged onto the same
// WAL frame. Instead of requiring a pre-determined wantVersion (the old strategy
// that lost the race when the replicator advanced past it), it uses a
// "converge-to-latest" strategy:
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

	readers := make([]*poolReader, k)
	versions := make([]string, k)

	for i := 0; i < k; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			for j := 0; j < i; j++ {
				readers[j].close(ctx)
			}
			return nil, fmt.Errorf("reader pool: acquire conn %d: %w", i, err)
		}
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			_ = conn.Close()
			for j := 0; j < i; j++ {
				readers[j].close(ctx)
			}
			return nil, fmt.Errorf("reader pool: BEGIN conn %d: %w", i, err)
		}
		var ver string
		if err := conn.QueryRowContext(ctx, stateVersionSQL).Scan(&ver); err != nil {
			_ = conn.Close()
			for j := 0; j < i; j++ {
				readers[j].close(ctx)
			}
			return nil, fmt.Errorf("reader pool: read stateVersion conn %d: %w", i, err)
		}
		readers[i] = &poolReader{conn: conn, stmts: map[string]*sql.Stmt{}}
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
				if _, err := r.conn.ExecContext(ctx, "ROLLBACK"); err != nil {
					for _, r2 := range readers {
						r2.close(ctx)
					}
					return nil, fmt.Errorf("reader pool: converge ROLLBACK: %w", err)
				}
				if _, err := r.conn.ExecContext(ctx, "BEGIN"); err != nil {
					for _, r2 := range readers {
						r2.close(ctx)
					}
					return nil, fmt.Errorf("reader pool: converge BEGIN: %w", err)
				}
				var ver string
				if err := r.conn.QueryRowContext(ctx, stateVersionSQL).Scan(&ver); err != nil {
					for _, r2 := range readers {
						r2.close(ctx)
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
			}
			for _, r := range readers {
				p.free <- r
			}
			return p, nil
		}
	}

	for _, r := range readers {
		r.close(ctx)
	}
	return nil, fmt.Errorf("reader pool: could not converge %d readers after %d attempts", k, maxConvergeAttempts)
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
