package tablesource

// TxCache: per-CG-per-advance read-transaction cache.
//
// IVM correctness wants every query in a single CG's advance epoch to see
// the same SQLite snapshot — otherwise IVM operator state diverges from
// what advance() promised. SQLite (WAL mode) gives us this for free if we
// hold one read transaction across the whole epoch: BEGIN snapshots the
// WAL frame, all queries on that tx see exactly that frame, and concurrent
// writes by the TS replicator only become visible after we Commit and
// re-BEGIN.
//
// One *sql.Tx is NOT goroutine-safe, so queries within a single CG
// serialize through it. That's an intentional trade: intra-CG leaf
// concurrency goes away (the engine's intra-CG work is conceptually
// serial during advance anyway), but inter-CG parallelism is fully
// preserved because each CG has its own tx (and its own backing conn
// from the pool).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

// TxCache keeps at most one read transaction per CG, opened against db.
//
// The Acquire / release model serializes concurrent queries on the same
// CG. Acquire blocks if another caller is using the CG's tx; the returned
// release func MUST be called once the query is done, even on error.
type TxCache struct {
	db *sql.DB

	mu      sync.Mutex
	entries map[string]*txEntry
	closed  bool
}

// txEntry is the per-CG record. mu serializes queries on tx; epoch
// identifies the snapshot the tx pinned.
type txEntry struct {
	mu    sync.Mutex
	tx    *sql.Tx
	epoch uint64
}

// NewTxCache wraps db in a tx cache. The caller retains ownership of db
// — Close releases cached txs, not the pool.
func NewTxCache(db *sql.DB) *TxCache {
	return &TxCache{
		db:      db,
		entries: make(map[string]*txEntry),
	}
}

// Acquire returns the cached tx for cgID at epoch and acquires the
// per-CG lock that serializes use of that tx. The returned release func
// MUST be called exactly once when the caller is done with the tx.
//
// If the cached epoch differs from the requested epoch, the old tx is
// committed and a new BEGIN is issued before returning. If no tx is
// cached, one is created.
func (c *TxCache) Acquire(ctx context.Context, cgID string, epoch uint64) (*sql.Tx, func(), error) {
	if cgID == "" {
		return nil, nil, errors.New("tablesource.TxCache.Acquire: empty cgID")
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, errors.New("tablesource.TxCache.Acquire: cache is closed")
	}
	entry, ok := c.entries[cgID]
	if !ok {
		entry = &txEntry{}
		c.entries[cgID] = entry
	}
	c.mu.Unlock()

	// Take the per-CG lock OUTSIDE the cache lock so concurrent acquires
	// on different CGs don't head-of-line block on each other.
	entry.mu.Lock()

	// Roll the tx if the epoch advanced (or if we don't yet have one).
	// On any error we release the per-CG lock before returning so the
	// next caller isn't blocked forever.
	if entry.tx == nil || entry.epoch != epoch {
		if entry.tx != nil {
			// Best-effort commit of the previous snapshot. Failures are
			// non-fatal — we'll get a fresh tx either way and the old
			// one is unreferenced.
			_ = entry.tx.Commit()
			entry.tx = nil
		}
		// BeginTx with read-only=true asks SQLite for a deferred read tx;
		// modernc.org/sqlite honors ReadOnly by issuing BEGIN with no
		// upgrade-to-write path. Combined with query_only(true) on the
		// pool, writes from this conn are impossible.
		tx, err := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			entry.mu.Unlock()
			return nil, nil, fmt.Errorf("tablesource.TxCache.Acquire: begin: %w", err)
		}
		entry.tx = tx
		entry.epoch = epoch
	}

	tx := entry.tx
	release := entry.mu.Unlock
	return tx, release, nil
}

// Drop ends the tx (if any) for cgID and removes the entry. Safe to call
// on a cgID with no entry — returns nil. Useful when the engine evicts a
// CG (sidecar's idle reaper) so we don't pin a SQLite conn for a CG that
// will never query again.
func (c *TxCache) Drop(cgID string) error {
	c.mu.Lock()
	entry, ok := c.entries[cgID]
	if ok {
		delete(c.entries, cgID)
	}
	c.mu.Unlock()
	if !ok {
		return nil
	}

	// Take the per-CG lock to wait for any in-flight query to finish
	// before we close its tx. Without this we'd race with a caller
	// mid-query and trigger "sql: transaction has already been committed
	// or rolled back" on their Scan().
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.tx != nil {
		err := entry.tx.Commit()
		entry.tx = nil
		if err != nil {
			return fmt.Errorf("tablesource.TxCache.Drop %s: %w", cgID, err)
		}
	}
	return nil
}

// Close drops all cached txs. The underlying *sql.DB is NOT closed —
// callers own it. After Close, Acquire returns an error.
func (c *TxCache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	// Snapshot the entries so we can release c.mu before taking per-CG
	// locks (which could be held by an in-flight query).
	entries := make(map[string]*txEntry, len(c.entries))
	for k, v := range c.entries {
		entries[k] = v
	}
	c.entries = nil
	c.mu.Unlock()

	var firstErr error
	for cgID, entry := range entries {
		entry.mu.Lock()
		if entry.tx != nil {
			if err := entry.tx.Commit(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("tablesource.TxCache.Close %s: %w", cgID, err)
			}
			entry.tx = nil
		}
		entry.mu.Unlock()
	}
	return firstErr
}
