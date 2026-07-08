package tablesource

// reader_cache.go — the worker-wide reader-shell cache (2026-07-09).
//
// Under Option B every pool build was K fresh raw SQLite opens and every
// teardown K closes — pure conn provisioning (sqlite3_open + schema parse +
// shm map + fds) paid on every CG lifecycle event. That cost is why the
// 7addd28 build-slot gate existed (≤2 concurrent builds), and the gate's
// skip-to-serial is why warm pin-rate read 80% in churn windows: the product
// (parallel hydrate) silently not happening at exactly the moment it
// mattered. This cache deletes the COST so the gate — and its routine-serial
// side effect — can be deleted with it.
//
// What is cached: idle poolReader SHELLS (raw driver.Conn + its per-reader
// prepared-stmt cache), keyed per replica *sql.DB (same registry pattern as
// readPoolDSNs / probedTables). A cached shell holds NO transaction —
// ReaderPool.Close ROLLBACKs before returning it — so it pins no WAL frame,
// fights no checkpoint, and satisfies the coread arm's TXN_NONE
// precondition. LIFO: the most recently returned shell is reused first,
// keeping its prepared-stmt cache warmest (compiled sqlite3 bytecode
// survives tx boundaries and pool generations; SQLITE_SCHEMA reprepare
// covers rare DDL).
//
// What is NOT saved: pager page-cache warmth across FRAME changes. The
// coread arm forces a pager reset (`*pChanged = 1` — "its page cache is
// stale for this snapshot", sqlite3.c:71343-71346) and a converge BEGIN on
// a moved header does the same — so budget the cache's standing footprint
// as cap × stmt-cache C-heap (bounded by stmtCachePerConnCap), NOT page
// cache.
//
// Frame-safety across generations (the linchpin, verified in the fork):
// sqlite3_wal2_coread_open is arm→begin→DISARM atomic inside the call
// (sqlite3.c:193001-193008) — pWal->pCoRead never survives it, so a shell
// from a torn-down coread pool carries no dangling arm; the next BEGIN
// (converge) or arm (coread) pins whatever frame ITS anchor dictates.
// Pinned at the Go level by the gen-crossing tests in
// reader_cache_test.go / coread_test.go.
//
// Self-healing: a cached shell that fails its next BEGIN/arm/read (conn
// died while idle; a leaked tx failing BEGIN closed) is discarded and
// replaced with ONE fresh open — see provisionReader.
//
// Admission: only ReaderPool.Close's HEALTHY path caches (all readers
// free). Build-failure unwinds and the borrowed-readers BUG path close
// outright — a shell is cached only when provably idle.

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"time"
)

// readerShellCache is one replica's bounded LIFO of idle reader shells.
type readerShellCache struct {
	mu     sync.Mutex
	shells []cachedShell // index len-1 = most recently returned (LIFO top)
	cap    int
}

type cachedShell struct {
	r        *poolReader
	cachedAt time.Time
}

// readerShellCaches maps a replica read pool to its shell cache. Entries
// exist only for dbs that opted in via EnableReaderShellCache (the sidecar's
// getReplicaDB); everything else keeps open/close-per-build behavior.
var readerShellCaches sync.Map // *sql.DB → *readerShellCache

// readerCacheHits / readerCacheMisses count provisioning outcomes across all
// enabled caches (PERF-POOL telemetry). A miss is counted only when a cache
// IS enabled and empty — uncached dbs don't pollute the reuse-rate.
var (
	readerCacheHits   atomic.Int64
	readerCacheMisses atomic.Int64
)

// EnableReaderShellCache turns on shell caching for db with the given
// capacity. capN <= 0 is a no-op (caching stays off). Idempotent per db
// (re-enabling replaces the cap only if no cache exists yet).
func EnableReaderShellCache(db *sql.DB, capN int) {
	if db == nil || capN <= 0 {
		return
	}
	readerShellCaches.LoadOrStore(db, &readerShellCache{cap: capN})
}

// ReaderShellCacheSize reports the number of idle shells cached for db
// (tests + PERF telemetry). 0 when caching is not enabled.
func ReaderShellCacheSize(db *sql.DB) int {
	c := shellCacheFor(db)
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.shells)
}

// ReaderShellCacheCounters reports the cumulative provisioning hit/miss
// counts (PERF-POOL reports per-window deltas).
func ReaderShellCacheCounters() (hits, misses int64) {
	return readerCacheHits.Load(), readerCacheMisses.Load()
}

// SweepReaderShellCache closes cached shells idle longer than maxAge and
// returns how many it closed. maxAge <= 0 closes ALL cached shells. Called
// from the sidecar reaper tick (bounds standing fds + stmt-cache C-heap).
func SweepReaderShellCache(db *sql.DB, maxAge time.Duration) int {
	c := shellCacheFor(db)
	if c == nil {
		return 0
	}
	cutoff := time.Now().Add(-maxAge)
	c.mu.Lock()
	keep := c.shells[:0]
	var drop []*poolReader
	for _, s := range c.shells {
		if maxAge <= 0 || s.cachedAt.Before(cutoff) {
			drop = append(drop, s.r)
		} else {
			keep = append(keep, s)
		}
	}
	c.shells = keep
	c.mu.Unlock()
	for _, r := range drop {
		r.closeConn()
	}
	return len(drop)
}

// CloseReaderShellCache closes every cached shell for db and removes the
// cache from the registry (Server.closeAll — cached raw conns are invisible
// to db.Close and would otherwise outlive it).
func CloseReaderShellCache(db *sql.DB) {
	v, ok := readerShellCaches.LoadAndDelete(db)
	if !ok {
		return
	}
	c := v.(*readerShellCache)
	c.mu.Lock()
	shells := c.shells
	c.shells = nil
	c.mu.Unlock()
	for _, s := range shells {
		s.r.closeConn()
	}
}

func shellCacheFor(db *sql.DB) *readerShellCache {
	if db == nil {
		return nil
	}
	v, ok := readerShellCaches.Load(db)
	if !ok {
		return nil
	}
	return v.(*readerShellCache)
}

// popCachedShell returns the most recently cached shell for db, or nil.
// Counts hits/misses only for cache-enabled dbs.
func popCachedShell(db *sql.DB) *poolReader {
	c := shellCacheFor(db)
	if c == nil {
		return nil
	}
	c.mu.Lock()
	var r *poolReader
	if n := len(c.shells); n > 0 {
		r = c.shells[n-1].r
		c.shells = c.shells[:n-1]
	}
	c.mu.Unlock()
	if r != nil {
		readerCacheHits.Add(1)
	} else {
		readerCacheMisses.Add(1)
	}
	return r
}

// put stores an idle, tx-free shell (LIFO top). A full cache closes the
// overflow shell instead — the bound is the whole point (standing fd +
// stmt-cache C-heap budget).
func (c *readerShellCache) put(r *poolReader) {
	c.mu.Lock()
	if len(c.shells) >= c.cap {
		c.mu.Unlock()
		r.closeConn()
		return
	}
	c.shells = append(c.shells, cachedShell{r: r, cachedAt: time.Now()})
	c.mu.Unlock()
}
