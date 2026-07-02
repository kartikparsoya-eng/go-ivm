package tablesource

// Open: read-side SQLite connection pool for the Go IVM TableSource leaf.
// Mirrors the design-doc constraints — WAL is asserted (NOT switched on by
// us — the TS replicator owns the file), query_only protects against
// accidental writes from this process, and the pool is sized for true
// per-goroutine reader parallelism.
//
// We deliberately do NOT use mode=ro. WAL mode requires the reader process
// to write to the -shm and -wal sidecar files; mode=ro forbids that and
// causes "attempt to write a readonly database" on connect. query_only=ON
// gives equivalent safety at the SQL layer without breaking WAL.
//
// Driver: mattn/go-sqlite3 (CGO). With -tags libsqlite3 at build time,
// links against the system libsqlite3.so, which the deployment makes
// rocicorp's patched build (the same library zero-cache writes the
// replica with — required to read the wal2 journal_mode it produces).
// Without the tag, mattn falls back to its bundled upstream SQLite,
// which is fine for tests that produce plain-WAL files.

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Defaults chosen to match the design doc (parallelization is a hard
// constraint, not a tuning knob).
const (
	// Per-conn busy timeout. Long enough to ride out a WAL checkpoint
	// stall by the writer, short enough to surface a wedge promptly.
	defaultBusyTimeoutMs = 5000

	// Pool ceiling. Sized so a reconnect-flood burst doesn't starve:
	// each CG holds ~7 Sources × 1 dedicated prevConn for the lifetime
	// of the CG. With 20+ concurrent CGs during churn, 64 is too low
	// (causes indefinite Conn() blocking → 120s RPC timeout on the TS
	// side). 256 supports ~36 concurrent CGs comfortably.
	defaultMaxOpenConns = 256
	defaultMaxIdleConns = 32

	// How long a conn may sit in the pool's idle list before database/sql's
	// cleaner closes it. THE memory-release valve for the replica pools:
	// every open SQLite conn pins its page cache (CacheSizeKB of C-side
	// malloc — invisible to the Go heap, GOMEMLIMIT, and pprof) plus fds
	// and lookaside. Without an idle deadline, a churn burst that fans the
	// pool out to MaxOpenConns parks up to MaxIdleConns of them FOREVER —
	// RSS ratchets up run after run and never comes back (the ART chaos
	// finding: ~500MB growth in 147s, Go heap flat, heap diff empty).
	// Checked-out conns (Source prevConns, snapshotter frame conns, reader
	// pools) are never touched — database/sql only reaps the idle list.
	defaultConnMaxIdle = 90 * time.Second
)

// OpenOptions configures the read-side pool. Zero-valued fields fall back
// to package defaults so callers can pass an empty struct.
type OpenOptions struct {
	// MaxOpenConns caps the pool. 0 → defaultMaxOpenConns.
	MaxOpenConns int
	// MaxIdleConns caps idle conns kept ready. 0 → defaultMaxIdleConns.
	MaxIdleConns int
	// BusyTimeoutMs is the per-conn SQLite busy timeout. 0 → defaultBusyTimeoutMs.
	BusyTimeoutMs int
	// CacheSizeKB sets each connection's SQLite page-cache budget
	// (PRAGMA cache_size, negative-KB form). 0 → SQLite default (~2MB).
	// Matters at scale: the per-conn cache is C-side malloc — OUTSIDE
	// GOMEMLIMIT's view — and total C-side memory is conns × cache, so at
	// MaxOpenConns=1024 the default costs up to ~2GB per pool. Hot pages
	// are also in the (shared, evictable, file-backed) OS page cache, so
	// shrinking the per-conn cache trades a little repeat-read locality
	// for a hard cap on invisible memory.
	CacheSizeKB int
	// ConnMaxIdle is how long an idle pooled conn survives before the
	// pool cleaner closes it (releasing its fd + page cache — see
	// defaultConnMaxIdle). 0 → defaultConnMaxIdle; negative → no idle
	// deadline (pre-2026-07 behavior, conns park forever).
	ConnMaxIdle time.Duration
}

// Open returns a *sql.DB pool aimed at the SQLite file at path, configured
// for safe read-only access alongside a TS-side writer. It verifies the
// database is in WAL mode and returns an error otherwise — read concurrency
// is the whole point and silently falling back to journal-mode locking
// would defeat the port.
func Open(path string, opts OpenOptions) (*sql.DB, error) {
	if path == "" {
		return nil, fmt.Errorf("tablesource.Open: path is required")
	}
	busyMs := opts.BusyTimeoutMs
	if busyMs <= 0 {
		busyMs = defaultBusyTimeoutMs
	}
	maxOpen := opts.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = defaultMaxOpenConns
	}
	maxIdle := opts.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = defaultMaxIdleConns
	}

	// mattn/go-sqlite3 honors SQLite URI form when DSN starts with
	// "file:". Per-conn pragmas use direct query params (not the
	// _pragma=KEY(VAL) form modernc used) so every new connection the
	// pool opens applies them — multi-conn parallelism keeps the
	// pragma guarantees on each backing connection.
	dsn := "file:" + path +
		"?_busy_timeout=" + strconv.Itoa(busyMs) +
		"&_query_only=true"
	if opts.CacheSizeKB > 0 {
		// Negative value = KB units (https://sqlite.org/pragma.html#pragma_cache_size).
		dsn += "&_cache_size=-" + strconv.Itoa(opts.CacheSizeKB)
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("tablesource.Open: sql.Open: %w", err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	applyConnMaxIdle(db, opts.ConnMaxIdle)

	if err := assertWAL(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// applyConnMaxIdle applies the idle-conn deadline policy (see
// OpenOptions.ConnMaxIdle) to a pool. Extracted so Open and OpenWritable
// stay in lockstep.
func applyConnMaxIdle(db *sql.DB, d time.Duration) {
	switch {
	case d == 0:
		db.SetConnMaxIdleTime(defaultConnMaxIdle)
	case d > 0:
		db.SetConnMaxIdleTime(d)
		// d < 0: caller opted out of the idle deadline.
	}
}

// assertWAL fails-closed if the database is not in a multi-reader-capable
// journal mode. Accepts plain "wal" (upstream SQLite) and "wal2"
// (rocicorp's checkpoint-without-stall patch). Both give the
// snapshot-pinning guarantees our TxCache relies on; rollback or
// memory journals would serialize readers behind any writer and break
// the parallelism the design doc commits to.
func assertWAL(db *sql.DB) error {
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		return fmt.Errorf("tablesource.Open: read journal_mode: %w", err)
	}
	if mode != "wal" && mode != "wal2" {
		return fmt.Errorf(
			"tablesource.Open: database is in journal_mode=%q, "+
				"want wal or wal2 (required for multi-reader concurrency)",
			mode)
	}
	return nil
}

// OpenWritable returns a *sql.DB pool aimed at the SQLite file at path,
// configured as the "prev snapshot" workspace for Sources. Each Source
// acquires one dedicated *sql.Conn from this pool, opens a BEGIN
// CONCURRENT (or plain BEGIN, if the build lacks rocicorp's wal2 patch)
// transaction on it, and uses that conn as its read/write surface for
// the lifetime of the source.
//
// Distinguishing features vs Open:
//   - No `_query_only=true` — we WILL run INSERT/UPDATE/DELETE on this
//     conn, against the prev-snapshot tx. The writes are never
//     committed (we always ROLLBACK at end of batch), so the file is
//     never mutated, but SQLite still requires the conn itself to be
//     writable to accept the statements.
//   - synchronous=OFF — the writes are ephemeral and discarded, so
//     fsync cost is pure waste. Matches TS Snapshotter's
//     `PRAGMA synchronous = OFF` (snapshotter.ts:283).
//
// The caller (sidecar startup) opens one of these pools per process and
// passes it to every Source.New so all sources of a given CG share the
// pool. Each Source then acquires a dedicated *sql.Conn from it.
func OpenWritable(path string, opts OpenOptions) (*sql.DB, error) {
	if path == "" {
		return nil, fmt.Errorf("tablesource.OpenWritable: path is required")
	}
	busyMs := opts.BusyTimeoutMs
	if busyMs <= 0 {
		busyMs = defaultBusyTimeoutMs
	}
	maxOpen := opts.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = defaultMaxOpenConns
	}
	maxIdle := opts.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = defaultMaxIdleConns
	}

	// No `_query_only` — we run INSERT/UPDATE/DELETE on the prev tx.
	// `_synchronous=OFF` matches TS Snapshotter (writes never commit).
	dsn := "file:" + path +
		"?_busy_timeout=" + strconv.Itoa(busyMs) +
		"&_synchronous=OFF"
	if opts.CacheSizeKB > 0 {
		// Negative value = KB units (see Open).
		dsn += "&_cache_size=-" + strconv.Itoa(opts.CacheSizeKB)
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("tablesource.OpenWritable: sql.Open: %w", err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	applyConnMaxIdle(db, opts.ConnMaxIdle)

	if err := assertWAL(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
