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

	_ "github.com/mattn/go-sqlite3"
)

// Defaults chosen to match the design doc (parallelization is a hard
// constraint, not a tuning knob).
const (
	// Per-conn busy timeout. Long enough to ride out a WAL checkpoint
	// stall by the writer, short enough to surface a wedge promptly.
	defaultBusyTimeoutMs = 5000

	// Pool ceiling. Sized so a multi-CG burst doesn't queue: 16 CGs ×
	// ~4 concurrent queries each = 64 conns headroom. Bench-tunable;
	// see PERF-REVIEW once Phase 1c lands microbenchmarks.
	defaultMaxOpenConns = 64
	defaultMaxIdleConns = 16
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

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("tablesource.Open: sql.Open: %w", err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	if err := assertWAL(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// assertWAL fails-closed if the database is not in WAL mode. WAL is set by
// the TS replicator (writer); this side just verifies. Doing the check
// here means a misconfigured replica is caught at startup, not in the
// middle of a query.
func assertWAL(db *sql.DB) error {
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		return fmt.Errorf("tablesource.Open: read journal_mode: %w", err)
	}
	if mode != "wal" {
		return fmt.Errorf(
			"tablesource.Open: database is in journal_mode=%q, want wal "+
				"(WAL is required for multi-reader concurrency)", mode)
	}
	return nil
}
