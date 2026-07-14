package tablesource

// Open: read-side SQLite connection pool for the Go IVM TableSource leaf.
// WAL is asserted (NOT switched on by us — the TS replicator owns the file),
// query_only protects against accidental writes from this process, and the
// pool is sized for true per-goroutine reader parallelism.
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
	"database/sql/driver"
	"fmt"
	"strconv"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// goivmDriverName is the database/sql driver this package's pools open with.
// It is mattn/go-sqlite3 plus a ConnectHook that overrides SQLite's built-in
// ASCII-only lower() with a full Unicode case mapping.
//
// Why: zero 1.7.0 aligned SQL LIKE/ILIKE with Postgres (zqlite/db.ts sets
// `case_sensitive_like = ON`; zqlite/query-builder.ts lower()s both ILIKE
// operands). TS's lower() is the Unicode-aware ICU one that
// @rocicorp/zero-sqlite3 provides — mattn's bundled SQLite (and a non-ICU
// system libsqlite3) only lowercases ASCII, so `col ILIKE 'é%'` would
// diverge from TS on any non-ASCII text. The override applies per connection
// (application-defined functions shadow built-ins of the same name/arity),
// so every conn the pools open behaves like TS's replica connection.
const goivmDriverName = "sqlite3_goivm"

// goivmDriverInstance is the very driver value registered under
// goivmDriverName. Kept package-visible so the reader pool can open RAW
// driver connections (bypassing database/sql — Option B: one reader per
// hydrate goroutine with interleaved cursors) that carry the identical
// per-conn setup: SQLiteDriver.Open parses the same DSN pragmas
// (_busy_timeout, _query_only, _case_sensitive_like, _cache_size) and runs
// the same ConnectHook (Unicode lower()). Set once by registerGoivmDriver.
var goivmDriverInstance *sqlite3.SQLiteDriver

var registerGoivmDriver = sync.OnceValues(func() (string, error) {
	// Resolve the REAL→TEXT rendering mode of the linked SQLite before any
	// connection (and therefore any lower() UDF call) can exist. Probed by
	// behavior, not sqlite3_libversion_number(): what matters is what THIS
	// library prints, whatever fork or FP_DIGITS default it carries.
	digits, err := probeRealTextDigits()
	if err != nil {
		return "", err
	}
	realTextDigits = digits
	goivmDriverInstance = &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return conn.RegisterFunc("lower", unicodeLowerSQL, true)
		},
	}
	sql.Register(goivmDriverName, goivmDriverInstance)
	return goivmDriverName, nil
})

// RegisterGoivmDriver ensures the goivm driver is registered and returns
// its driver name. Exported for the snapshotter package (which opens raw
// driver conns and needs the driver registered first).
func RegisterGoivmDriver() (string, error) {
	return registerGoivmDriver()
}

// readPoolDSNs maps each pool opened by Open/OpenWritable to the DSN it was
// opened with, so rawOpenReaderConn can mint raw driver conns with the exact
// same per-conn pragmas + ConnectHook. Keyed by the *sql.DB pointer (like
// probedTables): entries are a string each, bounded by pool count.
var readPoolDSNs sync.Map // *sql.DB → string (DSN)

// RawOpenReaderConn is the exported form of rawOpenReaderConn for use by
// the snapshotter package (which needs raw driver conns for the advance
// read path — same bypass rationale as the reader pool).
func RawOpenReaderConn(db *sql.DB) (driver.Conn, error) {
	return rawOpenReaderConn(db)
}

// RegisterDSN registers a DSN for a *sql.DB so that RawOpenReaderConn can
// open raw driver conns against it. Used by tests that open their DB via
// sql.Open directly (instead of tablesource.Open/OpenWritable).
func RegisterDSN(db *sql.DB, dsn string) {
	readPoolDSNs.Store(db, dsn)
}

// rawOpenReaderConn opens ONE raw driver connection configured identically
// to db's pooled connections (same DSN → same pragmas + lower() hook), but
// OUTSIDE database/sql. Raw conns are the substrate of the Option B reader
// pool. the original motivation — "database/sql
// serializes a *sql.Conn behind one live Rows" — was empirically overstated:
// conn-prepared statements interleave live cursors on one *sql.Conn just
// fine. The real pillars are the stmt busy-checkout cache, the driver-level
// scan, shell reuse across pool generations, and — load-bearing here —
// pool-accounting bypass: raw opens are invisible to db's MaxOpenConns, so
// pool builds no longer compete with probes for pooled conns.)
//
// The caller OWNS the returned conn: it is invisible to db's MaxOpenConns
// accounting and idle reaper, and MUST be closed via driver.Conn.Close.
func rawOpenReaderConn(db *sql.DB) (driver.Conn, error) {
	dsnAny, ok := readPoolDSNs.Load(db)
	if !ok {
		return nil, fmt.Errorf("tablesource: rawOpenReaderConn: db was not opened by tablesource.Open/OpenWritable (no DSN recorded)")
	}
	if goivmDriverInstance == nil {
		return nil, fmt.Errorf("tablesource: rawOpenReaderConn: goivm driver not registered")
	}
	dc, err := goivmDriverInstance.Open(dsnAny.(string))
	if err != nil {
		return nil, fmt.Errorf("tablesource: rawOpenReaderConn: %w", err)
	}
	return dc, nil
}

// probeRealTextDigits asks the linked SQLite how it renders REAL→TEXT and
// maps the answer to the sqliteRealText mode (see realtext.go). CAST runs
// the very vdbeMemRenderNum the ICU-style lower() coercion would:
//
//	"0.333333333333333"   → 15  (≤3.51 `%!.15g`; the production wal2 fork
//	                             AND @rocicorp/zero-sqlite3 are 3.51.0)
//	"0.33333333333333332" → 17  (≥3.53 `%!.*g` nFpDigit=17; mattn bundled)
//
// Anything else fails Open loudly: a library whose rendering we have not
// ported must not silently coerce through the wrong algorithm (that is
// exactly the hydrate-vs-advance drift this probe exists to prevent).
func probeRealTextDigits() (int, error) {
	// "sqlite3" is mattn's own driver registration — same linked library,
	// no ConnectHook, so the probe cannot recurse into lower().
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return 0, fmt.Errorf("tablesource: realtext probe open: %w", err)
	}
	defer db.Close()
	var got string
	if err := db.QueryRow(`SELECT CAST(1.0/3.0 AS TEXT)`).Scan(&got); err != nil {
		return 0, fmt.Errorf("tablesource: realtext probe query: %w", err)
	}
	switch got {
	case "0.333333333333333":
		return 15, nil
	case "0.33333333333333332":
		return 17, nil
	}
	return 0, fmt.Errorf(
		"tablesource: linked SQLite renders CAST(1.0/3.0 AS TEXT) = %q — "+
			"neither the ≤3.51 15-digit nor the ≥3.53 17-digit form; "+
			"realtext.go needs a port of this library's REAL→TEXT algorithm",
		got)
}

// sqlLowerCaserPool amortizes cases.Lower(language.Und) construction (napi
// TS twin): the lower() override runs PER VALUE PER ROW during every
// ILIKE scan, and a cases.Caser is stateful (not concurrency-safe), so the
// previous per-call construction paid the language lookup + transformer
// build on every row.
var sqlLowerCaserPool = sync.Pool{
	New: func() any {
		c := cases.Lower(language.Und)
		return &c
	},
}

func sqlUnicodeLower(s string) string {
	c := sqlLowerCaserPool.Get().(*cases.Caser)
	out := c.String(s)
	sqlLowerCaserPool.Put(c)
	return out
}

// unicodeLowerSQL mirrors the full ICU lower() contract, not just its case
// mapping — the argument is `any` so mattn uses callbackArgGeneric, which
// accepts every SQLite type. The previous func(string) string registration
// routed through callbackArgString, which REJECTS SQLITE_NULL/INTEGER/FLOAT
// ("argument must be BLOB or TEXT") — so the query_builder ILIKE shape
// `lower(col) LIKE lower(?)` errored mid-scan on the first NULL in any
// nullable column, and Source.Fetch panicked on rows.Err() → a
// deterministic hydrate-failure loop.
//
// Per-type contract (matches ext/icu icuCaseFunc16 + sqlite3_value_text):
//   - NULL → NULL (ICU returns without setting a result). mattn delivers
//     SQLITE_NULL as a nil []byte; a genuine empty BLOB arrives as a
//     non-nil empty slice, so an empty blob literal still lowers to an empty string.
//   - TEXT/BLOB → full Unicode case mapping including context-sensitive
//     rules (Greek final sigma "ΟΔΟΣ"→"οδος"), locale-independent.
//     strings.ToLower applies only simple unconditional mappings and would
//     diverge from TS on those. BLOBs are bytes-as-text, like value_text.
//   - INTEGER/FLOAT → SQLite's own text coercion (Int64ToText for ints;
//     for reals, the linked library's algorithm — 15- or 17-digit, probed
//     at driver registration — via sqliteRealText), then lowercase is a
//     no-op on digits.
//
// Known residual: integral REALs in [1e17, 2^63) stored int-serial-encoded
// surface inside SQLite as MEM_IntReal and stringify as "…000.0", but mattn
// collapses the arg to a plain double before we see it, so those render
// exponential ("1.0e+17"). zql only ILIKEs string columns; unreachable via
// replication.
func unicodeLowerSQL(v any) any {
	switch x := v.(type) {
	case nil: // defensive; mattn encodes NULL as []byte(nil), not nil any
		return nil
	case []byte:
		if x == nil {
			return nil // SQLITE_NULL
		}
		return sqlUnicodeLower(string(x))
	case string:
		return sqlUnicodeLower(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return sqliteRealText(x)
	default: // unreachable: callbackArgGeneric yields only the above
		return nil
	}
}

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
	// MaxIdleConns caps idle conns kept ready. 0 → MaxOpenConns (keep-warm:
	// An idle cap much smaller than the open cap makes every
	// pool-demand burst churn fresh SQLite opens against the replica
	// (wal-index mmap + cold page cache), a uniform tax on the warm
	// hydrate/advance path: 70s cumulative read-pool wait per 10s window
	// with sampled in-use as low as 0–25/128. Conn COUNT is capped by
	// MaxOpenConns and memory by ConnMaxIdle's time-based reaping, so a
	// small idle cap buys nothing but reopen churn). Values above
	// MaxOpenConns clamp to it.
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
	// deadline (conns park forever when negative).
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
	if maxIdle <= 0 || maxIdle > maxOpen {
		maxIdle = maxOpen // keep-warm: see OpenOptions.MaxIdleConns
	}

	// mattn/go-sqlite3 honors SQLite URI form when DSN starts with
	// "file:". Per-conn pragmas use direct query params (not the
	// _pragma=KEY(VAL) form modernc used) so every new connection the
	// pool opens applies them — multi-conn parallelism keeps the
	// pragma guarantees on each backing connection.
	//
	// _case_sensitive_like: zero 1.7.0 runs every replica connection with
	// `PRAGMA case_sensitive_like = ON` (zqlite/db.ts) so bare LIKE matches
	// Postgres; the generated SQL (sqlite/query_builder.go) relies on it.
	dsn := "file:" + path +
		"?_busy_timeout=" + strconv.Itoa(busyMs) +
		"&_query_only=true" +
		"&_case_sensitive_like=true"
	if opts.CacheSizeKB > 0 {
		// Negative value = KB units (https://sqlite.org/pragma.html#pragma_cache_size).
		dsn += "&_cache_size=-" + strconv.Itoa(opts.CacheSizeKB)
	}

	driverName, err := registerGoivmDriver()
	if err != nil {
		return nil, fmt.Errorf("tablesource.Open: %w", err)
	}
	db, err := sql.Open(driverName, dsn)
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
	readPoolDSNs.Store(db, dsn)
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
	if maxIdle <= 0 || maxIdle > maxOpen {
		maxIdle = maxOpen // keep-warm: see OpenOptions.MaxIdleConns
	}

	// No `_query_only` — we run INSERT/UPDATE/DELETE on the prev tx.
	// `_synchronous=OFF` matches TS Snapshotter (writes never commit).
	// `_case_sensitive_like=true` matches every TS replica connection
	// (zqlite/db.ts @ 1.7.0) — see Open.
	dsn := "file:" + path +
		"?_busy_timeout=" + strconv.Itoa(busyMs) +
		"&_synchronous=OFF" +
		"&_case_sensitive_like=true"
	if opts.CacheSizeKB > 0 {
		// Negative value = KB units (see Open).
		dsn += "&_cache_size=-" + strconv.Itoa(opts.CacheSizeKB)
	}

	driverName, err := registerGoivmDriver()
	if err != nil {
		return nil, fmt.Errorf("tablesource.OpenWritable: %w", err)
	}
	db, err := sql.Open(driverName, dsn)
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
	readPoolDSNs.Store(db, dsn)
	return db, nil
}
