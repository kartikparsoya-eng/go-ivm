package sqlite

// DatabaseStorage provides SQLite-backed key-value storage for operators
// (primarily Take). It uses a single "storage" table keyed by
// (clientGroupID, opID, key) and stores JSON-encoded values.
//
// In production, each worker gets its own DatabaseStorage file, configured
// for ephemeral (RAM-like) storage with durability OFF.

import (
	"container/list"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/ivm"

	_ "modernc.org/sqlite"
)

// storageDrift wraps an operator-storage infrastructure failure (failed
// INSERT/DELETE/COMMIT/BEGIN/prepare) as a *ivm.DriftError so the panic rides
// the engine's existing drift-recovery path: engine.Advance / the hydrate
// goroutines recover *DriftError, drop the in-flight work, and TS re-inits
// from SQLite truth. Before this, Exec errors were silently discarded — a
// failed Take-bound write corrupted the window state and only surfaced much
// later as a stale-bound panic in take.go, far from the cause.
func storageDrift(op string, err error) *ivm.DriftError {
	return &ivm.DriftError{
		Table:    "_operator_storage",
		Op:       op + ": " + err.Error(),
		PK:       map[string]ivm.Value{},
		HasCount: -1,
	}
}

const createStorageTable = `
  CREATE TABLE IF NOT EXISTS storage (
    clientGroupID TEXT,
    op INTEGER,
    key TEXT,
    val TEXT,
    PRIMARY KEY(clientGroupID, op, key)
  )
`

const defaultCommitInterval = 5000

// defaultTakeStateCacheMax bounds the per-Take in-memory take-state LRU when
// no operator override is set. The cache key varies over a Take's distinct
// partition values (GetTakeStateKey), so a high-cardinality partition under a
// huge scan would otherwise grow the cache without limit for the life of the
// pipeline (the Take operator never Del's stale partitions, and the idle
// reaper is days away). 10k hot partitions per operator is generous for real
// dashboards while capping the pathological case. Tunable / disable-able via
// GO_IVM_TAKE_STATE_CACHE_MAX (0 = unbounded, restoring the old behavior).
const defaultTakeStateCacheMax = 10000

// takeStateCacheMax is the effective per-Take cache cap, resolved once at
// process start. An explicit GO_IVM_TAKE_STATE_CACHE_MAX=0 is honored as
// "unbounded"; unset or malformed falls back to the default.
var takeStateCacheMax = readTakeStateCacheMax()

func readTakeStateCacheMax() int {
	if v := os.Getenv("GO_IVM_TAKE_STATE_CACHE_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return defaultTakeStateCacheMax
}

// TakeStateCacheMax returns the effective per-Take take-state cache cap so the
// sidecar can log it at startup. 0 means unbounded.
func TakeStateCacheMax() int { return takeStateCacheMax }

// DatabaseStorage manages operator state in SQLite.
type DatabaseStorage struct {
	mu             sync.Mutex
	db             *sql.DB
	commitInterval int
	numWrites      int
	tx             *sql.Tx

	// Prepared statements (on current tx)
	stmtGet  *sql.Stmt
	stmtSet  *sql.Stmt
	stmtDel  *sql.Stmt
	stmtScan *sql.Stmt
}

// NewDatabaseStorage opens a SQLite database at path and creates the storage table.
func NewDatabaseStorage(path string) (*DatabaseStorage, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open database storage: %w", err)
	}
	// Single connection — matches TS single-writer model, avoids SQLITE_BUSY
	db.SetMaxOpenConns(1)

	// Configure for ephemeral, single-writer usage (matching TS pragmas)
	pragmas := []string{
		"PRAGMA locking_mode = EXCLUSIVE",
		"PRAGMA foreign_keys = OFF",
		"PRAGMA journal_mode = OFF",
		"PRAGMA synchronous = OFF",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("pragma %s: %w", p, err)
		}
	}

	if _, err := db.Exec(createStorageTable); err != nil {
		db.Close()
		return nil, fmt.Errorf("create storage table: %w", err)
	}

	ds := &DatabaseStorage{
		db:             db,
		commitInterval: defaultCommitInterval,
	}
	if err := ds.beginTx(); err != nil {
		db.Close()
		return nil, err
	}
	return ds, nil
}

func (ds *DatabaseStorage) beginTx() error {
	tx, err := ds.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	// Prepare errors must not be discarded: a nil stmt would NPE on first use,
	// and (pre-fix) the `, _` pattern turned a prepare failure into a crash at
	// an unrelated call site.
	stmts := []struct {
		dst **sql.Stmt
		sql string
	}{
		{&ds.stmtGet, `SELECT val FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?`},
		{&ds.stmtSet, `INSERT INTO storage (clientGroupID, op, key, val) VALUES(?, ?, ?, ?) ON CONFLICT(clientGroupID, op, key) DO UPDATE SET val = excluded.val`},
		{&ds.stmtDel, `DELETE FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?`},
		{&ds.stmtScan, `SELECT key, val FROM storage WHERE clientGroupID = ? AND op = ? AND key >= ?`},
	}
	for _, s := range stmts {
		st, err := tx.Prepare(s.sql)
		if err != nil {
			_ = tx.Rollback()
			ds.tx = nil
			return fmt.Errorf("prepare %q: %w", s.sql, err)
		}
		*s.dst = st
	}
	ds.tx = tx
	return nil
}

func (ds *DatabaseStorage) maybeCheckpoint() {
	ds.numWrites++
	if ds.numWrites >= ds.commitInterval {
		ds.checkpoint()
	}
}

// checkpoint commits the rolling tx and opens a fresh one. Commit/begin
// failures panic with a *DriftError: losing the committed window state is a
// state corruption the engine must not paper over — the drift-recovery path
// drops the in-flight advance and re-inits from SQLite truth.
func (ds *DatabaseStorage) checkpoint() {
	if ds.tx != nil {
		if err := ds.tx.Commit(); err != nil {
			ds.tx = nil
			panic(storageDrift("storage-checkpoint-commit", err))
		}
	}
	if err := ds.beginTx(); err != nil {
		panic(storageDrift("storage-checkpoint-begin", err))
	}
	ds.numWrites = 0
}

// Close commits and closes the database.
func (ds *DatabaseStorage) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.tx != nil {
		ds.tx.Commit()
		ds.tx = nil
	}
	return ds.db.Close()
}

func (ds *DatabaseStorage) get(cgID string, opID int, key string) (json.RawMessage, bool) {
	ds.maybeCheckpoint()
	var val string
	err := ds.stmtGet.QueryRow(cgID, opID, key).Scan(&val)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false
		}
		// Infra failure (busy, I/O, closed conn) is NOT "key absent" — treating
		// it as absent makes Take re-run initialFetch against live state with a
		// stale window, silently corrupting the bound.
		panic(storageDrift("storage-get", err))
	}
	return json.RawMessage(val), true
}

func (ds *DatabaseStorage) set(cgID string, opID int, key string, val json.RawMessage) {
	ds.maybeCheckpoint()
	if _, err := ds.stmtSet.Exec(cgID, opID, key, string(val)); err != nil {
		panic(storageDrift("storage-set", err))
	}
}

func (ds *DatabaseStorage) del(cgID string, opID int, key string) {
	ds.maybeCheckpoint()
	if _, err := ds.stmtDel.Exec(cgID, opID, key); err != nil {
		panic(storageDrift("storage-del", err))
	}
}

func (ds *DatabaseStorage) scan(cgID string, opID int, prefix string) [][2]string {
	ds.maybeCheckpoint()
	rows, err := ds.stmtScan.Query(cgID, opID, prefix)
	if err != nil {
		panic(storageDrift("storage-scan", err))
	}
	defer rows.Close()

	var results [][2]string
	for rows.Next() {
		var key, val string
		if err := rows.Scan(&key, &val); err != nil {
			panic(storageDrift("storage-scan-row", err))
		}
		if !strings.HasPrefix(key, prefix) {
			break
		}
		results = append(results, [2]string{key, val})
	}
	if err := rows.Err(); err != nil {
		panic(storageDrift("storage-scan-rows", err))
	}
	return results
}

// CreateClientGroupStorage returns a ClientGroupStorage for the given ID.
func (ds *DatabaseStorage) CreateClientGroupStorage(cgID string) *ClientGroupStorage {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	// Clear existing storage for this client group
	if _, err := ds.tx.Exec("DELETE FROM storage WHERE clientGroupID = ?", cgID); err != nil {
		panic(storageDrift("storage-cg-clear", err))
	}
	ds.checkpoint()

	return &ClientGroupStorage{
		ds:       ds,
		cgID:     cgID,
		nextOpID: 1,
	}
}

// ClientGroupStorage creates Storage instances for operators within a client group.
type ClientGroupStorage struct {
	mu       sync.Mutex
	ds       *DatabaseStorage
	cgID     string
	nextOpID int
}

// CreateStorage returns a new operator Storage instance.
func (cgs *ClientGroupStorage) CreateStorage() *OperatorStorage {
	cgs.mu.Lock()
	defer cgs.mu.Unlock()
	opID := cgs.nextOpID
	cgs.nextOpID++
	return &OperatorStorage{ds: cgs.ds, cgID: cgs.cgID, opID: opID}
}

// CreateTakeStorage returns a TakeStorage backed by SQLite.
func (cgs *ClientGroupStorage) CreateTakeStorage() ivm.TakeStorage {
	return &SQLiteTakeStorage{storage: cgs.CreateStorage(), maxStates: takeStateCacheMax}
}

// Destroy deletes all storage for this client group.
func (cgs *ClientGroupStorage) Destroy() {
	cgs.ds.mu.Lock()
	defer cgs.ds.mu.Unlock()
	if _, err := cgs.ds.tx.Exec("DELETE FROM storage WHERE clientGroupID = ?", cgs.cgID); err != nil {
		panic(storageDrift("storage-cg-destroy", err))
	}
	cgs.ds.checkpoint()
}

// OperatorStorage is the generic KV storage for a single operator.
// Mirrors the Storage interface from operator.ts.
type OperatorStorage struct {
	ds   *DatabaseStorage
	cgID string
	opID int
}

func (s *OperatorStorage) Get(key string) (json.RawMessage, bool) {
	s.ds.mu.Lock()
	defer s.ds.mu.Unlock()
	return s.ds.get(s.cgID, s.opID, key)
}

func (s *OperatorStorage) Set(key string, val json.RawMessage) {
	s.ds.mu.Lock()
	defer s.ds.mu.Unlock()
	s.ds.set(s.cgID, s.opID, key, val)
}

func (s *OperatorStorage) Del(key string) {
	s.ds.mu.Lock()
	defer s.ds.mu.Unlock()
	s.ds.del(s.cgID, s.opID, key)
}

func (s *OperatorStorage) Scan(prefix string) [][2]string {
	s.ds.mu.Lock()
	defer s.ds.mu.Unlock()
	return s.ds.scan(s.cgID, s.opID, prefix)
}

// SQLiteTakeStorage implements ivm.TakeStorage using OperatorStorage, with a
// bounded in-memory write-through LRU cache in front of the SQLite rows.
//
// Why the cache: every Take state touch used to be a JSON marshal + SQL
// round-trip through the engine-wide DatabaseStorage mutex + its single
// connection — the per-CG serializer right behind GOGC on the parallel-path
// profile for Take-heavy dashboards. With write-through, reads are map hits
// (including negative hits for the very common "no state yet → drop push"
// probe) and SQLite is only touched on writes. SQLite stays authoritative:
// writes land there FIRST (and panic via storageDrift on failure, leaving the
// cache unchanged), then update the cache — so cache and DB can't diverge.
//
// Why bounded: the cache key varies over the Take's distinct partition values,
// and the operator never Del's a partition whose window has emptied — so for a
// high-cardinality partition under a huge scan the cache would otherwise grow
// without limit for the whole life of the pipeline, becoming the dominant
// retained allocation and pressing GOMEMLIMIT. The LRU caps it at maxStates
// entries (0 = unbounded). Eviction is observationally
// transparent: SQLite is authoritative, so an evicted key just re-reads (and
// re-caches) on next access — the exact pre-cache code path, never a drift.
//
// Lifecycle safety: each SQLiteTakeStorage belongs to exactly one Take in one
// pipeline. Re-registering a queryID tears down the old pipeline (and this
// cache with it) and CreateClientGroupStorage DELETEs the cgID's rows before
// fresh instances are built, so a stale cache can never outlive its rows. The
// mutex is belt-and-braces for the parallel hydrate/push fan-out (a single
// instance is only ever touched from one goroutine at a time today).
type SQLiteTakeStorage struct {
	storage *OperatorStorage

	mu sync.Mutex
	// states maps key→LRU element; lru orders elements most-recently-used at
	// the front. An element whose takeStateEntry.state is nil is a NEGATIVE
	// entry ("known absent") so repeated misses skip SQLite too. Bounded by
	// maxStates (see cacheState).
	states map[string]*list.Element
	lru    *list.List
	// maxStates caps the cache (0 = unbounded). Set from takeStateCacheMax at
	// construction; overridable per-instance in tests.
	maxStates int
	// maxBound caches the "maxBound" row once loaded; maxBoundLoaded
	// distinguishes "not yet read" from "read and absent (nil)".
	maxBound       ivm.Row
	maxBoundLoaded bool
}

// takeStateEntry is one LRU node: the cache key plus its state (nil = negative
// "known absent"). The key is duplicated here so eviction of the back element
// can delete the corresponding map entry in O(1).
type takeStateEntry struct {
	key   string
	state *ivm.TakeState
}

func (s *SQLiteTakeStorage) GetTakeState(key string) *ivm.TakeState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.states[key]; ok {
		s.lru.MoveToFront(el)
		cached := el.Value.(*takeStateEntry).state
		if cached == nil {
			return nil
		}
		c := *cached
		return &c
	}
	raw, ok := s.storage.Get(key)
	if !ok {
		s.cacheState(key, nil)
		return nil
	}
	var state ivm.TakeState
	if err := json.Unmarshal(raw, &state); err != nil {
		// A row we wrote that no longer parses is corrupted state, not "absent".
		panic(storageDrift("take-state-unmarshal "+key, err))
	}
	s.cacheState(key, &state)
	c := state
	return &c
}

func (s *SQLiteTakeStorage) SetTakeState(key string, state ivm.TakeState) {
	validateBoundRow("TakeState.Bound", state.Bound)
	data, err := json.Marshal(state)
	if err != nil {
		panic(fmt.Sprintf("marshal TakeState: %v", err))
	}
	// Cache the DECODED form of what was written, not the caller's value, so
	// a cached read is shape-identical to a fresh SQLite read (e.g. an int
	// bound value normalizes to float64 either way). Production rows are
	// pre-normalized so this is usually a no-op, but it keeps the cache
	// observationally transparent.
	var roundTripped ivm.TakeState
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		panic(fmt.Sprintf("re-decode TakeState: %v", err))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storage.Set(key, data) // panics on failure → cache left unchanged
	s.cacheState(key, &roundTripped)
}

func (s *SQLiteTakeStorage) GetMaxBound() ivm.Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxBoundLoaded {
		return s.maxBound
	}
	raw, ok := s.storage.Get("maxBound")
	if !ok {
		s.maxBound = nil
		s.maxBoundLoaded = true
		return nil
	}
	var row ivm.Row
	if err := json.Unmarshal(raw, &row); err != nil {
		panic(storageDrift("take-maxBound-unmarshal", err))
	}
	s.maxBound = row
	s.maxBoundLoaded = true
	return row
}

func (s *SQLiteTakeStorage) SetMaxBound(bound ivm.Row) {
	validateBoundRow("maxBound", bound)
	data, err := json.Marshal(bound)
	if err != nil {
		panic(fmt.Sprintf("marshal maxBound: %v", err))
	}
	// Round-trip-faithful cache — see SetTakeState.
	var roundTripped ivm.Row
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		panic(fmt.Sprintf("re-decode maxBound: %v", err))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storage.Set("maxBound", data) // panics on failure → cache left unchanged
	s.maxBound = roundTripped
	s.maxBoundLoaded = true
}

func (s *SQLiteTakeStorage) Del(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storage.Del(key)
	s.cacheState(key, nil)
}

// cacheState records key→state (nil = known absent) in the LRU, refreshing
// recency, and evicts least-recently-used entries when maxStates>0 and the
// cache would exceed it. MUST be called with s.mu held.
func (s *SQLiteTakeStorage) cacheState(key string, state *ivm.TakeState) {
	if s.states == nil {
		s.states = make(map[string]*list.Element)
		s.lru = list.New()
	}
	if el, ok := s.states[key]; ok {
		el.Value.(*takeStateEntry).state = state
		s.lru.MoveToFront(el)
		return
	}
	s.states[key] = s.lru.PushFront(&takeStateEntry{key: key, state: state})
	if s.maxStates > 0 {
		for s.lru.Len() > s.maxStates {
			oldest := s.lru.Back()
			if oldest == nil {
				break
			}
			delete(s.states, oldest.Value.(*takeStateEntry).key)
			s.lru.Remove(oldest)
		}
	}
}

// validateBoundRow panics if a bound row holds a value that does NOT survive
// the JSON round-trip with its comparison semantics intact. The allowed set
// is exactly the canonical post-FromSQLiteType shapes: nil, string, bool,
// numerics (JSON re-reads them as float64 — consistent with the single-Number
// model), and JSON-column maps/slices. Everything else — []byte (re-read as a
// base64 STRING, silently mis-ranking against fresh blob rows), time.Time
// (marshals to an RFC3339 string), or any other struct — is the same "silent
// shape change" family as the mattn time.Time bug, so fail loud at write time
// instead of corrupting the window.
func validateBoundRow(what string, row ivm.Row) {
	for col, v := range row {
		switch v.(type) {
		case nil, string, bool,
			float64, float32, int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			map[string]interface{}, []interface{}:
			// JSON round-trip safe.
		default:
			panic(fmt.Sprintf(
				"%s: column %q holds %T which does not survive the JSON round-trip "+
					"through operator storage — sort/bound columns must be "+
					"nil/string/bool/number/json", what, col, v))
		}
	}
}
