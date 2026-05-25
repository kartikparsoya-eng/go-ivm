package sqlite

// DatabaseStorage provides SQLite-backed key-value storage for operators
// (primarily Take). It uses a single "storage" table keyed by
// (clientGroupID, opID, key) and stores JSON-encoded values.
//
// In production, each worker gets its own DatabaseStorage file, configured
// for ephemeral (RAM-like) storage with durability OFF.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/ivm"

	_ "modernc.org/sqlite"
)

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
	ds.tx = tx
	ds.stmtGet, _ = tx.Prepare(`SELECT val FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?`)
	ds.stmtSet, _ = tx.Prepare(`INSERT INTO storage (clientGroupID, op, key, val) VALUES(?, ?, ?, ?) ON CONFLICT(clientGroupID, op, key) DO UPDATE SET val = excluded.val`)
	ds.stmtDel, _ = tx.Prepare(`DELETE FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?`)
	ds.stmtScan, _ = tx.Prepare(`SELECT key, val FROM storage WHERE clientGroupID = ? AND op = ? AND key >= ?`)
	return nil
}

func (ds *DatabaseStorage) maybeCheckpoint() {
	ds.numWrites++
	if ds.numWrites >= ds.commitInterval {
		ds.checkpoint()
	}
}

func (ds *DatabaseStorage) checkpoint() {
	if ds.tx != nil {
		ds.tx.Commit()
	}
	ds.beginTx()
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
		return nil, false
	}
	return json.RawMessage(val), true
}

func (ds *DatabaseStorage) set(cgID string, opID int, key string, val json.RawMessage) {
	ds.maybeCheckpoint()
	ds.stmtSet.Exec(cgID, opID, key, string(val))
}

func (ds *DatabaseStorage) del(cgID string, opID int, key string) {
	ds.maybeCheckpoint()
	ds.stmtDel.Exec(cgID, opID, key)
}

func (ds *DatabaseStorage) scan(cgID string, opID int, prefix string) [][2]string {
	ds.maybeCheckpoint()
	rows, err := ds.stmtScan.Query(cgID, opID, prefix)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var results [][2]string
	for rows.Next() {
		var key, val string
		if err := rows.Scan(&key, &val); err != nil {
			break
		}
		if !strings.HasPrefix(key, prefix) {
			break
		}
		results = append(results, [2]string{key, val})
	}
	return results
}

// CreateClientGroupStorage returns a ClientGroupStorage for the given ID.
func (ds *DatabaseStorage) CreateClientGroupStorage(cgID string) *ClientGroupStorage {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	// Clear existing storage for this client group
	ds.tx.Exec("DELETE FROM storage WHERE clientGroupID = ?", cgID)
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
	return &SQLiteTakeStorage{storage: cgs.CreateStorage()}
}

// Destroy deletes all storage for this client group.
func (cgs *ClientGroupStorage) Destroy() {
	cgs.ds.mu.Lock()
	defer cgs.ds.mu.Unlock()
	cgs.ds.tx.Exec("DELETE FROM storage WHERE clientGroupID = ?", cgs.cgID)
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

// SQLiteTakeStorage implements ivm.TakeStorage using OperatorStorage.
type SQLiteTakeStorage struct {
	storage *OperatorStorage
}

func (s *SQLiteTakeStorage) GetTakeState(key string) *ivm.TakeState {
	raw, ok := s.storage.Get(key)
	if !ok {
		return nil
	}
	var state ivm.TakeState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil
	}
	return &state
}

func (s *SQLiteTakeStorage) SetTakeState(key string, state ivm.TakeState) {
	data, err := json.Marshal(state)
	if err != nil {
		panic(fmt.Sprintf("marshal TakeState: %v", err))
	}
	s.storage.Set(key, data)
}

func (s *SQLiteTakeStorage) GetMaxBound() ivm.Row {
	raw, ok := s.storage.Get("maxBound")
	if !ok {
		return nil
	}
	var row ivm.Row
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil
	}
	return row
}

func (s *SQLiteTakeStorage) SetMaxBound(bound ivm.Row) {
	data, err := json.Marshal(bound)
	if err != nil {
		panic(fmt.Sprintf("marshal maxBound: %v", err))
	}
	s.storage.Set("maxBound", data)
}

func (s *SQLiteTakeStorage) Del(key string) {
	s.storage.Del(key)
}
