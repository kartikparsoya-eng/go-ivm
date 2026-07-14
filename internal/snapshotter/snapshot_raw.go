package snapshotter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
)

// snapshotStmt is a cached prepared statement on the snapshot's raw conn.
type snapshotStmt struct {
	st driver.Stmt
}

// stmtCacheCap bounds the per-snapshot prepared-statement cache. Each
// snapshot serves GetRow (1 shape per table), GetRows (1-3 shapes per table
// depending on which unique keys are non-NULL), NumChangesSince (1 shape),
// ChangesSince (1 shape), and stateVersion (1 shape). With ~40 tables, the
// upper bound is ~160 entries.
const stmtCacheCap = 512

// initRawConn extracts the underlying driver.Conn from the *sql.Conn via
// Raw(). This gives us direct access to the mattn driver's conn (bypassing
// database/sql.withLock) WITHOUT opening a second connection — critical
// because a second BEGIN CONCURRENT on a separate conn would create a
// second WAL2 snapshot, blocking the write-worker's commits under load
// (SQLITE_BUSY_SNAPSHOT).
func (s *Snapshot) initRawConn() error {
	return s.conn.Raw(func(raw any) error {
		dc, ok := raw.(driver.Conn)
		if !ok {
			return fmt.Errorf("snapshotter: underlying conn is not a driver.Conn (got %T)", raw)
		}
		s.rawConn = dc
		return nil
	})
}

// rawExec runs a no-result statement (BEGIN/ROLLBACK) on the raw conn.
func (s *Snapshot) rawExec(ctx context.Context, query string) error {
	ec, ok := s.rawConn.(driver.ExecerContext)
	if !ok {
		return fmt.Errorf("snapshotter: raw conn does not implement ExecerContext")
	}
	_, err := ec.ExecContext(ctx, query, nil)
	return err
}

// rawStateVersion reads the replication state version on the raw conn.
func (s *Snapshot) rawStateVersion(ctx context.Context) (string, error) {
	qc, ok := s.rawConn.(driver.QueryerContext)
	if !ok {
		return "", fmt.Errorf("snapshotter: raw conn does not implement QueryerContext")
	}
	rows, err := qc.QueryContext(ctx, `SELECT stateVersion FROM "_zero.replicationState"`, nil)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return "", fmt.Errorf("snapshotter: stateVersion row: %w", err)
	}
	switch v := dest[0].(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("snapshotter: stateVersion has unexpected type %T", dest[0])
	}
}

// getStmt returns a prepared statement from the cache, preparing a fresh
// one on first use. The advance path is serialized (e.mu + group.mu), so
// only one goroutine accesses the cache at a time — no locking needed.
// Cursors are fully consumed before the next query (no nesting), so a
// simple get-or-prepare without checkout/remove is safe.
func (s *Snapshot) getStmt(ctx context.Context, query string) (driver.Stmt, error) {
	if s.stmts == nil {
		s.stmts = make(map[string]*snapshotStmt, 64)
	}
	if e, ok := s.stmts[query]; ok {
		return e.st, nil
	}
	pc, ok := s.rawConn.(driver.ConnPrepareContext)
	if !ok {
		return nil, fmt.Errorf("snapshotter: raw conn does not implement ConnPrepareContext")
	}
	st, err := pc.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	s.stmts[query] = &snapshotStmt{st: st}
	if len(s.stmts) > stmtCacheCap {
		s.evictOldestStmt()
	}
	return st, nil
}

// evictOldestStmt closes and drops the oldest prepared statement when the
// cache exceeds its bound. Steady-state reuse of existing shapes never
// triggers this.
func (s *Snapshot) evictOldestStmt() {
	var oldestKey string
	found := false
	for k := range s.stmts {
		if !found {
			oldestKey = k
			found = true
		}
	}
	if found {
		if e, ok := s.stmts[oldestKey]; ok {
			_ = e.st.Close()
			delete(s.stmts, oldestKey)
		}
	}
}

// finalizeStmts closes all cached prepared statements. Called on snapshot close.
func (s *Snapshot) finalizeStmts() {
	for k, e := range s.stmts {
		_ = e.st.Close()
		delete(s.stmts, k)
	}
}

// rawQueryRow executes a query returning a single row via the raw conn's
// stmt cache. Returns (rowMap, found, error). A nil rowMap with found=false
// means no matching row (io.EOF from the driver).
func (s *Snapshot) rawQueryRow(ctx context.Context, query string, args []driver.Value, colNames []string) (map[string]any, bool, error) {
	st, err := s.getStmt(ctx, query)
	if err != nil {
		return nil, false, err
	}
	rows, err := st.Query(args)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	dest := make([]driver.Value, len(colNames))
	err = rows.Next(dest)
	if err == io.EOF {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	raw := make(map[string]any, len(colNames))
	for i, c := range colNames {
		raw[c] = dest[i]
	}
	return raw, true, nil
}

// rawQueryRows executes a query returning multiple rows via the raw conn's
// stmt cache. Returns a slice of row maps.
func (s *Snapshot) rawQueryRows(ctx context.Context, query string, args []driver.Value, colNames []string) ([]map[string]any, error) {
	st, err := s.getStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := st.Query(args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	dest := make([]driver.Value, len(colNames))
	for {
		err = rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		raw := make(map[string]any, len(colNames))
		for i, c := range colNames {
			raw[c] = dest[i]
		}
		out = append(out, raw)
	}
	return out, nil
}

// rawQueryInt executes a query returning a single integer value via the
// stmt cache.
func (s *Snapshot) rawQueryInt(ctx context.Context, query string, args []driver.Value) (int, error) {
	st, err := s.getStmt(ctx, query)
	if err != nil {
		return 0, err
	}
	rows, err := st.Query(args)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return 0, fmt.Errorf("snapshotter: count row: %w", err)
	}
	switch v := dest[0].(type) {
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("snapshotter: count has unexpected type %T", dest[0])
	}
}
