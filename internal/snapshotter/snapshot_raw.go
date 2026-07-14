package snapshotter

import (
	"context"
	"database/sql"
	"io"
)

// snapshotStmt is a cached prepared statement on the snapshot's *sql.Conn.
type snapshotStmt struct {
	st *sql.Stmt
}

// stmtCacheCap bounds the per-snapshot prepared-statement cache. Each
// snapshot serves GetRow (1 shape per table), GetRows (1-3 shapes per table
// depending on which unique keys are non-NULL), NumChangesSince (1 shape),
// ChangesSince (1 shape), and stateVersion (1 shape). With ~40 tables, the
// upper bound is ~160 entries.
const stmtCacheCap = 512

// getStmt returns a prepared statement from the cache, preparing a fresh
// one on first use. The advance path is serialized (e.mu + group.mu), so
// only one goroutine accesses the cache at a time — no locking needed.
// Cursors are fully consumed before the next query (no nesting), so a
// simple get-or-prepare without checkout/remove is safe.
func (s *Snapshot) getStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	if s.stmts == nil {
		s.stmts = make(map[string]*snapshotStmt, 64)
	}
	if e, ok := s.stmts[query]; ok {
		return e.st, nil
	}
	st, err := s.conn.PrepareContext(ctx, query)
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

// cachedQueryRow executes a query returning a single row via the stmt cache.
// Returns (rowMap, found, error). A nil rowMap with found=false means no
// matching row (sql.ErrNoRows).
func (s *Snapshot) cachedQueryRow(ctx context.Context, query string, args []any, colNames []string) (map[string]any, bool, error) {
	st, err := s.getStmt(ctx, query)
	if err != nil {
		return nil, false, err
	}
	row := st.QueryRowContext(ctx, args...)
	raw, err := scanRawRow(row, colNames)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// cachedQueryRows executes a query returning multiple rows via the stmt cache.
func (s *Snapshot) cachedQueryRows(ctx context.Context, query string, args []any, colNames []string) ([]map[string]any, error) {
	st, err := s.getStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := st.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		raw, err := scanRawRow(rows, colNames)
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// cachedQueryInt executes a query returning a single integer via the stmt cache.
func (s *Snapshot) cachedQueryInt(ctx context.Context, query string, args ...any) (int, error) {
	st, err := s.getStmt(ctx, query)
	if err != nil {
		return 0, err
	}
	var count int
	err = st.QueryRowContext(ctx, args...).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// rowScanner abstracts *sql.Row and *sql.Rows for scanRawRow.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanRawRow scans the current row into a name→value map using Go-native
// SQLite scan types (string/int64/float64/[]byte/nil). selectColList wraps
// every column in the unary-+ no-op, which strips the declared type and so
// disables mattn/go-sqlite3's decltype conversions — a nullable temporal
// column arrives as its raw int64 epoch-ms (never time.Time), exactly what
// TS's better-sqlite3 yields. coerceRow → sqlite.FromSQLiteType applies the
// logical-type coercion at emit; FromSQLiteType panics if a time.Time ever
// reaches it (a SELECT site missing the wrap).
func scanRawRow(sc rowScanner, cols []string) (map[string]any, error) {
	dest := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	if err := sc.Scan(ptrs...); err != nil {
		return nil, err
	}
	raw := make(map[string]any, len(cols))
	for i, c := range cols {
		raw[c] = dest[i]
	}
	return raw, nil
}

var _ = io.EOF
