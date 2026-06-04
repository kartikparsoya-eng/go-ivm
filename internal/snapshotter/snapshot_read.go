package snapshotter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// changeLogEntry is one row of the diff cursor — the fields snapshotter.ts's
// changeLogEntrySchema keeps (pos and backfillingColumnVersions excluded).
// rowKey is the raw JSON text; for table-wide ops (t/r) it carries the version
// rather than a row key, but those ops abort before rowKey is parsed.
type changeLogEntry struct {
	stateVersion string
	table        string
	rowKey       string
	op           string
}

// NumChangesSince counts change-log entries with stateVersion > prevVersion.
// Mirrors numChangesSince() (318-324).
func (s *Snapshot) NumChangesSince(prevVersion string) (int, error) {
	ctx := context.Background()
	var count int
	err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM "_zero.changeLog2" WHERE stateVersion > ?`,
		prevVersion).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("snapshotter: numChangesSince: %w", err)
	}
	return count, nil
}

// ChangesSince returns the change-log entries in (prevVersion, head], ordered
// by (stateVersion ASC, pos ASC). Mirrors changesSince() (326-337).
//
// Deviation from TS (forced, not optional): TS returns a streaming cursor and
// reads row CONTENTS lazily per entry. Go's database/sql allows only ONE active
// query per *sql.Conn, and the diff must issue getRow on this same (curr) conn
// while iterating — so we buffer the (small, identifier-only) change-log
// entries first, freeing the conn for the per-entry getRow/getRows. Row
// CONTENTS are still fetched lazily per entry, so memory stays bounded by the
// number of change-log entries (each a few hundred bytes), not by row data.
// The change log holds at most one entry per row (UNIQUE(table,rowKey)), so the
// buffer is bounded by the catch-up size exactly as TS's cursor is.
func (s *Snapshot) ChangesSince(prevVersion string) ([]changeLogEntry, error) {
	ctx := context.Background()
	rows, err := s.conn.QueryContext(ctx,
		`SELECT "stateVersion", "table", "rowKey", "op" FROM "_zero.changeLog2"
		   WHERE "stateVersion" > ? ORDER BY "stateVersion" ASC, "pos" ASC`,
		prevVersion)
	if err != nil {
		return nil, fmt.Errorf("snapshotter: changesSince: %w", err)
	}
	defer rows.Close()

	var out []changeLogEntry
	for rows.Next() {
		var e changeLogEntry
		if err := rows.Scan(&e.stateVersion, &e.table, &e.rowKey, &e.op); err != nil {
			return nil, fmt.Errorf("snapshotter: changesSince scan: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("snapshotter: changesSince rows: %w", err)
	}
	return out, nil
}

// GetRow reads a single row's RAW contents at this snapshot's frame, keyed by
// the given rowKey columns. Returns (raw, found, err). Mirrors getRow() (339).
//
// "Raw" means Go-native SQLite scan types (string/int64/float64/[]byte/nil) —
// NOT yet coerced via FromSQLiteType. The Diff runs its version + permissions
// checks on raw values (matching TS, which reads them off the better-sqlite3
// row before fromSQLiteTypes) and coerces only at emit time.
func (s *Snapshot) GetRow(spec *TableSpec, rowKey map[string]any) (map[string]any, bool, error) {
	keyCols := sortedKeys(rowKey)
	conds := make([]string, len(keyCols))
	binds := make([]any, len(keyCols))
	for i, c := range keyCols {
		conds[i] = quoteIdent(c) + "=?"
		binds[i] = rowKey[c]
	}
	q := "SELECT " + spec.selectColList() +
		" FROM " + quoteIdent(spec.Name) +
		" WHERE " + strings.Join(conds, " AND ")

	row := s.conn.QueryRowContext(context.Background(), q, binds...)
	raw, err := scanRawRow(row, spec.cols())
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("snapshotter: getRow %s: %w", spec.Name, err)
	}
	return raw, true, nil
}

// GetRows reads all rows that conflict on ANY unique key with the given row —
// the rows IVM must REMOVE before adding nextValue. Mirrors getRows() (357-390).
//
// Unique keys with any NULL/absent column are filtered out: NULL can't violate
// uniqueness (NULL != NULL in SQL) AND SQLite's MULTI-INDEX-OR optimization
// collapses to a full table scan when any OR branch binds NULL (PR #5542).
func (s *Snapshot) GetRows(spec *TableSpec, uniqueKeys [][]string, row map[string]any) ([]map[string]any, error) {
	var validKeys [][]string
	for _, key := range uniqueKeys {
		ok := true
		for _, c := range key {
			if v, present := row[c]; !present || v == nil {
				ok = false
				break
			}
		}
		if ok {
			validKeys = append(validKeys, key)
		}
	}
	if len(validKeys) == 0 {
		return nil, nil
	}

	orConds := make([]string, len(validKeys))
	var binds []any
	for i, key := range validKeys {
		andConds := make([]string, len(key))
		for j, c := range key {
			andConds[j] = quoteIdent(c) + "=?"
			binds = append(binds, row[c])
		}
		orConds[i] = "(" + strings.Join(andConds, " AND ") + ")"
	}
	q := "SELECT " + spec.selectColList() +
		" FROM " + quoteIdent(spec.Name) +
		" WHERE " + strings.Join(orConds, " OR ")

	rows, err := s.conn.QueryContext(context.Background(), q, binds...)
	if err != nil {
		return nil, fmt.Errorf("snapshotter: getRows %s: %w", spec.Name, err)
	}
	defer rows.Close()

	cols := spec.cols()
	var out []map[string]any
	for rows.Next() {
		raw, err := scanRawRow(rows, cols)
		if err != nil {
			return nil, fmt.Errorf("snapshotter: getRows %s scan: %w", spec.Name, err)
		}
		out = append(out, raw)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("snapshotter: getRows %s rows: %w", spec.Name, err)
	}
	return out, nil
}

// rowScanner abstracts *sql.Row and *sql.Rows for scanRawRow.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanRawRow scans the current row into a name→value map using Go-native
// SQLite scan types (the driver yields string/int64/float64/[]byte/nil).
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
