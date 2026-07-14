package snapshotter

import (
	"context"
	"database/sql/driver"
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
// Mirrors numChangesSince() (318-324). Uses the raw conn's stmt cache.
func (s *Snapshot) NumChangesSince(prevVersion string) (int, error) {
	ctx := context.Background()
	return s.rawQueryInt(ctx,
		`SELECT COUNT(*) FROM "_zero.changeLog2" WHERE stateVersion > ?`,
		[]driver.Value{prevVersion})
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
//
// With the raw conn, GetRow/GetRows use a separate conn from ChangesSince,
// so a streaming cursor would be possible — but the buffer is small and the
// current contract is well-tested, so we keep it.
func (s *Snapshot) ChangesSince(prevVersion string) ([]changeLogEntry, error) {
	ctx := context.Background()
	colNames := []string{"stateVersion", "table", "rowKey", "op"}
	rows, err := s.rawQueryRows(ctx,
		`SELECT "stateVersion", "table", "rowKey", "op" FROM "_zero.changeLog2"
		   WHERE "stateVersion" > ? ORDER BY "stateVersion" ASC, "pos" ASC`,
		[]driver.Value{prevVersion}, colNames)
	if err != nil {
		return nil, fmt.Errorf("snapshotter: changesSince: %w", err)
	}
	out := make([]changeLogEntry, 0, len(rows))
	for _, r := range rows {
		var e changeLogEntry
		if v, ok := r["stateVersion"]; ok {
			e.stateVersion, _ = v.(string)
		}
		if v, ok := r["table"]; ok {
			e.table, _ = v.(string)
		}
		if v, ok := r["rowKey"]; ok {
			e.rowKey, _ = v.(string)
		}
		if v, ok := r["op"]; ok {
			e.op, _ = v.(string)
		}
		out = append(out, e)
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
//
// Uses the raw conn's prepared-statement cache (eliminates
// sqlite3_prepare_v2 overhead) and bypasses database/sql.withLock.
func (s *Snapshot) GetRow(spec *TableSpec, rowKey map[string]any) (map[string]any, bool, error) {
	keyCols := sortedKeys(rowKey)
	conds := make([]string, len(keyCols))
	binds := make([]driver.Value, len(keyCols))
	for i, c := range keyCols {
		conds[i] = quoteIdent(c) + "=?"
		binds[i] = rowKey[c]
	}
	q := "SELECT " + spec.selectColList() +
		" FROM " + quoteIdent(spec.Name) +
		" WHERE " + strings.Join(conds, " AND ")

	colNames := spec.cols()
	raw, found, err := s.rawQueryRow(context.Background(), q, binds, colNames)
	if err != nil {
		return nil, false, fmt.Errorf("snapshotter: getRow %s: %w", spec.Name, err)
	}
	return raw, found, nil
}

// GetRows reads all rows that conflict on ANY unique key with the given row —
// the rows IVM must REMOVE before adding nextValue. Mirrors getRows() (357-390).
//
// Unique keys with any NULL/absent column are filtered out: NULL can't violate
// uniqueness (NULL != NULL in SQL) AND SQLite's MULTI-INDEX-OR optimization
// collapses to a full table scan when any OR branch binds NULL.
//
// Uses the raw conn's prepared-statement cache and bypasses database/sql.
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
	var binds []driver.Value
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

	colNames := spec.cols()
	out, err := s.rawQueryRows(context.Background(), q, binds, colNames)
	if err != nil {
		return nil, fmt.Errorf("snapshotter: getRows %s: %w", spec.Name, err)
	}
	return out, nil
}
