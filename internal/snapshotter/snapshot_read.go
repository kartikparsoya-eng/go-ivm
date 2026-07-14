package snapshotter

import (
	"context"
	"fmt"
	"strings"
)

// changeLogEntry is one row of the diff cursor.
type changeLogEntry struct {
	stateVersion string
	table        string
	rowKey       string
	op           string
}

// NumChangesSince counts change-log entries with stateVersion > prevVersion.
// Uses the stmt cache to avoid re-preparing on every call.
func (s *Snapshot) NumChangesSince(prevVersion string) (int, error) {
	ctx := context.Background()
	return s.cachedQueryInt(ctx,
		`SELECT COUNT(*) FROM "_zero.changeLog2" WHERE stateVersion > ?`,
		prevVersion)
}

// ChangesSince returns the change-log entries in (prevVersion, head], ordered
// by (stateVersion ASC, pos ASC).
func (s *Snapshot) ChangesSince(prevVersion string) ([]changeLogEntry, error) {
	ctx := context.Background()
	colNames := []string{"stateVersion", "table", "rowKey", "op"}
	rows, err := s.cachedQueryRows(ctx,
		`SELECT "stateVersion", "table", "rowKey", "op" FROM "_zero.changeLog2"
		   WHERE "stateVersion" > ? ORDER BY "stateVersion" ASC, "pos" ASC`,
		[]any{prevVersion}, colNames)
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
// the given rowKey columns. Returns (raw, found, err).
//
// Uses the stmt cache to eliminate sqlite3_prepare_v2 overhead.
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

	colNames := spec.cols()
	raw, found, err := s.cachedQueryRow(context.Background(), q, binds, colNames)
	if err != nil {
		return nil, false, fmt.Errorf("snapshotter: getRow %s: %w", spec.Name, err)
	}
	return raw, found, nil
}

// GetRows reads all rows that conflict on ANY unique key with the given row.
// Uses the stmt cache to eliminate sqlite3_prepare_v2 overhead.
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

	colNames := spec.cols()
	out, err := s.cachedQueryRows(context.Background(), q, binds, colNames)
	if err != nil {
		return nil, fmt.Errorf("snapshotter: getRows %s: %w", spec.Name, err)
	}
	return out, nil
}
