package snapshotter

import (
	"sort"
	"strings"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// TableSpec is the subset of TS's LiteTableSpecWithKeysAndVersion that the
// Diff needs. Mirrors the fields snapshotter.ts reads off tableSpec/zqlSpec:
//
//   - Name          → id(table.name) in SELECTs.
//   - Columns       → Object.keys(table.columns) for the SELECT column list,
//     and the per-column type that drives fromSQLiteTypes coercion.
//     MUST include `_0_version` (the diff-validity checks read it).
//   - UniqueKeys    → tableSpec.uniqueKeys for getRows() conflict detection.
//     Each inner slice is one unique key (composite keys allowed); always
//     includes the primary key.
//   - MinRowVersion → tableSpec.minRowVersion for the catch-up invariant assert.
type TableSpec struct {
	Name          string
	Columns       map[string]sqlite.ColumnSchema
	UniqueKeys    [][]string
	MinRowVersion string
}

// cols returns the table's column names in a stable (sorted) order, so the
// generated SELECT column list is deterministic. TS gets this implicitly from
// Object.keys insertion order; we sort to be reproducible across runs.
func (t *TableSpec) cols() []string {
	cols := make([]string, 0, len(t.Columns))
	for c := range t.Columns {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

// selectColList renders the column list for a SELECT. Each column is wrapped
// in SQLite's unary `+` no-op and aliased back to its bare name — an
// expression result carries no declared type, which disables
// mattn/go-sqlite3's decltype-driven conversions (nullable temporal columns
// → time.Time with a |v| <= 1e12 ⇒ seconds heuristic that multiplied
// pre-2001 epoch-ms values by 1000; boolean columns → `val > 0`). TS's
// better-sqlite3 ships raw cells; this makes the Go read byte-identical.
// Shared rationale: sqlite.BuildSelectQuery (query_builder.go).
func (t *TableSpec) selectColList() string {
	cols := t.cols()
	quoted := make([]string, len(cols))
	for i, c := range cols {
		q := quoteIdent(c)
		quoted[i] = "+" + q + " AS " + q
	}
	return strings.Join(quoted, ",")
}

// quoteIdent double-quotes a SQLite identifier, escaping embedded quotes.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// sortedKeys returns the keys of m in ascending order — TS's
// normalizedKeyOrder applied to a rowKey before building its WHERE clause.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
