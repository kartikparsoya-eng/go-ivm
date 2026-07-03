package tablesource

import (
	"database/sql"
	"slices"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// Executes the multiConstraints SQL that BuildSelectQuery generates against
// a REAL SQLite database — proving the single-column IN and compound
// `(a,b) IN (VALUES …)` forms are valid SQLite and return exactly the
// matching rows in order (TS verifies the same via its query-builder tests
// + EXPLAIN QUERY PLAN, zero 1.7.0 #5928).

func execSelect(t *testing.T, db *sql.DB, q sqlite.QueryResult) []map[string]any {
	t.Helper()
	rows, err := db.Query(q.SQL, q.Params...)
	if err != nil {
		t.Fatalf("query %q: %v", q.SQL, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var out []map[string]any
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			m[c] = raw[i]
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func TestMultiConstraintSQLExecutesOnSQLite(t *testing.T) {
	path := newWALFixture(t)
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE tk (id TEXT, org TEXT, n REAL, PRIMARY KEY (id))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range [][3]any{
		{"a", "o1", 1.0}, {"b", "o1", 2.0}, {"c", "o2", 3.0}, {"d", "o2", 4.0}, {"e", "o3", 5.0},
	} {
		if _, err := db.Exec(`INSERT INTO tk VALUES (?,?,?)`, r[0], r[1], r[2]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	cols := map[string]sqlite.ColumnSchema{
		"id":  {Type: "string"},
		"org": {Type: "string"},
		"n":   {Type: "number"},
	}
	order := ivm.Ordering{{"id", "asc"}}

	t.Run("single column IN", func(t *testing.T) {
		q := sqlite.BuildSelectQuery("tk", cols, nil, nil, order, false, nil,
			[]ivm.MultiConstraint{{{"id": "d"}, {"id": "a"}, {"id": "c"}}})
		got := execSelect(t, db, q)
		var gotIDs []string
		for _, m := range got {
			gotIDs = append(gotIDs, m["id"].(string))
		}
		if want := []string{"a", "c", "d"}; !slices.Equal(gotIDs, want) {
			t.Fatalf("ids = %v, want %v", gotIDs, want)
		}
	})

	t.Run("compound VALUES IN", func(t *testing.T) {
		q := sqlite.BuildSelectQuery("tk", cols, nil, nil, order, false, nil,
			[]ivm.MultiConstraint{{
				{"id": "b", "org": "o1"},
				{"id": "c", "org": "o2"},
				{"id": "e", "org": "WRONG"}, // must not match — tuple compare
			}})
		got := execSelect(t, db, q)
		var gotIDs []string
		for _, m := range got {
			gotIDs = append(gotIDs, m["id"].(string))
		}
		if want := []string{"b", "c"}; !slices.Equal(gotIDs, want) {
			t.Fatalf("ids = %v, want %v", gotIDs, want)
		}
	})

	t.Run("two multis AND with constraint", func(t *testing.T) {
		constraint := ivm.Constraint{"org": "o2"}
		q := sqlite.BuildSelectQuery("tk", cols, &constraint, nil, order, false, nil,
			[]ivm.MultiConstraint{
				{{"id": "c"}, {"id": "d"}, {"id": "e"}},
				{{"n": float64(4)}, {"n": float64(5)}},
			})
		got := execSelect(t, db, q)
		if len(got) != 1 || got[0]["id"].(string) != "d" {
			t.Fatalf("rows = %v, want just d (org=o2 AND id∈{c,d,e} AND n∈{4,5})", got)
		}
	})
}

// TestApplyOverlayGatedByMultiConstraints pins the overlay window gate
// (TS applyMultiConstraintsToOverlays): an in-flight ADD that matches no
// multi entry must NOT be spliced into a batched fetch's results; an edit
// whose old row is outside the multi window degrades to a pure add.
func TestApplyOverlayGatedByMultiConstraints(t *testing.T) {
	cmp := ivm.MakeComparator(ivm.Ordering{{"id", "asc"}}, false)
	pk := []string{"id"}
	base := []ivm.Node{{Row: ivm.Row{"id": "a"}}, {Row: ivm.Row{"id": "c"}}}
	multis := []ivm.MultiConstraint{{{"id": "a"}, {"id": "b"}, {"id": "c"}}}

	t.Run("matching add spliced", func(t *testing.T) {
		out := applyOverlay(slices.Clone(base),
			ivm.MakeSourceChangeAdd(ivm.Row{"id": "b"}), cmp, nil, multis, nil, pk)
		if len(out) != 3 || out[1].Row["id"] != "b" {
			t.Fatalf("out = %v, want a,b,c", out)
		}
	})
	t.Run("non-matching add dropped", func(t *testing.T) {
		out := applyOverlay(slices.Clone(base),
			ivm.MakeSourceChangeAdd(ivm.Row{"id": "z"}), cmp, nil, multis, nil, pk)
		if len(out) != 2 {
			t.Fatalf("out = %v, want overlay add outside multi window dropped", out)
		}
	})
	t.Run("edit old row outside window degrades to add", func(t *testing.T) {
		// old z (outside multi) -> new b (inside): nothing to remove from
		// the IN-filtered set; the new row is spliced.
		out := applyOverlay(slices.Clone(base),
			ivm.SourceChange{Type: ivm.ChangeTypeEdit, Row: ivm.Row{"id": "b"}, OldRow: ivm.Row{"id": "z"}},
			cmp, nil, multis, nil, pk)
		if len(out) != 3 || out[1].Row["id"] != "b" {
			t.Fatalf("out = %v, want a,b,c (edit degraded to add)", out)
		}
	})
	t.Run("splice plan mirrors gate", func(t *testing.T) {
		add, remove := overlaySplicePlan(
			ivm.MakeSourceChangeAdd(ivm.Row{"id": "z"}), cmp, nil, multis, nil)
		if add != nil || remove != nil {
			t.Fatalf("plan = add %v remove %v, want both nil", add, remove)
		}
		add, _ = overlaySplicePlan(
			ivm.MakeSourceChangeAdd(ivm.Row{"id": "b"}), cmp, nil, multis, nil)
		if add == nil || add["id"] != "b" {
			t.Fatalf("plan add = %v, want b", add)
		}
	})
}
