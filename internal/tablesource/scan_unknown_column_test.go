package tablesource

// Regression test for faithfulness #8: a SELECTed column absent from the
// synced schema must panic like TS fromSQLiteTypes throws
// (table-source.ts:608-614) — it signals schema drift (e.g. a replica column
// added after the spec was loaded). Pre-fix Go silently dropped the column,
// shipping rows missing data with no signal.

import (
	"slices"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestScanRowsUnknownColumnPanics(t *testing.T) {
	path := seedTypedReplica(t)
	w := openWritableForTest(t, path)
	t.Cleanup(func() { w.Close() })
	// Simulate schema drift: the replica gains a column the synced schema
	// (userSchema: id/name/score/active) doesn't know about.
	if _, err := w.Exec(`ALTER TABLE users ADD COLUMN extra TEXT`); err != nil {
		t.Fatalf("ALTER TABLE: %v", err)
	}

	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	src, err := New(db, w, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	in := src.Connect(nil, nil, nil, nil)
	si, ok := in.(*sourceInput)
	if !ok {
		t.Fatalf("Connect returned %T, want *sourceInput", in)
	}

	// SELECT * returns the drifted column set — the shape scanRows would see
	// after a replica schema change.
	rows, err := db.Query(`SELECT * FROM users`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	colNames, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if !slices.Contains(colNames, "extra") {
		t.Fatalf("fixture broken: SELECT * columns %v missing drifted column", colNames)
	}

	mustPanicTS(t,
		`Invalid column "extra" for table "users". Synced columns include active, id, name, score`,
		func() {
			src.scanRows(rows, colNames, si.conn, ivm.FetchRequest{}, false)
		})
}
