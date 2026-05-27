package tablesource

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "modernc.org/sqlite"
)

// seedTypedReplica builds a WAL-mode SQLite with a typed table the Source
// can introspect. Returns the path so each test gets a clean fixture via
// the t.TempDir lifecycle.
func seedTypedReplica(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE users (
			id    INTEGER PRIMARY KEY,
			name  TEXT,
			score INTEGER,
			active INTEGER
		)`,
		`INSERT INTO users VALUES
			(1, 'alice', 90, 1),
			(2, 'bob',   80, 0),
			(3, 'carol', 70, 1)`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	return path
}

func userSchema() map[string]sqlite.ColumnSchema {
	return map[string]sqlite.ColumnSchema{
		"id":     {Type: "number"},
		"name":   {Type: "string"},
		"score":  {Type: "number"},
		"active": {Type: "boolean"},
	}
}

func newUserSource(t *testing.T) (*Source, *sql.DB) {
	t.Helper()
	db, err := Open(seedTypedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	src, err := New(db, "users", userSchema(), []string{"id"})
	if err != nil {
		db.Close()
		t.Fatalf("New: %v", err)
	}
	return src, db
}

func TestNewRejectsUnknownTable(t *testing.T) {
	db, err := Open(seedTypedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	if _, err := New(db, "nonexistent", userSchema(), []string{"id"}); err == nil {
		t.Fatalf("New on missing table succeeded; expected error")
	}
}

func TestNewRejectsEmptyPK(t *testing.T) {
	db, err := Open(seedTypedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	if _, err := New(db, "users", userSchema(), nil); err == nil {
		t.Fatalf("New with empty PK succeeded; expected error")
	}
}

func TestNewRejectsPKNotInColumns(t *testing.T) {
	db, err := Open(seedTypedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cols := userSchema()
	if _, err := New(db, "users", cols, []string{"missing_col"}); err == nil {
		t.Fatalf("New with PK not in columns succeeded; expected error")
	}
}

func TestTableNameAndPrimaryKey(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	if src.TableName() != "users" {
		t.Fatalf("TableName = %q, want users", src.TableName())
	}
	if !reflect.DeepEqual(src.PrimaryKey(), []string{"id"}) {
		t.Fatalf("PrimaryKey = %v, want [id]", src.PrimaryKey())
	}
}

func TestNormalizeRow(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// Mimic a row arriving from msgpack: numbers as float64, booleans as
	// JSON-shaped 1/0 numbers. NormalizeRow must coerce to TS shapes.
	row := ivm.Row{
		"id":     int64(7),     // → float64 via "number"
		"name":   "dave",       // → string (untouched)
		"score":  float64(42.5),
		"active": int64(1), // → bool true via "boolean"
		"junk":   "ignored",    // not in schema; left as-is
	}
	src.NormalizeRow(row)

	if row["id"] != float64(7) {
		t.Errorf("id = %#v, want float64(7)", row["id"])
	}
	if row["active"] != true {
		t.Errorf("active = %#v, want true", row["active"])
	}
	if row["name"] != "dave" {
		t.Errorf("name = %#v, want dave", row["name"])
	}
	if row["junk"] != "ignored" {
		t.Errorf("junk = %#v, want ignored (untouched)", row["junk"])
	}
}

func TestFetchDefaultOrderingByPK(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	nodes := in.Fetch(ivm.FetchRequest{})
	if len(nodes) != 3 {
		t.Fatalf("got %d rows, want 3", len(nodes))
	}
	wantIDs := []float64{1, 2, 3}
	for i, n := range nodes {
		if n.Row["id"] != wantIDs[i] {
			t.Errorf("row[%d].id = %#v, want %v", i, n.Row["id"], wantIDs[i])
		}
	}
}

func TestFetchExplicitSortDesc(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(ivm.Ordering{{"score", "desc"}, {"id", "asc"}}, nil, nil)
	nodes := in.Fetch(ivm.FetchRequest{})
	wantIDs := []float64{1, 2, 3} // scores 90, 80, 70
	if len(nodes) != 3 {
		t.Fatalf("got %d rows, want 3", len(nodes))
	}
	for i, n := range nodes {
		if n.Row["id"] != wantIDs[i] {
			t.Errorf("row[%d].id = %#v, want %v", i, n.Row["id"], wantIDs[i])
		}
	}
}

func TestFetchReverseFlipsDirection(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	nodes := in.Fetch(ivm.FetchRequest{Reverse: true})
	wantIDs := []float64{3, 2, 1}
	if len(nodes) != 3 {
		t.Fatalf("got %d rows, want 3", len(nodes))
	}
	for i, n := range nodes {
		if n.Row["id"] != wantIDs[i] {
			t.Errorf("row[%d].id = %#v, want %v", i, n.Row["id"], wantIDs[i])
		}
	}
}

func TestFetchFilterPredicate(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// Only active=true users.
	in := src.Connect(nil, func(r ivm.Row) bool {
		v, _ := r["active"].(bool)
		return v
	}, nil)
	nodes := in.Fetch(ivm.FetchRequest{})
	wantIDs := []float64{1, 3}
	if len(nodes) != 2 {
		t.Fatalf("got %d rows, want 2 (active only)", len(nodes))
	}
	for i, n := range nodes {
		if n.Row["id"] != wantIDs[i] {
			t.Errorf("row[%d].id = %#v, want %v", i, n.Row["id"], wantIDs[i])
		}
	}
}

func TestPushPanicsPhase2a(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Push did not panic; Phase 2a must fail loud")
		}
	}()
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": float64(99)}))
}

func TestFetchPanicsOnConstraint(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Fetch with Constraint did not panic; Phase 2a must fail loud")
		}
	}()
	c := ivm.Constraint{"id": float64(1)}
	in.Fetch(ivm.FetchRequest{Constraint: &c})
}

func TestFetchPanicsOnStart(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Fetch with Start did not panic; Phase 2a must fail loud")
		}
	}()
	in.Fetch(ivm.FetchRequest{Start: &ivm.Start{Row: ivm.Row{"id": float64(1)}, Basis: "at"}})
}

func TestDestroyRemovesConnection(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	src.mu.Lock()
	before := len(src.connections)
	src.mu.Unlock()
	if before != 1 {
		t.Fatalf("connections before destroy = %d, want 1", before)
	}
	in.Destroy()
	src.mu.Lock()
	after := len(src.connections)
	src.mu.Unlock()
	if after != 0 {
		t.Fatalf("connections after destroy = %d, want 0", after)
	}
}
