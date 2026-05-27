package tablesource

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// seedTypedReplica builds a WAL-mode SQLite with a typed table the Source
// can introspect. Returns the path so each test gets a clean fixture via
// the t.TempDir lifecycle.
func seedTypedReplica(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
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

// recordingOutput is a test sink that records every Change it gets
// pushed and returns no downstream changes. Used to verify Push fanout
// hits the right connections with the right Change shape.
type recordingOutput struct {
	pushed []ivm.Change
}

func (r *recordingOutput) Push(c ivm.Change, _ ivm.InputBase) []ivm.Change {
	r.pushed = append(r.pushed, c)
	return nil
}

func TestPushFanoutAddToConnection(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	row := ivm.Row{"id": float64(99), "name": "dave", "score": float64(50), "active": true}
	src.Push(ivm.MakeSourceChangeAdd(row))

	if len(rec.pushed) != 1 {
		t.Fatalf("got %d pushed changes, want 1", len(rec.pushed))
	}
	if rec.pushed[0].Type != ivm.ChangeTypeAdd {
		t.Errorf("change.Type = %v, want Add", rec.pushed[0].Type)
	}
	if rec.pushed[0].Node.Row["id"] != float64(99) {
		t.Errorf("change.Node.Row.id = %#v, want 99", rec.pushed[0].Node.Row["id"])
	}
}

func TestPushFilterDropsRow(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, func(r ivm.Row) bool { v, _ := r["active"].(bool); return v }, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	// active=false row → should be dropped by the filter, no Push downstream
	row := ivm.Row{"id": float64(99), "name": "dave", "score": float64(50), "active": false}
	src.Push(ivm.MakeSourceChangeAdd(row))

	if len(rec.pushed) != 0 {
		t.Fatalf("filter should have dropped row, got %d pushed", len(rec.pushed))
	}
}

func TestPushEditFilterTransitionEmitsRemove(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, func(r ivm.Row) bool { v, _ := r["active"].(bool); return v }, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	// Edit alice (active=true → active=false). Old passed filter, new doesn't:
	// downstream sees Remove for the old row.
	oldRow := ivm.Row{"id": float64(1), "name": "alice", "score": float64(90), "active": true}
	newRow := ivm.Row{"id": float64(1), "name": "alice", "score": float64(90), "active": false}
	src.Push(ivm.MakeSourceChangeEdit(newRow, oldRow))

	if len(rec.pushed) != 1 {
		t.Fatalf("got %d pushed, want 1 (Remove)", len(rec.pushed))
	}
	if rec.pushed[0].Type != ivm.ChangeTypeRemove {
		t.Errorf("change.Type = %v, want Remove", rec.pushed[0].Type)
	}
}

func TestPushEditFilterTransitionEmitsAdd(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, func(r ivm.Row) bool { v, _ := r["active"].(bool); return v }, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	// Edit bob (active=false → active=true). Old failed filter, new passes:
	// downstream sees Add for the new row.
	oldRow := ivm.Row{"id": float64(2), "name": "bob", "score": float64(80), "active": false}
	newRow := ivm.Row{"id": float64(2), "name": "bob", "score": float64(80), "active": true}
	src.Push(ivm.MakeSourceChangeEdit(newRow, oldRow))

	if len(rec.pushed) != 1 {
		t.Fatalf("got %d pushed, want 1 (Add)", len(rec.pushed))
	}
	if rec.pushed[0].Type != ivm.ChangeTypeAdd {
		t.Errorf("change.Type = %v, want Add", rec.pushed[0].Type)
	}
}

func TestPushSplitEditEmitsRemoveAdd(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	splitKeys := map[string]bool{"score": true}
	in := src.Connect(nil, nil, splitKeys)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	// Edit alice score 90 → 100. score is a split key, so emit Remove+Add.
	oldRow := ivm.Row{"id": float64(1), "name": "alice", "score": float64(90), "active": true}
	newRow := ivm.Row{"id": float64(1), "name": "alice", "score": float64(100), "active": true}
	src.Push(ivm.MakeSourceChangeEdit(newRow, oldRow))

	if len(rec.pushed) != 2 {
		t.Fatalf("got %d pushed, want 2 (Remove+Add)", len(rec.pushed))
	}
	if rec.pushed[0].Type != ivm.ChangeTypeRemove {
		t.Errorf("change[0].Type = %v, want Remove", rec.pushed[0].Type)
	}
	if rec.pushed[1].Type != ivm.ChangeTypeAdd {
		t.Errorf("change[1].Type = %v, want Add", rec.pushed[1].Type)
	}
}

// overlayProbingOutput re-fetches from the same Source during Push so we
// can assert that the overlay is visible to a downstream Fetch fired
// from inside Output.Push (the contract the legacy MemorySource holds).
type overlayProbingOutput struct {
	in            ivm.Input
	observedAfter int // node count seen by the re-fetch
}

func (o *overlayProbingOutput) Push(_ ivm.Change, _ ivm.InputBase) []ivm.Change {
	nodes := o.in.Fetch(ivm.FetchRequest{})
	o.observedAfter = len(nodes)
	return nil
}

// TestTxPinHidesExternalWriteUntilPushRolls is the Phase 4 contract.
// An external writer (modeling the TS replicator) commits a new row.
// Fetches via Source MUST NOT see it until a Push has rolled the
// pinned read tx — that's the snapshot-consistency guarantee that
// lets overlay produce a correct post-push view inside a Push fanout.
func TestTxPinHidesExternalWriteUntilPushRolls(t *testing.T) {
	path := seedTypedReplica(t)

	// Our read-side pool (query_only, WAL).
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	src, err := New(db, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close()

	// Separate writer connection — models the TS replicator. NOT
	// query_only, so it can actually INSERT.
	writer, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("writer open: %v", err)
	}
	defer writer.Close()

	in := src.Connect(nil, nil, nil)
	in.SetOutput(&recordingOutput{}) // discard, just need a wired output

	// Step 1: first Fetch pins the read tx at the current WAL frame (3 rows).
	if got := len(in.Fetch(ivm.FetchRequest{})); got != 3 {
		t.Fatalf("baseline fetch = %d rows, want 3", got)
	}

	// Step 2: external writer commits a new row.
	if _, err := writer.Exec("INSERT INTO users VALUES (10, 'eve', 100, 1)"); err != nil {
		t.Fatalf("external insert: %v", err)
	}

	// Step 3: re-fetch via Source — pinned tx should still see 3.
	// If the tx weren't pinned, we'd see 4 here and overlay would
	// double-apply during the next Push.
	if got := len(in.Fetch(ivm.FetchRequest{})); got != 3 {
		t.Fatalf("post-external-write fetch = %d rows, want 3 (tx must be pinned)", got)
	}

	// Step 4: trigger a Push so snapshotEpoch bumps in the deferred cleanup.
	// The Push's change itself is irrelevant — what matters is that the
	// deferred snapshotEpoch++ runs and the cache rolls the tx.
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id": float64(99), "name": "dave", "score": float64(50), "active": true,
	}))

	// Step 5: re-fetch — tx must have rolled, now sees external writer's row.
	if got := len(in.Fetch(ivm.FetchRequest{})); got != 4 {
		t.Fatalf("post-Push fetch = %d rows, want 4 (tx must have rolled)", got)
	}
}

func TestOverlayVisibleDuringPush(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	probe := &overlayProbingOutput{in: in}
	in.SetOutput(probe)

	// Before push: SQLite has 3 rows. Push an Add via overlay (no SQLite
	// write). The probe's re-fetch during Push should see 4 (SQL's 3 +
	// the overlay's add).
	row := ivm.Row{"id": float64(99), "name": "dave", "score": float64(50), "active": true}
	src.Push(ivm.MakeSourceChangeAdd(row))

	if probe.observedAfter != 4 {
		t.Fatalf("overlay not visible during push: observed %d nodes, want 4",
			probe.observedAfter)
	}

	// After push completes, overlay clears; re-fetch should be back to 3.
	if got := len(in.Fetch(ivm.FetchRequest{})); got != 3 {
		t.Fatalf("overlay should clear post-Push: got %d nodes, want 3", got)
	}
}

func TestFetchConstraintEquality(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	c := ivm.Constraint{"id": float64(2)}
	nodes := in.Fetch(ivm.FetchRequest{Constraint: &c})
	if len(nodes) != 1 {
		t.Fatalf("got %d rows, want 1", len(nodes))
	}
	if nodes[0].Row["name"] != "bob" {
		t.Fatalf("row.name = %#v, want bob", nodes[0].Row["name"])
	}
}

func TestFetchConstraintMultiKey(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	// score=80 AND active=false → only bob
	c := ivm.Constraint{"score": float64(80), "active": false}
	nodes := in.Fetch(ivm.FetchRequest{Constraint: &c})
	if len(nodes) != 1 {
		t.Fatalf("got %d rows, want 1", len(nodes))
	}
	if nodes[0].Row["id"] != float64(2) {
		t.Fatalf("row.id = %#v, want 2", nodes[0].Row["id"])
	}
}

func TestFetchConstraintNoMatch(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil)
	c := ivm.Constraint{"id": float64(999)}
	nodes := in.Fetch(ivm.FetchRequest{Constraint: &c})
	if len(nodes) != 0 {
		t.Fatalf("got %d rows, want 0 (no match)", len(nodes))
	}
}

func TestFetchConstraintWithFilterAndSort(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// active=true users (cuts bob), sorted DESC by score → carol then alice
	in := src.Connect(
		ivm.Ordering{{"score", "asc"}},
		func(r ivm.Row) bool { v, _ := r["active"].(bool); return v },
		nil,
	)
	c := ivm.Constraint{"active": true}
	nodes := in.Fetch(ivm.FetchRequest{Constraint: &c, Reverse: true})
	wantIDs := []float64{1, 3} // score 90 then 70 (DESC of asc = DESC)
	if len(nodes) != 2 {
		t.Fatalf("got %d rows, want 2", len(nodes))
	}
	for i, n := range nodes {
		if n.Row["id"] != wantIDs[i] {
			t.Errorf("row[%d].id = %#v, want %v", i, n.Row["id"], wantIDs[i])
		}
	}
}

func TestFetchStartAtInclusive(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// PK ASC, cursor at id=2 "at" basis → rows from 2 onward, inclusive.
	in := src.Connect(nil, nil, nil)
	nodes := in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{Row: ivm.Row{"id": float64(2)}, Basis: "at"},
	})
	wantIDs := []float64{2, 3}
	if len(nodes) != 2 {
		t.Fatalf("got %d rows, want 2", len(nodes))
	}
	for i, n := range nodes {
		if n.Row["id"] != wantIDs[i] {
			t.Errorf("row[%d].id = %#v, want %v", i, n.Row["id"], wantIDs[i])
		}
	}
}

func TestFetchStartAfterExclusive(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// PK ASC, cursor at id=2 "after" → only id=3.
	in := src.Connect(nil, nil, nil)
	nodes := in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{Row: ivm.Row{"id": float64(2)}, Basis: "after"},
	})
	if len(nodes) != 1 || nodes[0].Row["id"] != float64(3) {
		t.Fatalf("got %v, want single row id=3", nodes)
	}
}

func TestFetchStartReverseFromTop(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// PK ASC + Reverse → effective DESC.
	// Cursor at id=2 "after" in DESC direction = strictly less than 2 = id=1.
	in := src.Connect(nil, nil, nil)
	nodes := in.Fetch(ivm.FetchRequest{
		Start:   &ivm.Start{Row: ivm.Row{"id": float64(2)}, Basis: "after"},
		Reverse: true,
	})
	if len(nodes) != 1 || nodes[0].Row["id"] != float64(1) {
		t.Fatalf("got %v, want single row id=1", nodes)
	}
}

func TestFetchStartMultiColumnOrder(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// ORDER BY score ASC, id ASC. Rows in that order: (70,3), (80,2), (90,1).
	// Cursor at (score=80, id=2) "after" → (90, 1) only.
	in := src.Connect(
		ivm.Ordering{{"score", "asc"}, {"id", "asc"}},
		nil, nil,
	)
	nodes := in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{
			Row:   ivm.Row{"score": float64(80), "id": float64(2)},
			Basis: "after",
		},
	})
	if len(nodes) != 1 || nodes[0].Row["id"] != float64(1) {
		t.Fatalf("got %v, want single row id=1 (score 90)", nodes)
	}
}

func TestFetchStartCombinedWithConstraintAndFilter(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// active=true rows: id=1 (score 90), id=3 (score 70).
	// PK ASC, cursor id=1 "after" → id=3 only.
	in := src.Connect(nil, func(r ivm.Row) bool {
		v, _ := r["active"].(bool)
		return v
	}, nil)
	c := ivm.Constraint{"active": true}
	nodes := in.Fetch(ivm.FetchRequest{
		Constraint: &c,
		Start:      &ivm.Start{Row: ivm.Row{"id": float64(1)}, Basis: "after"},
	})
	if len(nodes) != 1 || nodes[0].Row["id"] != float64(3) {
		t.Fatalf("got %v, want single row id=3", nodes)
	}
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
