package tablesource

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"slices"
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

// openWritableForTest opens the companion writable pool for a Source.
// Most tests just need a working writableDB; the path is the same one
// returned by seedTypedReplica.
func openWritableForTest(t *testing.T, path string) *sql.DB {
	t.Helper()
	w, err := OpenWritable(path, OpenOptions{})
	if err != nil {
		t.Fatalf("OpenWritable: %v", err)
	}
	return w
}

func newUserSource(t *testing.T) (*Source, *sql.DB) {
	t.Helper()
	path := seedTypedReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })
	src, err := New(db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		db.Close()
		t.Fatalf("New: %v", err)
	}
	return src, db
}

// newJSONSource builds a Source over a docs(id, payload) table whose payload is
// a json column, for exercising the json-coercion boundary in NormalizeRow.
func newJSONSource(t *testing.T) (*Source, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE docs (id INTEGER PRIMARY KEY, payload TEXT)`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			w.Close()
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	w.Close()

	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })
	src, err := New(db, wdb, "docs", map[string]sqlite.ColumnSchema{
		"id":      {Type: "number"},
		"payload": {Type: "json"},
	}, []string{"id"})
	if err != nil {
		db.Close()
		t.Fatalf("New: %v", err)
	}
	return src, db
}

func TestNewRejectsUnknownTable(t *testing.T) {
	path := seedTypedReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()
	if _, err := New(db, wdb, "nonexistent", userSchema(), []string{"id"}); err == nil {
		t.Fatalf("New on missing table succeeded; expected error")
	}
}

func TestNewRejectsEmptyPK(t *testing.T) {
	path := seedTypedReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()
	if _, err := New(db, wdb, "users", userSchema(), nil); err == nil {
		t.Fatalf("New with empty PK succeeded; expected error")
	}
}

func TestNewRejectsPKNotInColumns(t *testing.T) {
	path := seedTypedReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()
	cols := userSchema()
	if _, err := New(db, wdb, "users", cols, []string{"missing_col"}); err == nil {
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
		"id":     int64(7), // → float64 via "number"
		"name":   "dave",   // → string (untouched)
		"score":  float64(42.5),
		"active": int64(1),  // → bool true via "boolean"
		"junk":   "ignored", // not in schema; left as-is
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

// TestNormalizeRow_JSONAlreadyParsedNoReparse is the W1 regression. json values
// arrive over the wire ALREADY parsed: TS's snapshotter runs fromSQLiteTypes
// before shipping (snapshotter.ts:577-581) and TS's push path consumes the
// change row AS-IS without re-coercing (table-source.ts genPush/#writeChange
// never call fromSQLiteType). So a json scalar string like "Payment Failures"
// reaches Go as a bare string, NOT as JSON text. NormalizeRow must pass it
// through unchanged — re-running FromSQLiteType's strict JSON.parse on it would
// panic. Every other json shape must also pass through, and non-json columns
// must still be re-boxed (int64 → float64).
func TestNormalizeRow_JSONAlreadyParsedNoReparse(t *testing.T) {
	src, db := newJSONSource(t)
	defer db.Close()

	cases := []struct {
		name    string
		payload any
	}{
		{"scalar string", "Payment Failures"}, // the W1 panic case
		{"scalar string that looks quoted", `"quoted"`},
		{"object", map[string]any{"k": "v"}},
		{"array", []any{float64(1), float64(2)}},
		{"number", float64(42)},
		{"bool", true},
		{"null", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := ivm.Row{"id": int64(1), "payload": tc.payload}
			// Must not panic.
			src.NormalizeRow(row)
			if !reflect.DeepEqual(row["payload"], tc.payload) {
				t.Errorf("payload = %#v, want %#v (passed through unchanged)",
					row["payload"], tc.payload)
			}
			// Non-json column is still normalized: int64 → float64.
			if row["id"] != float64(1) {
				t.Errorf("id = %#v, want float64(1)", row["id"])
			}
		})
	}
}

func TestFetchDefaultOrderingByPK(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{}))
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

	in := src.Connect(ivm.Ordering{{"score", "desc"}, {"id", "asc"}}, nil, nil, nil)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{}))
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

	in := src.Connect(nil, nil, nil, nil)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{Reverse: true}))
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
	in := src.Connect(nil, nil, func(r ivm.Row) bool {
		v, _ := r["active"].(bool)
		return v
	}, nil)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{}))
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

// TestFetchLimitPushdown locks the FetchRequest.Limit contract that powers the
// Take O(table)→O(limit) hydrate optimization: the source stops after Limit
// rows in the request's effective order, counted AFTER its own filter predicate
// — never under-fetching (a filtered row must not consume the limit budget) and
// never over-fetching (Limit larger than the result set returns the full set).
func TestFetchLimitPushdown(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	ids := func(nodes []ivm.Node) []float64 {
		out := make([]float64, len(nodes))
		for i, n := range nodes {
			out[i], _ = n.Row["id"].(float64)
		}
		return out
	}

	// score<=80 passes id=2(80) and id=3(70); id=1(90) is filtered. So the
	// filtered id=1 sits FIRST in id order — a correct limit must skip it
	// without spending budget, returning id=2 as the first row.
	pred := func(r ivm.Row) bool { s, _ := r["score"].(float64); return s <= 80 }
	in := src.Connect(nil, nil, pred, nil)

	cases := []struct {
		name string
		req  ivm.FetchRequest
		want []float64
	}{
		{"limit1_skips_filtered_first_row", ivm.FetchRequest{Limit: 1}, []float64{2}},
		{"limit2_full_post_predicate_set", ivm.FetchRequest{Limit: 2}, []float64{2, 3}},
		{"limit_over_set_no_overfetch", ivm.FetchRequest{Limit: 99}, []float64{2, 3}},
		{"limit0_unlimited", ivm.FetchRequest{Limit: 0}, []float64{2, 3}},
		{"reverse_limit1_first_in_desc_order", ivm.FetchRequest{Reverse: true, Limit: 1}, []float64{3}},
	}
	for _, c := range cases {
		got := ids(slices.Collect(in.Fetch(c.req)))
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: Fetch(%+v) ids = %v, want %v", c.name, c.req, got, c.want)
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

	in := src.Connect(nil, nil, nil, nil)
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

// TestPushDriftOnDuplicateAdd verifies that an ADD for a row already
// present in the source raises a *ivm.DriftError BEFORE any fanout —
// matching TS TableSource.genPush's `assert(!exists(row))`
// (table-source.ts:399-413 + memory-source.ts:531). The prev-tx
// writeChange would otherwise raw-panic "UNIQUE constraint failed",
// which is NOT recoverable by engine.Advance and crashes the cg/sidecar.
func TestPushDriftOnDuplicateAdd(t *testing.T) {
	// Seed already has users id=1,2,3 (see seedTypedReplica).
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("dup-Add should panic with *DriftError; no panic")
		}
		d, ok := r.(*ivm.DriftError)
		if !ok {
			t.Fatalf("dup-Add panic type = %T, want *ivm.DriftError (raw panic crashes the sidecar)", r)
		}
		if d.Op != "Add" {
			t.Errorf("DriftError.Op = %q, want \"Add\"", d.Op)
		}
		if len(rec.pushed) != 0 {
			t.Errorf("DriftError must be raised BEFORE fanout; got %d pushed changes", len(rec.pushed))
		}
	}()

	// id=1 already exists in the seed → ADD must drift, not INSERT.
	row := ivm.Row{"id": float64(1), "name": "alice", "score": float64(90), "active": true}
	src.Push(ivm.MakeSourceChangeAdd(row))
}

// TestPushDriftOnRemoveMissing verifies a REMOVE of an absent row raises a
// *ivm.DriftError before fanout (TS: assert(exists(row))).
func TestPushDriftOnRemoveMissing(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	rec := &recordingOutput{}
	in.SetOutput(rec)

	defer func() {
		r := recover()
		d, ok := r.(*ivm.DriftError)
		if !ok {
			t.Fatalf("missing-Remove panic type = %T, want *ivm.DriftError", r)
		}
		if d.Op != "Remove" {
			t.Errorf("DriftError.Op = %q, want \"Remove\"", d.Op)
		}
		if len(rec.pushed) != 0 {
			t.Errorf("DriftError must be raised BEFORE fanout; got %d pushed", len(rec.pushed))
		}
	}()

	// id=999 not in seed → REMOVE must drift.
	row := ivm.Row{"id": float64(999), "name": "ghost", "score": float64(0), "active": false}
	src.Push(ivm.MakeSourceChangeRemove(row))
}

func TestPushFilterDropsRow(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, func(r ivm.Row) bool { v, _ := r["active"].(bool); return v }, nil)
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

	in := src.Connect(nil, nil, func(r ivm.Row) bool { v, _ := r["active"].(bool); return v }, nil)
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

	in := src.Connect(nil, nil, func(r ivm.Row) bool { v, _ := r["active"].(bool); return v }, nil)
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
	in := src.Connect(nil, nil, nil, splitKeys)
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
	nodes := slices.Collect(o.in.Fetch(ivm.FetchRequest{}))
	o.observedAfter = len(nodes)
	return nil
}

// TestPrevTxAppliesWritesAndRollback is the prev-snapshot tx contract
// for the new architecture (matches TS Snapshotter):
//   - The Source's prev tx is pinned at the WAL frame at first use.
//   - Push within the batch DOES become visible (writeChange runs on
//     the prev tx; subsequent reads see it via read-your-own-writes).
//   - OnAdvanceEnd rolls back the prev tx (discarding writeChange writes)
//     and lazily re-BEGINs on next use.
//
// This is the test that exercises the full writeChange + rollback cycle
// without any external writer interleaving. The external-writer scenario
// is covered separately when BEGIN CONCURRENT is available (rocicorp's
// wal2 patch) — plain BEGIN can't isolate an in-tx write from a parallel
// committed write on the same file in default mattn-bundled builds.
func TestPrevTxAppliesWritesAndRollback(t *testing.T) {
	path := seedTypedReplica(t)

	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()
	src, err := New(db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close()

	in := src.Connect(nil, nil, nil, nil)
	in.SetOutput(&recordingOutput{})

	// Step 1: first Fetch starts the prev tx (3 seeded rows).
	if got := len(slices.Collect(in.Fetch(ivm.FetchRequest{}))); got != 3 {
		t.Fatalf("baseline fetch = %d rows, want 3", got)
	}

	// Step 2: Push a row. writeChange applies it to the prev tx.
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id": float64(99), "name": "dave", "score": float64(50), "active": true,
	}))

	// Step 3: re-fetch within batch — Push's writeChange visible.
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{}))
	if len(nodes) != 4 {
		t.Fatalf("post-Push fetch = %d rows, want 4", len(nodes))
	}
	hasDave := false
	for _, n := range nodes {
		if id, _ := n.Row["id"].(float64); id == 99 {
			hasDave = true
		}
	}
	if !hasDave {
		t.Fatalf("post-Push fetch missing in-tx 'dave' — writeChange didn't run")
	}

	// Step 4: OnAdvanceEnd — rollback (discards in-tx 'dave').
	src.OnAdvanceEnd()

	// Step 5: re-fetch — back to 3 rows (rollback discarded the write).
	nodes = slices.Collect(in.Fetch(ivm.FetchRequest{}))
	if len(nodes) != 3 {
		t.Fatalf("post-OnAdvanceEnd fetch = %d rows, want 3 (rollback should discard)", len(nodes))
	}
	for _, n := range nodes {
		if id, _ := n.Row["id"].(float64); id == 99 {
			t.Fatalf("post-OnAdvanceEnd fetch still has in-tx 'dave' — rollback didn't discard")
		}
	}
}

// TestRefreshSnapshotRollsTx exercises the drift-audit contract: an
// external caller (e.g. the audit) calls RefreshSnapshot when no Push
// is in flight; the prev tx rolls back + repins at the current frame,
// making external writes since the last pin visible.
func TestRefreshSnapshotRollsTx(t *testing.T) {
	path := seedTypedReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()
	src, err := New(db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close()
	writer, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	defer writer.Close()

	in := src.Connect(nil, nil, nil, nil)
	in.SetOutput(&recordingOutput{})

	// First Fetch pins tx at the seeded 3 rows.
	if got := len(slices.Collect(in.Fetch(ivm.FetchRequest{}))); got != 3 {
		t.Fatalf("baseline = %d, want 3", got)
	}
	// External commit.
	if _, err := writer.Exec("INSERT INTO users VALUES (10, 'eve', 100, 1)"); err != nil {
		t.Fatalf("external insert: %v", err)
	}
	// Still 3 — tx pinned.
	if got := len(slices.Collect(in.Fetch(ivm.FetchRequest{}))); got != 3 {
		t.Fatalf("pre-refresh = %d, want 3 (tx still pinned)", got)
	}
	// External caller (drift audit) refreshes.
	src.RefreshSnapshot()
	if got := len(slices.Collect(in.Fetch(ivm.FetchRequest{}))); got != 4 {
		t.Fatalf("post-refresh = %d, want 4 (tx rolled)", got)
	}
}

// TestRefreshSnapshotNoOpDuringPush proves the safety property: rolling
// the tx mid-Push would break overlay's pre-state assumption. We start
// a Push, have its downstream call RefreshSnapshot, and verify the
// snapshot stays pinned for the duration of the Push.
//
// Requires the snapshot pinning path to be available — the readViaSnapshot
// gate on overlay (source.go) skips overlay when the pool-fallback is
// used, so the in-process test must have snapshot capability for this
// test to exercise the overlay code path at all.
func TestRefreshSnapshotNoOpDuringPush(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	if !snapshotAvailable(t) {
		t.Skip("requires libsqlite3 build with SQLITE_ENABLE_SNAPSHOT — fallback path skips overlay by design")
	}

	in := src.Connect(nil, nil, nil, nil)
	probe := &refreshDuringPushOutput{src: src, in: in}
	in.SetOutput(probe)

	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id": float64(99), "name": "dave", "score": float64(50), "active": true,
	}))

	if !probe.observedOverlay {
		t.Fatalf("RefreshSnapshot during Push should have left overlay visible")
	}
}

type refreshDuringPushOutput struct {
	src             *Source
	in              ivm.Input
	observedOverlay bool
}

func (o *refreshDuringPushOutput) Push(_ ivm.Change, _ ivm.InputBase) []ivm.Change {
	// Try to refresh while the Push is still in-flight. The Source must
	// recognize overlay != nil and skip the bump — otherwise the next
	// Fetch (this one) would roll the tx, see post-replicator state,
	// and double-apply the overlay's add.
	o.src.RefreshSnapshot()
	// SQLite has 3 rows; overlay adds the 4th. If we double-applied,
	// we'd see 5.
	nodes := slices.Collect(o.in.Fetch(ivm.FetchRequest{}))
	o.observedOverlay = len(nodes) == 4
	return nil
}

func TestOverlayVisibleDuringPush(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// Overlay is gated on readViaSnapshot (source.go): only applied when
	// the Fetch read from a pinned pre-Push snapshot. Without snapshot
	// capability the pool returns post-replicator-commit state directly,
	// so applying overlay would double-count. This test mimics the
	// MemorySource-equivalence path that only holds for the snapshot
	// production build.
	if !snapshotAvailable(t) {
		t.Skip("requires libsqlite3 build with SQLITE_ENABLE_SNAPSHOT — fallback path skips overlay by design")
	}

	in := src.Connect(nil, nil, nil, nil)
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
	if got := len(slices.Collect(in.Fetch(ivm.FetchRequest{}))); got != 3 {
		t.Fatalf("overlay should clear post-Push: got %d nodes, want 3", got)
	}
}

func TestFetchConstraintEquality(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	c := ivm.Constraint{"id": float64(2)}
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{Constraint: &c}))
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

	in := src.Connect(nil, nil, nil, nil)
	// score=80 AND active=false → only bob
	c := ivm.Constraint{"score": float64(80), "active": false}
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{Constraint: &c}))
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

	in := src.Connect(nil, nil, nil, nil)
	c := ivm.Constraint{"id": float64(999)}
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{Constraint: &c}))
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
		nil,
		func(r ivm.Row) bool { v, _ := r["active"].(bool); return v },
		nil,
	)
	c := ivm.Constraint{"active": true}
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{Constraint: &c, Reverse: true}))
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
	in := src.Connect(nil, nil, nil, nil)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{Row: ivm.Row{"id": float64(2)}, Basis: "at"},
	}))
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
	in := src.Connect(nil, nil, nil, nil)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{Row: ivm.Row{"id": float64(2)}, Basis: "after"},
	}))
	if len(nodes) != 1 || nodes[0].Row["id"] != float64(3) {
		t.Fatalf("got %v, want single row id=3", nodes)
	}
}

func TestFetchStartReverseFromTop(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// PK ASC + Reverse → effective DESC.
	// Cursor at id=2 "after" in DESC direction = strictly less than 2 = id=1.
	in := src.Connect(nil, nil, nil, nil)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{
		Start:   &ivm.Start{Row: ivm.Row{"id": float64(2)}, Basis: "after"},
		Reverse: true,
	}))
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
		nil, nil, nil,
	)
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{
			Row:   ivm.Row{"score": float64(80), "id": float64(2)},
			Basis: "after",
		},
	}))
	if len(nodes) != 1 || nodes[0].Row["id"] != float64(1) {
		t.Fatalf("got %v, want single row id=1 (score 90)", nodes)
	}
}

func TestFetchStartCombinedWithConstraintAndFilter(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// active=true rows: id=1 (score 90), id=3 (score 70).
	// PK ASC, cursor id=1 "after" → id=3 only.
	in := src.Connect(nil, nil, func(r ivm.Row) bool {
		v, _ := r["active"].(bool)
		return v
	}, nil)
	c := ivm.Constraint{"active": true}
	nodes := slices.Collect(in.Fetch(ivm.FetchRequest{
		Constraint: &c,
		Start:      &ivm.Start{Row: ivm.Row{"id": float64(1)}, Basis: "after"},
	}))
	if len(nodes) != 1 || nodes[0].Row["id"] != float64(3) {
		t.Fatalf("got %v, want single row id=3", nodes)
	}
}

func TestDestroyRemovesConnection(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
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

// TestOnAdvanceEndSkipsRollbackWhenOverlaySet pins the Fix #4 TOCTOU guard.
// RefreshSnapshot reads s.overlay WITHOUT holding s.mu, so a Push can install
// the overlay in the window between that unlocked read and OnAdvanceEnd
// acquiring the lock. If OnAdvanceEnd then rolls the prev tx and re-pins to
// head, it tears the pre-Push snapshot that the in-flight fanout's downstream
// Fetches read from. The guard (source.go: `if s.overlay != nil { return }`
// after Lock) makes OnAdvanceEnd a no-op whenever an overlay is live.
//
// Observable: an externally-committed row that lands AFTER the pin must stay
// invisible across OnAdvanceEnd. Without the guard, the roll+re-pin jumps the
// snapshot to head and the row leaks in (Fetch returns 4 instead of 3).
func TestOnAdvanceEndSkipsRollbackWhenOverlaySet(t *testing.T) {
	path := seedTypedReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()
	src, err := New(db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close()
	writer, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	defer writer.Close()

	in := src.Connect(nil, nil, nil, nil)
	in.SetOutput(&recordingOutput{})

	// First Fetch pins the prev tx at the seeded 3 rows.
	if got := len(slices.Collect(in.Fetch(ivm.FetchRequest{}))); got != 3 {
		t.Fatalf("baseline = %d, want 3", got)
	}
	// External writer commits a 4th row AFTER the pin; a correctly pinned
	// snapshot must not see it. This also proves the tx is actually pinned.
	if _, err := writer.Exec("INSERT INTO users VALUES (10, 'eve', 100, 1)"); err != nil {
		t.Fatalf("external insert: %v", err)
	}
	if got := len(slices.Collect(in.Fetch(ivm.FetchRequest{}))); got != 3 {
		t.Fatalf("pre-overlay = %d, want 3 (tx still pinned)", got)
	}

	// Simulate the race: a Push set the overlay (under s.mu, exactly as
	// genPushAndWrite does at source.go:667) and the engine's signalAdvanceEnd
	// fires before the Push's fanout clears it.
	src.mu.Lock()
	src.overlay = &ivm.Overlay{
		Epoch: src.pushEpoch + 1,
		Change: ivm.MakeSourceChangeAdd(ivm.Row{
			"id": float64(99), "name": "dave", "score": float64(50), "active": true,
		}),
	}
	src.mu.Unlock()

	src.OnAdvanceEnd()

	// Clear the overlay the way the real Push would once fanout completes.
	src.mu.Lock()
	src.overlay = nil
	src.mu.Unlock()

	// The guard must have made OnAdvanceEnd a no-op: the prev tx is still the
	// ORIGINAL pinned snapshot, so the Fetch still sees 3 — not the externally
	// committed 4. Without the guard this returns 4 (the mid-push snapshot tear).
	if got := len(slices.Collect(in.Fetch(ivm.FetchRequest{}))); got != 3 {
		t.Fatalf("OnAdvanceEnd tore the pinned snapshot while overlay was live: Fetch = %d, want 3", got)
	}
}

// TestCloseCancelsContext pins the Fix #5 lifecycle guarantee: Close cancels
// the Source's context so an in-flight SQLite call (e.g. a Fetch blocked on a
// WAL busy-wait) unblocks instead of leaking its goroutine past teardown.
func TestCloseCancelsContext(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	if err := src.ctx.Err(); err != nil {
		t.Fatalf("Source ctx canceled before Close: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if src.ctx.Err() == nil {
		t.Fatal("Close did not cancel the Source context")
	}
	// Idempotent: a second Close must not panic (double cancel is safe).
	if err := src.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// snapshotAvailable probes whether the build supports SQLite's
// sqlite3_snapshot_get (real impl) or stubs out (default impl).
// Tests that depend on snapshot-pinning semantics skip on stub builds.
func snapshotAvailable(t *testing.T) bool {
	t.Helper()
	db, err := Open(seedTypedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("snapshotAvailable: Open: %v", err)
	}
	defer db.Close()
	snap, err := CaptureSnapshot(context.Background(), db)
	if err != nil {
		return false
	}
	snap.Free()
	return true
}
