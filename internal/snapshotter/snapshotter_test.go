package snapshotter

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// ---------------------------------------------------------------------------
// Fixture: a temp wal2-substitute (plain WAL, since mattn-bundled SQLite lacks
// rocicorp's BEGIN CONCURRENT patch — the snapshotter transparently falls back
// to plain BEGIN, and plain WAL still gives each connection its own consistent
// read snapshot, which is all the leapfrog needs for these single-writer tests).
// ---------------------------------------------------------------------------

const appID = "myapp"

func ver(n int) string { return fmt.Sprintf("%010d", n) }

type fixture struct {
	t    *testing.T
	db   *sql.DB
	snap *Snapshotter
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.db")
	dsn := "file:" + path + "?_busy_timeout=5000&_synchronous=OFF&_journal_mode=WAL"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	f := &fixture{t: t, db: db}

	// Replication metadata tables (the diff reads stateVersion + changeLog2).
	f.exec(`CREATE TABLE "_zero.replicationState" (
		stateVersion TEXT NOT NULL,
		writeTimeMs INTEGER,
		lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))`)
	f.exec(`CREATE TABLE "_zero.changeLog2" (
		"stateVersion" TEXT NOT NULL,
		"pos" INT NOT NULL,
		"table" TEXT NOT NULL,
		"rowKey" TEXT NOT NULL,
		"op" TEXT NOT NULL,
		"backfillingColumnVersions" TEXT DEFAULT '{}',
		PRIMARY KEY("stateVersion","pos"),
		UNIQUE("table","rowKey"))`)
	return f
}

func (f *fixture) exec(q string, args ...any) {
	f.t.Helper()
	if _, err := f.db.Exec(q, args...); err != nil {
		f.t.Fatalf("exec %q: %v", q, err)
	}
}

func (f *fixture) setStateVersion(v string) {
	f.exec(`INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES (?, 1)`, v)
}

// logSet / logDelete append a row-change entry. logTableWide appends a t/r entry
// at pos=-1 (sorts first within its version), exactly like change-log.ts.
func (f *fixture) logSet(version string, pos int, table, rowKeyJSON string) {
	f.exec(`INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES (?,?,?,?,?)`,
		version, pos, table, rowKeyJSON, opSet)
}
func (f *fixture) logDelete(version string, pos int, table, rowKeyJSON string) {
	f.exec(`INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES (?,?,?,?,?)`,
		version, pos, table, rowKeyJSON, opDel)
}
func (f *fixture) logTableWide(version, table, op string) {
	f.exec(`INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES (?,?,?,?,?)`,
		version, -1, table, version, op)
}

func (f *fixture) initSnapshotter() {
	f.t.Helper()
	s, err := New(f.db, appID)
	if err != nil {
		f.t.Fatalf("New: %v", err)
	}
	if err := s.Init(); err != nil {
		f.t.Fatalf("Init: %v", err)
	}
	f.t.Cleanup(s.Destroy)
	f.snap = s
}

// issue table: id (PK, string), title, owner, number (unique key), _0_version.
func (f *fixture) createIssueTable() {
	f.exec(`CREATE TABLE "issue" (
		"id" TEXT PRIMARY KEY,
		"title" TEXT,
		"owner" TEXT,
		"number" INTEGER,
		"_0_version" TEXT)`)
}

func issueSpec() *TableSpec {
	return &TableSpec{
		Name: "issue",
		Columns: map[string]sqlite.ColumnSchema{
			"id":         {Type: "string"},
			"title":      {Type: "string"},
			"owner":      {Type: "string"},
			"number":     {Type: "number"},
			"_0_version": {Type: "string"},
		},
		UniqueKeys: [][]string{{"id"}, {"number"}},
	}
}

func (f *fixture) upsertIssue(id, title, owner string, number int, version string) {
	f.exec(`INSERT OR REPLACE INTO "issue" ("id","title","owner","number","_0_version") VALUES (?,?,?,?,?)`,
		id, title, owner, number, version)
}
func (f *fixture) deleteIssue(id string) {
	f.exec(`DELETE FROM "issue" WHERE "id"=?`, id)
}

func syncable(specs ...*TableSpec) map[string]*TableSpec {
	m := make(map[string]*TableSpec, len(specs))
	for _, s := range specs {
		m[s.Name] = s
	}
	return m
}

func allNames(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// findChange returns the emitted Change whose RowKey["id"] == id, or fails.
func findChange(t *testing.T, changes []Change, id string) Change {
	t.Helper()
	for _, c := range changes {
		if c.RowKey["id"] == id {
			return c
		}
	}
	t.Fatalf("no change for id=%s in %+v", id, changes)
	return Change{}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Set/edit/delete in one advance, asserted in (version,pos) order.
func TestDiff_SetEditDelete(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	// V1 baseline.
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.upsertIssue("2", "two", "bob", 2, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// V2 batch: add id=3, edit id=1, delete id=2.
	f.upsertIssue("3", "three", "carol", 3, ver(2))
	f.upsertIssue("1", "one-edited", "alice", 1, ver(2))
	f.deleteIssue("2")
	f.logSet(ver(2), 0, "issue", `{"id":"3"}`)
	f.logSet(ver(2), 1, "issue", `{"id":"1"}`)
	f.logDelete(ver(2), 2, "issue", `{"id":"2"}`)
	f.setStateVersion(ver(2))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if diff.Changes != 3 {
		t.Errorf("Changes count = %d, want 3", diff.Changes)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(changes) != 3 {
		t.Fatalf("got %d changes, want 3: %+v", len(changes), changes)
	}
	// Order: V2 pos 0,1,2 → id3 (add), id1 (edit), id2 (delete).
	if changes[0].RowKey["id"] != "3" || changes[1].RowKey["id"] != "1" || changes[2].RowKey["id"] != "2" {
		t.Errorf("order wrong: %s,%s,%s", changes[0].RowKey["id"], changes[1].RowKey["id"], changes[2].RowKey["id"])
	}

	add := findChange(t, changes, "3")
	if len(add.PrevValues) != 0 || add.NextValue == nil {
		t.Errorf("add id=3: want prevValues=[] nextValue!=nil, got %+v", add)
	}
	if add.NextValue["title"] != "three" || add.NextValue["number"] != float64(3) {
		t.Errorf("add id=3 nextValue wrong: %+v", add.NextValue)
	}

	edit := findChange(t, changes, "1")
	if len(edit.PrevValues) != 1 || edit.NextValue == nil {
		t.Fatalf("edit id=1: want 1 prevValue + nextValue, got %+v", edit)
	}
	if edit.PrevValues[0]["title"] != "one" || edit.NextValue["title"] != "one-edited" {
		t.Errorf("edit id=1 values wrong: prev=%v next=%v", edit.PrevValues[0], edit.NextValue)
	}

	del := findChange(t, changes, "2")
	if len(del.PrevValues) != 1 || del.NextValue != nil {
		t.Errorf("delete id=2: want 1 prevValue + nil nextValue, got %+v", del)
	}
	if del.PrevValues[0]["title"] != "two" {
		t.Errorf("delete id=2 prevValue wrong: %v", del.PrevValues[0])
	}
}

// Delete of a row absent in prev is filtered (no-op).
func TestDiff_NoOpDelete(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// Delete a row that never existed in prev.
	f.logDelete(ver(2), 0, "issue", `{"id":"999"}`)
	f.setStateVersion(ver(2))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("want 0 changes (no-op delete filtered), got %+v", changes)
	}
}

// A set whose new value unique-conflicts with a different prev row must report
// that prev row in prevValues (it has to be removed).
func TestDiff_UniqueConflict(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	// V1: id=A holds number=5.
	f.upsertIssue("A", "alpha", "alice", 5, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// V2: A is gone, B now holds number=5. Only the set for B is logged.
	f.deleteIssue("A")
	f.upsertIssue("B", "bravo", "bob", 5, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"B"}`)
	f.setStateVersion(ver(2))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("want 1 change, got %+v", changes)
	}
	c := changes[0]
	if c.NextValue["id"] != "B" {
		t.Errorf("nextValue id = %v, want B", c.NextValue["id"])
	}
	// prevValues must include A (the number=5 unique conflict in prev).
	if len(c.PrevValues) != 1 || c.PrevValues[0]["id"] != "A" {
		t.Errorf("want prevValues=[A] (unique conflict), got %+v", c.PrevValues)
	}
}

// TRUNCATE aborts iteration with a truncation ResetSignal.
func TestDiff_TruncateResets(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.logTableWide(ver(2), "issue", opTruncate)
	f.setStateVersion(ver(2))

	diff, _ := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	_, err := diff.Collect()
	rs, ok := IsReset(err)
	if !ok {
		t.Fatalf("want *ResetSignal, got %v", err)
	}
	if rs.Reason != ReasonTruncation {
		t.Errorf("reason = %q, want %q", rs.Reason, ReasonTruncation)
	}
}

// RESET (schema change) aborts iteration with a schema-change ResetSignal.
func TestDiff_ResetResets(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.logTableWide(ver(2), "issue", opReset)
	f.setStateVersion(ver(2))

	diff, _ := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	_, err := diff.Collect()
	rs, ok := IsReset(err)
	if !ok {
		t.Fatalf("want *ResetSignal, got %v", err)
	}
	if rs.Reason != ReasonSchemaChange {
		t.Errorf("reason = %q, want %q", rs.Reason, ReasonSchemaChange)
	}
}

// A change for a table that is neither syncable nor known is a hard error.
func TestDiff_UnknownTable(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.logSet(ver(2), 0, "ghost", `{"id":"1"}`)
	f.setStateVersion(ver(2))

	diff, _ := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	_, err := diff.Collect()
	if err == nil {
		t.Fatalf("want unknown-table error, got nil")
	}
	if _, ok := IsReset(err); ok {
		t.Fatalf("unknown table should be a plain error, not a ResetSignal")
	}
}

// A change for a known-but-non-syncable table is skipped; syncable changes in
// the same advance still emit.
func TestDiff_NonSyncableSkipped(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.upsertIssue("1", "edited", "alice", 1, ver(2))
	// "_litestream" is replicated but not syncable → skipped.
	f.logSet(ver(2), 0, "_litestream", `{"id":"x"}`)
	f.logSet(ver(2), 1, "issue", `{"id":"1"}`)
	f.setStateVersion(ver(2))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue", "_litestream"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(changes) != 1 || changes[0].RowKey["id"] != "1" {
		t.Errorf("want only the issue change, got %+v", changes)
	}
}

// A diff whose change-log entry is at a version beyond curr's pinned version is
// stale → InvalidDiffError.
func TestDiff_InvalidWhenCurrBehind(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// Row + change-log advance to V2, but replicationState stays at V1, so the
	// freshly pinned curr reports version V1 while the entry is at V2.
	f.upsertIssue("1", "edited", "alice", 1, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"1"}`)
	// NOTE: deliberately NOT bumping replicationState.

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	_, err = diff.Collect()
	if _, ok := err.(*InvalidDiffError); !ok {
		t.Fatalf("want *InvalidDiffError, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Permissions table
// ---------------------------------------------------------------------------

func (f *fixture) createPermissionsTable() {
	f.exec(`CREATE TABLE "myapp.permissions" (
		"lock" INTEGER PRIMARY KEY,
		"permissions" TEXT,
		"hash" TEXT,
		"_0_version" TEXT)`)
}

func permissionsSpec() *TableSpec {
	return &TableSpec{
		Name: "myapp.permissions",
		Columns: map[string]sqlite.ColumnSchema{
			"lock":        {Type: "number"},
			"permissions": {Type: "string"},
			"hash":        {Type: "string"},
			"_0_version":  {Type: "string"},
		},
		UniqueKeys: [][]string{{"lock"}},
	}
}

func (f *fixture) upsertPermissions(perms, hash, version string) {
	f.exec(`INSERT OR REPLACE INTO "myapp.permissions" ("lock","permissions","hash","_0_version") VALUES (1,?,?,?)`,
		perms, hash, version)
}

// A changed permissions value aborts with a permissions-change ResetSignal.
func TestDiff_PermissionsChangeResets(t *testing.T) {
	f := newFixture(t)
	f.createPermissionsTable()
	f.upsertPermissions("P1", "h1", ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.upsertPermissions("P2", "h2", ver(2))
	f.logSet(ver(2), 0, "myapp.permissions", `{"lock":1}`)
	f.setStateVersion(ver(2))

	diff, _ := f.snap.Advance(syncable(permissionsSpec()), allNames("myapp.permissions"))
	_, err := diff.Collect()
	rs, ok := IsReset(err)
	if !ok {
		t.Fatalf("want *ResetSignal, got %v", err)
	}
	if rs.Reason != ReasonPermissionsChange {
		t.Errorf("reason = %q, want %q", rs.Reason, ReasonPermissionsChange)
	}
}

// A permissions row whose permissions value is UNCHANGED (only hash differs)
// does NOT reset — it emits a normal edit.
func TestDiff_PermissionsUnchangedEmits(t *testing.T) {
	f := newFixture(t)
	f.createPermissionsTable()
	f.upsertPermissions("P1", "h1", ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.upsertPermissions("P1", "h2", ver(2)) // permissions same, hash changed
	f.logSet(ver(2), 0, "myapp.permissions", `{"lock":1}`)
	f.setStateVersion(ver(2))

	diff, _ := f.snap.Advance(syncable(permissionsSpec()), allNames("myapp.permissions"))
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("want no reset, got %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("want 1 edit change, got %+v", changes)
	}
}

// ---------------------------------------------------------------------------
// Leapfrog: two sequential advances reuse the connections and track versions.
// ---------------------------------------------------------------------------

func TestSnapshotter_LeapfrogTwoAdvances(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// Advance 1: V1 → V2 (add id=2).
	f.upsertIssue("2", "two", "bob", 2, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"2"}`)
	f.setStateVersion(ver(2))
	d1, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("advance1: %v", err)
	}
	if d1.Prev().Version() != ver(1) || d1.Curr().Version() != ver(2) {
		t.Errorf("advance1 versions prev=%s curr=%s, want %s/%s",
			d1.Prev().Version(), d1.Curr().Version(), ver(1), ver(2))
	}
	c1, err := d1.Collect()
	if err != nil || len(c1) != 1 || c1[0].RowKey["id"] != "2" {
		t.Fatalf("advance1 changes wrong: %+v err=%v", c1, err)
	}

	// Advance 2: V2 → V3 (edit id=1). This exercises resetToHead on the reused
	// prev connection.
	f.upsertIssue("1", "one-v3", "alice", 1, ver(3))
	f.logSet(ver(3), 0, "issue", `{"id":"1"}`)
	f.setStateVersion(ver(3))
	d2, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("advance2: %v", err)
	}
	if d2.Prev().Version() != ver(2) || d2.Curr().Version() != ver(3) {
		t.Errorf("advance2 versions prev=%s curr=%s, want %s/%s",
			d2.Prev().Version(), d2.Curr().Version(), ver(2), ver(3))
	}
	c2, err := d2.Collect()
	if err != nil {
		t.Fatalf("advance2 collect: %v", err)
	}
	if len(c2) != 1 || c2[0].RowKey["id"] != "1" {
		t.Fatalf("advance2 changes wrong: %+v", c2)
	}
	edit := c2[0]
	if len(edit.PrevValues) != 1 || edit.PrevValues[0]["title"] != "one" ||
		edit.NextValue["title"] != "one-v3" {
		t.Errorf("advance2 edit wrong: prev=%v next=%v", edit.PrevValues, edit.NextValue)
	}
}

// Init must be exactly once; Current/Advance require init.
func TestSnapshotter_Lifecycle(t *testing.T) {
	f := newFixture(t)
	f.setStateVersion(ver(1))

	s, err := New(f.db, appID)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Initialized() {
		t.Error("Initialized() true before Init")
	}
	if _, err := s.Current(); err == nil {
		t.Error("Current() before Init should error")
	}
	if _, err := s.Advance(nil, nil); err == nil {
		t.Error("Advance() before Init should error")
	}
	if err := s.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := s.Init(); err == nil {
		t.Error("second Init should error")
	}
	cur, err := s.Current()
	if err != nil || cur.Version() != ver(1) {
		t.Errorf("Current after Init: %v ver=%v", err, cur)
	}
	s.Destroy()
}

// Guard: coercion of a raw row matches FromSQLiteType (number→float64 etc.).
func TestCoerceRow_MatchesFromSQLiteType(t *testing.T) {
	spec := issueSpec()
	raw := map[string]any{
		"id": "7", "title": "t", "owner": "o",
		"number": int64(42), "_0_version": ver(9),
	}
	got := coerceRow(raw, spec)
	want := ivm.Row{
		"id": "7", "title": "t", "owner": "o",
		"number": float64(42), "_0_version": ver(9),
	}
	for k, wv := range want {
		if got[k] != wv {
			t.Errorf("coerce[%s] = %v (%T), want %v (%T)", k, got[k], got[k], wv, wv)
		}
	}
}
