package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// makeReplica builds a temp WAL replica with the _zero metadata tables + an
// issue table (id PK, number unique, _0_version) seeded at version v1, and
// returns its path plus a writer handle for staging subsequent advances.
func makeReplica(t *testing.T) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.db")
	dsn := "file:" + path + "?_busy_timeout=5000&_journal_mode=WAL"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	exec := func(q string, a ...any) {
		if _, err := db.Exec(q, a...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	exec(`CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL, writeTimeMs INTEGER, lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))`)
	exec(`CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL,"pos" INT NOT NULL,"table" TEXT NOT NULL,"rowKey" TEXT NOT NULL,"op" TEXT NOT NULL,"backfillingColumnVersions" TEXT DEFAULT '{}',PRIMARY KEY("stateVersion","pos"),UNIQUE("table","rowKey"))`)
	exec(`CREATE TABLE "issue" ("id" TEXT PRIMARY KEY,"title" TEXT,"number" INTEGER,"_0_version" TEXT)`)
	exec(`INSERT INTO "issue" VALUES ('1','one',1,'0000000001')`)
	exec(`INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000001', 1)`)
	return path, db
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func mustMarshal(t *testing.T, v any) msgpack.RawMessage {
	t.Helper()
	b, err := mpMarshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return msgpack.RawMessage(b)
}

func TestAdvanceToHead_DerivesDiff(t *testing.T) {
	path, db := makeReplica(t)

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, initParams{
		ClientGroupID: "cg1",
		AppID:         "myapp",
		Tables: map[string]tableSchemaParams{
			"issue": {
				Columns: map[string]sqlite.ColumnSchema{
					"id":         {Type: "string"},
					"title":      {Type: "string"},
					"number":     {Type: "number"},
					"_0_version": {Type: "string"},
				},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}, {"number"}},
			},
		},
	})}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}

	// The Snapshotter must have been armed for this CG.
	group := srv.getGroup("cg1", false)
	if group == nil || group.snap == nil {
		t.Fatalf("snapshotter not armed after init (group=%v)", group)
	}

	// Stage a V2 advance on the replica: add issue id=2.
	if _, err := db.Exec(`INSERT INTO "issue" VALUES ('2','two',2,'0000000002')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',0,'issue','{"id":"2"}','s')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`); err != nil {
		t.Fatal(err)
	}

	advReq := RPCRequest{Method: "advanceToHead", ID: 2, Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1",
		InitEpoch:     group.initEpoch,
	})}
	resp := srv.handleAdvanceToHead(advReq)
	if resp.Error != nil {
		t.Fatalf("advanceToHead error: %+v", resp.Error)
	}
	res, ok := resp.Result.(advanceToHeadResult)
	if !ok {
		t.Fatalf("result type = %T, want advanceToHeadResult", resp.Result)
	}
	if res.Version != "0000000002" {
		t.Errorf("version = %q, want 0000000002", res.Version)
	}
	if res.Reset != nil {
		t.Fatalf("unexpected reset: %+v", res.Reset)
	}
	if len(res.Changes) != 1 {
		t.Fatalf("got %d changes, want 1: %+v", len(res.Changes), res.Changes)
	}
	c := res.Changes[0]
	if c.Table != "issue" || c.RowKey["id"] != "2" || c.NextValue == nil {
		t.Errorf("change wrong: %+v", c)
	}
	if c.NextValue["title"] != "two" || c.NextValue["number"] != float64(2) {
		t.Errorf("nextValue wrong: %+v", c.NextValue)
	}
	if len(c.PrevValues) != 0 {
		t.Errorf("want no prevValues for an add, got %+v", c.PrevValues)
	}
}

// Drive mode (P2): advanceToHead applies Go's own derived diff to Go's engine
// (frame-coordinated against the Snapshotter's prev frame) and returns the
// resulting RowChanges. This is the test the naive-drive spike FAILED on (the
// tablesource leaf re-pinned at head and double-saw the row); it now passes
// because the leaf reads/writes go through the Snapshotter's pinned conn.
// beginConcurrentSupported reports whether this build's SQLite has rocicorp's
// wal2 BEGIN CONCURRENT patch. Drive mode applies the diff into a PAST-pinned
// snapshot (prev), which only BEGIN CONCURRENT on wal2 permits; plain BEGIN
// (mattn's bundled SQLite, used by `go test`) rejects the write with
// "database is locked". The full drive path is validated by the soak (which
// runs the libsqlite3-tagged wal2 build), so this test skips otherwise.
func beginConcurrentSupported(t *testing.T, db *sql.DB) bool {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return false
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "BEGIN CONCURRENT"); err != nil {
		return false
	}
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
	return true
}

func TestAdvanceToHead_DriveProducesRowChanges(t *testing.T) {
	path, db := makeReplica(t)
	if !beginConcurrentSupported(t, db) {
		t.Skip("drive mode writes into a past-pinned snapshot — requires BEGIN CONCURRENT (wal2/libsqlite3 build); validated via the rust-test soak")
	}

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	srv.advanceDriveEnabled = true
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, initParams{
		ClientGroupID: "cg1",
		Tables: map[string]tableSchemaParams{
			"issue": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "title": {Type: "string"}, "number": {Type: "number"}, "_0_version": {Type: "string"}},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}},
			},
		},
	})}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	// Hydrate a query (reads the Snapshotter's curr frame via the bound conn).
	addReq := RPCRequest{Method: "addQuery", ID: 2, Params: mustMarshal(t, addQueryParams{
		ClientGroupID: "cg1",
		QueryID:       "q1",
		AST:           builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
		InitEpoch:     group.initEpoch,
	})}
	if resp := srv.handleAddQuery(addReq); resp.Error != nil {
		t.Fatalf("addQuery error: %+v", resp.Error)
	}

	// V2: add issue id=2.
	mustExec(t, db, `INSERT INTO "issue" VALUES ('2','two',2,'0000000002')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',0,'issue','{"id":"2"}','s')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	advReq := RPCRequest{Method: "advanceToHead", ID: 3, Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch,
	})}
	resp := srv.handleAdvanceToHead(advReq)
	if resp.Error != nil {
		t.Fatalf("advanceToHead(drive) error: %+v", resp.Error)
	}
	res := resp.Result.(advanceToHeadResult)
	if res.Version != "0000000002" {
		t.Errorf("version = %q, want 0000000002", res.Version)
	}
	if len(res.Changes) != 0 {
		t.Errorf("drive mode should omit derived Changes, got %+v", res.Changes)
	}
	if len(res.RowChanges) != 1 {
		t.Fatalf("want 1 RowChange (add id=2), got %d: %+v", len(res.RowChanges), res.RowChanges)
	}
	rc := res.RowChanges[0]
	if rc.Type != 0 || rc.QueryID != "q1" || rc.Table != "issue" || rc.RowKey["id"] != "2" {
		t.Errorf("RowChange wrong: %+v", rc)
	}

	// A SECOND advance exercises the leapfrog reuse (old curr → new prev).
	mustExec(t, db, `UPDATE "issue" SET "title"='one-v3', "_0_version"='0000000003' WHERE "id"='1'`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000003',0,'issue','{"id":"1"}','s')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000003', 1)`)
	advReq2 := RPCRequest{Method: "advanceToHead", ID: 4, Params: mustMarshal(t, advanceToHeadParams{
		ClientGroupID: "cg1", InitEpoch: group.initEpoch,
	})}
	resp2 := srv.handleAdvanceToHead(advReq2)
	if resp2.Error != nil {
		t.Fatalf("advanceToHead(drive) #2 error: %+v", resp2.Error)
	}
	res2 := resp2.Result.(advanceToHeadResult)
	if res2.Version != "0000000003" || len(res2.RowChanges) != 1 {
		t.Fatalf("advance #2: version=%q rowChanges=%+v", res2.Version, res2.RowChanges)
	}
	if res2.RowChanges[0].Type != 2 || res2.RowChanges[0].RowKey["id"] != "1" {
		t.Errorf("advance #2 expected edit of id=1, got %+v", res2.RowChanges[0])
	}
}

// A change for a replicated-but-non-syncable table is skipped (not an error),
// thanks to allTableNames being read from sqlite_master.
func TestAdvanceToHead_SkipsNonSyncableTable(t *testing.T) {
	path, db := makeReplica(t)
	// A replicated table the sidecar does NOT serve.
	if _, err := db.Exec(`CREATE TABLE "lmids" ("clientID" TEXT PRIMARY KEY, "lmid" INTEGER, "_0_version" TEXT)`); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(tablesource.ModeTable, path)
	srv.appID = "myapp"
	srv.advanceToHeadEnabled = true
	t.Cleanup(srv.closeAll)

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, initParams{
		ClientGroupID: "cg1",
		Tables: map[string]tableSchemaParams{
			"issue": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "title": {Type: "string"}, "number": {Type: "number"}, "_0_version": {Type: "string"}},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}},
			},
		},
	})}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup("cg1", false)

	// V2: a change ONLY to the non-syncable lmids table.
	if _, err := db.Exec(`INSERT INTO "lmids" VALUES ('client-a',7,'0000000002')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',0,'lmids','{"clientID":"client-a"}','s')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`); err != nil {
		t.Fatal(err)
	}

	advReq := RPCRequest{Method: "advanceToHead", ID: 2, Params: mustMarshal(t, advanceToHeadParams{ClientGroupID: "cg1", InitEpoch: group.initEpoch})}
	resp := srv.handleAdvanceToHead(advReq)
	if resp.Error != nil {
		t.Fatalf("advanceToHead error (non-syncable should be skipped, not errored): %+v", resp.Error)
	}
	res := resp.Result.(advanceToHeadResult)
	if len(res.Changes) != 0 {
		t.Errorf("want 0 changes (lmids skipped), got %+v", res.Changes)
	}
	if res.NumChanges != 1 {
		t.Errorf("NumChanges (raw changelog count) = %d, want 1", res.NumChanges)
	}
}
