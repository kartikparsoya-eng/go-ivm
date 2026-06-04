package main

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
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
