package main

// Shared table-mode test fixtures for the sidecar suite. Every fixture is
// replica-backed: the removal sweep deleted memory mode (loadRows-fed
// MemorySource), so a Server is always constructed over a real SQLite file
// with the _zero metadata tables the Snapshotter requires (handleInit now
// builds the per-CG Snapshotter unconditionally and fails loudly without
// them).

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/kartikparsoya-eng/go-ivm/builder"
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

// newIssueServer is the standard table-mode Server fixture: a replica from
// makeReplica + a Server over it (drive advance is the only mode). Returns
// the server and the replica writer handle.
func newIssueServer(t *testing.T) (*Server, *sql.DB) {
	t.Helper()
	path, db := makeReplica(t)
	srv := NewServer(path)
	srv.appID = "myapp"
	t.Cleanup(srv.closeAll)
	return srv, db
}

// initIssueCG runs handleInit for cgID over the issue table and returns the
// group's initEpoch.
func initIssueCG(t *testing.T, srv *Server, cgID string) uint64 {
	t.Helper()
	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams(cgID))}
	if resp := srv.handleInit(initReq); resp.Error != nil {
		t.Fatalf("init error: %+v", resp.Error)
	}
	group := srv.getGroup(cgID, false)
	if group == nil {
		t.Fatalf("group %s missing after init", cgID)
	}
	return group.initEpoch.Load()
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

// oneQueryStreamParams builds addQueriesParams for a single query — the
// setup-hydrate shape shared by the advance/coread tests. The unary
// addQuery/addQueries RPCs were removed in the Phase-2 RPC-surface cleanup;
// addQueriesStream is the production hydrate path.
func oneQueryStreamParams(cgID, queryID string, ast builder.AST, initEpoch uint64) addQueriesParams {
	return addQueriesParams{
		ClientGroupID: cgID,
		Queries: []struct {
			QueryID string      `json:"queryID"`
			AST     builder.AST `json:"ast"`
		}{{QueryID: queryID, AST: ast}},
		InitEpoch:  initEpoch,
		RowMode:    true,
		PullMode:   true,
		PullWindow: 1024,
	}
}

// hydrateOneStreamOK hydrates a single query through handleAddQueriesStream
// (discarding partial frames), failing the test on an error response. Runs
// the cold/warm reader-pool arming (refreshSnapForInitialHydrateLocked +
// buildWarmReaderPoolLocked live in the streaming handler), so
// pool-sensitive tests keep their setup semantics.
func hydrateOneStreamOK(t *testing.T, srv *Server, cgID, queryID string, ast builder.AST, initEpoch uint64) {
	t.Helper()
	origDeliver := srv.abiDeliver
	if srv.abiDeliver == nil {
		col := newSinkCollector()
		srv.abiDeliver = col.sink
		defer func() { srv.abiDeliver = origDeliver }()
	}
	req := RPCRequest{Method: "addQueriesStream", ID: 2,
		Params: mustMarshal(t, oneQueryStreamParams(cgID, queryID, ast, initEpoch))}
	if resp := srv.handleAddQueriesStream(req, func(interface{}, interface{}) {}); resp.Error != nil {
		t.Fatalf("addQueriesStream(%s): %+v", queryID, resp.Error)
	}
}

// beginConcurrentSupported reports whether this build's SQLite has rocicorp's
// wal2 BEGIN CONCURRENT patch. Drive mode applies the diff into a PAST-pinned
// snapshot (prev), which only BEGIN CONCURRENT on wal2 permits; plain BEGIN
// (mattn's bundled SQLite, used by `go test`) rejects the write with
// "database is locked". The full drive path is validated by the wal2-tagged
// CI job + the soak, so drive-apply tests skip otherwise.
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

// makeReplicaPathOnly is makeReplica for fixtures that only need the path
// (the writer handle is managed by t.Cleanup either way).
func makeReplicaPathOnly(t *testing.T) string {
	t.Helper()
	path, _ := makeReplica(t)
	return path
}
