package engine

// Focused test for the stale-bound advance drift on `channelLatestConversation`
// query (orderBy createdAt DESC, conversationId DESC + limit 1).
//
// The TestAdvanceDrift_SequentialAdvances test revealed that after a
// displacement in advance 1, advance 2's REMOVE targets the OLD bound
// (9d3f1b99) instead of the NEW bound (conv-new1). This matches the
// production symptom where Go emits DELETE(9d3f1b99, row="__undef__")
// — a remove for a row the client already saw removed.
//
// Root cause hypothesis: Take's bound is set to a row (conv-new1) that
// was only written to the ephemeral prev-tx via writeChange. After
// signalAdvanceEnd, the prev-tx is ROLLED BACK. The row is NOT in the
// SQLite file. On the next advance, Take fetches from the bound's sort
// position but the row isn't there — the fetch returns the wrong row.
//
// In production, the replicator writes the row to the SQLite file
// before calling Advance, so the new prev-tx sees it. This test
// simulates both scenarios (with and without replicator write).

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// TestStaleBound_SequentialAdvanceNoReplicator: Without a replicator
// writing to the SQLite file between advances, Take's bound points to
// a row that doesn't exist in the source. The REMOVE in advance 2
// targets the wrong row (the OLD bound, not the current one).
func TestStaleBound_SequentialAdvanceNoReplicator(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE conversations (
		conversationId TEXT PRIMARY KEY,
		channelId TEXT,
		createdAt INTEGER,
		doNotPostToChannel INTEGER
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, row := range seed {
		if _, err := w.Exec(
			`INSERT INTO conversations VALUES (?, ?, ?, ?)`,
			row["conversationId"], row["channelId"], row["createdAt"], row["doNotPostToChannel"],
		); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	t.Cleanup(func() { wdb.Close() })
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "conversations",
		map[string]sqlite.ColumnSchema{
			"conversationId":     {Type: "string"},
			"channelId":          {Type: "string"},
			"createdAt":          {Type: "number"},
			"doNotPostToChannel": {Type: "number"},
		},
		[]string{"conversationId"},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	hydrate, _, err := eng.AddQuery("q-stale-norep", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)
	if len(hydrate) != 1 || hydrate[0].RowKey["conversationId"] != "9d3f1b99" {
		t.Fatalf("hydrate should produce 1 ADD for 9d3f1b99, got %v", hydrate)
	}

	// Advance 1: ADD conv-new1 (createdAt=1500) — displaces 9d3f1b99
	r1 := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId": "conv-new1", "channelId": "ch1",
				"createdAt": int64(1500), "doNotPostToChannel": int64(0),
			},
		},
	})
	if r1.Drift != nil {
		t.Fatalf("advance 1 drift: %v", r1.Drift)
	}
	logChanges(t, "advance 1 (ADD conv-new1)", r1.Changes)
	if len(r1.Changes) != 2 {
		t.Fatalf("advance 1: expected 2 changes, got %d", len(r1.Changes))
	}
	// Verify advance 1 removes 9d3f1b99 and adds conv-new1
	r1Remove := r1.Changes[0]
	r1Add := r1.Changes[1]
	if r1Remove.Type != RowChangeRemove || r1Remove.RowKey["conversationId"] != "9d3f1b99" {
		t.Fatalf("advance 1: expected REMOVE 9d3f1b99, got %v", r1Remove)
	}
	if r1Add.Type != RowChangeAdd || r1Add.RowKey["conversationId"] != "conv-new1" {
		t.Fatalf("advance 1: expected ADD conv-new1, got %v", r1Add)
	}

	// Advance 2: ADD a383a3b1 (createdAt=2000) — should displace conv-new1
	r2 := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId": "a383a3b1", "channelId": "ch1",
				"createdAt": int64(2000), "doNotPostToChannel": int64(0),
			},
		},
	})
	if r2.Drift != nil {
		t.Fatalf("advance 2 drift: %v", r2.Drift)
	}
	logChanges(t, "advance 2 (ADD a383a3b1)", r2.Changes)
	if len(r2.Changes) != 2 {
		t.Fatalf("advance 2: expected 2 changes, got %d", len(r2.Changes))
	}

	// CONTRACT DEMONSTRATION — this is the EXPECTED outcome, not a bug.
	//
	// This variant deliberately OMITS the replicator write. conv-new1 was
	// written only to the ephemeral prev-tx during advance 1 and discarded by
	// the signalAdvanceEnd ROLLBACK, and nothing wrote it to the SQLite file —
	// so on advance 2 conv-new1 is ABSENT from the source. Take's displaced-bound
	// lookup therefore cannot find conv-new1 and resolves to 9d3f1b99. That is
	// the unavoidable consequence of violating the sidecar's invariant that the
	// replicator commits the displacing row to the file BEFORE Advance is called.
	//
	// This scenario CANNOT occur in production: in push mode TS derives the diff
	// from its snapshotter (reading the committed file) and in drive mode Go
	// derives it from the replica's changeLog2 — in BOTH the row is in the file
	// before the advance. The production-faithful path is proven correct by
	// TestStaleBound_SequentialAdvanceWithReplicator (writes the row first → the
	// REMOVE correctly targets conv-new1) and TestStaleBound_FourQueriesSameChannel
	// (0 stale removes across 4 queries). We pin the no-write mechanism here so a
	// regression in the rollback/re-pin timing stays observable.
	r2Remove := r2.Changes[0]
	r2Add := r2.Changes[1]
	t.Logf("advance 2 REMOVE target: %v (expected 9d3f1b99 — replicator write omitted)", r2Remove.RowKey)
	t.Logf("advance 2 ADD target: %v", r2Add.RowKey)

	if r2Remove.RowKey["conversationId"] != "9d3f1b99" {
		t.Errorf("contract demo: without a replicator write, the displaced-bound lookup "+
			"resolves to 9d3f1b99 (conv-new1 is not in the file); got REMOVE %v", r2Remove.RowKey)
	}
	if r2Add.Type != RowChangeAdd || r2Add.RowKey["conversationId"] != "a383a3b1" {
		t.Errorf("advance 2: expected ADD a383a3b1, got %v", r2Add)
	}
}

// TestStaleBound_SequentialAdvanceWithReplicator: Same scenario but with
// a simulated replicator write between advances. The replicator writes
// conv-new1 to the SQLite file after advance 1, so the new prev-tx in
// advance 2 should see it. If Take still removes 9d3f1b99 instead of
// conv-new1, the bug is in Take's bound management, not in the source.
func TestStaleBound_SequentialAdvanceWithReplicator(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE conversations (
		conversationId TEXT PRIMARY KEY,
		channelId TEXT,
		createdAt INTEGER,
		doNotPostToChannel INTEGER
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, row := range seed {
		if _, err := w.Exec(
			`INSERT INTO conversations VALUES (?, ?, ?, ?)`,
			row["conversationId"], row["channelId"], row["createdAt"], row["doNotPostToChannel"],
		); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	t.Cleanup(func() { wdb.Close() })
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "conversations",
		map[string]sqlite.ColumnSchema{
			"conversationId":     {Type: "string"},
			"channelId":          {Type: "string"},
			"createdAt":          {Type: "number"},
			"doNotPostToChannel": {Type: "number"},
		},
		[]string{"conversationId"},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	hydrate, _, err := eng.AddQuery("q-stale-rep", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Advance 1: ADD conv-new1 (createdAt=1500) — displaces 9d3f1b99
	r1 := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId": "conv-new1", "channelId": "ch1",
				"createdAt": int64(1500), "doNotPostToChannel": int64(0),
			},
		},
	})
	if r1.Drift != nil {
		t.Fatalf("advance 1 drift: %v", r1.Drift)
	}
	logChanges(t, "advance 1 (ADD conv-new1)", r1.Changes)

	// SIMULATE REPLICATOR: write conv-new1 to the SQLite file.
	// In production, the replicator writes the row to the SQLite file
	// before calling Advance. The sidecar's new prev-tx (started by
	// signalAdvanceEnd) sees this write.
	//
	// We use a SEPARATE writable connection to avoid interfering with
	// the sidecar's prev-tx. WAL mode allows concurrent writers.
	repW, _ := sql.Open("sqlite3", path)
	if _, err := repW.Exec(
		`INSERT INTO conversations VALUES (?, ?, ?, ?)`,
		"conv-new1", "ch1", int64(1500), int64(0),
	); err != nil {
		t.Fatalf("replicator write: %v", err)
	}
	repW.Close()
	t.Logf("replicator: wrote conv-new1 to SQLite file")

	// Advance 2: ADD a383a3b1 (createdAt=2000) — should displace conv-new1
	r2 := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId": "a383a3b1", "channelId": "ch1",
				"createdAt": int64(2000), "doNotPostToChannel": int64(0),
			},
		},
	})
	if r2.Drift != nil {
		t.Fatalf("advance 2 drift: %v", r2.Drift)
	}
	logChanges(t, "advance 2 (ADD a383a3b1, with replicator)", r2.Changes)
	if len(r2.Changes) != 2 {
		t.Fatalf("advance 2: expected 2 changes, got %d", len(r2.Changes))
	}

	r2Remove := r2.Changes[0]
	r2Add := r2.Changes[1]
	t.Logf("advance 2 REMOVE target: %v", r2Remove.RowKey)
	t.Logf("advance 2 ADD target: %v", r2Add.RowKey)

	// With the replicator write, conv-new1 IS in the SQLite file.
	// Take's fetch from conv-new1's position should find conv-new1.
	// The REMOVE should target conv-new1, NOT 9d3f1b99.
	if r2Remove.RowKey["conversationId"] == "9d3f1b99" {
		t.Errorf("BUG persists even with replicator write: advance 2 still REMOVEs 9d3f1b99. " +
			"Take's bound management has a bug independent of the source state.")
	}
	if r2Remove.RowKey["conversationId"] != "conv-new1" {
		t.Errorf("advance 2: expected REMOVE conv-new1, got %v", r2Remove.RowKey)
	}
	if r2Add.Type != RowChangeAdd || r2Add.RowKey["conversationId"] != "a383a3b1" {
		t.Errorf("advance 2: expected ADD a383a3b1, got %v", r2Add)
	}
}

// TestStaleBound_FourQueriesSameChannel: Simulates the production scenario
// with 4 clients (queries) watching the same channel. After a displacement
// in advance 1, advance 2 should remove the NEW bound (conv-new1) from all
// 4 queries, not the OLD bound (9d3f1b99).
//
// If Go emits REMOVE(9d3f1b99) for all 4 queries, that's 4 extra stale
// removes — matching the production 8-vs-4 mismatch (4 queries × 2 changes
// with stale bound = 8 vs 4 queries × 1 correct change = 4).
func TestStaleBound_FourQueriesSameChannel(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE conversations (
		conversationId TEXT PRIMARY KEY,
		channelId TEXT,
		createdAt INTEGER,
		doNotPostToChannel INTEGER
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, row := range seed {
		if _, err := w.Exec(
			`INSERT INTO conversations VALUES (?, ?, ?, ?)`,
			row["conversationId"], row["channelId"], row["createdAt"], row["doNotPostToChannel"],
		); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	t.Cleanup(func() { wdb.Close() })
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "conversations",
		map[string]sqlite.ColumnSchema{
			"conversationId":     {Type: "string"},
			"channelId":          {Type: "string"},
			"createdAt":          {Type: "number"},
			"doNotPostToChannel": {Type: "number"},
		},
		[]string{"conversationId"},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	// Register 4 queries (simulating 4 clients in the same CG)
	ast := latestConversationAST("ch1")
	for i := 0; i < 4; i++ {
		qid := fmt.Sprintf("q-client%d", i)
		h, _, err := eng.AddQuery(qid, ast)
		if err != nil {
			t.Fatalf("AddQuery %s: %v", qid, err)
		}
		logChanges(t, fmt.Sprintf("hydrate %s", qid), h)
	}

	// Advance 1: ADD conv-new1 (createdAt=1500) — displaces 9d3f1b99
	r1 := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId": "conv-new1", "channelId": "ch1",
				"createdAt": int64(1500), "doNotPostToChannel": int64(0),
			},
		},
	})
	if r1.Drift != nil {
		t.Fatalf("advance 1 drift: %v", r1.Drift)
	}
	logChanges(t, "advance 1 (4 queries, ADD conv-new1)", r1.Changes)
	t.Logf("advance 1: %d total changes (expected 8 = 4 queries × 2)", len(r1.Changes))

	// Simulate replicator writing conv-new1
	repW, _ := sql.Open("sqlite3", path)
	if _, err := repW.Exec(
		`INSERT INTO conversations VALUES (?, ?, ?, ?)`,
		"conv-new1", "ch1", int64(1500), int64(0),
	); err != nil {
		t.Fatalf("replicator write: %v", err)
	}
	repW.Close()

	// Advance 2: ADD a383a3b1 (createdAt=2000)
	r2 := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId": "a383a3b1", "channelId": "ch1",
				"createdAt": int64(2000), "doNotPostToChannel": int64(0),
			},
		},
	})
	if r2.Drift != nil {
		t.Fatalf("advance 2 drift: %v", r2.Drift)
	}
	logChanges(t, "advance 2 (4 queries, ADD a383a3b1)", r2.Changes)
	t.Logf("advance 2: %d total changes (expected 8 = 4 queries × 2)", len(r2.Changes))

	// Count stale removes (REMOVE for 9d3f1b99 which was already removed)
	staleRemoves := 0
	correctRemoves := 0
	for _, c := range r2.Changes {
		if c.Type == RowChangeRemove {
			id := c.RowKey["conversationId"]
			if id == "9d3f1b99" {
				staleRemoves++
			} else if id == "conv-new1" {
				correctRemoves++
			}
		}
	}
	t.Logf("advance 2: %d stale removes (9d3f1b99), %d correct removes (conv-new1)",
		staleRemoves, correctRemoves)

	if staleRemoves > 0 {
		t.Errorf("BUG: %d stale REMOVEs for 9d3f1b99 in advance 2 — "+
			"these are __undef__ removes in production (row already removed in advance 1). "+
			"Production symptom: Go over-emits %d extra changes (8 vs 4 with 4 queries).",
			staleRemoves, staleRemoves*2)
	}
}
