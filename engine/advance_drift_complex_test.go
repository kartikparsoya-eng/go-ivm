package engine

// Additional advance drift tests with more complex scenarios:
// - Multiple queries on the same source (production has 10 clients)
// - Larger seed (3+ conversations in channel)
// - Mixed advance batches (conversations + other tables)
// - SplitEdit key scenario (channelId change with CSQ splitEditKeys)

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// requireConcurrentWrites skips a test unless the linked SQLite build supports
// BEGIN CONCURRENT (rocicorp's wal2 patch). A test that WRITES to two or more
// tablesources within ONE advance acquires a writable conn per source; under
// plain BEGIN (the mattn-bundled build used by local `go test`) the second
// writer deadlocks with "database is locked". Production runs the wal2 build
// where BEGIN CONCURRENT lets the per-source writers coexist, so this is a
// build-capability gap in the local harness, not a product defect. Probing a
// throwaway DB keeps the gate self-contained.
func requireConcurrentWrites(t *testing.T) {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "concurrent-probe.sqlite"))
	if err != nil {
		t.Fatalf("concurrent probe: open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("concurrent probe: wal: %v", err)
	}
	if _, err := db.Exec("BEGIN CONCURRENT"); err != nil {
		t.Skipf("SQLite build lacks BEGIN CONCURRENT (wal2 patch); "+
			"multi-tablesource advance deadlocks under plain BEGIN — runs on the prod build: %v", err)
	}
	_, _ = db.Exec("ROLLBACK")
}

// TestAdvanceDrift_MultiQuerySameSource: Two queries watching the SAME
// channel. When a new conversation is added, both queries should produce
// the same changes. The total should be 2x the single-query output.
func TestAdvanceDrift_MultiQuerySameSource(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-old", "channelId": "ch1", "createdAt": int64(800), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	ast := latestConversationAST("ch1")
	h1, _, err := eng.AddQuery("q-client1", ast)
	if err != nil {
		t.Fatalf("AddQuery q1: %v", err)
	}
	h2, _, err := eng.AddQuery("q-client2", ast)
	if err != nil {
		t.Fatalf("AddQuery q2: %v", err)
	}
	logChanges(t, "hydrate q1", h1)
	logChanges(t, "hydrate q2", h2)

	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":     "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (2 queries, pure ADD)", result.Changes)

	// Each query should produce REMOVE(9d3f1b99) + ADD(a383a3b1) = 2 changes.
	// Total: 4 changes.
	q1Changes := 0
	q2Changes := 0
	for _, c := range result.Changes {
		if c.QueryID == "q-client1" {
			q1Changes++
		} else if c.QueryID == "q-client2" {
			q2Changes++
		}
	}
	t.Logf("q-client1: %d changes, q-client2: %d changes", q1Changes, q2Changes)
	if q1Changes != 2 {
		t.Errorf("q-client1: expected 2 changes, got %d", q1Changes)
	}
	if q2Changes != 2 {
		t.Errorf("q-client2: expected 2 changes, got %d", q2Changes)
	}
}

// TestAdvanceDrift_ThreeRowSeed: With 3 conversations in the channel,
// add a new one that's newer. The displaced-bound fetch should find
// the correct next row.
func TestAdvanceDrift_ThreeRowSeed(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-old", "channelId": "ch1", "createdAt": int64(800), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-3row", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Add a new conversation that's newer than all existing ones
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":     "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (3-row seed, ADD newest)", result.Changes)
	if len(result.Changes) != 2 {
		t.Errorf("expected 2 changes, got %d", len(result.Changes))
	}
}

// TestAdvanceDrift_EditBoundWithSplitEditKeys: Test the splitEdit path.
// If the query has a CSQ that correlates on channelId, and an EDIT
// changes channelId, the edit should be split into REMOVE + ADD.
// This tests whether the split causes extra changes.
func TestAdvanceDrift_EditBoundWithSplitEditKeys(t *testing.T) {
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
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
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
		t.Fatalf("tablesource.New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	// AST with a CSQ that correlates on channelId — this makes
	// splitEditKeys include "channelId". An EDIT that changes channelId
	// will be split into REMOVE + ADD by the source.
	limit := 1
	ast := builder.AST{
		Table: "conversations",
		Where: &builder.Condition{
			Type: "and",
			Conditions: []builder.Condition{
				{
					Type:  "simple",
					Op:    "=",
					Left:  &builder.ValuePos{Type: "column", Name: "channelId"},
					Right: &builder.ValuePos{Type: "literal", Value: "ch1"},
				},
				{
					Type: "correlatedSubquery",
					Op:   "EXISTS",
					Related: &builder.CorrelatedSubquery{
						System: "client",
						Correlation: builder.Correlation{
							ParentField: []string{"channelId"},
							ChildField:  []string{"id"},
						},
						Subquery: builder.AST{
							Table: "conversations",
							Alias: "zsubq_conv",
						},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{
			{"createdAt", "desc"},
			{"conversationId", "desc"},
		},
		Limit: &limit,
	}

	hydrate, _, err := eng.AddQuery("q-split", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate (with CSQ)", hydrate)

	// EDIT 9d3f1b99: change channelId from ch1 to ch2.
	// Since splitEditKeys includes channelId (from the CSQ correlation),
	// this EDIT will be SPLIT into REMOVE(9d3f1b99, ch1) + ADD(9d3f1b99, ch2).
	// The REMOVE passes the filter (channelId=ch1 matches old value),
	// the ADD fails the filter (channelId=ch2 != ch1).
	// So Take sees: REMOVE(9d3f1b99) — the bound is removed.
	// Take should: find new bound (conv-mid), emit REMOVE(9d3f1b99) + ADD(conv-mid) = 2.
	// The ADD(9d3f1b99, ch2) is filtered out → 0 changes.
	// Total: 2 changes.
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":     "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":     "9d3f1b99",
				"channelId":          "ch2",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (EDIT channelId with splitEditKeys)", result.Changes)
}

// TestAdvanceDrift_MixedBatchMultipleTables: Advance batch contains
// changes for conversations AND a different table. The conversations
// change should only affect the conversations query.
func TestAdvanceDrift_MixedBatchMultipleTables(t *testing.T) {
	requireConcurrentWrites(t) // writes conversations + messages in one advance
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
		t.Fatalf("create conversations: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE messages (
		id TEXT PRIMARY KEY,
		conversationId TEXT,
		createdAt INTEGER
	)`); err != nil {
		t.Fatalf("create messages: %v", err)
	}
	convSeed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	for _, row := range convSeed {
		if _, err := w.Exec(
			`INSERT INTO conversations VALUES (?, ?, ?, ?)`,
			row["conversationId"], row["channelId"], row["createdAt"], row["doNotPostToChannel"],
		); err != nil {
			t.Fatalf("seed conv: %v", err)
		}
	}
	if _, err := w.Exec(`INSERT INTO messages VALUES (?, ?, ?)`, "msg-1", "9d3f1b99", int64(950)); err != nil {
		t.Fatalf("seed msg: %v", err)
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	t.Cleanup(func() { wdb.Close() })
	t.Cleanup(func() { db.Close() })

	convSrc, err := tablesource.New(db, wdb, "conversations",
		map[string]sqlite.ColumnSchema{
			"conversationId":     {Type: "string"},
			"channelId":          {Type: "string"},
			"createdAt":          {Type: "number"},
			"doNotPostToChannel": {Type: "number"},
		},
		[]string{"conversationId"},
	)
	if err != nil {
		t.Fatalf("conv New: %v", err)
	}
	msgSrc, err := tablesource.New(db, wdb, "messages",
		map[string]sqlite.ColumnSchema{
			"id":             {Type: "string"},
			"conversationId": {Type: "string"},
			"createdAt":      {Type: "number"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("msg New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(convSrc)
	eng.RegisterSource(msgSrc)

	hydrate, _, err := eng.AddQuery("q-latest", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Advance: ADD new conversation + ADD new message
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":     "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
		{
			Table: "messages",
			NextValue: ivm.Row{
				"id":             "msg-2",
				"conversationId": "a383a3b1",
				"createdAt":      int64(2001),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (mixed batch)", result.Changes)

	// Only the conversations query is registered, so only conversations
	// changes should appear. messages changes should not (no query reads
	// from messages).
	convChanges := 0
	for _, c := range result.Changes {
		if c.Table == "conversations" {
			convChanges++
		}
	}
	if convChanges != 2 {
		t.Errorf("expected 2 conversations changes, got %d", convChanges)
	}
}

// TestAdvanceDrift_SequentialAdvances: Two advances in sequence.
// First advance adds a conversation, second advance adds another.
// After the first advance, Take's bound is updated. The second
// advance should work correctly with the new bound.
func TestAdvanceDrift_SequentialAdvances(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-seq", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// First advance: add conv-new1 (createdAt=1500)
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
	logChanges(t, "advance 1", r1.Changes)
	if len(r1.Changes) != 2 {
		t.Errorf("advance 1: expected 2 changes, got %d", len(r1.Changes))
	}

	// Second advance: add a383a3b1 (createdAt=2000)
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
	logChanges(t, "advance 2", r2.Changes)
	if len(r2.Changes) != 2 {
		t.Errorf("advance 2: expected 2 changes, got %d", len(r2.Changes))
	}
}

// TestAdvanceDrift_RemoveBoundThenAdd: REMOVE the current bound
// (pure delete), then ADD a new conversation. The REMOVE should
// promote the next row, and the ADD should displace it.
func TestAdvanceDrift_RemoveBoundThenAdd(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-del-add", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// REMOVE 9d3f1b99 then ADD a383a3b1
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":     "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			}},
		},
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":     "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (REMOVE bound + ADD new)", result.Changes)

	// REMOVE 9d3f1b99: Take sees REMOVE for bound, promotes conv-mid
	//   → REMOVE(9d3f1b99) + ADD(conv-mid) = 2
	// ADD a383a3b1: Take sees ADD, size==1, bound=conv-mid
	//   → REMOVE(conv-mid) + ADD(a383a3b1) = 2
	// Total: 4 changes
	if len(result.Changes) != 4 {
		t.Errorf("expected 4 changes, got %d", len(result.Changes))
	}
}

// TestAdvanceDrift_EditNonBoundThenAdd: EDIT a non-bound conversation
// (one that's below the bound in the sort order), then ADD a new
// conversation that's newer. The EDIT should be dropped (outside the
// bound), and the ADD should displace the bound.
func TestAdvanceDrift_EditNonBoundThenAdd(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-edit-nonbound", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// EDIT conv-mid (change doNotPostToChannel, not sort key) — conv-mid
	// is BELOW the bound (9d3f1b99), so this EDIT is outside the window.
	// Take should drop it (oldCmp > 0, newCmp > 0 → both outside).
	// Then ADD a383a3b1 (createdAt=2000) — displaces the bound.
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":     "conv-mid",
				"channelId":          "ch1",
				"createdAt":          int64(900),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":     "conv-mid",
				"channelId":          "ch1",
				"createdAt":          int64(900),
				"doNotPostToChannel": int64(1),
			},
		},
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":     "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (EDIT non-bound + ADD)", result.Changes)

	// EDIT conv-mid: outside bound → dropped (0 changes)
	// ADD a383a3b1: displaces bound → REMOVE(9d3f1b99) + ADD(a383a3b1) = 2
	// Total: 2 changes
	if len(result.Changes) != 2 {
		t.Errorf("expected 2 changes, got %d", len(result.Changes))
	}
}

// TestAdvanceDrift_EditNonBoundRaisesIntoBoundThenAdd: EDIT a non-bound
// conversation that RAISES its createdAt above the current bound, then
// ADD a new conversation. The EDIT should bring the row into the window,
// displacing the old bound. Then the ADD should displace the new bound.
func TestAdvanceDrift_EditNonBoundRaisesIntoBoundThenAdd(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-edit-raise", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// EDIT conv-mid: createdAt 900→1500 (raises above the bound 9d3f1b99@1000)
	// oldCmp = compareRows(conv-mid_old(900), bound=9d3f1b99(1000)) in DESC:
	//   900 < 1000 → conv-mid sorts after → oldCmp > 0 (outside)
	// newCmp = compareRows(conv-mid_new(1500), bound=9d3f1b99(1000)) in DESC:
	//   1500 > 1000 → conv-mid sorts before → newCmp < 0 (inside)
	// oldCmp > 0, newCmp < 0 → "old outside, new inside" path
	// Take fetches to find new bound and old bound, emits:
	//   REMOVE(oldBoundNode) + ADD(conv-mid_new) = 2 changes
	//   (oldBoundNode is the old bound = 9d3f1b99)
	// Wait, let me re-read the code...
	// Actually: oldCmp > 0, newCmp < 0:
	//   fetch with Start=bound, Basis="at", Reverse=true
	//   gets: bound(9d3f1b99), conv-mid_old(900), ...
	//   but overlay has EDIT: removes conv-mid_old, inserts conv-mid_new(1500)
	//   After overlay: [9d3f1b99(1000), ..., conv-mid_new(1500) is before 9d3f1b99 in DESC → filtered by start gate]
	//   So first two: 9d3f1b99(1000), ... (conv-mid_old removed)
	//   oldBoundNode = 9d3f1b99, newBoundNode = next row (or conv-old if exists)
	//
	// Then ADD a383a3b1 (createdAt=2000): displaces the new bound.
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":     "conv-mid",
				"channelId":          "ch1",
				"createdAt":          int64(900),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":     "conv-mid",
				"channelId":          "ch1",
				"createdAt":          int64(1500),
				"doNotPostToChannel": int64(0),
			},
		},
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":     "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Logf("advance returned drift=%v", result.Drift)
	}
	logChanges(t, "advance (EDIT non-bound raise + ADD)", result.Changes)
}

// TestAdvanceDrift_SingleConversationOnly: Edge case where there's only
// ONE conversation in the channel. When a new conversation is added,
// the bound should be displaced. With no "next row" to promote, the
// REMOVE path might behave differently.
func TestAdvanceDrift_SingleConversationOnly(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-single", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// ADD a383a3b1 (newer) — should displace the single bound
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":     "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (single conv, ADD)", result.Changes)
	if len(result.Changes) != 2 {
		t.Errorf("expected 2 changes, got %d", len(result.Changes))
	}
}

// TestAdvanceDrift_DebugLogLabels: Enable Take debug labels to trace
// the exact Take state transitions during advance.
func TestAdvanceDrift_DebugLogLabels(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, src := setupConversationsEngine(t, seed)
	_ = src

	// Enable debug output
	limit := 1
	ast := builder.AST{
		Table: "conversations",
		Where: &builder.Condition{
			Type:  "simple",
			Op:    "=",
			Left:  &builder.ValuePos{Type: "column", Name: "channelId"},
			Right: &builder.ValuePos{Type: "literal", Value: "ch1"},
		},
		OrderBy: ivm.Ordering{
			{"createdAt", "desc"},
			{"conversationId", "desc"},
		},
		Limit: &limit,
	}
	hydrate, _, err := eng.AddQuery("q-debug", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Try the EDIT + ADD combo that's most likely to trigger the drift
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":     "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":     "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(1),
			},
		},
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":     "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (EDIT + ADD with debug)", result.Changes)
	t.Logf("total changes: %d", len(result.Changes))
	// Print a summary
	adds, removes, edits := 0, 0, 0
	for _, c := range result.Changes {
		switch c.Type {
		case RowChangeAdd:
			adds++
		case RowChangeRemove:
			removes++
		case RowChangeEdit:
			edits++
		}
	}
	t.Logf("summary: %d ADDs, %d REMOVEs, %d EDITs", adds, removes, edits)
	_ = fmt.Sprintf("")
}
