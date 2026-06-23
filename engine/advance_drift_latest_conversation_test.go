package engine

// Reproduction test for the advance drift on `channelLatestConversation`
// query (orderBy createdAt DESC, conversationId DESC + limit 1 + one()).
//
// Production symptom: during a 10-client 3-min load test, the shadow
// comparison detected Go producing 8 changes vs TS's 4 on advance
// `81b2dyns88`. Go over-emitted 2 extra changes on the `conversations`
// table:
//   - DELETE conversations/9d3f1b99 (row="__undef__")
//   - ADD conversations/a383a3b1 (new conversation row)
//
// The query shape: conversations.where('channelId', channelId)
//   .orderBy('createdAt', 'desc').orderBy('conversationId', 'desc')
//   .limit(1).one()
//
// This is a top-1 query with a filter on channelId and a two-column
// orderBy (createdAt DESC, conversationId DESC). When a new conversation
// is inserted that's newer than the current bound, Take should emit
// REMOVE(old bound) + ADD(new row) = 2 changes. If Go emits more,
// there's a bug in Take's displaced-bound handling during advance.
//
// The test tries multiple scenarios to find which one produces the drift.

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

// setupConversationsEngine creates a conversations table, seeds it,
// registers a TableSource, and returns the engine + source for advance
// testing.
func setupConversationsEngine(t *testing.T, seed []ivm.Row) (*Engine, *tablesource.Source) {
	t.Helper()
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
			`INSERT INTO conversations (conversationId, channelId, createdAt, doNotPostToChannel) VALUES (?, ?, ?, ?)`,
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
			"conversationId":    {Type: "string"},
			"channelId":         {Type: "string"},
			"createdAt":         {Type: "number"},
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

	return eng, src
}

// latestConversationAST builds the AST for the channelLatestConversation
// query: WHERE channelId = <channelId>, ORDER BY createdAt DESC,
// conversationId DESC, LIMIT 1.
func latestConversationAST(channelID string) builder.AST {
	limit := 1
	return builder.AST{
		Table: "conversations",
		Where: &builder.Condition{
			Type: "simple",
			Op:   "=",
			Left:  &builder.ValuePos{Type: "column", Name: "channelId"},
			Right: &builder.ValuePos{Type: "literal", Value: channelID},
		},
		OrderBy: ivm.Ordering{
			{"createdAt", "desc"},
			{"conversationId", "desc"},
		},
		Limit: &limit,
	}
}

// logChanges logs every RowChange with its type, table, and row key.
func logChanges(t *testing.T, label string, changes []RowChange) {
	t.Logf("%s: %d changes", label, len(changes))
	typeNames := map[int]string{0: "ADD", 1: "REMOVE", 2: "EDIT"}
	for i, c := range changes {
		rowSummary := ""
		if c.Row != nil {
			rowSummary = fmt.Sprintf("createdAt=%v conversationId=%v channelId=%v",
				c.Row["createdAt"], c.Row["conversationId"], c.Row["channelId"])
		} else {
			rowSummary = "(nil row)"
		}
		t.Logf("  [%d] %s table=%s rowKey=%v %s",
			i, typeNames[c.Type], c.Table, c.RowKey, rowSummary)
	}
}

// TestAdvanceDrift_PureAddNewConversation: Insert a new conversation
// that's newer than the current bound. Take should emit REMOVE(old
// bound) + ADD(new row) = 2 changes. If Go emits more, there's a bug.
func TestAdvanceDrift_PureAddNewConversation(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)
	if len(hydrate) != 1 {
		t.Fatalf("hydrate should produce 1 ADD, got %d", len(hydrate))
	}
	if hydrate[0].RowKey["conversationId"] != "9d3f1b99" {
		t.Fatalf("hydrate should contain 9d3f1b99, got %v", hydrate[0].RowKey)
	}

	// Advance: pure ADD of a new conversation newer than the bound.
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":    "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (pure ADD)", result.Changes)

	// Expected: REMOVE(9d3f1b99) + ADD(a383a3b1) = 2 changes.
	if len(result.Changes) != 2 {
		t.Errorf("expected 2 changes (REMOVE old bound + ADD new), got %d", len(result.Changes))
	}
}

// TestAdvanceDrift_EditNonSortKeyThenAdd: First an EDIT of the current
// bound (changing a non-sort column), then an ADD of a new conversation
// that's newer. The EDIT should forward as a single EDIT change, and
// the ADD should displace the bound. Total: 1 EDIT + 1 REMOVE + 1 ADD = 3.
func TestAdvanceDrift_EditNonSortKeyThenAdd(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv2", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Advance: EDIT 9d3f1b99 (change doNotPostToChannel) + ADD a383a3b1
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(1),
			},
		},
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":    "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (EDIT non-sort + ADD)", result.Changes)

	// Expected: EDIT(9d3f1b99) + REMOVE(9d3f1b99) + ADD(a383a3b1) = 3
	if len(result.Changes) != 3 {
		t.Errorf("expected 3 changes, got %d", len(result.Changes))
	}
}

// TestAdvanceDrift_EditSortKeyThenAdd: EDIT the bound to LOWER its
// createdAt (it drops out of the window), then ADD a new conversation
// that's newer. The EDIT should cause REMOVE(old bound) + ADD(next row),
// and the ADD should displace that. Total: 2 + 2 = 4.
func TestAdvanceDrift_EditSortKeyThenAdd(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv3", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Advance: EDIT 9d3f1b99 (createdAt: 1000→500, drops below conv-mid)
	// then ADD a383a3b1 (createdAt=2000)
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(500),
				"doNotPostToChannel": int64(0),
			},
		},
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":    "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (EDIT sort-key + ADD)", result.Changes)
}

// TestAdvanceDrift_PrevValuesWithAdd: SnapshotChange has PrevValues
// containing the old bound (simulates a unique-key conflict where
// inserting the new row displaces the old one). This produces
// REMOVE(old bound) + ADD(new) as SourceChanges. Take sees the REMOVE
// first, promotes the next row, then sees the ADD and displaces it.
// Total: REMOVE(old) + ADD(next) + REMOVE(next) + ADD(new) = 4.
func TestAdvanceDrift_PrevValuesWithAdd(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv4", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// SnapshotChange with PrevValues=[9d3f1b99] and NextValue=a383a3b1.
	// This simulates a unique-key conflict (e.g., a unique index on
	// (channelId, isLatest) where both rows have isLatest=true).
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":    "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (PrevValues + Add)", result.Changes)
}

// TestAdvanceDrift_EditSortKeyRaiseThenAdd: EDIT the bound to RAISE its
// createdAt (it stays as the bound, but with a higher sort value), then
// ADD a new conversation that's even newer. The EDIT should forward
// (oldCmp=0, newCmp<0 for limit=1 → replaceBoundAndForward), then the
// ADD should displace. Total: 1 + 2 = 3.
func TestAdvanceDrift_EditSortKeyRaiseThenAdd(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv5", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// EDIT 9d3f1b99: createdAt 1000→1500 (stays as top, sort key raised)
	// then ADD a383a3b1: createdAt=2000 (displaces 9d3f1b99)
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1500),
				"doNotPostToChannel": int64(0),
			},
		},
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":    "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (EDIT raise + ADD)", result.Changes)
}

// TestAdvanceDrift_MultipleAddsSameBatch: Multiple new conversations
// inserted in the same advance batch, all newer than the current bound.
// Each ADD should displace the previous bound. With limit=1, only the
// newest survives. Total: 2 * N changes (REMOVE + ADD per new row).
func TestAdvanceDrift_MultipleAddsSameBatch(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv6", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Add 3 new conversations, each newer than the last
	result := eng.Advance([]SnapshotChange{
		{Table: "conversations", NextValue: ivm.Row{
			"conversationId": "conv-new1", "channelId": "ch1", "createdAt": int64(1500), "doNotPostToChannel": int64(0),
		}},
		{Table: "conversations", NextValue: ivm.Row{
			"conversationId": "conv-new2", "channelId": "ch1", "createdAt": int64(1800), "doNotPostToChannel": int64(0),
		}},
		{Table: "conversations", NextValue: ivm.Row{
			"conversationId": "a383a3b1", "channelId": "ch1", "createdAt": int64(2000), "doNotPostToChannel": int64(0),
		}},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (3 ADDs)", result.Changes)

	// Expected: each ADD displaces the current bound.
	// ADD conv-new1: REMOVE(9d3f1b99) + ADD(conv-new1) = 2
	// ADD conv-new2: REMOVE(conv-new1) + ADD(conv-new2) = 2
	// ADD a383a3b1: REMOVE(conv-new2) + ADD(a383a3b1) = 2
	// Total: 6 changes
	if len(result.Changes) != 6 {
		t.Errorf("expected 6 changes (3 displaced bounds), got %d", len(result.Changes))
	}
}

// TestAdvanceDrift_EditSameSortThenAdd: EDIT the bound where the new
// sort value TIES with the new ADD's sort value (same createdAt).
// With conversationId DESC as tiebreaker, the new row with a higher
// conversationId sorts first. This is the tie scenario from the
// stale-bound test, but with an ADD instead of a second EDIT.
func TestAdvanceDrift_EditSameSortThenAdd(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv7", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// EDIT 9d3f1b99: createdAt 1000→2000
	// ADD a383a3b1: createdAt=2000 (SAME createdAt, but a383a3b1 > 9d3f1b99 in conversationId DESC)
	// So a383a3b1 should sort FIRST at the tie, displacing 9d3f1b99_new
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			}},
			NextValue: ivm.Row{
				"conversationId":    "9d3f1b99",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":    "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(2000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Logf("advance returned drift=%v", result.Drift)
	}
	logChanges(t, "advance (EDIT to same sort + ADD tie)", result.Changes)
}

// TestAdvanceDrift_AddWithSameSortTie: ADD a new conversation with the
// SAME createdAt as the current bound but a HIGHER conversationId (sorts
// first in DESC). This is the pure-ADD version of the tie scenario.
func TestAdvanceDrift_AddWithSameSortTie(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv8", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// ADD a383a3b1 with SAME createdAt=1000 as 9d3f1b99, but a383a3b1 > 9d3f1b99
	// in conversationId DESC, so a383a3b1 sorts first.
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":    "a383a3b1",
				"channelId":          "ch1",
				"createdAt":          int64(1000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (ADD with sort tie)", result.Changes)

	// Expected: a383a3b1 sorts first at the tie → displaces 9d3f1b99
	// REMOVE(9d3f1b99) + ADD(a383a3b1) = 2 changes
	if len(result.Changes) != 2 {
		t.Errorf("expected 2 changes (REMOVE old bound + ADD new), got %d", len(result.Changes))
	}
}

// TestAdvanceDrift_AddFromDifferentChannel: ADD a new conversation in a
// DIFFERENT channel. The filter (channelId=ch1) should drop it, so
// no changes should be emitted.
func TestAdvanceDrift_AddFromDifferentChannel(t *testing.T) {
	seed := []ivm.Row{
		{"conversationId": "9d3f1b99", "channelId": "ch1", "createdAt": int64(1000), "doNotPostToChannel": int64(0)},
		{"conversationId": "conv-mid", "channelId": "ch1", "createdAt": int64(900), "doNotPostToChannel": int64(0)},
	}
	eng, _ := setupConversationsEngine(t, seed)

	hydrate, _, err := eng.AddQuery("q-latest-conv9", latestConversationAST("ch1"))
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// ADD a conversation in ch2 (should be filtered out)
	result := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			NextValue: ivm.Row{
				"conversationId":    "other-conv",
				"channelId":          "ch2",
				"createdAt":          int64(5000),
				"doNotPostToChannel": int64(0),
			},
		},
	})
	if result.Drift != nil {
		t.Fatalf("unexpected drift: %v", result.Drift)
	}
	logChanges(t, "advance (ADD from different channel)", result.Changes)

	if len(result.Changes) != 0 {
		t.Errorf("expected 0 changes (filtered out), got %d", len(result.Changes))
	}
}
