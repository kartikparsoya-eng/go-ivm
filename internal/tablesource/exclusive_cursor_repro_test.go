package tablesource

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// Repro for the shadow-soak exclusive-cursor boundary over-include:
// conversations list, sort [createdAt asc, <pk> asc], start={createdAt}
// (PARTIAL — no pk), exclusive=true, where the cursor's createdAt is the
// NEWEST row. Replica ground-truth: correct page is EMPTY (nothing strictly
// after the cursor). TS over-included the boundary row (+1); Go over-included
// the boundary PLUS the next-older row (+2). Here `score` stands in for
// `createdAt` and `id` for the pk; score=90 (alice) is the max.
func TestExclusiveCursorPartialBound_FetchPath(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	sort := ivm.Ordering{{"score", "asc"}, {"id", "asc"}}

	// (a) Source.Fetch directly with the Start that Skip.getStart synthesizes
	//     for a partial exclusive bound: Basis="after", Row={score:90}.
	in := src.Connect(sort, nil, nil)
	direct := in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{Row: ivm.Row{"score": float64(90)}, Basis: "after"},
	})
	if len(direct) != 0 {
		t.Errorf("Source.Fetch(after partial {score:90}): got %d rows, want 0; rows=%v",
			len(direct), rowsOf(direct))
	}

	// (b) Full Skip path (what the pipeline actually builds).
	in2 := src.Connect(sort, nil, nil)
	skip := ivm.NewSkip(in2, ivm.Bound{
		Row:       ivm.Row{"score": float64(90)},
		Exclusive: true,
	})
	viaSkip := skip.Fetch(ivm.FetchRequest{})
	if len(viaSkip) != 0 {
		t.Errorf("Skip.Fetch(exclusive partial {score:90}): got %d rows, want 0; rows=%v",
			len(viaSkip), rowsOf(viaSkip))
	}
}

func rowsOf(nodes []ivm.Node) []ivm.Row {
	out := make([]ivm.Row, len(nodes))
	for i, n := range nodes {
		out[i] = n.Row
	}
	return out
}

// TestExclusiveCursorPartialBound_OptionalPK reproduces the production Bug (B)
// where channelConversationsPaginatedV3 over-includes the boundary row.
//
// Production scenario:
//   - Table: conversations, PK: conversationId (marked Optional in schema)
//   - Sort: [createdAt asc, conversationId asc] (PK appended by completeOrdering)
//   - Cursor: {createdAt: 1779863389596} (partial — no conversationId), exclusive=true
//   - Bug: gatherStartConstraints generates:
//     (createdAt > ?) OR (createdAt = ? AND (? IS NULL OR conversationId > ?))
//     The second clause evaluates to TRUE because NULL IS NULL → TRUE,
//     admitting the boundary row.
//   - Fix: stop generating OR clauses at the first sort field missing from start.Row.
func TestExclusiveCursorPartialBound_OptionalPK(t *testing.T) {
	// Create a conversations-like table with an Optional PK column
	path := filepath.Join(t.TempDir(), "conversations.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE conversations (
			conversationId TEXT,
			channelId TEXT,
			createdAt REAL
		)`,
		// Insert rows — the cursor value is 500.0. There's one row AT the cursor
		// and several before/after it.
		`INSERT INTO conversations VALUES
			('conv-a', 'ch1', 400.0),
			('conv-b', 'ch1', 500.0),
			('conv-c', 'ch1', 600.0),
			('conv-d', 'ch1', 700.0)`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	// Schema: conversationId marked Optional (this is the trigger for the bug)
	columns := map[string]sqlite.ColumnSchema{
		"conversationId": {Type: "string", Optional: true},
		"channelId":      {Type: "string"},
		"createdAt":      {Type: "number"},
	}

	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	wdb := openWritableForTest(t, path)
	defer wdb.Close()

	src, err := New(db, wdb, "conversations", columns, []string{"conversationId"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Sort: [createdAt asc, conversationId asc] (simulates completeOrdering)
	sort := ivm.Ordering{{"createdAt", "asc"}, {"conversationId", "asc"}}

	// Partial cursor: only createdAt specified, exclusive (basis "after").
	// Should return rows STRICTLY AFTER createdAt=500 → conv-c (600) and conv-d (700).
	in := src.Connect(sort, nil, nil)
	nodes := in.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{Row: ivm.Row{"createdAt": float64(500)}, Basis: "after"},
	})

	// Verify: should NOT include conv-b (createdAt=500, the cursor boundary row)
	for _, n := range nodes {
		if n.Row["createdAt"] == float64(500) {
			t.Errorf("boundary row (createdAt=500) incorrectly included; got %v", n.Row)
		}
	}
	if len(nodes) != 2 {
		t.Errorf("expected 2 rows (createdAt=600, 700), got %d: %v", len(nodes), rowsOf(nodes))
	}

	// Also test via Skip (the actual pipeline path)
	in2 := src.Connect(sort, nil, nil)
	skip := ivm.NewSkip(in2, ivm.Bound{
		Row:       ivm.Row{"createdAt": float64(500)},
		Exclusive: true,
	})
	viaSkip := skip.Fetch(ivm.FetchRequest{})
	for _, n := range viaSkip {
		if n.Row["createdAt"] == float64(500) {
			t.Errorf("Skip path: boundary row (createdAt=500) incorrectly included; got %v", n.Row)
		}
	}
	if len(viaSkip) != 2 {
		t.Errorf("Skip path: expected 2 rows, got %d: %v", len(viaSkip), rowsOf(viaSkip))
	}

	// Test inclusive (basis "at") with partial cursor — should include the boundary row
	in3 := src.Connect(sort, nil, nil)
	nodesAt := in3.Fetch(ivm.FetchRequest{
		Start: &ivm.Start{Row: ivm.Row{"createdAt": float64(500)}, Basis: "at"},
	})
	if len(nodesAt) != 3 {
		t.Errorf("basis=at: expected 3 rows (createdAt=500, 600, 700), got %d: %v",
			len(nodesAt), rowsOf(nodesAt))
	}
}
