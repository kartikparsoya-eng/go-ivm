package engine

// Gap coverage for the TableSource port. Targets specific AST shapes the
// existing tests + production soak don't exercise:
//
//   1. NOT EXISTS with compound-key correlation (symmetric to the EXISTS
//      compound-key bug fixed in take.go).
//   2. NULL value in a correlation column (BuildJoinConstraint returns
//      nil → Take.Fetch must handle the "no constraint" path without
//      regression).
//   3. Multi-level nested EXISTS (channels EXISTS conversations EXISTS
//      messages) — exercises the nested Take partition keys.
//   4. EXISTS combined with paginated cursor on the parent (Skip + Take
//      + EXISTS interaction — neither path tested in combination).
//
// All four are production-realistic AST shapes the xyne-spaces sandbox
// schema can emit; without coverage here we'd discover them as drift
// audit failures only after a real client triggers them.

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// seedNotExistsReplica: two channels (one with a participant matching
// userId=X, one without). Used to verify NOT EXISTS returns ONLY the
// channel WITHOUT a matching participant.
func seedNotExistsReplica(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE channels (id TEXT PRIMARY KEY, name TEXT)`,
		`CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)`,
		`INSERT INTO channels VALUES ('chan-with', 'has-member'), ('chan-without', 'no-member')`,
		`INSERT INTO channel_participants VALUES ('cp-1', 'chan-with', 'user-X')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	return path
}

// TestTableSourceNotExistsCompoundKey_HydratesUnmatchedChannel:
// symmetric to the EXISTS compound-key fix. With compound (duplicated)
// correlation, NOT EXISTS must return ONLY chan-without. Hits the same
// Take.Fetch path as positive EXISTS so the fix should cover it — this
// test catches a regression if either side breaks.
func TestTableSourceNotExistsCompoundKey_HydratesUnmatchedChannel(t *testing.T) {
	path := seedNotExistsReplica(t)
	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	chanSrc, _ := tablesource.New(db, wdb, "channels",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "name": {Type: "string"},
		}, []string{"id"})
	cpSrc, _ := tablesource.New(db, wdb, "channel_participants",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "channelId": {Type: "string"}, "userId": {Type: "string"},
		}, []string{"id"})

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(chanSrc)
	eng.RegisterSource(cpSrc)

	ast := builder.AST{
		Table: "channels",
		Where: &builder.Condition{
			Type: "correlatedSubquery",
			Op:   "NOT EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id", "id"},
					ChildField:  []string{"channelId", "channelId"},
				},
				Subquery: builder.AST{
					Table: "channel_participants",
					Alias: "zsubq_cp",
					Where: &builder.Condition{
						Type: "and",
						Conditions: []builder.Condition{{
							Type:  "simple",
							Left:  &builder.ValuePos{Type: "column", Name: "userId"},
							Op:    "=",
							Right: &builder.ValuePos{Type: "literal", Value: "user-X"},
						}},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	changes, _, err := eng.AddQuery("q-notexists", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	var channelIDs []string
	for _, c := range changes {
		if c.Table == "channels" {
			if id, _ := c.RowKey["id"].(string); id != "" {
				channelIDs = append(channelIDs, id)
			}
		}
	}
	if len(channelIDs) != 1 || channelIDs[0] != "chan-without" {
		t.Fatalf("NOT EXISTS should return only chan-without; got channels=%v (%d total changes)", channelIDs, len(changes))
	}
}

// TestTableSourceJoin_NullInCompoundCorrelation: when a parent row has
// a NULL value in a correlation column, BuildJoinConstraint returns nil
// — the Join should treat the child as unmatchable for that row. This
// is the boundary case for the BuildJoinConstraint null guard, which
// no existing test exercises.
func TestTableSourceJoin_NullInCompoundCorrelation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE parents (id TEXT PRIMARY KEY, joinCol TEXT)`,
		`CREATE TABLE kids (id TEXT PRIMARY KEY, parentCol TEXT)`,
		`INSERT INTO parents VALUES ('p-null', NULL), ('p-good', 'match-key')`,
		`INSERT INTO kids VALUES ('k-1', 'match-key')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	parents, _ := tablesource.New(db, wdb, "parents",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "joinCol": {Type: "string"}},
		[]string{"id"})
	kids, _ := tablesource.New(db, wdb, "kids",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "parentCol": {Type: "string"}},
		[]string{"id"})

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(parents)
	eng.RegisterSource(kids)

	ast := builder.AST{
		Table: "parents",
		Where: &builder.Condition{
			Type: "correlatedSubquery",
			Op:   "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"joinCol"},
					ChildField:  []string{"parentCol"},
				},
				Subquery: builder.AST{Table: "kids", Alias: "kids"},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	changes, _, err := eng.AddQuery("q-null-corr", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	// Only p-good should match (p-null's joinCol is NULL → no child match).
	var parentIDs []string
	for _, c := range changes {
		if c.Table == "parents" {
			if id, _ := c.RowKey["id"].(string); id != "" {
				parentIDs = append(parentIDs, id)
			}
		}
	}
	if len(parentIDs) != 1 || parentIDs[0] != "p-good" {
		t.Fatalf("expected only p-good (p-null has NULL joinCol → no match); got parents=%v", parentIDs)
	}
}

// TestTableSourceNestedExists_TwoLevels: channels EXISTS conversations
// where EXISTS messages. Exercises two-level Take stack with stacked
// partition keys. Production has 3+ levels via channels→conversations→
// messages→users (template idx 22 in the soak); 2-level here is the
// minimum to catch nested-Take bugs.
func TestTableSourceNestedExists_TwoLevels(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE channels (id TEXT PRIMARY KEY, name TEXT)`,
		`CREATE TABLE conversations (id TEXT PRIMARY KEY, channelId TEXT)`,
		`CREATE TABLE messages (id TEXT PRIMARY KEY, conversationId TEXT, content TEXT)`,
		`INSERT INTO channels VALUES ('ch-A','active'),('ch-B','empty')`,
		`INSERT INTO conversations VALUES ('conv-A','ch-A'),('conv-B','ch-B')`,
		// Only ch-A's conversation has a matching message.
		`INSERT INTO messages VALUES ('m-1','conv-A','hello')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	chans, _ := tablesource.New(db, wdb, "channels",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "name": {Type: "string"}},
		[]string{"id"})
	convs, _ := tablesource.New(db, wdb, "conversations",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "channelId": {Type: "string"}},
		[]string{"id"})
	msgs, _ := tablesource.New(db, wdb, "messages",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "conversationId": {Type: "string"}, "content": {Type: "string"},
		},
		[]string{"id"})

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(chans)
	eng.RegisterSource(convs)
	eng.RegisterSource(msgs)

	ast := builder.AST{
		Table: "channels",
		Where: &builder.Condition{
			Type: "correlatedSubquery", Op: "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id"}, ChildField: []string{"channelId"},
				},
				Subquery: builder.AST{
					Table: "conversations", Alias: "convs",
					Where: &builder.Condition{
						Type: "correlatedSubquery", Op: "EXISTS",
						Related: &builder.CorrelatedSubquery{
							System: "client",
							Correlation: builder.Correlation{
								ParentField: []string{"id"}, ChildField: []string{"conversationId"},
							},
							Subquery: builder.AST{Table: "messages", Alias: "msgs"},
						},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	changes, _, err := eng.AddQuery("q-nested", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	var channelIDs []string
	for _, c := range changes {
		if c.Table == "channels" {
			if id, _ := c.RowKey["id"].(string); id != "" {
				channelIDs = append(channelIDs, id)
			}
		}
	}
	if len(channelIDs) != 1 || channelIDs[0] != "ch-A" {
		t.Fatalf("nested EXISTS should yield only ch-A (only one with conversation→message chain); got channels=%v", channelIDs)
	}
}

// TestTableSourceExistsPlusCursorPagination: combines Skip (cursor) +
// EXISTS. Tickets sorted by id, cursor at ticket-3 exclusive, EXISTS
// channel_participants WHERE userId='user-X' (so only tickets whose
// channel has user-X visible). Exercises Take(parent) + EXISTS(child)
// pairing — a production shape (e.g., paginated "my tickets" view) that
// no existing test covers.
func TestTableSourceExistsPlusCursorPagination(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE tickets (id TEXT PRIMARY KEY, channelId TEXT, title TEXT)`,
		`CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)`,
		`INSERT INTO tickets VALUES
			('t-1','ch-A','ticket-1'),('t-2','ch-A','ticket-2'),
			('t-3','ch-B','ticket-3'),('t-4','ch-A','ticket-4'),
			('t-5','ch-B','ticket-5')`,
		// user-X only sees ch-A → tickets t-1, t-2, t-4.
		`INSERT INTO channel_participants VALUES ('cp-1','ch-A','user-X')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	tickets, _ := tablesource.New(db, wdb, "tickets",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "channelId": {Type: "string"}, "title": {Type: "string"},
		}, []string{"id"})
	cps, _ := tablesource.New(db, wdb, "channel_participants",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "channelId": {Type: "string"}, "userId": {Type: "string"},
		}, []string{"id"})

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(tickets)
	eng.RegisterSource(cps)

	limit := 10
	ast := builder.AST{
		Table: "tickets",
		Start: &builder.Bound{Row: ivm.Row{"id": "t-2"}, Exclusive: true},
		Limit: &limit,
		Where: &builder.Condition{
			Type: "correlatedSubquery", Op: "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"channelId"}, ChildField: []string{"channelId"},
				},
				Subquery: builder.AST{
					Table: "channel_participants", Alias: "cps",
					Where: &builder.Condition{
						Type: "and",
						Conditions: []builder.Condition{{
							Type:  "simple",
							Left:  &builder.ValuePos{Type: "column", Name: "userId"},
							Op:    "=",
							Right: &builder.ValuePos{Type: "literal", Value: "user-X"},
						}},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	changes, _, err := eng.AddQuery("q-cursor-exists", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	var ticketIDs []string
	for _, c := range changes {
		if c.Table == "tickets" {
			if id, _ := c.RowKey["id"].(string); id != "" {
				ticketIDs = append(ticketIDs, id)
			}
		}
	}
	// Expected: tickets after t-2 (exclusive) where channel has user-X.
	// In ch-A: t-4. Not t-1/t-2 (before cursor), not t-3/t-5 (ch-B has no
	// matching participant).
	if len(ticketIDs) != 1 || ticketIDs[0] != "t-4" {
		t.Fatalf("expected only t-4 (after cursor, in user-X's channel); got tickets=%v", ticketIDs)
	}
}

// TestPartialCursorExclusionWithMultiColumnSort: the Bug(B) regression.
// Sort: [createdAt ASC, id ASC], cursor: {createdAt: 100} exclusive (partial —
// only specifies the first sort column). Rows at createdAt=100 must be excluded
// from BOTH the initial hydrate AND the push path.
//
// Before the CompareWithPartialBound fix (ed7a302), Skip's shouldBePresent used
// the full comparator which treated nil < non-nil for the second sort column.
// A partial bound {createdAt:100} vs full row {createdAt:100, id:"X"}:
//   full comparator: createdAt equal, id: nil < "X" → cmp = -1 → row passes
// With CompareWithPartialBound: stops at first field not in bound → cmp = 0
// + Exclusive=true → row blocked. This test catches regressions in that path.
func TestPartialCursorExclusionWithMultiColumnSort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE conversations (id TEXT PRIMARY KEY, channelId TEXT, createdAt INTEGER)`,
		`CREATE TABLE members (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)`,
		// conv-1, conv-4: at cursor (createdAt=100), channel has member → should be EXCLUDED
		// conv-2: after cursor (createdAt=200), channel has member → INCLUDED
		// conv-3: after cursor (createdAt=300), channel has NO member → excluded by EXISTS
		`INSERT INTO conversations VALUES
			('conv-1','ch-1',100),
			('conv-2','ch-1',200),
			('conv-3','ch-2',300),
			('conv-4','ch-1',100)`,
		`INSERT INTO members VALUES ('m-1','ch-1','user-X')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	t.Cleanup(func() { db.Close() })

	convs, _ := tablesource.New(db, wdb, "conversations",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "channelId": {Type: "string"}, "createdAt": {Type: "number"},
		}, []string{"id"})
	members, _ := tablesource.New(db, wdb, "members",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "channelId": {Type: "string"}, "userId": {Type: "string"},
		}, []string{"id"})

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(convs)
	eng.RegisterSource(members)

	limit := 10
	ast := builder.AST{
		Table: "conversations",
		// PARTIAL cursor: only specifies createdAt, not id.
		Start: &builder.Bound{Row: ivm.Row{"createdAt": int64(100)}, Exclusive: true},
		Limit: &limit,
		Where: &builder.Condition{
			Type: "correlatedSubquery", Op: "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"channelId"}, ChildField: []string{"channelId"},
				},
				Subquery: builder.AST{
					Table: "members", Alias: "mbrs",
					Where: &builder.Condition{
						Type: "and",
						Conditions: []builder.Condition{{
							Type:  "simple",
							Left:  &builder.ValuePos{Type: "column", Name: "userId"},
							Op:    "=",
							Right: &builder.ValuePos{Type: "literal", Value: "user-X"},
						}},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"createdAt", "asc"}, {"id", "asc"}},
	}

	// --- Phase 1: Initial hydrate ---
	changes, _, err := eng.AddQuery("q-partial-cursor", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	var convIDs []string
	for _, c := range changes {
		if c.Table == "conversations" {
			if id, _ := c.RowKey["id"].(string); id != "" {
				convIDs = append(convIDs, id)
			}
		}
	}
	// Only conv-2 (createdAt=200, in ch-1 which has member).
	// conv-1/conv-4 (createdAt=100) excluded by partial cursor.
	// conv-3 (createdAt=300) excluded by EXISTS (ch-2 has no member).
	if len(convIDs) != 1 || convIDs[0] != "conv-2" {
		t.Fatalf("initial hydrate: expected [conv-2]; got %v", convIDs)
	}

	// --- Phase 2: Push — add a parent row AT the cursor boundary ---
	// conv-5 has createdAt=100 and is in ch-1 (has member). Should NOT appear.
	result := eng.Advance([]SnapshotChange{{
		Table:     "conversations",
		NextValue: ivm.Row{"id": "conv-5", "channelId": "ch-1", "createdAt": int64(100)},
	}})
	for _, c := range result.Changes {
		if c.Table == "conversations" {
			if id, _ := c.RowKey["id"].(string); id == "conv-5" {
				t.Fatalf("push path: conv-5 (createdAt=100) should be excluded by partial cursor, but it was emitted as %v", c.Type)
			}
		}
	}

	// --- Phase 3: Push — add a parent row AFTER the cursor ---
	// conv-6 has createdAt=150 and is in ch-1. Should appear.
	result = eng.Advance([]SnapshotChange{{
		Table:     "conversations",
		NextValue: ivm.Row{"id": "conv-6", "channelId": "ch-1", "createdAt": int64(150)},
	}})
	var addedAfterCursor []string
	for _, c := range result.Changes {
		if c.Table == "conversations" && c.Type == RowChangeAdd {
			if id, _ := c.RowKey["id"].(string); id != "" {
				addedAfterCursor = append(addedAfterCursor, id)
			}
		}
	}
	if len(addedAfterCursor) != 1 || addedAfterCursor[0] != "conv-6" {
		t.Fatalf("push path: expected conv-6 (createdAt=150) to be added; got %v", addedAfterCursor)
	}

	// --- Phase 4: Push — add child that flips EXISTS for a post-cursor parent ---
	// Adding a member to ch-2 should cause conv-3 (createdAt=300) to appear.
	result = eng.Advance([]SnapshotChange{{
		Table:     "members",
		NextValue: ivm.Row{"id": "m-2", "channelId": "ch-2", "userId": "user-X"},
	}})
	var existsFlipped []string
	for _, c := range result.Changes {
		if c.Table == "conversations" && c.Type == RowChangeAdd {
			if id, _ := c.RowKey["id"].(string); id != "" {
				existsFlipped = append(existsFlipped, id)
			}
		}
	}
	if len(existsFlipped) != 1 || existsFlipped[0] != "conv-3" {
		t.Fatalf("child push: expected conv-3 to appear (EXISTS flipped); got %v", existsFlipped)
	}
}
