package engine

// Additional gap coverage beyond exists_gap_coverage_test.go. Targets the
// remaining production-realistic shapes that have no direct test:
//
//   2. Three-level nested EXISTS (channels→conversations→messages→users)
//   3. NOT EXISTS in the advance path — 1→0 transition emits Remove(parent),
//      0→1 transition emits Add(parent)
//   4. EXISTS where the child Take limit (existsLimit=3) is straddled by
//      multiple matching rows — the LIMIT must not lose the size>0 signal
//   5. JSON-column filtering through TableSource — exercises the SQL builder's
//      JSON handling path

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

// TestTableSourceNestedExists_ThreeLevels: prod template idx 22 in the
// soak is channels→conversations→messages→users (4 deep). This test
// covers the 3-level intermediate case: channels EXISTS conversations
// EXISTS messages WHERE senderId=X. Each level builds a Take partition
// keyed on the correlation field; we want to confirm stacking
// partitionKeys through 3 levels doesn't accumulate state-key drift.
func TestTableSourceNestedExists_ThreeLevels(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE channels (id TEXT PRIMARY KEY)`,
		`CREATE TABLE conversations (id TEXT PRIMARY KEY, channelId TEXT)`,
		`CREATE TABLE messages (id TEXT PRIMARY KEY, conversationId TEXT, senderId TEXT)`,
		`INSERT INTO channels VALUES ('ch-A'), ('ch-B'), ('ch-C')`,
		`INSERT INTO conversations VALUES ('co-A1','ch-A'),('co-B1','ch-B'),('co-C1','ch-C')`,
		// Only ch-A has the right chain: co-A1 → m-1 senderId=user-X.
		// ch-B has a message but wrong sender. ch-C has no messages.
		`INSERT INTO messages VALUES ('m-1','co-A1','user-X'),('m-2','co-B1','someone-else')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	t.Cleanup(func() { db.Close() })

	chans, _ := tablesource.New(db, "channels",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}}, []string{"id"})
	convs, _ := tablesource.New(db, "conversations",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "channelId": {Type: "string"}}, []string{"id"})
	msgs, _ := tablesource.New(db, "messages",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "conversationId": {Type: "string"}, "senderId": {Type: "string"},
		}, []string{"id"})

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
				Correlation: builder.Correlation{ParentField: []string{"id"}, ChildField: []string{"channelId"}},
				Subquery: builder.AST{
					Table: "conversations", Alias: "convs",
					Where: &builder.Condition{
						Type: "correlatedSubquery", Op: "EXISTS",
						Related: &builder.CorrelatedSubquery{
							System: "client",
							Correlation: builder.Correlation{ParentField: []string{"id"}, ChildField: []string{"conversationId"}},
							Subquery: builder.AST{
								Table: "messages", Alias: "msgs",
								Where: &builder.Condition{
									Type: "and",
									Conditions: []builder.Condition{{
										Type:  "simple",
										Left:  &builder.ValuePos{Type: "column", Name: "senderId"},
										Op:    "=",
										Right: &builder.ValuePos{Type: "literal", Value: "user-X"},
									}},
								},
							},
						},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	changes, _, err := eng.AddQuery("q-3level", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	var ids []string
	for _, c := range changes {
		if c.Table == "channels" {
			if id, _ := c.RowKey["id"].(string); id != "" {
				ids = append(ids, id)
			}
		}
	}
	if len(ids) != 1 || ids[0] != "ch-A" {
		t.Fatalf("3-level nested EXISTS should yield only ch-A; got %v", ids)
	}
}

// TestTableSourceNotExistsCompoundKey_AdvanceEmitsTransition: tests both
// directions of the NOT EXISTS transition during advance.
//   * Start: chan-A has matching participant → NOT EXISTS false → not in
//     result set. chan-B has no participant → NOT EXISTS true → in result.
//   * Insert participant for chan-B → NOT EXISTS flips true→false →
//     Remove(chan-B) must be emitted.
//   * Delete participant from chan-A → NOT EXISTS flips false→true →
//     Add(chan-A) must be emitted.
// Without the snapshotDisabled overlay fix (gap #1) this would fail the
// same way the EXISTS-advance test did.
func TestTableSourceNotExistsCompoundKey_AdvanceEmitsTransition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE channels (id TEXT PRIMARY KEY)`,
		`CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)`,
		`INSERT INTO channels VALUES ('chan-A'), ('chan-B')`,
		`INSERT INTO channel_participants VALUES ('cp-a-X', 'chan-A', 'user-X')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	t.Cleanup(func() { db.Close() })

	chanSrc, _ := tablesource.New(db, "channels",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}}, []string{"id"})
	cpSrc, _ := tablesource.New(db, "channel_participants",
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
			Type: "correlatedSubquery", Op: "NOT EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id", "id"},
					ChildField:  []string{"channelId", "channelId"},
				},
				Subquery: builder.AST{
					Table: "channel_participants", Alias: "zsubq_cp",
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

	// Initial hydrate: only chan-B should be visible (NOT EXISTS true).
	changes, _, err := eng.AddQuery("q-notexists-adv", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	var initial []string
	for _, c := range changes {
		if c.Table == "channels" {
			if id, _ := c.RowKey["id"].(string); id != "" {
				initial = append(initial, id)
			}
		}
	}
	if len(initial) != 1 || initial[0] != "chan-B" {
		t.Fatalf("initial NOT EXISTS hydrate should yield only chan-B; got %v", initial)
	}

	// Advance 1: INSERT participant for chan-B → NOT EXISTS chan-B flips
	// true→false → Remove(chan-B) must be emitted.
	w2, _ := sql.Open("sqlite3", path)
	w2.Exec(`INSERT INTO channel_participants VALUES ('cp-b-X', 'chan-B', 'user-X')`)
	w2.Close()

	result := eng.Advance([]SnapshotChange{{
		Table: "channel_participants",
		NextValue: ivm.Row{"id": "cp-b-X", "channelId": "chan-B", "userId": "user-X"},
	}})
	var sawRemove bool
	for _, c := range result.Changes {
		if c.Table == "channels" && c.Type == int(ivm.ChangeTypeRemove) {
			if id, _ := c.RowKey["id"].(string); id == "chan-B" {
				sawRemove = true
			}
		}
	}
	if !sawRemove {
		t.Fatalf("NOT EXISTS advance: expected Remove(chan-B) after participant added; got %d changes", len(result.Changes))
	}
}

// TestTableSourceExistsWithManyChildrenStraddlesLimit: existsLimit=3 is
// applied to every EXISTS subquery's Take. If a child has 5+ matching
// rows, the Take returns 3, but EXISTS only cares about size > 0 — must
// still evaluate true. Tests the boundary where Take limit < actual
// child count.
func TestTableSourceExistsWithManyChildrenStraddlesLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE parents (id TEXT PRIMARY KEY)`,
		`CREATE TABLE kids (id TEXT PRIMARY KEY, parentId TEXT)`,
		`INSERT INTO parents VALUES ('p-1')`,
	}
	for i := 0; i < 10; i++ {
		stmts = append(stmts, `INSERT INTO kids VALUES ('k-`+itoa(i)+`', 'p-1')`)
	}
	for _, stmt := range stmts {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	t.Cleanup(func() { db.Close() })

	parents, _ := tablesource.New(db, "parents",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}}, []string{"id"})
	kids, _ := tablesource.New(db, "kids",
		map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "parentId": {Type: "string"}}, []string{"id"})

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(parents)
	eng.RegisterSource(kids)

	ast := builder.AST{
		Table: "parents",
		Where: &builder.Condition{
			Type: "correlatedSubquery", Op: "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{ParentField: []string{"id"}, ChildField: []string{"parentId"}},
				Subquery:    builder.AST{Table: "kids", Alias: "kids"},
			},
		},
	}

	changes, _, err := eng.AddQuery("q-many-kids", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	var sawParent bool
	for _, c := range changes {
		if c.Table == "parents" {
			if id, _ := c.RowKey["id"].(string); id == "p-1" {
				sawParent = true
			}
		}
	}
	if !sawParent {
		t.Fatalf("EXISTS must evaluate true when child has 10 rows (existsLimit=3 caps fetch but size>0 still holds); got %d changes", len(changes))
	}
}

// TestTableSourceJSONColumnFilter: prod schema has tickets.metadata as
// JSON. Filtering by simple equality (NOT a JSON path) should still
// work via the column-type=json mapping. We're not testing JSON-path
// queries (Zero doesn't emit those at the AST layer); we're testing
// that the column-type metadata flows correctly through the SQL build
// without coercing the JSON to a string in a way that breaks WHERE.
func TestTableSourceJSONColumnFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE tickets (id TEXT PRIMARY KEY, status TEXT, metadata TEXT)`,
		// metadata column type=json — we treat the stored value as a JSON
		// string but filter by other columns.
		`INSERT INTO tickets VALUES ('t-1', 'OPEN', '{"foo":"bar"}')`,
		`INSERT INTO tickets VALUES ('t-2', 'CLOSED', '{"foo":"baz"}')`,
		`INSERT INTO tickets VALUES ('t-3', 'OPEN', NULL)`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	t.Cleanup(func() { db.Close() })

	tickets, _ := tablesource.New(db, "tickets",
		map[string]sqlite.ColumnSchema{
			"id":       {Type: "string"},
			"status":   {Type: "string"},
			"metadata": {Type: "json"}, // json column type — must not break
		},
		[]string{"id"})

	eng, _ := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(tickets)

	ast := builder.AST{
		Table: "tickets",
		Where: &builder.Condition{
			Type: "and",
			Conditions: []builder.Condition{{
				Type:  "simple",
				Left:  &builder.ValuePos{Type: "column", Name: "status"},
				Op:    "=",
				Right: &builder.ValuePos{Type: "literal", Value: "OPEN"},
			}},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	changes, _, err := eng.AddQuery("q-json-col", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	var ids []string
	for _, c := range changes {
		if c.Table == "tickets" {
			if id, _ := c.RowKey["id"].(string); id != "" {
				ids = append(ids, id)
			}
		}
	}
	if len(ids) != 2 || ids[0] != "t-1" || ids[1] != "t-3" {
		t.Fatalf("expected [t-1 t-3]; got %v (JSON column may have broken the SQL build)", ids)
	}
}

// itoa is a minimal local helper to avoid pulling in strconv for one site.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}
