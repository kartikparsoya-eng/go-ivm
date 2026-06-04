package sqlite

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"

	_ "modernc.org/sqlite"
)

// collectingOutput captures pushes for testing.
type collectingOutput struct {
	changes []ivm.Change
}

func (o *collectingOutput) Push(change ivm.Change, _ ivm.InputBase) []ivm.Change {
	o.changes = append(o.changes, change)
	return nil
}

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		age REAL NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestTableSource_PushAndFetch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	columns := map[string]ColumnSchema{
		"id":   {Type: "string"},
		"name": {Type: "string"},
		"age":  {Type: "number"},
	}

	ts := NewTableSource(db, "users", columns, []string{"id"})

	sort := ivm.Ordering{{[2]string{"name", "asc"}[0], [2]string{"name", "asc"}[1]}, {[2]string{"id", "asc"}[0], [2]string{"id", "asc"}[1]}}
	input := ts.Connect(sort, nil, nil)

	output := &collectingOutput{}
	input.SetOutput(output)

	// Push add
	ts.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "Alice", "age": float64(30)}))
	ts.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "Bob", "age": float64(25)}))

	if len(output.changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(output.changes))
	}
	if output.changes[0].Type != ivm.ChangeTypeAdd {
		t.Fatalf("expected add change")
	}

	// Fetch all
	nodes := input.Fetch(ivm.FetchRequest{})
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	// Should be sorted by name: Alice < Bob
	if nodes[0].Row["name"] != "Alice" {
		t.Fatalf("expected Alice first, got %v", nodes[0].Row["name"])
	}
	if nodes[1].Row["name"] != "Bob" {
		t.Fatalf("expected Bob second, got %v", nodes[1].Row["name"])
	}

	// Push edit
	output.changes = nil
	ts.Push(ivm.MakeSourceChangeEdit(
		ivm.Row{"id": "1", "name": "Alice", "age": float64(31)},
		ivm.Row{"id": "1", "name": "Alice", "age": float64(30)},
	))
	if len(output.changes) != 1 {
		t.Fatalf("expected 1 edit change, got %d", len(output.changes))
	}
	if output.changes[0].Type != ivm.ChangeTypeEdit {
		t.Fatalf("expected edit change, got %d", output.changes[0].Type)
	}

	// Verify fetch after edit
	nodes = input.Fetch(ivm.FetchRequest{})
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after edit, got %d", len(nodes))
	}
	if nodes[0].Row["age"] != float64(31) {
		t.Fatalf("expected age 31 after edit, got %v", nodes[0].Row["age"])
	}

	// Push remove
	output.changes = nil
	ts.Push(ivm.MakeSourceChangeRemove(ivm.Row{"id": "2", "name": "Bob", "age": float64(25)}))
	if len(output.changes) != 1 {
		t.Fatalf("expected 1 remove change, got %d", len(output.changes))
	}

	nodes = input.Fetch(ivm.FetchRequest{})
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after remove, got %d", len(nodes))
	}
}

func TestTableSource_FetchWithConstraint(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	columns := map[string]ColumnSchema{
		"id":   {Type: "string"},
		"name": {Type: "string"},
		"age":  {Type: "number"},
	}

	ts := NewTableSource(db, "users", columns, []string{"id"})
	sort := ivm.Ordering{{"name", "asc"}, {"id", "asc"}}
	input := ts.Connect(sort, nil, nil)
	output := &collectingOutput{}
	input.SetOutput(output)

	ts.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "Alice", "age": float64(30)}))
	ts.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "Bob", "age": float64(25)}))

	constraint := ivm.Constraint{"id": "1"}
	nodes := input.Fetch(ivm.FetchRequest{Constraint: &constraint})
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node with constraint, got %d", len(nodes))
	}
	if nodes[0].Row["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", nodes[0].Row["name"])
	}
}

func TestTableSource_FilteredConnection(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	columns := map[string]ColumnSchema{
		"id":   {Type: "string"},
		"name": {Type: "string"},
		"age":  {Type: "number"},
	}

	ts := NewTableSource(db, "users", columns, []string{"id"})
	sort := ivm.Ordering{{"name", "asc"}, {"id", "asc"}}

	// Filter: age > 26
	filter := &Condition{
		Type:  "simple",
		Op:    ">",
		Left:  ValuePos{Type: "column", Name: "age"},
		Right: ValuePos{Type: "literal", Value: float64(26)},
	}
	input := ts.Connect(sort, filter, nil)
	output := &collectingOutput{}
	input.SetOutput(output)

	ts.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "1", "name": "Alice", "age": float64(30)}))
	ts.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": "2", "name": "Bob", "age": float64(25)}))

	// Only Alice should have been pushed (age 30 > 26)
	if len(output.changes) != 1 {
		t.Fatalf("expected 1 filtered push, got %d", len(output.changes))
	}
	if output.changes[0].Node.Row["name"] != "Alice" {
		t.Fatalf("expected Alice push, got %v", output.changes[0].Node.Row["name"])
	}

	// Fetch should only return Alice
	nodes := input.Fetch(ivm.FetchRequest{})
	if len(nodes) != 1 {
		t.Fatalf("expected 1 filtered node, got %d", len(nodes))
	}
}

func TestQueryBuilder_Basic(t *testing.T) {
	columns := map[string]ColumnSchema{
		"id":   {Type: "string"},
		"name": {Type: "string"},
	}

	result := BuildSelectQuery("users", columns, nil, nil, ivm.Ordering{{"name", "asc"}}, false, nil)
	expected := `SELECT "id", "name" FROM "users" ORDER BY "name" asc`
	if result.SQL != expected {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, result.SQL)
	}
}

// Regression: a Take/Skip start cursor over a NULLABLE order column with op
// ">" emits "(? IS NULL OR col > ?)" — two placeholders both bound to the
// start value. The param list must match, or SQLite panics at execute time
// with "not enough args to execute query: want 2 got 1" (observed crashing
// the sidecar mid-soak on a nullable-ordered hydrate).
func TestQueryBuilder_StartCursorOptionalColumn_ParamCount(t *testing.T) {
	columns := map[string]ColumnSchema{
		"id":   {Type: "string", Optional: true},
		"name": {Type: "string"},
	}
	start := ivm.Start{Row: ivm.Row{"id": "abc"}, Basis: "after"}
	result := BuildSelectQuery("tickets", columns, nil, nil,
		ivm.Ordering{{"id", "asc"}}, false, &start)

	placeholders := strings.Count(result.SQL, "?")
	if placeholders != len(result.Params) {
		t.Fatalf("placeholder/param mismatch: SQL has %d '?' but %d params\nSQL: %s\nParams: %v",
			placeholders, len(result.Params), result.SQL, result.Params)
	}
	// Both placeholders bind the same start value.
	for _, p := range result.Params {
		if p != "abc" {
			t.Errorf("expected all start params to be \"abc\", got %v", result.Params)
		}
	}
}

func TestQueryBuilder_WithConstraint(t *testing.T) {
	columns := map[string]ColumnSchema{
		"id":   {Type: "string"},
		"name": {Type: "string"},
	}
	constraint := ivm.Constraint{"id": "abc"}
	result := BuildSelectQuery("users", columns, &constraint, nil, nil, false, nil)
	expected := `SELECT "id", "name" FROM "users" WHERE "id" = ?`
	if result.SQL != expected {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, result.SQL)
	}
	if len(result.Params) != 1 || result.Params[0] != "abc" {
		t.Fatalf("expected params [abc], got %v", result.Params)
	}
}
