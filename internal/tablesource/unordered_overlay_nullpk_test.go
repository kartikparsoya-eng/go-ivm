package tablesource

// Hardening batch (porting-correctness review "removeByPK nil==nil" item):
// the UNORDERED overlay remove matcher must be TS's rowMatchesPK
// (memory-source.ts:953-959), which compares PK columns with valuesEqual —
// and valuesEqual treats null as UNEQUAL to itself (data.ts:112-118). So a
// remove overlay carrying a NULL PK value can never suppress a streamed row
// on the unordered path, even a row whose PK is also NULL. Go's old code
// matched with CompareValues (nil==nil → 0 → match) on BOTH paths, silently
// suppressing a row TS would deliver.
//
// The ORDERED path is different in TS too: generateWithOverlayInner matches
// the remove via the full comparator (memory-source.ts:865-871), and
// compareValues treats null==null as EQUAL for ordering — so ordered
// suppression of a NULL-PK row is TS behavior and Go's CompareValues
// convention there is faithful. Each path ports its own convention.
//
// Unreachable with a Postgres upstream (PK columns are NOT NULL), but SQLite
// itself permits NULL in non-INTEGER PRIMARY KEY columns, which is what this
// test uses to exercise the seam.

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// seedNullPKReplica creates a replica whose table has a TEXT primary key
// (NULLable in SQLite — the legacy quirk) holding one NULL-PK row and one
// normal row.
func seedNullPKReplica(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	writer, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer writer.Close()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"CREATE TABLE nt (id TEXT PRIMARY KEY, v TEXT)",
		"INSERT INTO nt (id, v) VALUES (NULL, 'a'), ('x', 'b')",
	} {
		if _, err := writer.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	return path
}

func newNullPKSource(t *testing.T) (*Source, ivm.Input, *connection) {
	t.Helper()
	path := seedNullPKReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	wdb, err := OpenWritable(path, OpenOptions{})
	if err != nil {
		t.Fatalf("OpenWritable: %v", err)
	}
	t.Cleanup(func() { _ = wdb.Close() })

	src, err := New(db, wdb, "nt", map[string]sqlite.ColumnSchema{
		"id": {Type: "string", Optional: true}, "v": {Type: "string"},
	}, []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	// nil sort → UNORDERED connection (TS table-source #fetch takes the
	// generateWithOverlayUnordered branch, table-source.ts:321-338).
	in := src.Connect(nil, nil, nil, nil)
	conn := in.(*sourceInput).conn
	return src, in, conn
}

// TestUnorderedOverlayRemove_NullPKNeverSuppresses covers both unordered
// production sites: the materialized path (fetchForConn → removeByPK
// replacement) and the streaming path (fetchDuringPushStream's inline
// suppression). With a NULL-PK remove overlay in flight, TS's rowMatchesPK
// yields valuesEqual(null,null)=false → the NULL-PK row is still delivered.
func TestUnorderedOverlayRemove_NullPKNeverSuppresses(t *testing.T) {
	src, _, conn := newNullPKSource(t)

	lazySetOverlay(src, conn, 1, true, ivm.MakeSourceChangeRemove(
		ivm.Row{"id": nil, "v": "a"},
	))
	t.Cleanup(func() { lazyClearOverlay(src) })

	req := ivm.FetchRequest{}

	// Materialized path (fetchForConn — the eager oracle).
	eager := src.fetchForConn(req, conn)
	if len(eager) != 2 {
		t.Fatalf("fetchForConn delivered %d rows, want 2 — a NULL-PK remove overlay must not suppress any row (TS rowMatchesPK/valuesEqual: null≠null, memory-source.ts:953-959); got %+v", len(eager), eager)
	}

	// Streaming path (fetchDuringPushStream — the lazy advance-time read).
	var lazy []ivm.Node
	for n := range src.fetchDuringPushStream(req, conn) {
		lazy = append(lazy, n)
	}
	if len(lazy) != 2 {
		t.Fatalf("fetchDuringPushStream delivered %d rows, want 2 (same TS convention); got %+v", len(lazy), lazy)
	}
}

// TestUnorderedOverlayRemove_RealPKStillSuppresses pins the fix boundary: a
// remove overlay with a real (non-NULL) PK must keep suppressing its row on
// both unordered paths — valuesEqual matches normally for non-null scalars.
func TestUnorderedOverlayRemove_RealPKStillSuppresses(t *testing.T) {
	src, _, conn := newNullPKSource(t)

	lazySetOverlay(src, conn, 1, true, ivm.MakeSourceChangeRemove(
		ivm.Row{"id": "x", "v": "b"},
	))
	t.Cleanup(func() { lazyClearOverlay(src) })

	req := ivm.FetchRequest{}

	eager := src.fetchForConn(req, conn)
	if len(eager) != 1 || eager[0].Row["id"] != nil {
		t.Fatalf("fetchForConn = %+v, want only the NULL-PK row (id='x' suppressed)", eager)
	}
	var lazy []ivm.Node
	for n := range src.fetchDuringPushStream(req, conn) {
		lazy = append(lazy, n)
	}
	if len(lazy) != 1 || lazy[0].Row["id"] != nil {
		t.Fatalf("fetchDuringPushStream = %+v, want only the NULL-PK row", lazy)
	}
}

// TestOrderedOverlayRemove_NullPKSuppresses pins the OTHER side of the fix
// boundary: on the ORDERED path TS matches the remove overlay with the full
// comparator (generateWithOverlayInner cmp===0, memory-source.ts:865-871)
// where compareValues(null,null)=0 — so a NULL-PK remove DOES suppress the
// NULL-PK row there. Go's CompareValues convention (removeByPK/pkRowsEqual)
// is the faithful port; this pin prevents an over-eager "fix" from switching
// the ordered path to valuesEqual too.
func TestOrderedOverlayRemove_NullPKSuppresses(t *testing.T) {
	path := seedNullPKReplica(t)
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	wdb, err := OpenWritable(path, OpenOptions{})
	if err != nil {
		t.Fatalf("OpenWritable: %v", err)
	}
	t.Cleanup(func() { _ = wdb.Close() })
	src, err := New(db, wdb, "nt", map[string]sqlite.ColumnSchema{
		"id": {Type: "string", Optional: true}, "v": {Type: "string"},
	}, []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	conn := in.(*sourceInput).conn

	lazySetOverlay(src, conn, 1, true, ivm.MakeSourceChangeRemove(
		ivm.Row{"id": nil, "v": "a"},
	))
	t.Cleanup(func() { lazyClearOverlay(src) })

	eager := src.fetchForConn(ivm.FetchRequest{}, conn)
	if len(eager) != 1 || eager[0].Row["id"] != "x" {
		t.Fatalf("ordered fetchForConn = %+v, want only id='x' (NULL-PK row suppressed — TS ordered comparator match)", eager)
	}
	var lazy []ivm.Node
	for n := range src.fetchDuringPushStream(ivm.FetchRequest{}, conn) {
		lazy = append(lazy, n)
	}
	if len(lazy) != 1 || lazy[0].Row["id"] != "x" {
		t.Fatalf("ordered fetchDuringPushStream = %+v, want only id='x'", lazy)
	}
}
