package tablesource

// Regression test for REVIEW-napi-transport F7: fetchDuringPushStream's
// locked setup window runs user-value-sensitive code — overlaySplicePlan
// invokes the effective comparator against req.Start (ivm.CompareValues
// panics with a DataError on non-scalar/mismatched sort keys) and the
// connection's filterPredicate. With the pre-fix manual Lock/Unlock pairs,
// such a panic escaped WITH s.mu held: every subsequent Push/Fetch on the
// source blocked forever — a silent CG wedge (no crash, no error frame, no
// restart trigger), strictly worse than the panic itself, which the engine
// recovers into a DataError CG teardown.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

func TestFetchDuringPushStream_PoisonOverlayReleasesMutex(t *testing.T) {
	path := seedReplica(t)
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

	src, err := New(db, wdb, "t", map[string]sqlite.ColumnSchema{
		"id": {Type: "number"}, "name": {Type: "string"},
	}, []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	// Sort on "name" so the comparator touches the poison column.
	in := src.Connect(ivm.Ordering{{"name", "asc"}, {"id", "asc"}}, nil, nil, nil)
	conn := in.(*sourceInput).conn

	// Arm an in-flight overlay whose row has a NON-SCALAR value in the sort
	// key. With req.Start non-nil, overlaySplicePlan compares the overlay row
	// against the start cursor INSIDE the locked window → CompareValues
	// panics (DataError) there.
	poison := ivm.MakeSourceChangeAdd(ivm.Row{
		"id":   float64(9),
		"name": map[string]interface{}{"poison": true},
	})
	lazySetOverlay(src, conn, 1, true, poison)

	req := ivm.FetchRequest{
		Start: &ivm.Start{
			Row:   ivm.Row{"id": float64(1), "name": "a"},
			Basis: "at",
		},
	}

	panicked := func() (p any) {
		defer func() { p = recover() }()
		for range in.Fetch(req) {
		}
		return nil
	}()
	if panicked == nil {
		t.Fatal("expected DataError panic from the poison sort key in overlaySplicePlan")
	}

	// THE regression assertion: the panic must not leave s.mu held.
	if !src.mu.TryLock() {
		t.Fatal("s.mu still held after the poison panic — every later Push/Fetch on this source wedges forever (F7)")
	}
	src.mu.Unlock()

	// And the source must remain fully usable — the panic fired BEFORE the
	// stmt checkout, so the cache is sane and the prev tx is intact.
	lazyClearOverlay(src)
	got := 0
	for range in.Fetch(ivm.FetchRequest{}) {
		got++
	}
	if got != 3 {
		t.Fatalf("post-panic fetch returned %d rows, want 3 — source unusable after recovered poison panic", got)
	}
}
