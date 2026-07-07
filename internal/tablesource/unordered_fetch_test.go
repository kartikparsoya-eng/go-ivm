package tablesource

// Absolute-shape tests for the UNORDERED fetch path (sort == nil — the
// Cap/EXISTS-child connect). The lazy-vs-eager parity matrix in
// lazy_advance_test.go proves the two Go paths agree; these tests pin the
// TS-defined shape itself (generateWithOverlayUnordered +
// generateWithOverlayInnerUnordered, memory-source.ts:885-951):
//
//   - the in-flight ADD overlay is injected FIRST, before any SQL row
//   - the in-flight REMOVE overlay suppresses the (single) PK-matching row
//   - an EDIT overlay is remove(old)+add(new) under the same rules
//   - the connection schema carries NO sort (table-source.ts:216)

import (
	"slices"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// unorderedFetchInPush pushes change into src and, from inside the fanout
// (the production shape: a Join/Exists re-fetch during Output.Push),
// collects a plain fetch on conn's input.
func unorderedFetchInPush(t *testing.T, src *Source, in ivm.Input, change ivm.SourceChange) []ivm.Row {
	t.Helper()
	var rows []ivm.Row
	fired := false
	probe := &lazyPushProbe{fn: func(_ ivm.Change) {
		fired = true
		for n := range in.Fetch(ivm.FetchRequest{}) {
			rows = append(rows, n.Row)
		}
	}}
	in.SetOutput(probe)
	src.Push(change)
	if !fired {
		t.Fatal("probe never fired — Push did not fan out")
	}
	return rows
}

func rowIDs(rows []ivm.Row) []float64 {
	ids := make([]float64, len(rows))
	for i, r := range rows {
		ids[i] = r["id"].(float64)
	}
	return ids
}

func TestUnorderedConnectSchemaHasNoSort(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	if got := in.GetSchema().Sort; got != nil {
		t.Fatalf("unordered connect schema.Sort = %v, want nil (table-source.ts:216)", got)
	}
	// CompareRows still exists (PK comparator) for overlay/consumer use.
	if in.GetSchema().CompareRows == nil {
		t.Fatal("unordered connect must still carry a PK compareRows")
	}

	ordered := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	if got := ordered.GetSchema().Sort; got == nil {
		t.Fatal("ordered connect schema.Sort = nil, want the explicit sort")
	}
}

// The ADD overlay is yielded FIRST — before any SQL row
// (generateWithOverlayInnerUnordered, memory-source.ts:934-937). The
// ordered path would place id=4 at its sorted position (last); pre-fix the
// unordered connect took that ordered path.
func TestUnorderedOverlayAddInjectedFirst(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	rows := unorderedFetchInPush(t, src, in, ivm.MakeSourceChangeAdd(lazyRow4()))

	ids := rowIDs(rows)
	if len(ids) != 4 {
		t.Fatalf("got %d rows, want 4", len(ids))
	}
	if ids[0] != 4 {
		t.Fatalf("overlay add must be yielded FIRST on unordered fetch: got %v", ids)
	}
	for _, want := range []float64{1, 2, 3} {
		if !slices.Contains(ids, want) {
			t.Fatalf("missing SQL row id=%v: %v", want, ids)
		}
	}
}

// The REMOVE overlay suppresses the PK-matching SQL row (the row is still
// in SQLite — writeChange runs after fanout).
func TestUnorderedOverlayRemoveSuppressed(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	bob := ivm.Row{"id": float64(2), "name": "bob", "score": float64(80), "active": false}
	rows := unorderedFetchInPush(t, src, in, ivm.MakeSourceChangeRemove(bob))

	ids := rowIDs(rows)
	if len(ids) != 2 || slices.Contains(ids, float64(2)) {
		t.Fatalf("removed row must be suppressed: %v", ids)
	}
}

// An EDIT overlay = suppress(old) + inject-first(new).
func TestUnorderedOverlayEdit(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	oldBob := ivm.Row{"id": float64(2), "name": "bob", "score": float64(80), "active": false}
	newBob := ivm.Row{"id": float64(2), "name": "bob", "score": float64(95), "active": true}
	rows := unorderedFetchInPush(t, src, in, ivm.MakeSourceChangeEdit(newBob, oldBob))

	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3: %v", len(rows), rows)
	}
	// New version first (add-injected), old version gone.
	if rows[0]["id"] != float64(2) || rows[0]["score"] != float64(95) {
		t.Fatalf("edited row must be injected first with NEW values: %v", rows[0])
	}
	for _, r := range rows[1:] {
		if r["id"] == float64(2) {
			t.Fatalf("old row version leaked past the remove suppression: %v", rows)
		}
	}
}

// Constraint-gated overlay: an in-flight add OUTSIDE the fetch's constraint
// window must not be injected (overlaysForConstraint half of
// generateWithOverlayUnordered).
func TestUnorderedOverlayAddGatedByConstraint(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	var rows []ivm.Row
	probe := &lazyPushProbe{fn: func(_ ivm.Change) {
		cFalse := ivm.Constraint{"active": false}
		for n := range in.Fetch(ivm.FetchRequest{Constraint: &cFalse}) {
			rows = append(rows, n.Row)
		}
	}}
	in.SetOutput(probe)
	// lazyRow4 is active=true — outside the active=false window.
	src.Push(ivm.MakeSourceChangeAdd(lazyRow4()))

	ids := rowIDs(rows)
	if slices.Contains(ids, float64(4)) {
		t.Fatalf("constraint-excluded overlay add leaked into unordered fetch: %v", ids)
	}
	if !slices.Contains(ids, float64(2)) {
		t.Fatalf("expected active=false row id=2 in result: %v", ids)
	}
}
