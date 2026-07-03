package tablesource

// Tests for the LazyAdvance streaming leaf fetch (fetchDuringPushStream) and
// the checkout statement cache it depends on.
//
// The correctness contract is PARITY: with an overlay live, the lazy path's
// yielded sequence must be element-for-element identical to the eager
// fetchForConn's slice for every (overlay change × fetch shape × connection
// shape) combination — fetchForConn is the shipped oracle. On top of parity,
// the nested-cursor tests exercise what only the lazy path does: multiple
// cursors held open simultaneously on the single prev-tx conn, including the
// same-SQL sibling case that silently corrupts with a shared (non-checkout)
// prepared-statement cache — a *sql.Stmt wraps one sqlite3_stmt, and
// re-running Query on it while a previous *sql.Rows is open RESETS the live
// cursor onto the second query's result set (verified experimentally: wrong
// rows, no error).

import (
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// lazySetOverlay installs a synthetic in-flight overlay, as genPushAndWrite
// would mid-fanout, and gates conn's epoch on/off relative to it.
func lazySetOverlay(src *Source, conn *connection, epoch int, gateOn bool, ch ivm.SourceChange) {
	src.mu.Lock()
	src.overlay = &ivm.Overlay{Epoch: epoch, Change: ch}
	src.mu.Unlock()
	if gateOn {
		conn.lastPushedEpoch = epoch
	} else {
		conn.lastPushedEpoch = epoch - 1
	}
}

func lazyClearOverlay(src *Source) {
	src.mu.Lock()
	src.overlay = nil
	src.mu.Unlock()
}

func lazyRow4() ivm.Row {
	return ivm.Row{"id": float64(4), "name": "dave", "score": float64(75), "active": true}
}

// TestLazyAdvanceFetchParity compares the lazy streamed sequence against the
// eager fetchForConn slice across a matrix of overlay changes × fetch shapes
// × connection shapes, with the overlay epoch gate both on and off.
func TestLazyAdvanceFetchParity(t *testing.T) {
	scoreSort := ivm.Ordering{{"score", "asc"}, {"id", "asc"}}
	scorePred := func(r ivm.Row) bool { return r["score"].(float64) >= 75 }

	conns := []struct {
		name string
		sort ivm.Ordering
		pred func(ivm.Row) bool
	}{
		{"pkSort", nil, nil},
		{"scoreSort", scoreSort, nil},
		{"scoreSortPred", scoreSort, scorePred},
	}

	bobOld := ivm.Row{"id": float64(2), "name": "bob", "score": float64(80), "active": false}
	overlays := []struct {
		name string
		ch   ivm.SourceChange
	}{
		{"addMid", ivm.MakeSourceChangeAdd(lazyRow4())},
		{"addFirst", ivm.MakeSourceChangeAdd(ivm.Row{"id": float64(0), "name": "aaa", "score": float64(1), "active": true})},
		{"addLast", ivm.MakeSourceChangeAdd(ivm.Row{"id": float64(9), "name": "zzz", "score": float64(999), "active": true})},
		{"removeBob", ivm.MakeSourceChangeRemove(bobOld)},
		{"removeAbsent", ivm.MakeSourceChangeRemove(ivm.Row{"id": float64(99), "name": "ghost", "score": float64(50), "active": true})},
		// Edit that moves bob's sort position up under scoreSort (80 → 95)
		// and flips active (predicate/constraint transitions).
		{"editMove", ivm.MakeSourceChangeEdit(
			ivm.Row{"id": float64(2), "name": "bob", "score": float64(95), "active": true}, bobOld)},
		// Edit whose OLD row fails an active=true constraint → the eager
		// path's pure-add branch.
		{"editOldOutsideConstraint", ivm.MakeSourceChangeEdit(
			ivm.Row{"id": float64(2), "name": "bob", "score": float64(85), "active": true}, bobOld)},
	}

	activeTrue := ivm.Constraint{"active": true}
	// reqsFor builds the fetch-shape matrix for a connection. Start cursors
	// must be keyed by the connection's sort columns (production Starts come
	// from the same ordering the fetch uses), so the cursor row differs per
	// conn: id-based for the PK sort, score-based for the score sort.
	reqsFor := func(sort ivm.Ordering) []struct {
		name string
		req  ivm.FetchRequest
	} {
		startRow := ivm.Row{"id": float64(2)}
		if sort != nil {
			startRow = ivm.Row{"score": float64(80)}
		}
		return []struct {
			name string
			req  ivm.FetchRequest
		}{
			{"plain", ivm.FetchRequest{}},
			{"constraintActive", ivm.FetchRequest{Constraint: &activeTrue}},
			{"startAt", ivm.FetchRequest{Start: &ivm.Start{Row: startRow, Basis: "at"}}},
			{"startAfter", ivm.FetchRequest{Start: &ivm.Start{Row: startRow, Basis: "after"}}},
			// Take's displaced-bound shape: reverse fetch from a bound.
			{"reverseAt", ivm.FetchRequest{Start: &ivm.Start{Row: startRow, Basis: "at"}, Reverse: true}},
			{"limit2", ivm.FetchRequest{Limit: 2}},
		}
	}

	for _, cs := range conns {
		src, db := newUserSource(t)
		in := src.Connect(cs.sort, nil, cs.pred, nil)
		conn := in.(*sourceInput).conn
		reqs := reqsFor(cs.sort)

		sawNonEmptyOracle := false
		for _, ov := range overlays {
			for _, rq := range reqs {
				for _, gate := range []bool{true, false} {
					name := fmt.Sprintf("%s/%s/%s/gate=%v", cs.name, ov.name, rq.name, gate)
					lazySetOverlay(src, conn, 7, gate, ov.ch)

					eager := src.fetchForConn(rq.req, conn)
					lazy := slices.Collect(src.fetchDuringPushStream(rq.req, conn))
					if len(eager) > 0 {
						sawNonEmptyOracle = true
					}

					eagerRows := make([]ivm.Row, len(eager))
					for i, n := range eager {
						eagerRows[i] = n.Row
					}
					lazyRows := make([]ivm.Row, len(lazy))
					for i, n := range lazy {
						lazyRows[i] = n.Row
					}
					if !reflect.DeepEqual(eagerRows, lazyRows) {
						t.Errorf("%s: lazy != eager\n eager: %v\n lazy:  %v", name, eagerRows, lazyRows)
					}
					lazyClearOverlay(src)
				}
			}
		}
		if !sawNonEmptyOracle {
			t.Fatalf("%s: oracle produced no rows for any case — matrix is degenerate", cs.name)
		}

		// Oracle sanity for two hand-checked cases so the parity above isn't
		// vacuously comparing wrong-but-equal outputs.
		lazySetOverlay(src, conn, 8, true, ivm.MakeSourceChangeAdd(lazyRow4()))
		got := src.fetchForConn(ivm.FetchRequest{}, conn)
		var ids []float64
		for _, n := range got {
			ids = append(ids, n.Row["id"].(float64))
		}
		if !slices.Contains(ids, float64(4)) {
			t.Fatalf("%s: eager oracle missing spliced overlay row id=4: %v", cs.name, ids)
		}
		lazyClearOverlay(src)

		lazySetOverlay(src, conn, 9, true, ivm.MakeSourceChangeRemove(bobOld))
		got = src.fetchForConn(ivm.FetchRequest{}, conn)
		for _, n := range got {
			if n.Row["id"].(float64) == 2 {
				t.Fatalf("%s: eager oracle failed to suppress removed row id=2", cs.name)
			}
		}
		lazyClearOverlay(src)

		db.Close()
	}
}

// TestLazyAdvanceNoOverlayDelegates covers the dispatch + delegation path:
// with LazyAdvance on but NO push in flight, sourceInput.Fetch must produce
// the eager path's rows unchanged.
func TestLazyAdvanceNoOverlayDelegates(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	prev := LazyAdvance
	LazyAdvance = true
	defer func() { LazyAdvance = prev }()

	in := src.Connect(nil, nil, nil, nil)
	conn := in.(*sourceInput).conn

	lazy := slices.Collect(in.Fetch(ivm.FetchRequest{}))
	eager := src.fetchForConn(ivm.FetchRequest{}, conn)
	if len(lazy) != len(eager) || len(lazy) != 3 {
		t.Fatalf("delegation mismatch: lazy=%d eager=%d want 3", len(lazy), len(eager))
	}
	for i := range lazy {
		if !reflect.DeepEqual(lazy[i].Row, eager[i].Row) {
			t.Fatalf("row %d mismatch: %v vs %v", i, lazy[i].Row, eager[i].Row)
		}
	}
}

// lazyPushProbe is a minimal ivm.Output whose Push runs fn — used to issue
// Fetches from INSIDE a real Source.Push fanout (the production shape: a
// Join/Exists re-fetch during Output.Push).
type lazyPushProbe struct {
	fn func(change ivm.Change)
}

func (p *lazyPushProbe) Push(change ivm.Change, _ ivm.InputBase) []ivm.Change {
	p.fn(change)
	return nil
}

// TestLazyAdvanceNestedFetchDuringPush drives a REAL Source.Push and, from
// inside the fanout, holds three cursors open simultaneously on the single
// prev-tx conn:
//
//	A (all rows, cursor open)
//	└── B1 (WHERE active = ?, param true — cursor open)
//	    └── B2 (WHERE active = ?, param false — SAME SQL text as B1)
//
// B1/B2 share one SQL string, so with a shared (non-checkout) stmt cache B2's
// Query would silently reset B1's live cursor onto B2's result set. The
// assertions below fail loudly in that world. All three fetches must also see
// the in-flight overlay row (id=4) spliced per the epoch gate.
func TestLazyAdvanceNestedFetchDuringPush(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	prev := LazyAdvance
	LazyAdvance = true
	defer func() { LazyAdvance = prev }()

	in := src.Connect(nil, nil, nil, nil)

	var a, b1, b2 []float64
	fired := false
	probe := &lazyPushProbe{fn: func(_ ivm.Change) {
		fired = true
		first := true
		for n := range in.Fetch(ivm.FetchRequest{}) {
			a = append(a, n.Row["id"].(float64))
			if first {
				first = false
				cTrue := ivm.Constraint{"active": true}
				firstB := true
				for nb := range in.Fetch(ivm.FetchRequest{Constraint: &cTrue}) {
					b1 = append(b1, nb.Row["id"].(float64))
					if firstB {
						firstB = false
						cFalse := ivm.Constraint{"active": false}
						for nc := range in.Fetch(ivm.FetchRequest{Constraint: &cFalse}) {
							b2 = append(b2, nc.Row["id"].(float64))
						}
					}
				}
			}
		}
	}}
	in.SetOutput(probe)

	src.Push(ivm.MakeSourceChangeAdd(lazyRow4()))

	if !fired {
		t.Fatal("probe never fired — Push did not fan out")
	}
	if want := []float64{1, 2, 3, 4}; !reflect.DeepEqual(a, want) {
		t.Errorf("outer cursor A corrupted or overlay missing: got %v want %v", a, want)
	}
	if want := []float64{1, 3, 4}; !reflect.DeepEqual(b1, want) {
		t.Errorf("nested cursor B1 (active=true) got %v want %v (same-SQL sibling corruption?)", b1, want)
	}
	if want := []float64{2}; !reflect.DeepEqual(b2, want) {
		t.Errorf("nested cursor B2 (active=false) got %v want %v", b2, want)
	}

	// After Push returns: writeChange landed in the prev tx, overlay cleared.
	// A plain fetch must now see id=4 via SQL directly.
	var after []float64
	for n := range in.Fetch(ivm.FetchRequest{}) {
		after = append(after, n.Row["id"].(float64))
	}
	if want := []float64{1, 2, 3, 4}; !reflect.DeepEqual(after, want) {
		t.Errorf("post-push fetch got %v want %v", after, want)
	}
}

// TestLazyAdvanceEarlyAbandonAndPanic verifies cursor/stmt cleanup when the
// consumer stops early (Take satisfying its limit) or panics mid-iteration
// (DriftError unwinding through the operator tree): the checked-out stmt must
// come back to the cache and subsequent fetches must work.
func TestLazyAdvanceEarlyAbandonAndPanic(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(nil, nil, nil, nil)
	conn := in.(*sourceInput).conn
	lazySetOverlay(src, conn, 3, true, ivm.MakeSourceChangeAdd(lazyRow4()))
	defer lazyClearOverlay(src)

	// Abandon after the first row, repeatedly.
	for i := 0; i < 3; i++ {
		for range src.fetchDuringPushStream(ivm.FetchRequest{}, conn) {
			break
		}
	}

	// Panic mid-iteration; the seq's defers must still run.
	func() {
		defer func() { _ = recover() }()
		for range src.fetchDuringPushStream(ivm.FetchRequest{}, conn) {
			panic("simulated drift mid-consume")
		}
	}()

	// Full parity fetch still works after abandons + panic.
	eager := src.fetchForConn(ivm.FetchRequest{}, conn)
	lazy := slices.Collect(src.fetchDuringPushStream(ivm.FetchRequest{}, conn))
	if len(eager) != 4 || len(lazy) != 4 {
		t.Fatalf("post-abandon fetch broken: eager=%d lazy=%d want 4", len(eager), len(lazy))
	}

	// The stmt must have been returned (not leaked): the prev-tx conn's
	// bucket holds it again.
	src.mu.Lock()
	n := len(src.stmtCache[src.prevConn])
	src.mu.Unlock()
	if n < 1 {
		t.Fatalf("stmt not returned to cache after abandon/panic: bucket size %d", n)
	}
}

// TestStmtCheckoutSemantics unit-tests the checkout cache: concurrent
// checkouts of one (conn, SQL) yield distinct stmts; at most one is cached on
// return (the second is closed); a re-checkout hands back the cached one.
func TestStmtCheckoutSemantics(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	const q = "SELECT id FROM users ORDER BY id"
	src.mu.Lock()
	if err := src.ensurePrevTxLocked(); err != nil {
		src.mu.Unlock()
		t.Fatalf("ensurePrevTx: %v", err)
	}
	conn := src.activeConn()
	st1, err := src.checkoutSelectLocked(conn, q)
	if err != nil {
		src.mu.Unlock()
		t.Fatalf("checkout 1: %v", err)
	}
	st2, err := src.checkoutSelectLocked(conn, q)
	if err != nil {
		src.mu.Unlock()
		t.Fatalf("checkout 2: %v", err)
	}
	if st1 == st2 {
		src.mu.Unlock()
		t.Fatal("two live checkouts of the same SQL returned the SAME stmt — cursor sharing corruption")
	}
	src.returnSelectStmtLocked(conn, q, st1)
	src.returnSelectStmtLocked(conn, q, st2) // slot occupied → must close st2
	if cached := src.stmtCache[conn][q]; cached == nil || cached.st != st1 {
		src.mu.Unlock()
		t.Fatal("cache does not hold the first-returned stmt")
	}
	st3, err := src.checkoutSelectLocked(conn, q)
	if err != nil {
		src.mu.Unlock()
		t.Fatalf("checkout 3: %v", err)
	}
	if st3 != st1 {
		src.mu.Unlock()
		t.Fatal("re-checkout did not reuse the cached stmt")
	}
	src.returnSelectStmtLocked(conn, q, st3)
	src.mu.Unlock()

	// st2 was the loser — it must be closed.
	if _, err := st2.Query(); err == nil {
		t.Fatal("second-returned stmt still usable — expected it closed")
	}
}

// TestLazyAdvanceConcurrentRefreshSnapshot hammers the drift audit's
// RefreshSnapshot concurrently with real Pushes whose fanout runs lazy
// fetches. RefreshSnapshot must no-op while the overlay is live (the
// TOCTOU guard), so every in-push fetch stays correct; run under -race.
func TestLazyAdvanceConcurrentRefreshSnapshot(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()
	prev := LazyAdvance
	LazyAdvance = true
	defer func() { LazyAdvance = prev }()

	in := src.Connect(nil, nil, nil, nil)

	var mu sync.Mutex
	var failures []string
	var pushedID float64
	probe := &lazyPushProbe{fn: func(_ ivm.Change) {
		var ids []float64
		for n := range in.Fetch(ivm.FetchRequest{}) {
			ids = append(ids, n.Row["id"].(float64))
		}
		// Base row id=1 is committed (always visible) and the in-flight
		// overlay row must be spliced regardless of audit rollbacks of the
		// prev tx between pushes.
		if !slices.Contains(ids, float64(1)) || !slices.Contains(ids, pushedID) {
			mu.Lock()
			failures = append(failures, fmt.Sprintf("push id=%v fetch saw %v", pushedID, ids))
			mu.Unlock()
		}
	}}
	in.SetOutput(probe)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				src.RefreshSnapshot()
			}
		}
	}()

	for i := 0; i < 50; i++ {
		pushedID = float64(100 + i)
		src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
			"id": pushedID, "name": fmt.Sprintf("u%d", i), "score": float64(10 + i), "active": true,
		}))
	}
	close(stop)
	wg.Wait()

	for _, f := range failures {
		t.Error(f)
	}
}
