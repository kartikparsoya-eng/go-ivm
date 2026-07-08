package tablesource

// Pins for the Option B single-reader resource model (one reader per hydrate
// pipeline, interleaved cursors — TS's better-sqlite3 shape: one connection
// per view-syncer, nested statement.iterate()).
//
// History: the predecessor of this file (pool_stream_fallback_test.go) pinned
// the bounded-acquire + fetchSerial fallback that e5b00b0 bolted onto the
// per-fetch acquire model after the 2026-07-07 G13-residual wedge (nested
// fetches acquiring readers while holding readers — hold-and-wait deadlock at
// K < demand). Option B deletes the deadlock CLASS: a pipeline acquires its
// single reader while holding nothing, and nested fetches NEVER acquire —
// they interleave cursors on the already-held reader. The old test asserted
// the fallback engaged at K=1; these assert the same shape now simply
// SUCCEEDS on one reader with zero fallback machinery involved (the fallback
// paths no longer exist to engage).

import (
	"context"
	"database/sql"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestNestedFetchOnSingleBoundReader is the Option B keystone: at K=1, an
// outer streaming fetch holds a live cursor on the pipeline's ONE reader,
// and a nested fetch — the production Join-child shape — with the IDENTICAL
// SQL (same request, same connection: the self-join / repeated-shape case,
// review addendum (b)) runs mid-iteration on the SAME reader. One
// sqlite3_stmt is one cursor, so this exercises the checkout stmt-cache
// directly: the outer cursor's stmt is checked out, the nested fetch must
// prepare a transient duplicate rather than re-bind (and thereby silently
// reset) the live outer cursor. Both iterations must yield the full,
// correctly-ordered rows.
func TestNestedFetchOnSingleBoundReader(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })

	src, err := New(db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { src.Close() })

	pool, perr := NewReaderPool(context.Background(), db, "", 1)
	if perr != nil {
		t.Fatalf("NewReaderPool: %v", perr)
	}
	t.Cleanup(func() { pool.Close() })
	src.BindReaderPool(pool)
	t.Cleanup(func() { src.UnbindReaderPool() })

	// Bind the pipeline: this connection carries group "q1", and the
	// pipeline's single reader is acquired up front (the engine's
	// hydrateOne discipline).
	src.SetNextConnectGroup("q1")
	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	release, ok := pool.AcquireForPipeline("q1", time.Second)
	if !ok {
		t.Fatal("AcquireForPipeline on a fresh K=1 pool did not grant")
	}
	defer release()

	// Warm the stmt cache: one complete fetch caches the shape's stmt.
	// The nested-fetch collision below is only reachable through a WARM
	// cache — the outer fetch checks the cached stmt OUT, and the nested
	// same-SQL fetch must prepare a duplicate rather than find (and
	// silently reset) the live cached cursor. A cold cache prepares fresh
	// stmts on both sides and can never collide, warm is the production
	// steady state.
	if warm := slices.Collect(in.Fetch(ivm.FetchRequest{})); len(warm) != 5 {
		t.Fatalf("warm-up fetch got %d rows, want 5", len(warm))
	}

	type result struct {
		outerIDs  []float64
		nestedIDs []float64
		nested2   []float64
	}
	done := make(chan result, 1)
	go func() {
		var r result
		first := true
		// Outer stream: live cursor on the pipeline's ONLY reader.
		for n := range in.Fetch(ivm.FetchRequest{}) {
			r.outerIDs = append(r.outerIDs, n.Row["id"].(float64))
			if first {
				first = false
				// Nested SAME-SQL fetch while the outer cursor is open.
				for cn := range in.Fetch(ivm.FetchRequest{}) {
					r.nestedIDs = append(r.nestedIDs, cn.Row["id"].(float64))
					// Third level: nesting depth is unbounded on one conn.
					if len(r.nestedIDs) == 2 && r.nested2 == nil {
						for cn2 := range in.Fetch(ivm.FetchRequest{}) {
							r.nested2 = append(r.nested2, cn2.Row["id"].(float64))
						}
					}
				}
			}
		}
		done <- r
	}()

	select {
	case r := <-done:
		want := []float64{1, 2, 3, 4, 5} // seedReplicaWithStateVersion's users
		if !slices.Equal(r.outerIDs, want) {
			t.Fatalf("outer ids = %v, want %v (nested cursor corrupted the outer one?)", r.outerIDs, want)
		}
		if !slices.Equal(r.nestedIDs, want) {
			t.Fatalf("nested ids = %v, want %v", r.nestedIDs, want)
		}
		if !slices.Equal(r.nested2, want) {
			t.Fatalf("third-level ids = %v, want %v", r.nested2, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nested fetch blocked on the single-reader pipeline — " +
			"a nested fetch is acquiring instead of riding the bound reader")
	}

	// The transient duplicate stmt died at return; the cached shape must
	// still be usable — a fresh (non-nested) fetch reuses it cleanly.
	again := slices.Collect(in.Fetch(ivm.FetchRequest{}))
	if len(again) != 5 {
		t.Fatalf("post-nesting fetch got %d rows, want 5", len(again))
	}
}

// TestBoundReaderFetchParityMatrix: the bound-reader read
// (fetchViaBoundReaderStream — raw driver conn, driver-level scan) is
// element-for-element identical to fetchSerial (the byte-identical oracle:
// database/sql on the bound conn, same pinned frame) across connection
// shapes × fetch shapes. Full-row comparison, not just IDs — the raw path
// re-implements the value plumbing (driver.Value → FromSQLiteType) that
// database/sql did on the serial path, and any divergence is drift.
func TestBoundReaderFetchParityMatrix(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v1")

	scoreSort := ivm.Ordering{{"score", "asc"}, {"id", "asc"}}
	scorePred := func(r ivm.Row) bool { return r["score"].(float64) >= 60 }
	conns := []struct {
		name string
		sort ivm.Ordering
		pred func(ivm.Row) bool
	}{
		{"pkSort", ivm.Ordering{{"id", "asc"}}, nil},
		{"scoreSort", scoreSort, nil},
		{"scoreSortPred", scoreSort, scorePred},
		// UNORDERED (Cap/EXISTS-child path): no ORDER BY at all.
		{"unordered", nil, nil},
		{"unorderedPred", nil, scorePred},
	}

	activeTrue := ivm.Constraint{"active": true}
	multi := []ivm.MultiConstraint{{
		{"id": float64(1), "active": true},
		{"id": float64(3), "active": true},
		{"id": float64(5), "active": false},
	}}
	reqsFor := func(sort ivm.Ordering) []struct {
		name string
		req  ivm.FetchRequest
	} {
		reqs := []struct {
			name string
			req  ivm.FetchRequest
		}{
			{"plain", ivm.FetchRequest{}},
			{"constraint", ivm.FetchRequest{Constraint: &activeTrue}},
			{"multiConstraints", ivm.FetchRequest{MultiConstraints: multi}},
		}
		if sort == nil {
			// Start/reverse are TS-forbidden without an ordering
			// (query-builder.ts asserts 'start requires ordering').
			return reqs
		}
		// Cursor keyed by the connection's sort columns.
		startRow := ivm.Row{"id": float64(3)}
		if sort[0][0] == "score" {
			startRow = ivm.Row{"score": float64(70), "id": float64(3)}
		}
		reqs = append(reqs,
			struct {
				name string
				req  ivm.FetchRequest
			}{"reverse", ivm.FetchRequest{Reverse: true}},
			struct {
				name string
				req  ivm.FetchRequest
			}{"startAt", ivm.FetchRequest{Start: &ivm.Start{Row: startRow, Basis: "at"}}},
			struct {
				name string
				req  ivm.FetchRequest
			}{"startAfter", ivm.FetchRequest{Start: &ivm.Start{Row: startRow, Basis: "after"}}},
			struct {
				name string
				req  ivm.FetchRequest
			}{"reverseAt", ivm.FetchRequest{Start: &ivm.Start{Row: startRow, Basis: "at"}, Reverse: true}},
		)
		return reqs
	}

	for _, cc := range conns {
		t.Run(cc.name, func(t *testing.T) {
			src := newUserSourceAt(t, path)
			pool, perr := NewReaderPool(context.Background(), func() *sql.DB {
				db, err := Open(path, OpenOptions{})
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				t.Cleanup(func() { db.Close() })
				return db
			}(), "v1", 1)
			if perr != nil {
				t.Fatalf("NewReaderPool: %v", perr)
			}
			t.Cleanup(func() { pool.Close() })
			src.BindReaderPool(pool)
			t.Cleanup(func() { src.UnbindReaderPool() })

			src.SetNextConnectGroup("q1")
			in := src.Connect(cc.sort, nil, cc.pred, nil)
			conn := src.connections[len(src.connections)-1]

			release, ok := pool.AcquireForPipeline("q1", time.Second)
			if !ok {
				t.Fatal("AcquireForPipeline did not grant")
			}
			t.Cleanup(release)

			for _, rq := range reqsFor(cc.sort) {
				t.Run(rq.name, func(t *testing.T) {
					// Oracle: the serial single-conn read on the same frame.
					want := src.fetchSerial(rq.req, conn)
					// Bound-reader read, twice: cold stmt (fresh prepare)
					// and warm stmt (checkout of the cached one).
					for _, pass := range []string{"cold", "warm"} {
						got := slices.Collect(in.Fetch(rq.req))
						if len(got) != len(want) {
							t.Fatalf("[%s] bound-reader len=%d, serial len=%d", pass, len(got), len(want))
						}
						for i := range want {
							if !reflect.DeepEqual(got[i].Row, want[i].Row) {
								t.Fatalf("[%s] row %d differs:\nbound-reader: %#v\nserial:       %#v",
									pass, i, got[i].Row, want[i].Row)
							}
						}
					}
				})
			}
		})
	}
}

// TestFetchOnTornDownSourcePanics pins the error classification: a fetch on
// a bound reader with a cancelled Source lifetime ctx (CG teardown) must
// panic — silently reading (or hanging) through a dead source would hide
// teardown bugs. The panic surfaces through the engine's hydrate recover as
// a clean batch error.
func TestFetchOnTornDownSourcePanics(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	src, err := NewWithContext(ctx, db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("NewWithContext: %v", err)
	}
	t.Cleanup(func() { src.Close() })

	pool, perr := NewReaderPool(context.Background(), db, "", 1)
	if perr != nil {
		t.Fatalf("NewReaderPool: %v", perr)
	}
	t.Cleanup(func() { pool.Close() })
	src.BindReaderPool(pool)
	t.Cleanup(func() { src.UnbindReaderPool() })

	src.SetNextConnectGroup("q1")
	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	release, ok := pool.AcquireForPipeline("q1", time.Second)
	if !ok {
		t.Fatal("AcquireForPipeline did not grant")
	}
	defer release()

	done := make(chan any, 1)
	go func() {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			first := true
			for range in.Fetch(ivm.FetchRequest{}) {
				if first {
					first = false
					cancel() // CG teardown mid-iteration
					// Nested fetch on the dead source → its query/step runs
					// with the cancelled s.ctx → panic, never a silent read.
					for range in.Fetch(ivm.FetchRequest{}) {
					}
				}
			}
		}()
		done <- recovered
	}()

	select {
	case recovered := <-done:
		if recovered == nil {
			t.Fatal("fetch on a torn-down source did not panic — " +
				"reads must not proceed through a dead source")
		}
		if msg, _ := recovered.(string); !strings.Contains(msg, "tablesource.Source.Fetch") {
			t.Fatalf("panic = %v, want the Fetch propagation", recovered)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fetch blocked despite cancelled source ctx")
	}
}

// TestAcquireForPipeline_QueueAndRelease pins the admission-gate primitive:
// an exhausted pool queues (ok=false within the wait bound, no hang), a
// release hands the reader to the next waiter, and a double-acquire for the
// same pipeline group panics loudly (the engine's pairing broke).
func TestAcquireForPipeline_QueueAndRelease(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v1")
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	pool, err := NewReaderPool(context.Background(), db, "", 1)
	if err != nil {
		t.Fatalf("NewReaderPool: %v", err)
	}
	defer pool.Close()

	releaseA, ok := pool.AcquireForPipeline("a", time.Second)
	if !ok {
		t.Fatal("first acquire did not grant")
	}

	// Exhausted: bounded wait elapses with ok=false, promptly.
	start := time.Now()
	if _, ok := pool.AcquireForPipeline("b", 100*time.Millisecond); ok {
		t.Fatal("acquire on an exhausted pool granted a second reader")
	}
	if e := time.Since(start); e > 2*time.Second {
		t.Fatalf("bounded wait took %v — not bounded", e)
	}

	// A queued waiter wakes on release.
	granted := make(chan func(), 1)
	go func() {
		release, ok := pool.AcquireForPipeline("b", 5*time.Second)
		if !ok {
			granted <- nil
			return
		}
		granted <- release
	}()
	time.Sleep(50 * time.Millisecond) // let it park
	releaseA()
	select {
	case releaseB := <-granted:
		if releaseB == nil {
			t.Fatal("queued waiter timed out despite the release")
		}
		releaseB()
	case <-time.After(2 * time.Second):
		t.Fatal("queued waiter never woke after release")
	}

	// Double-acquire for one group = engine pairing bug → loud panic.
	releaseC, ok := pool.AcquireForPipeline("c", time.Second)
	if !ok {
		t.Fatal("acquire c did not grant")
	}
	defer releaseC()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("double AcquireForPipeline for one group did not panic")
			}
		}()
		// K=1 and "c" holds it, so this would block; give it the reader by
		// racing a release — instead assert via a 2-reader pool would be
		// cleaner, but the panic fires only when a reader IS granted. Use a
		// second pool sized 2 for the pairing check.
		pool2, err := NewReaderPool(context.Background(), db, "", 2)
		if err != nil {
			t.Fatalf("NewReaderPool(2): %v", err)
		}
		defer pool2.Close()
		rel1, ok := pool2.AcquireForPipeline("dup", time.Second)
		if !ok {
			t.Fatal("pool2 acquire did not grant")
		}
		defer rel1()
		_, _ = pool2.AcquireForPipeline("dup", time.Second) // must panic
	}()
}
