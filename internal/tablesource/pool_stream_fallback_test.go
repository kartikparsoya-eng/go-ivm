package tablesource

// Pins for the pool-exhaustion hold-and-wait breaker (2026-07-07
// G13-residual wedge, root-caused from live goroutine dumps of two wedged
// production workers):
//
// The streaming hydrate leaf (fetchViaPoolStream) holds its frame-pinned
// pool reader for the WHOLE iteration, and a parent Join fetches its child
// INSIDE that iteration — nested fetches acquire additional readers while
// holding earlier ones. The pull-mode hydrate runs one goroutine per query
// (engine D6), so demand can exceed the pool's K = P × Cmax sizing; with
// the old unbounded acquire(s.ctx) (Background in production) that was a
// PERMANENT deadlock: every reader held by a goroutine waiting for another
// reader (184/212 goroutines parked in ReaderPool.acquire in the dumps),
// the RPC handler never returned, group.mu stayed held forever, and every
// later init/destroy for that CG timed out at the TS 120s RPC bound.
//
// The fix bounds the acquire with PoolAcquireTimeout and falls back to
// fetchSerial — the same pinned frame via the bound conn, eager
// borrow-drain-release so the fallback can never join the wait cycle.

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestPoolExhaustedNestedFetchFallsBackToSerial reproduces the production
// deadlock shape at K=1: an outer streaming fetch holds the pool's only
// reader mid-iteration (the parent Join's open cursor), and a nested fetch
// on the same source needs a second reader (the child fetch). Pre-fix the
// nested acquire blocked forever (watchdog fires); post-fix it falls back
// to the serial bound-conn read after PoolAcquireTimeout and completes
// with identical rows.
func TestPoolExhaustedNestedFetchFallsBackToSerial(t *testing.T) {
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

	setPoolAcquireTimeout(t, 200*time.Millisecond)

	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)

	type result struct {
		outerIDs  []float64
		nestedIDs []float64
	}
	done := make(chan result, 1)
	go func() {
		var r result
		first := true
		// Outer stream: holds the pool's ONLY reader across the iteration.
		for n := range in.Fetch(ivm.FetchRequest{}) {
			r.outerIDs = append(r.outerIDs, n.Row["id"].(float64))
			if first {
				first = false
				// Nested fetch while the outer cursor is open — the
				// production Join-child shape. K=1 → no reader free.
				for cn := range in.Fetch(ivm.FetchRequest{}) {
					r.nestedIDs = append(r.nestedIDs, cn.Row["id"].(float64))
				}
			}
		}
		done <- r
	}()

	select {
	case r := <-done:
		want := []float64{1, 2, 3, 4, 5} // seedReplicaWithStateVersion's users
		if !slices.Equal(r.nestedIDs, want) {
			t.Fatalf("nested (fallback) ids = %v, want %v", r.nestedIDs, want)
		}
		if !slices.Equal(r.outerIDs, want) {
			t.Fatalf("outer (pool) ids = %v, want %v", r.outerIDs, want)
		}
		if PoolStreamFallbackCount() == 0 {
			t.Fatal("fallback counter did not record the exhaustion fallback")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nested fetch deadlocked on the exhausted reader pool " +
			"(hold-and-wait: outer stream holds the only reader; the bounded " +
			"acquire + serial fallback is missing)")
	}
}

// TestPoolAcquireOnTornDownSourceStillPanics pins the error classification:
// the serial fallback engages ONLY for pool exhaustion. A cancelled Source
// lifetime ctx (CG teardown) must still panic — reading a dead source
// through the fallback would hide teardown bugs.
func TestPoolAcquireOnTornDownSourceStillPanics(t *testing.T) {
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

	setPoolAcquireTimeout(t, 200*time.Millisecond)
	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)

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
					// Nested fetch: pool exhausted AND ctx dead → panic,
					// not fallback.
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
			t.Fatal("nested fetch on a torn-down source did not panic — " +
				"the fallback must not read through a dead source")
		}
		if msg, _ := recovered.(string); !strings.Contains(msg, "pool acquire") {
			t.Fatalf("panic = %v, want the pool-acquire propagation", recovered)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nested fetch blocked despite cancelled source ctx")
	}
}
