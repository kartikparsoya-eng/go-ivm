package engine

// Tests for the pull-streaming engine changes (DESIGN-duplex-streaming §6
// steps 2-3):
//
//   D4 — bool-returning onResult: a false return cancels the stream, breaks
//        the producer's fetch range (iter.Seq defers unwind → cursor close /
//        reader release), and AddQueriesStream*(Chunked|Pull) returns
//        ErrStreamCancelled. Verified against BOTH source kinds: MemorySource
//        (production of results stops promptly) and tablesource (the source's
//        conn+tx survive a mid-cursor abandon: a later hydrate re-reads
//        cleanly and Close() releases everything without error).
//   D5 — e.mu is released for the drain phase, so unrelated engine work
//        (advances on other tables) proceeds while a pull producer parks.
//   D6 — AddQueriesStreamPull runs each query on its own goroutine (no lane
//        pool) and produces byte-identical results to the lane-pool path.
//
// All run with -race; the cancel tests assert no goroutine leaks via
// goleak (I7).

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// verifyNoStreamLeaks asserts no goroutine from the streaming machinery
// survives the test. database/sql internals (connectionOpener, Tx.awaitDone)
// are ignored: they belong to the engine storage / tablesource pool / prev-tx
// lifetimes, which the shared helpers close in t.Cleanup — AFTER this check
// runs. The leak signal that matters for I7 is a stuck hydrate producer or
// operator-fetch frame, whose stacks are not filtered.
func verifyNoStreamLeaks(t *testing.T) {
	goleak.VerifyNone(t,
		goleak.IgnoreCurrent(),
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
		goleak.IgnoreTopFunction("database/sql.(*Tx).awaitDone"),
	)
}

// TestStreamCancel_StopsProductionAndReturnsErr pins D4 on a MemorySource:
// onResult refuses after 3 chunks (chunkSize=1 ⇒ 3 rows) of a 500-row
// result; the producer must stop at the refusal (exactly 4 calls: 3
// accepted + the refused one) and the call must return ErrStreamCancelled.
// Fails pre-D4: onResult had no abort signal, so all 500 rows + the Final
// frame were produced and the call returned nil.
func TestStreamCancel_StopsProductionAndReturnsErr(t *testing.T) {
	defer verifyNoStreamLeaks(t)
	eng, _ := newStreamingTestEngine(t, 500)

	var calls atomic.Int64
	err := eng.AddQueriesStreamChunked(
		[]QuerySpec{simpleQuery("q1")}, 1,
		func(r QueryResult) bool {
			return calls.Add(1) <= 3
		})
	if !errors.Is(err, ErrStreamCancelled) {
		t.Fatalf("err = %v, want ErrStreamCancelled", err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("onResult called %d times, want exactly 4 (3 accepted + 1 refused)", got)
	}
}

// TestStreamCancel_TableSourceReleasesReader pins I7 on the production
// source path: cancelling mid-scan abandons an open SQLite cursor on the
// source's prev-tx conn. Breaking the range must unwind the operator chain
// (cursor Close) so that (a) a follow-up hydrate on the same source
// re-reads the full table — impossible if the conn were wedged mid-rows —
// and (b) Engine.Close can ROLLBACK the prev tx and return the conn
// without error. Fails pre-D4 (no way to cancel at all).
func TestStreamCancel_TableSourceReleasesReader(t *testing.T) {
	defer verifyNoStreamLeaks(t)
	eng := newTicketsTableEngine(t, 200)

	var calls atomic.Int64
	err := eng.AddQueriesStreamChunked(
		[]QuerySpec{{QueryID: "qc", AST: ticketsAST()}}, 1,
		func(r QueryResult) bool {
			return calls.Add(1) <= 5
		})
	if !errors.Is(err, ErrStreamCancelled) {
		t.Fatalf("err = %v, want ErrStreamCancelled", err)
	}

	// The abandoned cursor must be closed: a fresh hydrate re-reads all
	// 200 rows through the same source (same prev-tx conn).
	rows, finals := hydrateStream(t, eng, "q-after-cancel")
	if rows != 200 || finals != 1 {
		t.Fatalf("post-cancel hydrate = (%d rows, %d finals), want (200, 1)", rows, finals)
	}
	// And teardown must be clean: Close rolls back the prev tx — a live
	// cursor on that conn would surface as a close error.
	if err := eng.Close(); err != nil {
		t.Fatalf("Close after cancel: %v", err)
	}
}

// TestStreamCancel_PullBatchAllLanesStop pins the one-gate-per-RPC
// semantics at the engine level: when ANY query's onResult refuses, the
// whole pull batch settles as cancelled — sibling producers stop at their
// next flush or job pickup rather than draining to completion.
func TestStreamCancel_PullBatchAllLanesStop(t *testing.T) {
	defer verifyNoStreamLeaks(t)
	eng, _ := newStreamingTestEngine(t, 300)

	var calls atomic.Int64
	err := eng.AddQueriesStreamPull(
		[]QuerySpec{simpleQuery("p1"), simpleQuery("p2"), simpleQuery("p3")}, 1,
		func(r QueryResult) bool {
			// Refuse everything from the very first delivery.
			calls.Add(1)
			return false
		})
	if !errors.Is(err, ErrStreamCancelled) {
		t.Fatalf("err = %v, want ErrStreamCancelled", err)
	}
	// Each of the 3 producers can have at most ONE refused call in flight
	// before it observes cancellation (its own refusal or the shared flag
	// at its next flush). 300 rows/query must never drain.
	if got := calls.Load(); got > 6 {
		t.Fatalf("onResult called %d times after immediate refusal; want ≤ 6", got)
	}
}

// TestStreamPull_MatchesNonPullOutput pins D6: the per-query-goroutine
// path (pull) emits exactly the same per-query chunk sequence as the
// lane-pool path — same rows, same order, same Final/ChunkIndex contract.
func TestStreamPull_MatchesNonPullOutput(t *testing.T) {
	eng, _ := newStreamingTestEngine(t, 47)

	collect := func(usePull bool, ids []string) map[string][]QueryResult {
		specs := make([]QuerySpec, len(ids))
		for i, id := range ids {
			specs[i] = simpleQuery(id)
		}
		var mu sync.Mutex
		perQuery := make(map[string][]QueryResult)
		onResult := func(r QueryResult) bool {
			cp := make([]RowChange, len(r.Changes))
			copy(cp, r.Changes)
			r.Changes = cp
			mu.Lock()
			perQuery[r.QueryID] = append(perQuery[r.QueryID], r)
			mu.Unlock()
			return true
		}
		var err error
		if usePull {
			err = eng.AddQueriesStreamPull(specs, 10, onResult)
		} else {
			err = eng.AddQueriesStreamChunked(specs, 10, onResult)
		}
		if err != nil {
			t.Fatalf("stream (pull=%v): %v", usePull, err)
		}
		return perQuery
	}

	lane := collect(false, []string{"a1", "a2", "a3"})
	pull := collect(true, []string{"b1", "b2", "b3"})

	flatten := func(chunks []QueryResult) []string {
		var keys []string
		finals := 0
		for i, c := range chunks {
			if c.ChunkIndex != i {
				t.Fatalf("chunk index %d at position %d", c.ChunkIndex, i)
			}
			if c.Final {
				finals++
			}
			for _, rc := range c.Changes {
				keys = append(keys, rc.Row["id"].(string))
			}
		}
		if finals != 1 || !chunks[len(chunks)-1].Final {
			t.Fatalf("finals=%d (last=%v), want exactly one trailing Final", finals, chunks[len(chunks)-1].Final)
		}
		return keys
	}

	laneKeys := flatten(lane["a1"])
	pullKeys := flatten(pull["b1"])
	if len(laneKeys) != 47 || len(pullKeys) != 47 {
		t.Fatalf("row counts: lane=%d pull=%d, want 47 both", len(laneKeys), len(pullKeys))
	}
	for i := range laneKeys {
		if laneKeys[i] != pullKeys[i] {
			t.Fatalf("row %d differs: lane=%s pull=%s", i, laneKeys[i], pullKeys[i])
		}
	}
	// And chunk STRUCTURE matches (not just the flattened rows).
	if len(lane["a1"]) != len(pull["b1"]) {
		t.Fatalf("chunk counts differ: lane=%d pull=%d", len(lane["a1"]), len(pull["b1"]))
	}
}

// TestAdvanceRunsDuringParkedPullDrain pins the D5 payoff at the engine
// level: with a pull hydrate parked in its drain phase (e.mu released), a
// concurrent AdvanceStream on the SAME engine completes instead of
// deadlocking behind the parked producer. The advance targets a DIFFERENT
// table ("orders") than the parked fetch ("users") — mutating a table a
// suspended cursor is iterating stays the caller's (group.mu-level)
// responsibility; what the ENGINE must no longer do is serialize unrelated
// work behind a parked drain. Deadlocks (times out) pre-D5. (In production
// the sidecar's group.mu serializes same-group advance vs hydrate —
// TS-faithful; engines are per-group, so this models the engine-internal
// freeze hazard only.)
func TestAdvanceRunsDuringParkedPullDrain(t *testing.T) {
	defer verifyNoStreamLeaks(t)
	eng, _ := newStreamingTestEngine(t, 100)

	// Second, unrelated table for the concurrent advance.
	orders := ivm.NewMemorySource(
		"orders",
		map[string]string{"id": "string", "total": "number"},
		[]string{"id"},
	)
	eng.RegisterMemorySource(orders)

	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	done := make(chan error, 1)
	go func() {
		done <- eng.AddQueriesStreamPull(
			[]QuerySpec{simpleQuery("qp")}, 1,
			func(r QueryResult) bool {
				once.Do(func() {
					close(parked)
					<-release // park exactly like a zero-credit gate
				})
				return true
			})
	}()

	<-parked
	advanceDone := make(chan error, 1)
	go func() {
		advanceDone <- eng.AdvanceStream(
			[]SnapshotChange{{
				Table:     "orders",
				NextValue: ivm.Row{"id": "o-1", "total": float64(42)},
			}},
			func(AdvanceStreamPartial) {})
	}()

	select {
	case err := <-advanceDone:
		if err != nil {
			t.Fatalf("advance during parked drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		<-done
		t.Fatal("AdvanceStream blocked behind a parked pull drain (e.mu still held across drain?)")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("hydrate: %v", err)
	}
}
