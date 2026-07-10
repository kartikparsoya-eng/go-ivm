package tablesource

// Review item #4: reader-pool failure modes, reshaped for Option B (one
// reader per hydrate pipeline, interleaved cursors). The happy path is well
// covered (pinning, concurrency, single-conn equivalence, nested cursors);
// these pin the unhappy paths that would otherwise surface as CG wedges:
//   - a panic MID-CURSOR on the pipeline's bound reader (poison row tripping
//     FromSQLiteType's MAX_SAFE_INTEGER guard) must leave the READER healthy
//     — the checked-out stmt is closed/reset on unwind, later fetches on the
//     same reader work, and the release (the engine hydrateOne's defer)
//     returns a usable reader to the pool;
//   - the old acquire-cancellation tests (ctx-bounded per-fetch acquire,
//     Source.Close unblocking a parked fetch) are OBSOLETE: fetches no
//     longer acquire mid-flight at all. Admission-gate queueing/cancellation
//     now lives in engine.acquirePipelineReader (see engine tests) and
//     AcquireForPipeline's bounded wait (pool_single_reader_test.go).

import (
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestSourceFetch_PoolPoisonRow_ReaderSurvivesPanic: a row whose INTEGER
// value exceeds MAX_SAFE_INTEGER trips FromSQLiteType's guard MID-SCAN while
// the fetch iterates a cursor on the pipeline's bound reader. The panic is
// the designed outcome (DataError → -32102 teardown); what this test pins is
// the unwind hygiene — the live cursor closes, the suspect stmt is not
// poisoned in the cache, subsequent fetches on the SAME reader succeed, and
// after release the pool is whole (all K readers acquirable).
func TestSourceFetch_PoolPoisonRow_ReaderSurvivesPanic(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v9")
	// Poison: 2^53 + 1 in the score column (declared "number" in userSchema).
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Exec(`INSERT INTO users VALUES (6, 'poison', 9007199254740993, 1)`); err != nil {
		t.Fatalf("insert poison: %v", err)
	}
	w.Close()

	src := newUserSourceAt(t, path)
	poolDB, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer poolDB.Close()
	const k = 2
	pool, err := NewReaderPool(context.Background(), poolDB, "v9", k)
	if err != nil {
		t.Fatalf("NewReaderPool: %v", err)
	}
	defer pool.Close()
	src.BindReaderPool(pool)

	src.SetNextConnectGroup("q1")
	in := src.Connect(nil, nil, nil, nil)
	release, ok := pool.AcquireForPipeline("q1", time.Second)
	if !ok {
		t.Fatal("AcquireForPipeline did not grant")
	}

	// Drive the poisoned fetch several times ON THE SAME READER: each must
	// panic (not hang, not silently skip) and each must leave the reader
	// usable for the next attempt.
	for i := 0; i < 3; i++ {
		func() {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected the poison row to panic the fetch")
				}
				if _, ok := r.(*ivm.DataError); !ok {
					t.Fatalf("want *ivm.DataError (teardown classification), got %T: %v", r, r)
				}
			}()
			_ = slices.Collect(in.Fetch(ivm.FetchRequest{}))
		}()
	}

	// A clean fetch (poison excluded by constraint) on the SAME still-bound
	// reader must work — the unwound cursor did not poison the reader.
	got := slices.Collect(in.Fetch(ivm.FetchRequest{
		Constraint: &ivm.Constraint{"id": float64(1)},
	}))
	if len(got) != 1 {
		t.Fatalf("post-panic fetch got %d rows, want 1", len(got))
	}
	release()

	// The pool is whole: all K readers acquirable with a short bound.
	releases := make([]func(), 0, k)
	for i := 0; i < k; i++ {
		rel, ok := pool.AcquireForPipeline(string(rune('a'+i)), 2*time.Second)
		if !ok {
			t.Fatalf("reader %d/%d not acquirable after panic unwind (leaked)", i+1, k)
		}
		releases = append(releases, rel)
	}
	for _, rel := range releases {
		rel()
	}
}
