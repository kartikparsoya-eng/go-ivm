package engine

// M3 regression test: buildBatchLocked with duplicate query IDs must not
// leave a stale entry in the built slice.  Before the fix, the second
// build for the same QueryID called removeQueryLocked (destroying pipeline
// A's input) but left A in built.  Parallel hydrate then called Fetch on
// the destroyed pipeline → panic.

import (
	"testing"
)

func TestM3_DuplicateQueryID_NoStaleEntryInBuilt(t *testing.T) {
	eng, _ := newStreamingTestEngine(t, 3)

	// Batch with a duplicate query ID: q1 appears twice.
	// buildBatchLocked must remove the stale entry from built so
	// parallel hydrate doesn't Fetch on a destroyed pipeline.
	chunks := collectStream(t, eng, []QuerySpec{
		simpleQuery("q1"),
		simpleQuery("q1"),
	})

	// The duplicate build should succeed without panic.
	// Exactly one set of chunks for q1 (the second build replaces the first).
	perQuery := chunksFor(t, chunks, "q1")
	if len(perQuery) == 0 {
		t.Fatal("no chunks for q1 — duplicate query ID batch produced no results")
	}

	// Verify exactly one Final chunk.
	finals := 0
	for _, c := range perQuery {
		if c.Final {
			finals++
		}
	}
	if finals != 1 {
		t.Fatalf("expected exactly 1 Final chunk for q1, got %d", finals)
	}
}
