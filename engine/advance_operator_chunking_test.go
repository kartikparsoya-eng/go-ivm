package engine

import "testing"

// TestAdvanceFanout_OperatorChunking proves operator-level streaming (Win 2):
// a SINGLE source-change whose relationship fan-out exceeds advanceChunkSize now
// flushes MULTIPLE partial frames DURING the flatten, rather than buffering the
// whole source-change into one giant frame. This is the within-source-change
// streaming that makes Go's advance faithful to TS #streamNodes' yield* +
// #processChanges poke-every-CURSOR_PAGE_SIZE.
//
// Guard rationale: the divergence harness proves OUTPUT identity but NOT that the
// mid-flatten sink actually fired — if streaming silently regressed to
// buffer-whole, output would still match. This test fails if a single
// source-change stops splitting.
func TestAdvanceFanout_OperatorChunking(t *testing.T) {
	saved := advanceChunkSize
	advanceChunkSize = 50 // force splits well below the fan-out
	defer func() { advanceChunkSize = saved }()

	const childCount = 500 // one user ADD → 500 post children = ONE source-change
	eng := newFanoutEngine(t, childCount)

	frames, total := 0, 0
	finalSeen := false
	if err := eng.AdvanceStream(
		[]SnapshotChange{{Table: "users", NextValue: u1Row}},
		func(p AdvanceStreamPartial) {
			frames++
			total += len(p.Changes)
			if p.Final {
				finalSeen = true
			}
		},
	); err != nil {
		t.Fatalf("AdvanceStream: %v", err)
	}

	if !finalSeen {
		t.Fatal("no terminal Final frame")
	}
	if total != childCount+1 { // parent row + childCount children, no drops/dupes
		t.Fatalf("row total across frames = %d, want %d (parent + %d children)",
			total, childCount+1, childCount)
	}
	// 501 rows at chunk=50 → ≥10 partial frames. Pre-Win-2 this was 1 frame
	// (whole source-change buffered), so <10 means the flatten is NOT streaming.
	if frames < 10 {
		t.Fatalf("operator chunking did NOT split: %d frame(s) for %d rows at chunk=50 "+
			"(want ≥10 — a single source-change is still buffering whole)", frames, total)
	}
	t.Logf("childCount=%d chunk=50 → %d frames, %d rows: single source-change streamed mid-flatten",
		childCount, frames, total)
}
