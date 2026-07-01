package engine

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

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

// TestAdvanceFanout_OperatorChunking_ParallelSinkRace closes the coverage gap
// for Win 2's headline concurrency claim: "flushMu keeps chunkIndex monotonic
// across the parallel push-fanout" (engine.go sendFrame). The single-parent
// TestAdvanceFanout_OperatorChunking never exercises it — one parent row = one
// connection = GenPushParallel's single-conn fast path (parallel.go:71), so the
// sink is only ever called sequentially there.
//
// Here N identical queries give the users source N connections, so pushing ONE
// user ADD fans out across N goroutines concurrently (parallel.go:100-117), each
// flattening childCount+1 rows and calling the shared chunkSink (→ sendFrame)
// mid-flatten at chunk=50. That drives many concurrent sink calls.
//
// Two independent detectors guard the flushMu contract; run under -race:
//  1. the race detector flags any unsynchronized chunkIndex++/onResult in
//     production code if flushMu were insufficient;
//  2. the chunkIndex-contiguity assertion below fails if two concurrent flushes
//     ever duplicated, skipped, or reordered a frame index — because sendFrame
//     assigns chunkIndex AND calls onResult under the SAME flushMu, so arrival
//     order MUST equal 0,1,2,…,N with no gaps.
//
// Plus a no-loss/no-dup row-set check per query (parallel push must be
// output-equivalent to sequential).
func TestAdvanceFanout_OperatorChunking_ParallelSinkRace(t *testing.T) {
	saved := advanceChunkSize
	advanceChunkSize = 50 // force ~10 mid-flatten flushes per pipeline
	defer func() { advanceChunkSize = saved }()

	const (
		childCount = 500
		numQueries = 3 // >=2 connections → GenPushParallel fans out concurrently
	)

	// Build users+posts, seed childCount posts under u1 (u1 itself is NOT seeded;
	// the AdvanceStream ADD below drives the fan-out).
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"}, []string{"id"})
	posts := ivm.NewMemorySource("posts",
		map[string]string{"id": "string", "userId": "string"}, []string{"id"})
	seed := make([]ivm.Row, childCount)
	for i := range seed {
		seed[i] = ivm.Row{"id": fmt.Sprintf("p%06d", i), "userId": "u1"}
	}
	posts.BulkInsert(seed)

	eng, err := NewEngine(EngineConfig{StoragePath: filepath.Join(t.TempDir(), "storage.db")})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(posts)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetTableUniqueKeys("posts", [][]string{{"id"}})

	// N identical queries → N persistent connections on the users source, each
	// fanning u1 out to childCount posts. Distinct queryIDs keep rows attributable.
	queryIDs := make([]string, numQueries)
	for i := range queryIDs {
		id := fmt.Sprintf("q-%d", i)
		queryIDs[i] = id
		if _, _, err := eng.AddQuery(id, fanoutRelatedAST()); err != nil {
			t.Fatalf("AddQuery %s: %v", id, err)
		}
	}
	// Enable parallel push now that >=2 connections exist. Pushing u1 fans out
	// across the query pipelines concurrently → their Accumulate → sink calls
	// race on flushMu.
	users.SetParallel(true, 2)

	// Collector guarded by its own mutex so the TEST harness stays correct even
	// if the production flushMu contract were violated — that failure surfaces
	// via the race detector (on production state) and the contiguity assertion,
	// NOT as a hidden crash or silent miscount in the harness.
	var mu sync.Mutex
	var chunkIdxSeq []int
	finals := 0
	perQuery := map[string]map[string]int{} // queryID -> "table|id" -> count

	err = eng.AdvanceStream(
		[]SnapshotChange{{Table: "users", NextValue: u1Row}},
		func(p AdvanceStreamPartial) {
			mu.Lock()
			defer mu.Unlock()
			chunkIdxSeq = append(chunkIdxSeq, p.ChunkIndex)
			if p.Final {
				finals++
			}
			for _, rc := range p.Changes {
				q := perQuery[rc.QueryID]
				if q == nil {
					q = map[string]int{}
					perQuery[rc.QueryID] = q
				}
				id, _ := rc.RowKey["id"].(string)
				q[rc.Table+"|"+id]++
			}
		},
	)
	if err != nil {
		t.Fatalf("AdvanceStream: %v", err)
	}

	// 1) Exactly one terminal frame.
	if finals != 1 {
		t.Fatalf("want exactly 1 Final frame, got %d", finals)
	}
	// 2) chunkIndex arrived strictly 0,1,2,…,N-1 (contiguous, ordered). A broken
	//    lock shows up here as a duplicate, gap, or reorder.
	for i, ci := range chunkIdxSeq {
		if ci != i {
			t.Fatalf("chunkIndex not contiguous/ordered: frame %d has ChunkIndex=%d; seq=%v",
				i, ci, chunkIdxSeq)
		}
	}
	// 3) Enough frames that mid-flatten flushing actually fired across pipelines.
	//    Each of numQueries pipelines emits childCount+1 rows at chunk=50 → ~10
	//    full-chunk sink flushes; a single buffered frame would be way below this.
	if len(chunkIdxSeq) < numQueries*5 {
		t.Fatalf("too few frames (%d) — mid-flatten sink likely not exercised across pipelines",
			len(chunkIdxSeq))
	}
	// 4) No loss / no dup: every query saw u1 once + each post once, nothing else.
	if len(perQuery) != numQueries {
		t.Fatalf("want %d queries in output, got %d", numQueries, len(perQuery))
	}
	for _, id := range queryIDs {
		q := perQuery[id]
		if got := q["users|u1"]; got != 1 {
			t.Errorf("query %s: users|u1 appeared %d times, want 1", id, got)
		}
		if len(q) != childCount+1 { // u1 + childCount posts
			t.Errorf("query %s: distinct rowKeys=%d, want %d", id, len(q), childCount+1)
		}
		for i := 0; i < childCount; i++ {
			key := fmt.Sprintf("posts|p%06d", i)
			if got := q[key]; got != 1 {
				t.Errorf("query %s: %s appeared %d times, want 1", id, key, got)
			}
		}
	}
	t.Logf("parallel sink race: %d queries × %d rows, %d frames, chunkIndex 0..%d contiguous, 1 Final",
		numQueries, childCount+1, len(chunkIdxSeq), len(chunkIdxSeq)-1)
}
