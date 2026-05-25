package engine

// Tests for AdvanceStream chunked output. Validates:
//   - empty advance → single empty Final chunk
//   - small advance → single Final chunk with timings
//   - large advance crossing chunk boundary → multiple frames, monotonic
//     ChunkIndex, exactly one Final=true
//   - Final-only timings (non-final frames carry no timings)
//   - Total row count + ordering matches non-streaming Advance
//
// Uses the same direct source+connect pattern as TestEngineAdvance to
// keep the focus on the chunking contract rather than AST/builder
// machinery.

import (
	"sync"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func withAdvanceChunkSize(t *testing.T, size int) {
	t.Helper()
	prev := advanceChunkSize
	advanceChunkSize = size
	t.Cleanup(func() { advanceChunkSize = prev })
}

// newAdvanceTestEngine: engine + one source + N pipelines connected to
// the same source. Each pipeline emits one RowChange per source-change
// it sees, so producing M source changes against N pipelines yields
// roughly M*N RowChanges in the advance output — useful for crossing
// the chunk boundary with a small number of source changes.
func newAdvanceTestEngine(t *testing.T, pipelineCount int) (*Engine, *ivm.MemorySource) {
	t.Helper()
	storagePath := tempStoragePath(t)
	eng, err := NewEngine(EngineConfig{StoragePath: storagePath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	ms := ivm.NewMemorySource(
		"items",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"},
	)
	eng.RegisterMemorySource(ms)

	sort := ivm.Ordering{{"id", "asc"}}
	for i := 0; i < pipelineCount; i++ {
		input := ms.Connect(sort, nil, nil)
		schema := input.GetSchema()
		queryID := "q" + string(rune('1'+i))
		input.SetOutput(&pipelineOutput{engine: eng, queryID: queryID, schema: schema})
	}

	return eng, ms
}

func collectAdvanceStream(t *testing.T, eng *Engine, changes []SnapshotChange) []AdvanceStreamPartial {
	t.Helper()
	var mu sync.Mutex
	var frames []AdvanceStreamPartial
	err := eng.AdvanceStream(changes, func(p AdvanceStreamPartial) {
		// Defensive copy: the engine reuses the `pending` backing slice
		// across flushes; without this the test would see mutated chunks.
		cp := make([]RowChange, len(p.Changes))
		copy(cp, p.Changes)
		p.Changes = cp
		mu.Lock()
		frames = append(frames, p)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("AdvanceStream error: %v", err)
	}
	return frames
}

func assertAdvanceFrameInvariants(t *testing.T, frames []AdvanceStreamPartial) {
	t.Helper()
	if len(frames) == 0 {
		t.Fatalf("AdvanceStream emitted no frames")
	}
	// Monotonic ChunkIndex starting at 0
	for i, f := range frames {
		if f.ChunkIndex != i {
			t.Fatalf("frame[%d].ChunkIndex = %d, want %d", i, f.ChunkIndex, i)
		}
	}
	// Exactly one Final=true, and it's the last frame
	finals := 0
	for i, f := range frames {
		if f.Final {
			finals++
			if i != len(frames)-1 {
				t.Fatalf("Final=true at non-terminal index %d (of %d)", i, len(frames))
			}
		}
	}
	if finals != 1 {
		t.Fatalf("expected exactly 1 Final=true frame, got %d", finals)
	}
	// Timings only on the final frame
	for i, f := range frames {
		if !f.Final && len(f.Timings) > 0 {
			t.Fatalf("frame[%d] (non-final) carries timings (%d entries)", i, len(f.Timings))
		}
	}
}

func advanceTotalRows(frames []AdvanceStreamPartial) int {
	n := 0
	for _, f := range frames {
		n += len(f.Changes)
	}
	return n
}

// 1. Empty advance: single empty Final chunk + empty timings.
func TestAdvanceStream_Empty_EmitsSingleFinalChunk(t *testing.T) {
	withAdvanceChunkSize(t, 100)
	eng, _ := newAdvanceTestEngine(t, 1)

	frames := collectAdvanceStream(t, eng, nil)
	assertAdvanceFrameInvariants(t, frames)

	if len(frames) != 1 {
		t.Fatalf("expected 1 frame for empty advance, got %d", len(frames))
	}
	if advanceTotalRows(frames) != 0 {
		t.Fatalf("expected 0 rows, got %d", advanceTotalRows(frames))
	}
	if len(frames[0].Timings) != 0 {
		t.Fatalf("empty advance should have no timings, got %d", len(frames[0].Timings))
	}
}

// 2. Small advance (one change, one pipeline) → single chunk + Final + timings.
func TestAdvanceStream_Small_SingleChunkWithTimings(t *testing.T) {
	withAdvanceChunkSize(t, 100)
	eng, _ := newAdvanceTestEngine(t, 1)

	changes := []SnapshotChange{{
		Table:     "items",
		NextValue: ivm.Row{"id": "a", "name": "Widget"},
	}}
	frames := collectAdvanceStream(t, eng, changes)
	assertAdvanceFrameInvariants(t, frames)

	if len(frames) != 1 {
		t.Fatalf("expected 1 frame for small advance, got %d", len(frames))
	}
	if advanceTotalRows(frames) != 1 {
		t.Fatalf("expected 1 row, got %d", advanceTotalRows(frames))
	}
	if len(frames[0].Timings) != 1 {
		t.Fatalf("expected 1 timing entry, got %d", len(frames[0].Timings))
	}
	if frames[0].Timings[0].Table != "items" {
		t.Fatalf("unexpected timing table %q", frames[0].Timings[0].Table)
	}
}

// 3. Large advance crosses chunk boundary. 5 source changes × 3 pipelines =
// 15 RowChanges; with chunkSize=4 we get 4 frames (3 with 4 rows each + 1
// tail with 3 rows + Final=true).
//
// Wait — actually the flush happens when len(pending) >= chunkSize, so after
// the source change that pushes us over the threshold, we flush. With 3
// pipelines per source change adding 3 rows at a time and chunkSize=4:
//   sc1: pending=3        no flush
//   sc2: pending=6 ≥4 → FLUSH (6 rows), pending=0, chunkIdx=1
//   sc3: pending=3        no flush
//   sc4: pending=6 ≥4 → FLUSH (6 rows), pending=0, chunkIdx=2
//   sc5: pending=3        no flush
//   terminal flush(true): 3 rows + Final
// → 3 frames total: 6, 6, 3. Total 15 rows.
func TestAdvanceStream_CrossesChunkBoundary_MultipleFrames(t *testing.T) {
	withAdvanceChunkSize(t, 4)
	eng, _ := newAdvanceTestEngine(t, 3) // 3 pipelines → 3 RowChanges per source change

	changes := []SnapshotChange{
		{Table: "items", NextValue: ivm.Row{"id": "a", "name": "A"}},
		{Table: "items", NextValue: ivm.Row{"id": "b", "name": "B"}},
		{Table: "items", NextValue: ivm.Row{"id": "c", "name": "C"}},
		{Table: "items", NextValue: ivm.Row{"id": "d", "name": "D"}},
		{Table: "items", NextValue: ivm.Row{"id": "e", "name": "E"}},
	}
	frames := collectAdvanceStream(t, eng, changes)
	assertAdvanceFrameInvariants(t, frames)

	if len(frames) != 3 {
		t.Fatalf("expected 3 frames (boundary trigger every 2 source changes), got %d", len(frames))
	}
	expectedSizes := []int{6, 6, 3}
	for i, want := range expectedSizes {
		if got := len(frames[i].Changes); got != want {
			t.Fatalf("frame[%d] size: got %d, want %d", i, got, want)
		}
	}
	if !frames[2].Final {
		t.Fatalf("expected Final=true on terminal frame")
	}
	if len(frames[2].Timings) != 5 {
		t.Fatalf("expected 5 timing entries (one per source change), got %d", len(frames[2].Timings))
	}
	if advanceTotalRows(frames) != 15 {
		t.Fatalf("expected 15 total rows, got %d", advanceTotalRows(frames))
	}
}

// 4. Streaming and non-streaming paths produce identical row sets.
// Two parallel engines with the same source state + change batch — verify
// AdvanceStream's reassembled rows equal Advance's output (count + key set).
func TestAdvanceStream_OutputMatchesAdvance(t *testing.T) {
	withAdvanceChunkSize(t, 3) // small enough to force multi-frame output
	const pipelines = 2
	const changeCount = 10

	changes := make([]SnapshotChange, changeCount)
	for i := 0; i < changeCount; i++ {
		changes[i] = SnapshotChange{
			Table:     "items",
			NextValue: ivm.Row{"id": string(rune('a' + i)), "name": "n"},
		}
	}

	// Non-streaming path
	batchEng, _ := newAdvanceTestEngine(t, pipelines)
	batchResult := batchEng.Advance(changes)

	// Streaming path
	streamEng, _ := newAdvanceTestEngine(t, pipelines)
	frames := collectAdvanceStream(t, streamEng, changes)

	streamRows := advanceTotalRows(frames)
	batchRows := len(batchResult.Changes)

	if streamRows != batchRows {
		t.Fatalf("row count mismatch: stream=%d batch=%d", streamRows, batchRows)
	}
	if streamRows != changeCount*pipelines {
		t.Fatalf("expected %d rows (%d changes × %d pipelines), got %d",
			changeCount*pipelines, changeCount, pipelines, streamRows)
	}

	// Reassemble streamed rows + compare to batch by (queryID, rowKey) set.
	type key struct{ q, id string }
	streamKeys := make(map[key]int)
	for _, f := range frames {
		for _, rc := range f.Changes {
			id, _ := rc.RowKey["id"].(string)
			streamKeys[key{rc.QueryID, id}]++
		}
	}
	for _, rc := range batchResult.Changes {
		id, _ := rc.RowKey["id"].(string)
		k := key{rc.QueryID, id}
		if streamKeys[k] == 0 {
			t.Fatalf("batch row missing from stream: queryID=%s id=%s", rc.QueryID, id)
		}
		streamKeys[k]--
	}
	for k, n := range streamKeys {
		if n != 0 {
			t.Fatalf("stream had %d extra rows for %+v", n, k)
		}
	}

	// Final-frame timings count matches non-streaming timings count.
	final := frames[len(frames)-1]
	if len(final.Timings) != len(batchResult.Timings) {
		t.Fatalf("timing count: stream=%d batch=%d", len(final.Timings), len(batchResult.Timings))
	}
}
