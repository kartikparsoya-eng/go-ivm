package engine

// Tests for AddQueriesStream chunked output (REVIEW perf-opt streaming).
// Validates the wire-level chunking contract:
//   - every query produces ≥1 partial frame
//   - exactly one frame per query has Final=true (the last)
//   - ChunkIndex is monotonically increasing per query starting at 0
//   - total RowChange count and ordering match the non-streaming path
//   - TimingMs is recorded on the final chunk only

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// withChunkSize temporarily overrides the package-level hydrateChunkSize and
// restores it on test cleanup. Lets tests exercise chunk-boundary cases with
// small payloads instead of allocating 10k+ rows per case.
func withChunkSize(t *testing.T, size int) {
	t.Helper()
	prev := hydrateChunkSize
	hydrateChunkSize = size
	t.Cleanup(func() { hydrateChunkSize = prev })
}

// newStreamingTestEngine builds an engine with a single "users" table
// populated with the given row count. Rows have predictable keys (id=0..n-1)
// so tests can assert ordering.
func newStreamingTestEngine(t *testing.T, rowCount int) (*Engine, *ivm.MemorySource) {
	t.Helper()
	storagePath := tempStoragePath(t)
	eng, err := NewEngine(EngineConfig{StoragePath: storagePath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	ms := ivm.NewMemorySource(
		"users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"},
	)
	rows := make([]ivm.Row, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = ivm.Row{
			"id":   fmt.Sprintf("u%06d", i),
			"name": fmt.Sprintf("User-%d", i),
		}
	}
	ms.BulkInsert(rows)
	eng.RegisterMemorySource(ms)

	return eng, ms
}

// collectStream runs AddQueriesStream and returns chunks in arrival order.
// Wraps the user callback under a mutex so concurrent goroutines (one per
// query) don't race the test's chunk slice.
func collectStream(t *testing.T, eng *Engine, queries []QuerySpec) []QueryResult {
	t.Helper()
	var mu sync.Mutex
	var chunks []QueryResult
	err := eng.AddQueriesStream(queries, func(r QueryResult) {
		// Defensive copy of Changes — the engine reuses backing arrays across
		// flushes; without this the test would see mutated chunks once the
		// next iteration appends.
		cp := make([]RowChange, len(r.Changes))
		copy(cp, r.Changes)
		r.Changes = cp
		mu.Lock()
		chunks = append(chunks, r)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("AddQueriesStream error: %v", err)
	}
	return chunks
}

// chunksFor filters arrival-order chunks for a specific queryID and asserts
// chunk-index monotonicity + exactly one Final=true.
func chunksFor(t *testing.T, all []QueryResult, queryID string) []QueryResult {
	t.Helper()
	var perQuery []QueryResult
	for _, c := range all {
		if c.QueryID == queryID {
			perQuery = append(perQuery, c)
		}
	}
	// Per-query chunks should arrive in ChunkIndex order because a single
	// query is hydrated by one goroutine that calls onResult synchronously.
	for i := 1; i < len(perQuery); i++ {
		if perQuery[i].ChunkIndex != perQuery[i-1].ChunkIndex+1 {
			t.Fatalf("queryID=%s chunk index gap: got %d after %d",
				queryID, perQuery[i].ChunkIndex, perQuery[i-1].ChunkIndex)
		}
	}
	// Exactly one Final.
	finals := 0
	finalAt := -1
	for i, c := range perQuery {
		if c.Final {
			finals++
			finalAt = i
		}
	}
	if finals != 1 {
		t.Fatalf("queryID=%s expected exactly 1 Final chunk, got %d", queryID, finals)
	}
	if finalAt != len(perQuery)-1 {
		t.Fatalf("queryID=%s Final must be on the last chunk; got at index %d of %d",
			queryID, finalAt, len(perQuery))
	}
	return perQuery
}

func totalRows(chunks []QueryResult) int {
	n := 0
	for _, c := range chunks {
		n += len(c.Changes)
	}
	return n
}

func simpleQuery(id string) QuerySpec {
	return QuerySpec{
		QueryID: id,
		AST: builder.AST{
			Table:   "users",
			OrderBy: ivm.Ordering{{"id", "asc"}},
		},
	}
}

// 1. Small result: one chunk with Final=true.
func TestAddQueriesStream_SmallResult_SingleChunk(t *testing.T) {
	withChunkSize(t, 100)
	eng, _ := newStreamingTestEngine(t, 5)

	chunks := collectStream(t, eng, []QuerySpec{simpleQuery("q1")})
	perQuery := chunksFor(t, chunks, "q1")

	if len(perQuery) != 1 {
		t.Fatalf("expected 1 chunk for small result, got %d", len(perQuery))
	}
	if perQuery[0].ChunkIndex != 0 {
		t.Fatalf("expected ChunkIndex=0, got %d", perQuery[0].ChunkIndex)
	}
	if !perQuery[0].Final {
		t.Fatalf("expected Final=true on sole chunk")
	}
	if len(perQuery[0].Changes) != 5 {
		t.Fatalf("expected 5 changes, got %d", len(perQuery[0].Changes))
	}
	if perQuery[0].TimingMs <= 0 {
		t.Fatalf("expected positive TimingMs on final chunk, got %v", perQuery[0].TimingMs)
	}
}

// 2. Empty result: one empty chunk with Final=true.
func TestAddQueriesStream_EmptyResult_SingleFinalChunk(t *testing.T) {
	withChunkSize(t, 100)
	eng, _ := newStreamingTestEngine(t, 0)

	chunks := collectStream(t, eng, []QuerySpec{simpleQuery("q1")})
	perQuery := chunksFor(t, chunks, "q1")

	if len(perQuery) != 1 {
		t.Fatalf("expected 1 chunk for empty result, got %d", len(perQuery))
	}
	if !perQuery[0].Final {
		t.Fatalf("expected Final=true on terminal chunk")
	}
	if len(perQuery[0].Changes) != 0 {
		t.Fatalf("expected 0 changes, got %d", len(perQuery[0].Changes))
	}
}

// 3. Exact multiple of chunk size: emits one full chunk + one empty terminal
// chunk. Document the intentional empty-terminal trade-off (one extra small
// frame per query that lands on the boundary, in exchange for a uniform
// invariant: every query ends with Final=true).
func TestAddQueriesStream_ExactBoundary_EmitsTerminalEmptyChunk(t *testing.T) {
	withChunkSize(t, 10)
	eng, _ := newStreamingTestEngine(t, 10) // exactly chunkSize rows

	chunks := collectStream(t, eng, []QuerySpec{simpleQuery("q1")})
	perQuery := chunksFor(t, chunks, "q1")

	if len(perQuery) != 2 {
		t.Fatalf("expected 2 chunks (full + empty terminal), got %d", len(perQuery))
	}
	if perQuery[0].Final || len(perQuery[0].Changes) != 10 {
		t.Fatalf("first chunk: expected 10 rows + Final=false, got rows=%d final=%v",
			len(perQuery[0].Changes), perQuery[0].Final)
	}
	if !perQuery[1].Final || len(perQuery[1].Changes) != 0 {
		t.Fatalf("terminal chunk: expected 0 rows + Final=true, got rows=%d final=%v",
			len(perQuery[1].Changes), perQuery[1].Final)
	}
	if perQuery[0].TimingMs != 0 {
		t.Fatalf("non-final chunk should have TimingMs=0, got %v", perQuery[0].TimingMs)
	}
	if perQuery[1].TimingMs <= 0 {
		t.Fatalf("final chunk should have positive TimingMs, got %v", perQuery[1].TimingMs)
	}
}

// 4. Just-over boundary: full chunk + tail chunk with Final=true.
func TestAddQueriesStream_JustOverBoundary(t *testing.T) {
	withChunkSize(t, 10)
	eng, _ := newStreamingTestEngine(t, 11)

	chunks := collectStream(t, eng, []QuerySpec{simpleQuery("q1")})
	perQuery := chunksFor(t, chunks, "q1")

	if len(perQuery) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(perQuery))
	}
	if perQuery[0].Final || len(perQuery[0].Changes) != 10 {
		t.Fatalf("first chunk: expected 10 + Final=false, got %d + final=%v",
			len(perQuery[0].Changes), perQuery[0].Final)
	}
	if !perQuery[1].Final || len(perQuery[1].Changes) != 1 {
		t.Fatalf("tail chunk: expected 1 + Final=true, got %d + final=%v",
			len(perQuery[1].Changes), perQuery[1].Final)
	}
}

// 5. Many chunks: 3 full + 1 partial.
func TestAddQueriesStream_ManyChunks_TotalRowsMatch(t *testing.T) {
	withChunkSize(t, 10)
	const totalN = 35
	eng, _ := newStreamingTestEngine(t, totalN)

	chunks := collectStream(t, eng, []QuerySpec{simpleQuery("q1")})
	perQuery := chunksFor(t, chunks, "q1")

	if len(perQuery) != 4 {
		t.Fatalf("expected 4 chunks (3 full + 1 tail), got %d", len(perQuery))
	}
	if totalRows(perQuery) != totalN {
		t.Fatalf("total rows mismatch: expected %d, got %d", totalN, totalRows(perQuery))
	}
	for i, c := range perQuery {
		if c.ChunkIndex != i {
			t.Fatalf("chunk[%d].ChunkIndex = %d, want %d", i, c.ChunkIndex, i)
		}
	}

	// Verify row ordering is preserved across chunks: rows should be
	// id="u000000" through "u000034" in order.
	seen := make([]string, 0, totalN)
	for _, c := range perQuery {
		for _, rc := range c.Changes {
			seen = append(seen, rc.RowKey["id"].(string))
		}
	}
	expected := make([]string, totalN)
	for i := 0; i < totalN; i++ {
		expected[i] = fmt.Sprintf("u%06d", i)
	}
	sort.Strings(expected) // MemorySource sorts by PK
	for i, got := range seen {
		if got != expected[i] {
			t.Fatalf("row order at %d: got %s, want %s", i, got, expected[i])
		}
	}
}

// 6. Multiple queries: per-query chunking is independent. Both queries
// against the same source should each produce their own chunk sequence
// with their own Final marker.
func TestAddQueriesStream_MultipleQueries_IndependentChunking(t *testing.T) {
	withChunkSize(t, 10)
	eng, _ := newStreamingTestEngine(t, 25)

	chunks := collectStream(t, eng, []QuerySpec{simpleQuery("q1"), simpleQuery("q2")})

	// Both queries see same source → both produce same total rows.
	for _, qid := range []string{"q1", "q2"} {
		perQuery := chunksFor(t, chunks, qid)
		if totalRows(perQuery) != 25 {
			t.Fatalf("queryID=%s: expected 25 rows, got %d", qid, totalRows(perQuery))
		}
		// 25 rows with chunk size 10 → 3 chunks (10, 10, 5)
		if len(perQuery) != 3 {
			t.Fatalf("queryID=%s: expected 3 chunks, got %d", qid, len(perQuery))
		}
		if !perQuery[2].Final {
			t.Fatalf("queryID=%s: expected Final=true on last chunk", qid)
		}
	}
}

// 7b. After Close, streaming methods return ErrEngineClosed instead of
// silently producing an empty result. Guards against the silent-success
// failure mode where the engine's maps are nil but the loop body skips
// every iteration.
func TestStreamingMethods_AfterClose_ReturnErrEngineClosed(t *testing.T) {
	withChunkSize(t, 100)
	eng, _ := newStreamingTestEngine(t, 3)

	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// AddQueriesStream
	err := eng.AddQueriesStream(
		[]QuerySpec{simpleQuery("q1")},
		func(QueryResult) { t.Fatalf("onResult should not be called after Close") },
	)
	if err != ErrEngineClosed {
		t.Fatalf("AddQueriesStream after Close: got %v, want ErrEngineClosed", err)
	}

	// AdvanceStream
	err = eng.AdvanceStream(
		[]SnapshotChange{{Table: "users", NextValue: ivm.Row{"id": "x", "name": "n"}}},
		func(AdvanceStreamPartial) { t.Fatalf("onResult should not be called after Close") },
	)
	if err != ErrEngineClosed {
		t.Fatalf("AdvanceStream after Close: got %v, want ErrEngineClosed", err)
	}
}

// 7. Total rows from streaming path equals non-streaming AddQueries path.
// Guards against the chunking loop dropping or duplicating rows.
func TestAddQueriesStream_TotalRowsMatchNonStreaming(t *testing.T) {
	withChunkSize(t, 7) // odd size to exercise misaligned chunks
	const totalN = 100

	// Streaming path
	streamEng, _ := newStreamingTestEngine(t, totalN)
	streamChunks := collectStream(t, streamEng, []QuerySpec{simpleQuery("q1")})
	streamRows := totalRows(chunksFor(t, streamChunks, "q1"))

	// Non-streaming path (uses same chunkSize var, but AddQueries doesn't chunk)
	batchEng, _ := newStreamingTestEngine(t, totalN)
	results, err := batchEng.AddQueries([]QuerySpec{simpleQuery("q1")})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 batch result, got %d", len(results))
	}
	if !results[0].Final {
		t.Fatalf("AddQueries result should have Final=true (single chunk)")
	}
	if results[0].ChunkIndex != 0 {
		t.Fatalf("AddQueries result should have ChunkIndex=0, got %d", results[0].ChunkIndex)
	}
	batchRows := len(results[0].Changes)

	if streamRows != batchRows {
		t.Fatalf("row count mismatch: stream=%d batch=%d", streamRows, batchRows)
	}
	if streamRows != totalN {
		t.Fatalf("expected %d rows, got %d", totalN, streamRows)
	}
}

// TestStreamingChunkSizeComparison measures the Go-side effect of chunk size
// on streaming granularity: frame count, time-to-first-chunk, and total time.
// Uses 1000 rows so chunk=100 produces 10+ frames while chunk=10000 produces
// 1 (the terminal frame). This isolates the Go emit side; it does NOT reflect
// client-visible latency, which is gated downstream by CURSOR_PAGE_SIZE in
// view-syncer.ts (the consumer batches every 10000 deduped rows before poking
// the WebSocket).
func TestStreamingChunkSizeComparison(t *testing.T) {
	const rowCount = 1000

	for _, size := range []int{100, 10000} {
		t.Run(fmt.Sprintf("chunk=%d", size), func(t *testing.T) {
			withChunkSize(t, size)
			eng, _ := newStreamingTestEngine(t, rowCount)

			var mu sync.Mutex
			var chunks []QueryResult
			var firstChunkAt time.Time
			start := time.Now()

			err := eng.AddQueriesStream([]QuerySpec{simpleQuery("q1")}, func(r QueryResult) {
				mu.Lock()
				if firstChunkAt.IsZero() {
					firstChunkAt = time.Now()
				}
				cp := make([]RowChange, len(r.Changes))
				copy(cp, r.Changes)
				r.Changes = cp
				chunks = append(chunks, r)
				mu.Unlock()
			})
			if err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(start)
			ttc := firstChunkAt.Sub(start)

			t.Logf("chunk=%-6d  frames=%-4d  rows=%-4d  time-to-first=%-12v  total=%v",
				size, len(chunks), totalRows(chunks), ttc, elapsed)

			if totalRows(chunks) != rowCount {
				t.Fatalf("row count mismatch: got %d, want %d", totalRows(chunks), rowCount)
			}
			if size == 100 && len(chunks) < 10 {
				t.Fatalf("chunk=100 expected >=10 frames for 1000 rows, got %d", len(chunks))
			}
			if size == 10000 && len(chunks) != 1 {
				t.Fatalf("chunk=10000 expected 1 frame for 1000 rows, got %d", len(chunks))
			}
		})
	}
}

// simulateHydratePipeline models the TWO-stage streaming pipeline end-to-end so
// we can predict client-visible latency without touching the live container.
//
// Stage 1 — Go sidecar (engine.go AddQueriesStream): fetches rowCount rows and
// emits a partial frame every goChunk rows (engine.go:901 `len(chunk) >=
// hydrateChunkSize`), plus a terminal frame for the remainder. Each row costs
// perRowFetchCost — this models the SQLite-cursor read + msgpack serialize that
// the in-memory MemorySource does NOT incur but dominates real hydrate. The
// cost is applied per-chunk (n*perRowFetchCost before the chunk is delivered),
// which is when that chunk becomes visible to TS.
//
// Stage 2 — TS view-syncer (#processChanges): accumulates rows in a dedup map
// and pokes the client via processBatch() every CURSOR_PAGE_SIZE NEW rows
// (view-syncer.ts:2324 `rows.size % CURSOR_PAGE_SIZE === 0`, where processBatch
// calls rows.clear() at :2281 so the counter resets each flush), plus a final
// flush at end-of-stream (:2328 `if (rows.size) processBatch()`). All rows are
// unique ADDs during hydrate, so the dedup map grows by exactly 1 per row.
//
// Returns time-to-first-client-poke and total wall time. The first poke is the
// real perceived-latency metric (progressive render); the per-poke work is
// modeled as instant, so the FIRST-flush comparison is the faithful signal and
// total slightly understates production (ignores updater.received + poke cost).
func simulateHydratePipeline(rowCount, goChunk, cursorPage int, perRowFetchCost time.Duration) (firstFlush, total time.Duration) {
	start := time.Now()
	chunks := make(chan int, 1024) // each value = row count in that delivered chunk

	// Stage 1: Go sidecar producer.
	go func() {
		defer close(chunks)
		remaining := rowCount
		for remaining > 0 {
			n := goChunk
			if n > remaining {
				n = remaining // terminal partial chunk
			}
			time.Sleep(time.Duration(n) * perRowFetchCost) // fetch+serialize n rows
			chunks <- n
			remaining -= n
		}
	}()

	// Stage 2: TS #processChanges consumer with CURSOR_PAGE_SIZE batching.
	var firstFlushAt time.Time
	pending := 0
	flush := func() {
		if firstFlushAt.IsZero() {
			firstFlushAt = time.Now()
		}
		pending = 0 // mirrors rows.clear() at view-syncer.ts:2281
	}
	for nRows := range chunks {
		for k := 0; k < nRows; k++ {
			pending++
			if pending%cursorPage == 0 { // view-syncer.ts:2324
				flush()
			}
		}
	}
	if pending > 0 {
		flush() // final flush, view-syncer.ts:2328
	}

	total = time.Since(start)
	firstFlush = firstFlushAt.Sub(start)
	return firstFlush, total
}

// TestClientFlushSimulation_BothLeversRequired proves the key finding: a
// client-visible streaming win (early first poke) needs BOTH the Go chunk size
// AND ZERO_CURSOR_PAGE_SIZE lowered. Lowering only one keeps time-to-first-row
// pinned at full-hydrate time. Models a 1000-row query at 50µs/row (~50ms
// hydrate — a realistic mid-size result).
func TestClientFlushSimulation_BothLeversRequired(t *testing.T) {
	const (
		rowCount        = 1000
		perRowFetchCost = 50 * time.Microsecond
	)

	type combo struct {
		name             string
		goChunk          int
		cursorPage       int
		expectEarlyFlush bool
	}
	combos := []combo{
		{"go=100   cursor=100  (both low)", 100, 100, true},
		{"go=100   cursor=10000 (TS coarse)", 100, 10000, false},
		{"go=10000 cursor=100  (Go coarse)", 10000, 100, false},
		{"go=10000 cursor=10000 (both high)", 10000, 10000, false},
	}

	t.Logf("rowCount=%d perRowFetchCost=%v (ideal full hydrate ~%v)",
		rowCount, perRowFetchCost, time.Duration(rowCount)*perRowFetchCost)

	var bothLowFirst time.Duration
	for _, c := range combos {
		firstFlush, total := simulateHydratePipeline(rowCount, c.goChunk, c.cursorPage, perRowFetchCost)
		pct := float64(firstFlush) / float64(total) * 100
		t.Logf("%-34s  first-client-poke=%-10v  total=%-10v  (first=%.0f%% of total)",
			c.name, firstFlush.Round(time.Millisecond), total.Round(time.Millisecond), pct)

		if c.expectEarlyFlush {
			bothLowFirst = firstFlush
			// First poke should land near the first Go chunk (~10% of total),
			// well before the halfway point. Generous bound to absorb sleep jitter.
			if firstFlush > total/2 {
				t.Errorf("%s: expected early first poke (<50%% of total), got %.0f%%", c.name, pct)
			}
		} else {
			// First poke pinned at end-of-hydrate: must be in the final 20%.
			if firstFlush < total*4/5 {
				t.Errorf("%s: expected late first poke (>80%% of total), got %.0f%%", c.name, pct)
			}
		}
	}

	// The both-low first poke must be dramatically earlier than full hydrate —
	// this is the entire perceived-latency case for fine-grained streaming.
	if bothLowFirst > time.Duration(rowCount)*perRowFetchCost/3 {
		t.Errorf("both-low first poke %v not meaningfully earlier than full hydrate %v",
			bothLowFirst, time.Duration(rowCount)*perRowFetchCost)
	}
}
