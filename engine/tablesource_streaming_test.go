package engine

// Streaming-protocol tests against TableSource (vs the MemorySource-backed
// engine_streaming_test.go / advance_streaming_test.go).
//
// Motivation: the production shadow-tablesrc path streams through TableSource,
// which reads SQLite directly. Our existing chunking tests cover the protocol
// invariants with MemorySource only; if TableSource's Source contract drifts
// (e.g., a Push that doesn't fan out, a Fetch that buffers unexpectedly), the
// MemorySource tests won't catch it.
//
// These tests:
//   - seed a real WAL-mode SQLite file with N typed rows
//   - open via tablesource.Open (the same path the sidecar takes)
//   - register the TableSource on an engine
//   - drive hydrate + advance with chunk size set small enough to force
//     multi-frame output
//   - assert chunk-protocol invariants (monotonic ChunkIndex, exactly one
//     Final=true per query / per call, total row counts) hold byte-identical
//     to the MemorySource-backed variants
//
// Helper reuse:
//   - withChunkSize / withAdvanceChunkSize from engine_streaming_test.go
//   - assertAdvanceFrameInvariants / advanceTotalRows from advance_streaming_test.go
//   - chunksFor / totalRows / collectStream from engine_streaming_test.go

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// seedTicketReplica creates a WAL-mode SQLite at the temp dir with a "tickets"
// table populated with rowCount rows. Schema mirrors what the soak driver
// would see — id (TEXT PK), title (TEXT), priority (TEXT), createdAt (INTEGER).
// Returns the file path; test reuses it via OpenOptions{}.
func seedTicketReplica(t *testing.T, rowCount int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("seed WAL: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE tickets (
		id        TEXT PRIMARY KEY,
		title     TEXT,
		priority  TEXT,
		createdAt INTEGER
	)`); err != nil {
		t.Fatalf("seed CREATE: %v", err)
	}
	// Bulk insert in one tx — fast even at rowCount=10k.
	tx, err := w.Begin()
	if err != nil {
		t.Fatalf("seed begin: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO tickets (id, title, priority, createdAt) VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("seed prepare: %v", err)
	}
	for i := 0; i < rowCount; i++ {
		if _, err := stmt.Exec(
			fmt.Sprintf("t%06d", i),
			fmt.Sprintf("Ticket-%d", i),
			"HIGH",
			int64(1700000000+i),
		); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	return path
}

func ticketSchema() map[string]sqlite.ColumnSchema {
	return map[string]sqlite.ColumnSchema{
		"id":        {Type: "string"},
		"title":     {Type: "string"},
		"priority":  {Type: "string"},
		"createdAt": {Type: "number"},
	}
}

// newTableSourceStreamingEngine: replica with rowCount rows + engine with the
// tablesource.Source registered. Returns (engine, db) so callers can fire
// extra INSERTs/UPDATEs for advance scenarios.
func newTableSourceStreamingEngine(t *testing.T, rowCount int) (*Engine, *sql.DB) {
	t.Helper()
	path := seedTicketReplica(t, rowCount)
	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("tablesource.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, "tickets", ticketSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("tablesource.New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	// Forward unique-key metadata so the scalar-subquery resolver has the
	// same shape as production (sidecar's handleInit sets this from TS).
	eng.SetTableUniqueKeys("tickets", [][]string{{"id"}})

	return eng, db
}

func ticketsAllQuery(queryID string) QuerySpec {
	return QuerySpec{
		QueryID: queryID,
		AST: builder.AST{
			Table:   "tickets",
			OrderBy: ivm.Ordering{{"id", "asc"}},
		},
	}
}

// 1. Hydrate with a result set larger than the chunk size produces multiple
//    monotonic frames terminating in Final=true. This is the case the soak
//    driver couldn't exercise (auth filter rendered every result empty).
func TestTableSourceStream_Hydrate_MultiChunk(t *testing.T) {
	withChunkSize(t, 100)
	const rowCount = 547 // not a multiple of chunkSize → tail chunk has 47

	eng, _ := newTableSourceStreamingEngine(t, rowCount)
	chunks := collectStream(t, eng, []QuerySpec{ticketsAllQuery("q-multichunk")})
	perQuery := chunksFor(t, chunks, "q-multichunk")

	// Expect ceil(547/100) = 6 chunks. Chunk sizes: 100, 100, 100, 100, 100, 47.
	if len(perQuery) != 6 {
		t.Fatalf("expected 6 chunks for 547 rows / chunkSize=100, got %d", len(perQuery))
	}
	expectedSizes := []int{100, 100, 100, 100, 100, 47}
	for i, want := range expectedSizes {
		if got := len(perQuery[i].Changes); got != want {
			t.Fatalf("chunk[%d] size: got %d, want %d", i, got, want)
		}
	}
	if !perQuery[5].Final {
		t.Fatalf("Final=true must be on the terminal chunk (index 5)")
	}
	if totalRows(perQuery) != rowCount {
		t.Fatalf("total rows mismatch: got %d, want %d", totalRows(perQuery), rowCount)
	}
	// Timings recorded only on the terminal frame.
	for i, c := range perQuery {
		if i < 5 && c.TimingMs > 0 {
			t.Fatalf("non-final chunk[%d] should have TimingMs=0, got %f", i, c.TimingMs)
		}
	}
	if perQuery[5].TimingMs <= 0 {
		t.Fatalf("terminal chunk should carry TimingMs > 0, got %f", perQuery[5].TimingMs)
	}
}

// 2. Small result fits in one chunk with Final=true. Confirms TableSource's
//    AddQueriesStream behavior matches the MemorySource happy path.
func TestTableSourceStream_Hydrate_SmallResult_SingleChunk(t *testing.T) {
	withChunkSize(t, 100)
	eng, _ := newTableSourceStreamingEngine(t, 5)

	chunks := collectStream(t, eng, []QuerySpec{ticketsAllQuery("q-small")})
	perQuery := chunksFor(t, chunks, "q-small")

	if len(perQuery) != 1 {
		t.Fatalf("expected 1 chunk for 5 rows, got %d", len(perQuery))
	}
	if !perQuery[0].Final {
		t.Fatalf("sole chunk must have Final=true")
	}
	if len(perQuery[0].Changes) != 5 {
		t.Fatalf("expected 5 changes, got %d", len(perQuery[0].Changes))
	}
}

// 3. AddQueriesStream of two independent queries — each gets its own chunk
//    sequence with its own monotonic ChunkIndex starting at 0, and a Final
//    boundary per query. Validates that chunkFor's per-queryID partitioning
//    holds when TableSource backs the source.
func TestTableSourceStream_Hydrate_TwoQueries_IndependentChunking(t *testing.T) {
	withChunkSize(t, 50)
	eng, _ := newTableSourceStreamingEngine(t, 120)

	chunks := collectStream(t, eng, []QuerySpec{
		ticketsAllQuery("q-A"),
		ticketsAllQuery("q-B"),
	})

	aChunks := chunksFor(t, chunks, "q-A")
	bChunks := chunksFor(t, chunks, "q-B")

	// 120 / 50 = 2 full + 1 tail of 20 → 3 chunks each.
	for _, q := range [][]QueryResult{aChunks, bChunks} {
		if len(q) != 3 {
			t.Fatalf("each query expects 3 chunks (120/50 + tail), got %d", len(q))
		}
		if !q[2].Final {
			t.Fatalf("terminal chunk must have Final=true")
		}
		if totalRows(q) != 120 {
			t.Fatalf("expected 120 rows per query, got %d", totalRows(q))
		}
	}
}

// 4. AdvanceStream against TableSource: prev row + next row diffs fan through
//    Source.Push into multiple connections, producing N*pipelines RowChanges.
//    Crosses the advance chunk boundary at 100; we expect at least 2 frames
//    with monotonic ChunkIndex and Final=true on the last.
//
//    Note: each Push fans the SAME change to every connection; with 3 active
//    connections, one source-change adds 3 RowChanges to the pending buffer.
//    50 source changes → 150 RowChanges → with advanceChunkSize=100 we see
//    2 frames (one mid-stream flush at the boundary, one terminal flush).
func TestTableSourceStream_Advance_MultiChunk(t *testing.T) {
	withAdvanceChunkSize(t, 100)
	eng, _ := newTableSourceStreamingEngine(t, 0) // empty replica is fine for advance

	// Register 3 pipelines (queries) against the tickets table — each
	// connection will see every SnapshotChange and emit one RowChange per
	// source change. 3 pipelines × 50 changes = 150 RowChanges expected.
	for i := 0; i < 3; i++ {
		queryID := fmt.Sprintf("q-adv-%d", i)
		if _, _, err := eng.AddQuery(queryID, builder.AST{
			Table:   "tickets",
			OrderBy: ivm.Ordering{{"id", "asc"}},
		}); err != nil {
			t.Fatalf("AddQuery %s: %v", queryID, err)
		}
	}

	changes := make([]SnapshotChange, 50)
	for i := 0; i < 50; i++ {
		changes[i] = SnapshotChange{
			Table: "tickets",
			NextValue: ivm.Row{
				"id":        fmt.Sprintf("new-%03d", i),
				"title":     fmt.Sprintf("New-%d", i),
				"priority":  "HIGH",
				"createdAt": int64(1800000000 + i),
			},
		}
	}

	frames := collectAdvanceStream(t, eng, changes)
	assertAdvanceFrameInvariants(t, frames)

	if got := advanceTotalRows(frames); got != 150 {
		t.Fatalf("expected 150 RowChanges (50 changes × 3 pipelines), got %d", got)
	}
	if len(frames) < 2 {
		t.Fatalf("expected ≥2 frames to cross chunkSize=100, got %d", len(frames))
	}
	if !frames[len(frames)-1].Final {
		t.Fatalf("Final=true must be on terminal frame")
	}
	// Sanity: timings live only on the terminal frame (already covered by
	// assertAdvanceFrameInvariants — repeating for clarity).
	if len(frames[len(frames)-1].Timings) != 50 {
		t.Fatalf("expected 50 timing entries on terminal frame (one per source change), got %d",
			len(frames[len(frames)-1].Timings))
	}
}

// 5. End-to-end: hydrate produces a large multi-chunk result AND a subsequent
//    advance produces a multi-chunk result on the same engine. Validates the
//    state machine isn't disturbed by mid-test chunk-boundary crossings.
func TestTableSourceStream_HydrateThenAdvance_BothChunked(t *testing.T) {
	withChunkSize(t, 50)
	withAdvanceChunkSize(t, 40)
	const seedRows = 75

	eng, _ := newTableSourceStreamingEngine(t, seedRows)

	// Hydrate: 75 / 50 = 2 chunks (50 + 25 + Final).
	chunks := collectStream(t, eng, []QuerySpec{ticketsAllQuery("q-htA")})
	perQuery := chunksFor(t, chunks, "q-htA")
	if len(perQuery) != 2 {
		t.Fatalf("hydrate expected 2 chunks, got %d", len(perQuery))
	}
	if totalRows(perQuery) != seedRows {
		t.Fatalf("hydrate row count: got %d, want %d", totalRows(perQuery), seedRows)
	}

	// Advance: 1 pipeline × 50 changes = 50 RowChanges → with chunkSize=40
	// we get 2 frames (40 + 10 + Final).
	changes := make([]SnapshotChange, 50)
	for i := 0; i < 50; i++ {
		changes[i] = SnapshotChange{
			Table: "tickets",
			NextValue: ivm.Row{
				"id":        fmt.Sprintf("new-%03d", i),
				"title":     fmt.Sprintf("New-%d", i),
				"priority":  "HIGH",
				"createdAt": int64(1800000000 + i),
			},
		}
	}
	frames := collectAdvanceStream(t, eng, changes)
	assertAdvanceFrameInvariants(t, frames)
	if advanceTotalRows(frames) != 50 {
		t.Fatalf("advance row count: got %d, want 50", advanceTotalRows(frames))
	}
	if len(frames) < 2 {
		t.Fatalf("advance expected ≥2 frames to cross chunkSize=40, got %d", len(frames))
	}
}
