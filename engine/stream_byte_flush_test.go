package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Edge: BYTE-triggered chunk flushing. All existing streaming tests cross the
// ROW-count threshold; softChunkBytes had zero coverage. Fat rows (json blobs,
// long text) blow the 64MB wire frame long before 10k rows, so the byte lever
// is the one that actually protects prod against the addQuery fat-frame
// freeze (frame > maxFrameSize → TS reader skips it → 60s orphan timeout).
// These tests pin that a handful of fat rows still splits into multiple
// frames on every streaming surface: hydrate, advance main-loop, and the
// operator-level chunk sink inside a single source-change fan-out.

const fatPayloadBytes = 4 * 1024

func fatValue() string {
	return strings.Repeat("x", fatPayloadBytes)
}

func newFatRowEngine(t *testing.T, rowCount int) *Engine {
	t.Helper()
	users := ivm.NewMemorySource(
		"users",
		map[string]string{"id": "string", "blob": "string"},
		[]string{"id"},
	)
	rows := make([]ivm.Row, rowCount)
	for i := range rows {
		rows[i] = ivm.Row{"id": fmt.Sprintf("u%03d", i), "blob": fatValue()}
	}
	users.BulkInsert(rows)

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)
	return eng
}

func TestAddQueriesStream_FatRows_ByteFlush(t *testing.T) {
	savedBytes := softChunkBytes
	softChunkBytes = 10 * 1024 // ~2 fat rows per frame; row limit (10k) never reached
	defer func() { softChunkBytes = savedBytes }()

	const rowCount = 10
	eng := newFatRowEngine(t, rowCount)

	frames := 0
	total := 0
	finalSeen := false
	err := eng.AddQueriesStream([]QuerySpec{{
		QueryID: "q1",
		AST:     builder.AST{Table: "users", OrderBy: ivm.Ordering{{"id", "asc"}}},
	}}, func(r QueryResult) {
		frames++
		total += len(r.Changes)
		if r.Final {
			finalSeen = true
		}
	})
	if err != nil {
		t.Fatalf("AddQueriesStream: %v", err)
	}
	if !finalSeen {
		t.Fatal("no Final frame")
	}
	if total != rowCount {
		t.Fatalf("row total across frames = %d, want %d", total, rowCount)
	}
	// 10 rows × ~4KB at a 10KB byte limit → ≥3 frames. One frame means the
	// byte lever never fired (only the 10k row-count lever exists).
	if frames < 3 {
		t.Fatalf("byte-based flush did not split: %d frame(s) for %d fat rows", frames, total)
	}
}

func TestAdvanceStream_FatRows_ByteFlush(t *testing.T) {
	savedBytes := softChunkBytes
	softChunkBytes = 10 * 1024
	defer func() { softChunkBytes = savedBytes }()

	eng := newFatRowEngine(t, 0)
	if _, _, err := eng.AddQuery("q1", builder.AST{
		Table: "users", OrderBy: ivm.Ordering{{"id", "asc"}},
	}); err != nil {
		t.Fatal(err)
	}

	const changeCount = 10
	changes := make([]SnapshotChange, changeCount)
	for i := range changes {
		changes[i] = SnapshotChange{
			Table:     "users",
			NextValue: ivm.Row{"id": fmt.Sprintf("n%03d", i), "blob": fatValue()},
		}
	}

	frames := 0
	total := 0
	finals := 0
	if err := eng.AdvanceStream(changes, func(p AdvanceStreamPartial) {
		frames++
		total += len(p.Changes)
		if p.Final {
			finals++
		}
	}); err != nil {
		t.Fatalf("AdvanceStream: %v", err)
	}
	if finals != 1 {
		t.Fatalf("want exactly 1 Final frame, got %d", finals)
	}
	if total != changeCount {
		t.Fatalf("row total across frames = %d, want %d", total, changeCount)
	}
	if frames < 3 {
		t.Fatalf("advance byte-based flush did not split: %d frame(s) for %d fat rows", frames, total)
	}
}

// Byte lever inside the operator-level chunk sink (Win 2): ONE source-change
// whose fan-out rows are fat must flush mid-flatten on BYTES, not only on the
// row count. Uses the fan-out fixture from the operator-chunking test with a
// row limit high enough that only the byte limit can split.
func TestAdvanceStream_OperatorChunkSink_ByteFlush(t *testing.T) {
	savedBytes := softChunkBytes
	softChunkBytes = 64 * 1024
	defer func() { softChunkBytes = savedBytes }()
	savedChunk := advanceChunkSize
	advanceChunkSize = 1_000_000 // row lever effectively off
	defer func() { advanceChunkSize = savedChunk }()

	const childCount = 200
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"}, []string{"id"})
	posts := ivm.NewMemorySource("posts",
		map[string]string{"id": "string", "userId": "string", "blob": "string"}, []string{"id"})
	seed := make([]ivm.Row, childCount)
	for i := range seed {
		seed[i] = ivm.Row{"id": fmt.Sprintf("p%06d", i), "userId": "u1", "blob": fatValue()}
	}
	posts.BulkInsert(seed)

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(posts)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetTableUniqueKeys("posts", [][]string{{"id"}})
	if _, _, err := eng.AddQuery("q1", fanoutRelatedAST()); err != nil {
		t.Fatal(err)
	}

	frames := 0
	total := 0
	if err := eng.AdvanceStream(
		[]SnapshotChange{{Table: "users", NextValue: u1Row}},
		func(p AdvanceStreamPartial) {
			frames++
			total += len(p.Changes)
		},
	); err != nil {
		t.Fatalf("AdvanceStream: %v", err)
	}
	if total != childCount+1 {
		t.Fatalf("row total = %d, want %d", total, childCount+1)
	}
	// 201 rows × ~4KB ≈ 800KB at a 64KB byte limit → ≥10 frames if the sink's
	// byte lever works; 1 frame if only the (disabled) row lever exists.
	if frames < 5 {
		t.Fatalf("operator chunk-sink byte flush did not split: %d frame(s) for %d fat rows",
			frames, total)
	}
}
