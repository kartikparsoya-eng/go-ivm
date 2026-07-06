package engine

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Edge: the source-drift assert fires MID-BATCH, after earlier
// source-changes in the same advance already flushed partial frames to the
// wire. This pins the follow-TS disposition on the streaming path:
//   - the stream still terminates with exactly one Final frame (the C5
//     clean-wire invariant: a missing Final would trip the TS accumulator's
//     protocol-violation path, masking the real failure),
//   - the Final frame carries EMPTY changes (partial output is dropped —
//     the CG is being torn down; emitting a partial diff would desync the
//     CVR from Go's half-advanced state),
//   - chunkIndex stays contiguous across the pre-drift partials and the
//     terminal frame (the TS accumulator throws on gaps/reorders),
//   - the drift panic re-raises AFTER the Final flush so the sidecar
//     handler converts it to the RPC error TS classifies → teardown,
//   - the engine advances cleanly afterwards with no residue.
func TestAdvanceStream_DriftAfterFlushedChunks(t *testing.T) {
	saved := advanceChunkSize
	advanceChunkSize = 1 // every successful push flushes its own frame
	defer func() { advanceChunkSize = saved }()

	eng, _ := setupSimpleEngine(t)

	var frames []AdvanceStreamPartial
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = eng.AdvanceStream([]SnapshotChange{
			{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "A"}}, // ok → frame
			{Table: "users", NextValue: ivm.Row{"id": "u2", "name": "B"}}, // ok → frame
			{ // Edit of a row that doesn't exist → source-drift panic
				Table:      "users",
				PrevValues: []ivm.Row{{"id": "u9", "name": "ghost"}},
				NextValue:  ivm.Row{"id": "u9", "name": "boo"},
			},
		}, func(p AdvanceStreamPartial) {
			frames = append(frames, p)
		})
	}()
	if recovered == nil {
		t.Fatal("mid-batch drift must re-raise out of AdvanceStream after the Final flush")
	}
	err, ok := recovered.(error)
	if !ok || !strings.Contains(err.Error(), "source drift: table=users op=Edit") {
		t.Fatalf("expected the users/Edit source-drift panic, got %T: %v", recovered, recovered)
	}

	if len(frames) != 3 {
		t.Fatalf("want 3 frames (2 flushed partials + empty terminal), got %d: %+v", len(frames), frames)
	}
	finals := 0
	total := 0
	for i, f := range frames {
		if f.ChunkIndex != i {
			t.Fatalf("chunkIndex not contiguous: frame %d has ChunkIndex=%d", i, f.ChunkIndex)
		}
		if f.Final {
			finals++
			if len(f.Changes) != 0 {
				t.Fatalf("terminal frame after a drift panic must carry EMPTY changes, got %+v", f.Changes)
			}
		}
		total += len(f.Changes)
	}
	if finals != 1 {
		t.Fatalf("want exactly one Final frame, got %d", finals)
	}
	if last := frames[len(frames)-1]; !last.Final {
		t.Fatal("Final frame must be the LAST frame")
	}
	// u1 + u2 adds were flushed BEFORE the drift point (already on the
	// wire — nothing can recall them); the terminal frame adds nothing.
	if total != 2 {
		t.Fatalf("want exactly the 2 pre-drift adds across frames, got %d changes", total)
	}

	// Engine self-heals: the failed batch's dedup/batch state was cleared
	// via signalAdvanceEnd on the panic path, so a fresh advance is clean.
	var after []AdvanceStreamPartial
	if err := eng.AdvanceStream(
		[]SnapshotChange{{Table: "users", NextValue: ivm.Row{"id": "u3", "name": "C"}}},
		func(p AdvanceStreamPartial) { after = append(after, p) },
	); err != nil {
		t.Fatalf("follow-up advance failed: %v", err)
	}
	for _, f := range after {
		for _, rc := range f.Changes {
			if id, _ := rc.RowKey["id"].(string); id != "u3" {
				t.Fatalf("stale row leaked into the next advance: %+v", rc)
			}
		}
	}
}
