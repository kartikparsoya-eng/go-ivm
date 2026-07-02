package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Edge: drift raised MID-BATCH, after earlier source-changes in the same
// advance already flushed partial frames to the wire. TestAdvanceStream_
// DriftRecovery only covers drift on the FIRST change (no frames flushed
// yet); this pins the harder contract:
//   - the stream still terminates with exactly one Final frame,
//   - Drift rides ONLY the Final frame,
//   - chunkIndex stays contiguous across the pre-drift partials and the
//     terminal drift frame (the TS accumulator throws on gaps/reorders),
//   - output produced by the successful pre-drift pushes is emitted (TS
//     emits its partial computation before the reset; go-ivm matches — the
//     drift signal then triggers a full re-hydrate which supersedes it),
//   - the engine advances cleanly afterwards with no residue.
func TestAdvanceStream_DriftAfterFlushedChunks(t *testing.T) {
	saved := advanceChunkSize
	advanceChunkSize = 1 // every successful push flushes its own frame
	defer func() { advanceChunkSize = saved }()

	eng, _ := setupSimpleEngine(t)

	var frames []AdvanceStreamPartial
	err := eng.AdvanceStream([]SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "A"}}, // ok → frame
		{Table: "users", NextValue: ivm.Row{"id": "u2", "name": "B"}}, // ok → frame
		{ // Edit of a row that doesn't exist → *ivm.DriftError
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u9", "name": "ghost"}},
			NextValue:  ivm.Row{"id": "u9", "name": "boo"},
		},
	}, func(p AdvanceStreamPartial) {
		frames = append(frames, p)
	})
	if err != nil {
		t.Fatalf("drift must be reported in-band on the Final frame, not as error: %v", err)
	}

	if len(frames) < 3 {
		t.Fatalf("want >=3 frames (2 flushed partials + terminal drift), got %d: %+v", len(frames), frames)
	}
	finals := 0
	total := 0
	for i, f := range frames {
		if f.ChunkIndex != i {
			t.Fatalf("chunkIndex not contiguous: frame %d has ChunkIndex=%d", i, f.ChunkIndex)
		}
		if f.Final {
			finals++
			if f.Drift == nil {
				t.Fatal("terminal frame must carry the Drift signal")
			}
			if f.Drift.Table != "users" || f.Drift.Op != "Edit" {
				t.Fatalf("unexpected drift payload: %+v", f.Drift)
			}
		} else if f.Drift != nil {
			t.Fatalf("Drift leaked onto a non-Final frame: %+v", f)
		}
		total += len(f.Changes)
	}
	if finals != 1 {
		t.Fatalf("want exactly one Final frame, got %d", finals)
	}
	if last := frames[len(frames)-1]; !last.Final {
		t.Fatal("Final frame must be the LAST frame")
	}
	// u1 + u2 adds were flushed before the drift; the ghost edit contributes
	// nothing. (TS discards + re-hydrates on drift, so over-emission here is
	// harmless — but under-emission would diverge from TS's partial-emit.)
	if total != 2 {
		t.Fatalf("want the 2 pre-drift adds across frames, got %d changes", total)
	}

	// Engine self-heals: the failed batch's dedup/batch state was cleared
	// via signalAdvanceEnd on the drift path, so a fresh advance is clean.
	var after []AdvanceStreamPartial
	if err := eng.AdvanceStream(
		[]SnapshotChange{{Table: "users", NextValue: ivm.Row{"id": "u3", "name": "C"}}},
		func(p AdvanceStreamPartial) { after = append(after, p) },
	); err != nil {
		t.Fatalf("follow-up advance failed: %v", err)
	}
	for _, f := range after {
		if f.Drift != nil {
			t.Fatalf("drift state leaked into the next advance: %v", f.Drift)
		}
		for _, rc := range f.Changes {
			if id, _ := rc.RowKey["id"].(string); id != "u3" {
				t.Fatalf("stale row leaked into the next advance: %+v", rc)
			}
		}
	}
}
