package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Edge: the onResult sink passed to AdvanceStream is caller-supplied and may
// panic (e.g. a wire-write failure surfacing as panic). Pre-fix, sendFrame
// held flushMu across the onResult call WITHOUT a deferred unlock, so a
// panicking sink left flushMu locked; the recover captured the panic as
// nonDriftPanic and the guaranteed terminal flush(true) then deadlocked on
// flushMu.Lock() — wedging the engine with e.mu held (every CG sharing the
// engine blocks behind it). This test drives that exact sequence and fails
// by timeout if the deadlock ever comes back.
//
// Post-fix contract: the sink panic escapes AdvanceStream (the sidecar's
// handleStreamWithRecover converts it to an RPC error), e.mu and the chunk
// sink are released by defers, and the engine stays fully usable.
func TestAdvanceStream_PanickingSink_NoDeadlockAndEngineReusable(t *testing.T) {
	saved := advanceChunkSize
	advanceChunkSize = 1 // force a mid-loop flush(false) so the panic happens
	defer func() { advanceChunkSize = saved }()

	eng, _ := setupSimpleEngine(t)

	done := make(chan any, 1)
	go func() {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_ = eng.AdvanceStream(
				[]SnapshotChange{
					{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "A"}},
					{Table: "users", NextValue: ivm.Row{"id": "u2", "name": "B"}},
				},
				func(AdvanceStreamPartial) {
					panic("sink write failed")
				},
			)
		}()
		done <- recovered
	}()

	select {
	case recovered := <-done:
		if recovered == nil {
			t.Fatal("expected the sink panic to escape AdvanceStream, got clean return")
		}
		if s, ok := recovered.(string); !ok || !strings.Contains(s, "sink write failed") {
			t.Fatalf("expected the original sink panic value, got %v", recovered)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AdvanceStream deadlocked on flushMu after sink panic (pre-fix bug)")
	}

	// Engine must be reusable: e.mu released, chunk sink cleared, streamer
	// drained. The aborted advance's u1 add was dropped mid-flight, so drift
	// validation state depends on how far the loop got — use fresh rows.
	var frames []AdvanceStreamPartial
	err := eng.AdvanceStream(
		[]SnapshotChange{{Table: "users", NextValue: ivm.Row{"id": "u3", "name": "C"}}},
		func(p AdvanceStreamPartial) { frames = append(frames, p) },
	)
	if err != nil {
		t.Fatalf("engine not reusable after sink panic: %v", err)
	}
	finals := 0
	total := 0
	for _, f := range frames {
		if f.Final {
			finals++
		}
		total += len(f.Changes)
		if f.Drift != nil {
			t.Fatalf("unexpected drift on the follow-up advance: %v", f.Drift)
		}
		for _, rc := range f.Changes {
			if id, _ := rc.RowKey["id"].(string); id != "u3" {
				t.Fatalf("stale row from the aborted advance leaked into the next one: %+v", rc)
			}
		}
	}
	if finals != 1 {
		t.Fatalf("want exactly 1 Final frame on follow-up advance, got %d", finals)
	}
	if total != 1 {
		t.Fatalf("want exactly the u3 add on follow-up advance, got %d changes", total)
	}
}
