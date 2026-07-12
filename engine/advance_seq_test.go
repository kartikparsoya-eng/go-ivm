package engine

// Engine tests for AdvanceStreamChunkedSeq, which feeds the push loop
// from a lazy change sequence.
//
//   - parity: a seq-fed advance emits exactly the frames a slice-fed one does
//   - laziness: changes are pulled interleaved with emission (never
//     materialized up front)
//   - cursor error: a mid-seq error settles the stream as an ERROR — the
//     loop stops, no terminal Final frame is emitted (a half-applied diff
//     must never look complete), and the engine stays reusable

import (
	"errors"
	"fmt"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// seqAdvanceEngine builds an engine with an EMPTY users memory source and a
// registered query, so advance pushes produce RowChanges.
func seqAdvanceEngine(t *testing.T) *Engine {
	t.Helper()
	eng, _ := newStreamingTestEngine(t, 0)
	err := eng.AddQueriesStream([]QuerySpec{simpleQuery("q1")}, func(QueryResult) {})
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	return eng
}

func userAdd(i int) SnapshotChange {
	return SnapshotChange{
		Table:     "users",
		NextValue: ivm.Row{"id": fmt.Sprintf("u%04d", i), "name": fmt.Sprintf("N%d", i)},
	}
}

func TestAdvanceStreamSeq_MatchesSliceOutput(t *testing.T) {
	const n = 25
	changes := make([]SnapshotChange, n)
	for i := range changes {
		changes[i] = userAdd(i)
	}

	collect := func(run func(onResult func(AdvanceStreamPartial)) error) []AdvanceStreamPartial {
		var out []AdvanceStreamPartial
		if err := run(func(p AdvanceStreamPartial) {
			cp := make([]RowChange, len(p.Changes))
			copy(cp, p.Changes)
			p.Changes = cp
			out = append(out, p)
		}); err != nil {
			t.Fatalf("advance: %v", err)
		}
		return out
	}

	viaSlice := collect(func(onResult func(AdvanceStreamPartial)) error {
		return seqAdvanceEngine(t).AdvanceStreamChunked(changes, 7, onResult)
	})
	viaSeq := collect(func(onResult func(AdvanceStreamPartial)) error {
		return seqAdvanceEngine(t).AdvanceStreamChunkedSeq(func(yield func(SnapshotChange, error) bool) {
			for _, c := range changes {
				if !yield(c, nil) {
					return
				}
			}
		}, 7, onResult)
	})

	if len(viaSlice) != len(viaSeq) {
		t.Fatalf("frame counts differ: slice=%d seq=%d", len(viaSlice), len(viaSeq))
	}
	for i := range viaSlice {
		s, q := viaSlice[i], viaSeq[i]
		if s.ChunkIndex != q.ChunkIndex || s.Final != q.Final || len(s.Changes) != len(q.Changes) {
			t.Fatalf("frame %d differs: slice={idx:%d final:%v rows:%d} seq={idx:%d final:%v rows:%d}",
				i, s.ChunkIndex, s.Final, len(s.Changes), q.ChunkIndex, q.Final, len(q.Changes))
		}
		for j := range s.Changes {
			if s.Changes[j].Row["id"] != q.Changes[j].Row["id"] {
				t.Fatalf("frame %d row %d differs: %v vs %v", i, j, s.Changes[j].Row["id"], q.Changes[j].Row["id"])
			}
		}
	}
}

// TestAdvanceStreamSeq_LazyConsumption verifies the memory property: the
// seq is pulled interleaved with emission. At chunkSize=1 every applied
// change flushes before the next is pulled, so when partial N arrives, at
// most N+1 changes have been read — nothing materializes the diff up
// front.
func TestAdvanceStreamSeq_LazyConsumption(t *testing.T) {
	const n = 50
	eng := seqAdvanceEngine(t)

	pulled := 0
	seq := func(yield func(SnapshotChange, error) bool) {
		for i := 0; i < n; i++ {
			pulled++
			if !yield(userAdd(i), nil) {
				return
			}
		}
	}

	emitted := 0
	err := eng.AdvanceStreamChunkedSeq(seq, 1, func(p AdvanceStreamPartial) {
		if len(p.Changes) > 0 {
			emitted += len(p.Changes)
			if pulled > emitted+1 {
				t.Fatalf("seq over-pulled: %d changes read but only %d emitted (diff materialized ahead of the push loop)",
					pulled, emitted)
			}
		}
	})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if pulled != n || emitted != n {
		t.Fatalf("pulled=%d emitted=%d, want %d/%d", pulled, emitted, n, n)
	}
}

// TestAdvanceStreamSeq_CursorErrorNoFinal verifies the error contract: a
// mid-seq cursor error stops the loop, returns the error, and emits no
// terminal Final frame — the caller's rpcError is the stream terminal, so
// a half-applied diff can never settle as a clean advance. The engine
// remains reusable afterwards.
func TestAdvanceStreamSeq_CursorErrorNoFinal(t *testing.T) {
	eng := seqAdvanceEngine(t)
	cursorErr := errors.New("synthetic changelog read failure")

	var frames []AdvanceStreamPartial
	err := eng.AdvanceStreamChunkedSeq(func(yield func(SnapshotChange, error) bool) {
		if !yield(userAdd(0), nil) {
			return
		}
		if !yield(userAdd(1), nil) {
			return
		}
		yield(SnapshotChange{}, cursorErr)
	}, 1, func(p AdvanceStreamPartial) {
		frames = append(frames, p)
	})

	if !errors.Is(err, cursorErr) {
		t.Fatalf("err = %v, want the cursor error", err)
	}
	for _, f := range frames {
		if f.Final {
			t.Fatalf("terminal Final frame emitted despite cursor error (half-applied diff would look complete): %+v", f)
		}
	}
	// The two applied changes flushed as non-final partials (chunkSize=1).
	if len(frames) != 2 {
		t.Fatalf("emitted %d partial frames before the error, want 2", len(frames))
	}

	// Engine reusable: a follow-up slice advance completes with a Final.
	finals := 0
	if err := eng.AdvanceStream([]SnapshotChange{userAdd(2)}, func(p AdvanceStreamPartial) {
		if p.Final {
			finals++
		}
	}); err != nil {
		t.Fatalf("follow-up advance: %v", err)
	}
	if finals != 1 {
		t.Fatalf("follow-up advance finals = %d, want 1", finals)
	}
}
