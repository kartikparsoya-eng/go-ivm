package engine

import (
	"sync"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestStreamer_PreservesIntraCallOrder is D8 contract part 1:
// changes within a single Accumulate call appear in Stream() in their
// input slice order. This is the basis for downstream-sorting comparators
// (TS shadow-compare) being able to assume per-call order.
func TestStreamer_PreservesIntraCallOrder(t *testing.T) {
	s := NewStreamer()
	schema := &ivm.SourceSchema{
		TableName:  "t",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string"},
	}
	// Three changes, distinct PKs so we can identify each post-stream.
	changes := []ivm.Change{
		ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": "a"}}),
		ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": "b"}}),
		ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": "c"}}),
	}
	s.Accumulate("q", schema, changes)
	result := s.Stream()
	if len(result) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(result))
	}
	wantOrder := []string{"a", "b", "c"}
	for i, want := range wantOrder {
		got := result[i].RowKey["id"]
		if got != want {
			t.Fatalf("change %d: want id=%s, got %v", i, want, got)
		}
	}
}

// TestStreamer_AccumulateConcurrentNoLoss is D8 contract part 2:
// under N concurrent Accumulate goroutines, Stream() emits exactly the
// union of inputs (no dropped or duplicated changes). Cross-queryID order
// in the output is intentionally not asserted — callers requiring
// determinism must sort downstream.
func TestStreamer_AccumulateConcurrentNoLoss(t *testing.T) {
	s := NewStreamer()
	schema := &ivm.SourceSchema{
		TableName:  "t",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string"},
	}
	const goroutines = 32
	const perGoroutine = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := pkID(gid, i)
				s.Accumulate("q", schema, []ivm.Change{
					ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": id}}),
				})
			}
		}(g)
	}
	wg.Wait()
	out := s.Stream()
	want := goroutines * perGoroutine
	if len(out) != want {
		t.Fatalf("expected %d changes, got %d (concurrent accumulate lost or doubled changes)", want, len(out))
	}
	// Membership check: every (gid,i) must appear exactly once.
	seen := make(map[string]int, want)
	for _, c := range out {
		id := c.RowKey["id"].(string)
		seen[id]++
	}
	if len(seen) != want {
		t.Fatalf("expected %d distinct ids, got %d (duplicates: see counts)", want, len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("id %s appeared %d times (expected 1)", id, count)
		}
	}
}

// TestStreamer_StreamResetsBuffer is D8 contract part 3: after Stream()
// returns, the internal buffer is cleared. The next Accumulate starts
// against a fresh slice — a one-time large advance doesn't permanently
// retain memory (the comment in streamer.go calls this out).
func TestStreamer_StreamResetsBuffer(t *testing.T) {
	s := NewStreamer()
	schema := &ivm.SourceSchema{
		TableName:  "t",
		PrimaryKey: []string{"id"},
		Columns:    map[string]string{"id": "string"},
	}
	s.Accumulate("q", schema, []ivm.Change{
		ivm.MakeAddChange(ivm.Node{Row: ivm.Row{"id": "x"}}),
	})
	first := s.Stream()
	if len(first) != 1 {
		t.Fatalf("first stream: want 1 change, got %d", len(first))
	}
	second := s.Stream()
	if len(second) != 0 {
		t.Fatalf("second stream: want 0 changes (buffer cleared), got %d", len(second))
	}
}

func pkID(gid, i int) string {
	// Compact unique id without importing fmt — keeps allocations bounded
	// when the test runs under -race with many iterations.
	const hex = "0123456789abcdef"
	var b [10]byte
	b[0] = hex[(gid>>4)&0xf]
	b[1] = hex[gid&0xf]
	b[2] = '-'
	b[3] = hex[(i>>12)&0xf]
	b[4] = hex[(i>>8)&0xf]
	b[5] = hex[(i>>4)&0xf]
	b[6] = hex[i&0xf]
	return string(b[:7])
}
