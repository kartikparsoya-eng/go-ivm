package engine

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestAdvanceStreamChunked_CrossPushWireOrder is the regression test for
// scale-review C1: cross-push row-order inversion in advanceStreamChunked.
//
// Mechanism under test: push N's sub-threshold output sits buffered in
// `pending` (not flushed — below chunkSize); push N+1's fan-out crosses the
// chunk threshold mid-flatten, and the streamer's chunkSink flushes those
// full chunks DIRECTLY to the wire. Pre-fix that bypassed `pending`, so push
// N+1's rows arrived on the wire BEFORE push N's — chunkIndex stayed
// monotonic, so nothing downstream could detect the inversion.
//
// The scenario drives the user-visible corruption on ONE row:
//
//	change 1: ADD posts row "p-new" under u1   → 1 RowChange → pending
//	change 2: REMOVE u1 (500+ child fan-out)   → chunkSink frames
//
// "p-new" sorts FIRST among u1's children ('-' < '0'), so remove(p-new) rides
// the very first mid-flatten chunk while add(p-new) is still buffered.
// Pre-fix wire order: remove(p-new) … add(p-new) — a client applying that
// keeps p-new alive under a parent that no longer exists (phantom row); the
// mirror case (remove buffered, add chunk-flushed) permanently deletes a live
// row. Post-fix, the chunkSink drains `pending` before emitting any chunk, so
// add(p-new) precedes remove(p-new).
func TestAdvanceStreamChunked_CrossPushWireOrder(t *testing.T) {
	const childCount = 500
	const chunkSize = 50

	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"}, []string{"id"})
	posts := ivm.NewMemorySource("posts",
		map[string]string{"id": "string", "userId": "string"}, []string{"id"})
	seed := make([]ivm.Row, childCount)
	for i := range seed {
		seed[i] = ivm.Row{"id": fmt.Sprintf("p%06d", i), "userId": "u1"}
	}
	posts.BulkInsert(seed)
	// u1 IS seeded (unlike newFanoutEngine): the advance below removes it.
	users.BulkInsert([]ivm.Row{{"id": "u1", "name": "Alice"}})

	eng, err := NewEngine(EngineConfig{StoragePath: filepath.Join(t.TempDir(), "storage.db")})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(posts)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetTableUniqueKeys("posts", [][]string{{"id"}})
	if _, _, err := eng.AddQuery("q-order", fanoutRelatedAST()); err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	// Collect every row in wire-arrival order. Copy via append: frames may
	// reuse their backing array after onResult returns (T1-5 invariant).
	var wire []RowChange
	frames := 0
	err = eng.AdvanceStreamChunked(
		[]SnapshotChange{
			// push 1: sub-threshold — 1 row, parks in pending.
			{Table: "posts", NextValue: ivm.Row{"id": "p-new", "userId": "u1"}},
			// push 2: remove u1 → fan-out of u1 + 501 children (incl. p-new)
			// → ~10 full chunks flushed mid-flatten via the chunkSink.
			{Table: "users", PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}}},
		},
		chunkSize,
		func(p AdvanceStreamPartial) {
			frames++
			wire = append(wire, p.Changes...)
		},
	)
	if err != nil {
		t.Fatalf("AdvanceStreamChunked: %v", err)
	}

	// Sanity: the scenario actually produced mid-flatten chunk frames (502
	// remove rows at chunk=50 → ≥10 chunk frames + final). Without this the
	// test could silently stop exercising the inversion window.
	if frames < 10 {
		t.Fatalf("scenario regressed: only %d frames — chunkSink did not fire mid-flatten", frames)
	}
	// No loss: add(p-new) + (u1 + 501 children) removes.
	if len(wire) != 1+childCount+2 {
		t.Fatalf("row total = %d, want %d", len(wire), 1+childCount+2)
	}

	addIdx, removeIdx := -1, -1
	for i, rc := range wire {
		if rc.Table != "posts" {
			continue
		}
		if id, _ := rc.RowKey["id"].(string); id != "p-new" {
			continue
		}
		switch rc.Type {
		case RowChangeAdd:
			if addIdx == -1 {
				addIdx = i
			}
		case RowChangeRemove:
			if removeIdx == -1 {
				removeIdx = i
			}
		}
	}
	if addIdx == -1 || removeIdx == -1 {
		t.Fatalf("p-new missing from wire: addIdx=%d removeIdx=%d", addIdx, removeIdx)
	}
	// THE C1 assertion: push order == wire order. add(p-new) came from push 1,
	// remove(p-new) from push 2; a client applying remove-then-add resurrects
	// a row whose parent was just removed (or, mirrored, loses a live row).
	if addIdx >= removeIdx {
		t.Fatalf("cross-push inversion: add(p-new) at wire index %d arrived AFTER remove(p-new) at %d — "+
			"push N+1's chunk frames overtook push N's buffered residual", addIdx, removeIdx)
	}
	t.Logf("wire order OK: add(p-new)@%d < remove(p-new)@%d across %d frames", addIdx, removeIdx, frames)
}
