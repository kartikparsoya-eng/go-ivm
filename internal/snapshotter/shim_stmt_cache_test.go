package snapshotter

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
)

// withShim turns the C step-rows shim on for the duration of a test and
// restores the prior value. The snapshotter's GetRow/GetRows dispatch to
// StepRowsShimCached (populating the Snapshot's rawStmts cache) only when this
// flag is set.
func withShim(t *testing.T) {
	t.Helper()
	prev := tablesource.UseStepRowsShim
	tablesource.UseStepRowsShim = true
	t.Cleanup(func() { tablesource.UseStepRowsShim = prev })
}

// TestShimStmtCache_ReuseAndSurvivesRepin verifies the raw-stmt cache is
// populated by shim reads, reused (bounded — not one entry per call), and
// survives a resetToHead re-pin (prepared statements are conn-scoped, so they
// outlive ROLLBACK+BEGIN and read the new frame on reuse).
func TestShimStmtCache_ReuseAndSurvivesRepin(t *testing.T) {
	withShim(t)
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// V2: a change → diff read pulls row contents through the shim.
	f.upsertIssue("2", "two", "bob", 2, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"2"}`)
	f.setStateVersion(ver(2))
	d1, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("first Advance: %v", err)
	}
	if _, err := d1.Collect(); err != nil {
		t.Fatalf("first Collect: %v", err)
	}

	// After the diff read, prev (the snapshot the diff read row contents from)
	// holds a populated raw cache.
	prevSnap := f.snap.prev
	if prevSnap == nil || len(prevSnap.rawStmts) == 0 {
		t.Fatalf("expected raw stmt cache populated after shim read, got %v", prevSnap)
	}
	n1 := len(prevSnap.rawStmts)

	// A second advance re-pins prev (ROLLBACK+BEGIN) into the next curr. Read
	// again through the shim: the cache must be REUSED (bounded), not grow
	// per-call, and the stmts must still work against the new frame.
	f.upsertIssue("3", "three", "carol", 3, ver(3))
	f.logSet(ver(3), 0, "issue", `{"id":"3"}`)
	f.setStateVersion(ver(3))
	d2, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("second Advance: %v", err)
	}
	changes, err := d2.Collect()
	if err != nil {
		t.Fatalf("second Collect (reused cache after re-pin): %v", err)
	}
	if len(changes) != 1 || changes[0].RowKey["id"] != "3" {
		t.Fatalf("second diff wrong: want 1 change id=3, got %+v", changes)
	}
	// Bounded: distinct query SHAPES per table, not one entry per read.
	if got := len(f.snap.prev.rawStmts); got > n1+2 {
		t.Errorf("raw cache grew unexpectedly (reuse broken): first=%d now=%d", n1, got)
	}
}

// TestShimStmtCache_FinalizeBeforeClose is the gotcha-#2 regression guard: the
// raw driver stmts MUST be finalized BEFORE the connection is closed
// (finalizing a stmt after its conn is gone is a use-after-free). Populate the
// cache, capture the Snapshot, Destroy (→ close() → finalizeAllStmts →
// conn.Close), and assert the cache was drained with no crash. Run under -race;
// a reordering (finalize after close) races/UAFs here.
func TestShimStmtCache_FinalizeBeforeClose(t *testing.T) {
	withShim(t)
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.upsertIssue("2", "two", "bob", 2, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"2"}`)
	f.setStateVersion(ver(2))
	d, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if _, err := d.Collect(); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	// Capture both snapshots BEFORE Destroy nils them, so we can inspect their
	// caches afterward.
	curr, prev := f.snap.curr, f.snap.prev
	populated := (curr != nil && len(curr.rawStmts) > 0) ||
		(prev != nil && len(prev.rawStmts) > 0)
	if !populated {
		t.Fatal("expected at least one snapshot's raw cache populated before Destroy")
	}

	// close() must finalizeAllStmts BEFORE conn.Close — no UAF, cache drained.
	f.snap.Destroy()

	if curr != nil && len(curr.rawStmts) != 0 {
		t.Errorf("curr raw cache not drained on Destroy: %d entries", len(curr.rawStmts))
	}
	if prev != nil && len(prev.rawStmts) != 0 {
		t.Errorf("prev raw cache not drained on Destroy: %d entries", len(prev.rawStmts))
	}
}
