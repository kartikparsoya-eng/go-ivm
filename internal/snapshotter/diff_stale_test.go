package snapshotter

import (
	"errors"
	"testing"
)

// TestDiff_StaleAfterNextAdvance verifies the deterministic stale-diff guard:
// a Diff retained across a subsequent Advance is rejected at Each/Collect
// entry with *InvalidDiffError, before iterating any row. Advance reuses the
// prev connection and re-pins it, so iterating an old diff would silently read
// the wrong frame — the guard turns that latent misuse into a loud, typed error.
func TestDiff_StaleAfterNextAdvance(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// V2: one change.
	f.upsertIssue("2", "two", "bob", 2, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"2"}`)
	f.setStateVersion(ver(2))

	staleDiff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("first Advance: %v", err)
	}

	// V3: another change. This second Advance re-pins the reused prev conn,
	// invalidating staleDiff.
	f.upsertIssue("3", "three", "carol", 3, ver(3))
	f.logSet(ver(3), 0, "issue", `{"id":"3"}`)
	f.setStateVersion(ver(3))
	if _, err := f.snap.Advance(syncable(issueSpec()), allNames("issue")); err != nil {
		t.Fatalf("second Advance: %v", err)
	}

	// Consuming the stale diff must fail loud, not read the wrong frame.
	var invalid *InvalidDiffError
	if _, err := staleDiff.Collect(); !errors.As(err, &invalid) {
		t.Fatalf("stale diff Collect: want *InvalidDiffError, got %v", err)
	}

	err = staleDiff.Each(func(Change) error {
		t.Fatal("Each emitted a row from a stale diff")
		return nil
	})
	if !errors.As(err, &invalid) {
		t.Fatalf("stale diff Each: want *InvalidDiffError, got %v", err)
	}
}

// TestDiff_StaleAfterDestroy verifies a diff outstanding at Destroy is rejected
// (its snapshots are closed underneath it).
func TestDiff_StaleAfterDestroy(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.upsertIssue("2", "two", "bob", 2, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"2"}`)
	f.setStateVersion(ver(2))
	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}

	f.snap.Destroy()

	var invalid *InvalidDiffError
	if _, err := diff.Collect(); !errors.As(err, &invalid) {
		t.Fatalf("post-Destroy Collect: want *InvalidDiffError, got %v", err)
	}
}

// TestDiff_ValidBeforeNextAdvance is the negative control: a freshly-produced
// diff consumed before any further Advance iterates normally.
func TestDiff_ValidBeforeNextAdvance(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.upsertIssue("2", "two", "bob", 2, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"2"}`)
	f.setStateVersion(ver(2))
	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}

	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("fresh diff Collect: unexpected error %v", err)
	}
	if len(changes) != 1 || changes[0].RowKey["id"] != "2" {
		t.Fatalf("fresh diff: want 1 change id=2, got %+v", changes)
	}
}
