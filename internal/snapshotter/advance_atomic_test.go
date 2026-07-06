package snapshotter

// Pins for Advance's failure-atomicity: the prev/curr swap must commit only
// after the diff exists. Pre-fix, a newDiff failure (transient changelog
// read error) stranded the swap — the NEXT Advance diffed from the moved
// position while the engine still held the old content, silently skipping a
// window (drift). Proven to fail pre-fix by reverting the reorder: the retry
// diff came back with 0 changes instead of 1.

import "testing"

func TestAdvance_FailureAtomic_RetryKeepsWindow(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("1", "one", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// Stage v2: one logged change past the pinned snapshot.
	f.upsertIssue("2", "two", "bob", 2, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"2"}`)
	f.setStateVersion(ver(2))

	// Sabotage newDiff's NumChangesSince read (the only fallible step after
	// the head pin) by renaming the changelog away.
	f.exec(`ALTER TABLE "_zero.changeLog2" RENAME TO "_zero.changeLog2_bak"`)
	if _, err := f.snap.Advance(syncable(issueSpec()), allNames("issue")); err == nil {
		t.Fatal("Advance should fail with the changelog missing")
	}

	// ATOMICITY: curr is untouched — still the v1 pin the engine's applied
	// content corresponds to.
	cur, err := f.snap.Current()
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if got := cur.Version(); got != ver(1) {
		t.Fatalf("curr version after failed Advance = %q, want %q (swap must not commit)", got, ver(1))
	}

	// Heal and retry IN PLACE (the rpcCodeAdvanceCleanRetryable contract):
	// the diff must cover the ORIGINAL (v1 → head] window — a stranded swap
	// would diff (v2 → v2] and silently drop the id=2 change.
	f.exec(`ALTER TABLE "_zero.changeLog2_bak" RENAME TO "_zero.changeLog2"`)
	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("retry Advance: %v", err)
	}
	if diff.Prev().Version() != ver(1) || diff.Curr().Version() != ver(2) {
		t.Fatalf("retry window = (%s → %s], want (%s → %s]",
			diff.Prev().Version(), diff.Curr().Version(), ver(1), ver(2))
	}
	if diff.Changes != 1 {
		t.Fatalf("retry diff.Changes = %d, want 1 (the staged change must not be skipped)", diff.Changes)
	}
	seen := 0
	if err := diff.Each(func(c Change) error {
		seen++
		if c.Table != "issue" || c.NextValue == nil || c.NextValue["id"] != "2" {
			t.Fatalf("unexpected change: %+v", c)
		}
		return nil
	}); err != nil {
		t.Fatalf("Each: %v", err)
	}
	if seen != 1 {
		t.Fatalf("changes iterated = %d, want 1", seen)
	}
}
