package snapshotter

// Review item #3: multi-commit catch-up. When the sidecar was busy (long
// hydrate, restart, backpressure) the replicator commits SEVERAL versions
// before the next Advance — prev@V1, curr@V3+. The change-log's
// UNIQUE("table","rowKey") + INSERT OR REPLACE means each row carries only
// its LATEST op across the span, and the diff reads row CONTENT from the
// snapshot endpoints (prev/curr), never from intermediate versions. These
// tests pin the catch-up semantics that fall out of that design — the cases
// a single-version test can never reach.

import (
	"strings"
	"testing"
)

// A row born at V2 and deleted at V3 must vanish from the diff entirely:
// the coalesced log entry is a delete, and prev(V1) never had the row →
// no-op filter. Emitting either the birth or the death would push a change
// for a row the client should never see.
func TestDiffMultiCommit_BornAndDiedWithinSpan_NoOp(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("keep", "keep", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// V2: insert "flash". V3: delete it. Log coalesces to d@V3.
	f.logSet(ver(2), 0, "issue", `{"id":"flash"}`)
	f.logDelete(ver(3), 0, "issue", `{"id":"flash"}`) // INSERT OR REPLACE overwrites the set
	f.setStateVersion(ver(3))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("born+died-within-span row must be a no-op, got %+v", changes)
	}
}

// A row deleted at V2 and re-inserted at V3 (with new content) coalesces to
// one SET entry; the diff must emit a single EDIT-shaped change from the V1
// content straight to the V3 content — never a remove+add pair, and never
// the phantom intermediate state.
func TestDiffMultiCommit_DeleteThenReinsert_SingleEdit(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("R", "original", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// V2: delete R. V3: re-insert with new content. Log coalesces to s@V3.
	f.logDelete(ver(2), 0, "issue", `{"id":"R"}`)
	f.upsertIssue("R", "reborn", "bob", 1, ver(3))
	f.logSet(ver(3), 0, "issue", `{"id":"R"}`)
	f.setStateVersion(ver(3))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("want exactly 1 coalesced change, got %+v", changes)
	}
	c := changes[0]
	if len(c.PrevValues) != 1 || c.PrevValues[0]["title"] != "original" {
		t.Fatalf("prevValues must carry the V1 content, got %+v", c.PrevValues)
	}
	if c.NextValue == nil || c.NextValue["title"] != "reborn" || c.NextValue["owner"] != "bob" {
		t.Fatalf("nextValue must carry the V3 content, got %+v", c.NextValue)
	}
}

// Repeated updates across the span coalesce to one change: V1 content →
// final content, one entry, intermediate values never surface.
func TestDiffMultiCommit_RepeatedUpdates_Coalesce(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("U", "v1", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.logSet(ver(2), 0, "issue", `{"id":"U"}`) // update @V2 (content overwritten below)
	f.upsertIssue("U", "v3", "carol", 1, ver(3))
	f.logSet(ver(3), 0, "issue", `{"id":"U"}`) // coalesces the V2 entry away
	f.setStateVersion(ver(3))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("want 1 coalesced change, got %+v", changes)
	}
	if changes[0].PrevValues[0]["title"] != "v1" || changes[0].NextValue["title"] != "v3" {
		t.Fatalf("must jump V1→V3 content directly: %+v", changes[0])
	}
	if diff.Changes != 1 {
		t.Errorf("Changes count = %d, want 1 (coalesced)", diff.Changes)
	}
}

// Entries from different versions interleave in (stateVersion ASC, pos ASC)
// order — the order clients see pokes. A V2 change must precede every V3
// change even when the V3 rows were logged "first" within their version.
func TestDiffMultiCommit_CrossVersionOrdering(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.upsertIssue("b", "b", "x", 2, ver(2))
	f.upsertIssue("a", "a", "x", 1, ver(3))
	f.upsertIssue("c", "c", "x", 3, ver(3))
	f.logSet(ver(3), 0, "issue", `{"id":"a"}`)
	f.logSet(ver(3), 1, "issue", `{"id":"c"}`)
	f.logSet(ver(2), 5, "issue", `{"id":"b"}`) // higher pos, EARLIER version
	f.setStateVersion(ver(3))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(changes) != 3 {
		t.Fatalf("want 3 changes, got %+v", changes)
	}
	got := []string{
		changes[0].RowKey["id"].(string),
		changes[1].RowKey["id"].(string),
		changes[2].RowKey["id"].(string),
	}
	if got[0] != "b" || got[1] != "a" || got[2] != "c" {
		t.Fatalf("order = %v, want [b a c] (stateVersion ASC, pos ASC)", got)
	}
}

// A truncate LATE in the span (after emittable row changes at earlier
// versions) still aborts with a ResetSignal — and the caller must discard
// whatever was emitted before the abort. Pins the partial-emit-then-reset
// shape: emit fires for the pre-truncate rows, then Each returns the reset.
func TestDiffMultiCommit_LateTruncate_ResetAfterPartialEmit(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.upsertIssue("early", "early", "x", 1, ver(2))
	f.logSet(ver(2), 0, "issue", `{"id":"early"}`)
	f.logTableWide(ver(3), "issue", opTruncate)
	f.setStateVersion(ver(3))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	emitted := 0
	err = diff.Each(func(Change) error {
		emitted++
		return nil
	})
	rs, ok := IsReset(err)
	if !ok {
		t.Fatalf("want ResetSignal from late truncate, got %v", err)
	}
	if rs.Reason != ReasonTruncation {
		t.Fatalf("reason = %q, want truncation", rs.Reason)
	}
	if emitted != 1 {
		t.Fatalf("want 1 pre-truncate emit before the abort (caller must discard on reset), got %d", emitted)
	}
}

// An EARLY truncate (V2, pos=-1) aborts before anything at V3 emits —
// table-wide ops sort first within their version and versions sort ASC.
func TestDiffMultiCommit_EarlyTruncate_NothingEmits(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	f.logTableWide(ver(2), "issue", opTruncate)
	f.upsertIssue("later", "later", "x", 1, ver(3))
	f.logSet(ver(3), 0, "issue", `{"id":"later"}`)
	f.setStateVersion(ver(3))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	emitted := 0
	err = diff.Each(func(Change) error {
		emitted++
		return nil
	})
	if _, ok := IsReset(err); !ok {
		t.Fatalf("want ResetSignal, got %v", err)
	}
	if emitted != 0 {
		t.Fatalf("early truncate must abort before any emit, got %d", emitted)
	}
}

// STALE-REMOVE seam (the OPEN upstream changeLog2-trim item): a delete entry
// whose row nonetheless EXISTS in curr — a log the replicator failed to trim
// after a re-insert. The diff TRUSTS the log (faithful to TS): it emits a
// REMOVE for a row curr still has, which downstream becomes drift. This test
// pins that trusting behavior so the eventual upstream trim fix (or a Go-side
// defensive check) flips an explicit assertion instead of silently changing
// semantics.
func TestDiffMultiCommit_StaleRemoveEntry_TrustsLog(t *testing.T) {
	f := newFixture(t)
	f.createIssueTable()
	f.upsertIssue("S", "stale", "alice", 1, ver(1))
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// The log says "deleted at V2" — but the row is still present in curr
	// (re-inserted without the log entry being replaced; upstream trim bug).
	// Keep its _0_version at V1 so checkValid's prev-side gate passes.
	f.logDelete(ver(2), 0, "issue", `{"id":"S"}`)
	f.setStateVersion(ver(2))

	diff, err := f.snap.Advance(syncable(issueSpec()), allNames("issue"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	// CURRENT-BEHAVIOR: the stale delete is emitted as a real remove.
	if len(changes) != 1 || changes[0].NextValue != nil {
		t.Fatalf("expected the stale delete to emit a remove (log-trusting behavior); got %+v — "+
			"if a defensive curr-existence check was added, update this test deliberately", changes)
	}
	if changes[0].PrevValues[0]["id"] != "S" {
		t.Fatalf("remove must target S, got %+v", changes[0].PrevValues)
	}
}

// minRowVersion catch-up gate at the span boundary: an entry AT the
// minRowVersion is a hard error (ops must be strictly newer — a
// minRowVersion set is always followed by a RESET), one version later is
// clean. Off-by-one here silently drops or double-applies backfill rows.
func TestDiffMultiCommit_MinRowVersionBoundary(t *testing.T) {
	run := func(minVer string, wantErr bool) {
		t.Helper()
		f := newFixture(t)
		f.createIssueTable()
		f.setStateVersion(ver(1))
		f.initSnapshotter()

		f.upsertIssue("m", "m", "x", 1, ver(2))
		f.logSet(ver(2), 0, "issue", `{"id":"m"}`)
		f.setStateVersion(ver(2))

		spec := issueSpec()
		spec.MinRowVersion = minVer
		diff, err := f.snap.Advance(syncable(spec), allNames("issue"))
		if err != nil {
			t.Fatalf("Advance: %v", err)
		}
		_, err = diff.Collect()
		if wantErr {
			if err == nil || !strings.Contains(err.Error(), "minRowVersion") {
				t.Fatalf("minRowVersion=%q: want the catch-up invariant error, got %v", minVer, err)
			}
		} else if err != nil {
			t.Fatalf("minRowVersion=%q: want clean diff, got %v", minVer, err)
		}
	}
	run(ver(2), true)  // entry AT minRowVersion → strictly-greater violated
	run(ver(1), false) // entry after minRowVersion → clean
}
