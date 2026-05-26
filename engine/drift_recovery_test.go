package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Verifies the self-heal contract: when MemorySource raises *ivm.DriftError
// on an Edit/Remove against a missing row (or duplicate Add), engine.Advance
// recovers gracefully — returns Drift in the result, drops any output the
// streamer was holding, and leaves the source's in-memory state untouched
// so a subsequent re-init can succeed cleanly. Mirrors the Pattern Z drift
// pattern documented in go-ivm/PERF-REVIEW.md and the prior session's
// handoff for the clients-table panic.

func setupSimpleEngine(t *testing.T) (*Engine, *ivm.MemorySource) {
	t.Helper()
	users := ivm.NewMemorySource(
		"users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"},
	)
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)

	// One simple query so source.Push has at least one connection to
	// exercise (otherwise the validate block still runs but the rest of
	// Push is a no-op — still a valid drift test, but less realistic).
	if _, _, err := eng.AddQuery("q", builder.AST{Table: "users"}); err != nil {
		t.Fatal(err)
	}
	return eng, users
}

func TestAdvance_DriftOnEditMissingRow_RecoversAndReports(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	// MemorySource has zero rows; an Edit must trip the missing-row guard.
	result := eng.Advance([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
		},
	})

	if result.Drift == nil {
		t.Fatalf("expected Drift signal, got Changes=%v", result.Changes)
	}
	if result.Drift.Table != "users" {
		t.Errorf("expected drift.Table=users, got %q", result.Drift.Table)
	}
	if result.Drift.Op != "Edit" {
		t.Errorf("expected drift.Op=Edit, got %q", result.Drift.Op)
	}
	if got := result.Drift.PK["id"]; got != "u1" {
		t.Errorf("expected drift.PK.id=u1, got %v", got)
	}
	if len(result.Changes) != 0 {
		t.Errorf("expected no Changes on drift, got %d", len(result.Changes))
	}
}

func TestAdvance_DriftOnRemoveMissingRow_RecoversAndReports(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	result := eng.Advance([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u-ghost", "name": "Ghost"}},
			NextValue:  nil,
		},
	})

	if result.Drift == nil {
		t.Fatalf("expected Drift signal")
	}
	if result.Drift.Op != "Remove" {
		t.Errorf("expected drift.Op=Remove, got %q", result.Drift.Op)
	}
}

// Verifies that source state is unchanged after a drift: a follow-up Add
// of the same row succeeds (would fail with "Row already exists" if the
// failed Edit somehow inserted a row).
func TestAdvance_DriftLeavesSourceUntouched(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	// First: drift on Edit of missing row.
	drift := eng.Advance([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
		},
	})
	if drift.Drift == nil {
		t.Fatal("expected drift")
	}

	// Then: a clean Add for the same row should succeed (source untouched).
	clean := eng.Advance([]SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "Alice"}},
	})
	if clean.Drift != nil {
		t.Fatalf("expected clean advance, got drift: %v", clean.Drift)
	}
	if len(clean.Changes) != 1 || clean.Changes[0].Type != RowChangeAdd {
		t.Fatalf("expected one ADD, got %+v", clean.Changes)
	}
}

// Verifies that streaming advance also recovers: terminal Final frame
// carries Drift, Changes is empty, no Final-twice / no missing-Final bugs.
func TestAdvanceStream_DriftRecovery(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	frames := []AdvanceStreamPartial{}
	err := eng.AdvanceStream([]SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
		},
	}, func(p AdvanceStreamPartial) {
		frames = append(frames, p)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected exactly one Final frame on drift, got %d", len(frames))
	}
	f := frames[0]
	if !f.Final {
		t.Fatalf("expected Final=true, got %+v", f)
	}
	if f.Drift == nil {
		t.Fatalf("expected Drift on Final frame")
	}
	if len(f.Changes) != 0 {
		t.Errorf("expected no Changes on drift Final frame, got %d", len(f.Changes))
	}
	if f.Drift.Table != "users" || f.Drift.Op != "Edit" {
		t.Errorf("unexpected drift: %+v", f.Drift)
	}
}

// Verifies that a duplicate Add (e.g., retry race) is also caught as drift
// — same recovery path as missing-row Edit/Remove.
func TestAdvance_DriftOnDuplicateAdd(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	// First Add succeeds.
	if r := eng.Advance([]SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "Alice"}},
	}); r.Drift != nil {
		t.Fatalf("setup advance unexpectedly drifted: %v", r.Drift)
	}

	// Re-Add the same row → duplicate → drift signal.
	result := eng.Advance([]SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "Alice"}},
	})
	if result.Drift == nil {
		t.Fatalf("expected drift on duplicate Add")
	}
	if result.Drift.Op != "Add" {
		t.Errorf("expected drift.Op=Add, got %q", result.Drift.Op)
	}
}
