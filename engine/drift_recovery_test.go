package engine

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Tests the source-drift disposition: when a source's pre-push validation
// detects an Edit/Remove against a missing row (or a duplicate Add), the
// panic propagates out of engine.Advance / engine.AdvanceStream. The engine
// drains its streamer before re-raising, and signalAdvanceEnd still rotates
// the sources, so a follow-up advance on the same engine is clean.

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

func TestAdvance_DriftOnEditMissingRow_Panics(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	// MemorySource has zero rows; an Edit must trip the missing-row guard.
	msg := advanceDriftPanic(t, eng, []SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
		},
	})
	if !strings.Contains(msg, "table=users") {
		t.Errorf("expected table=users in drift panic, got %q", msg)
	}
	if !strings.Contains(msg, "op=Edit") {
		t.Errorf("expected op=Edit in drift panic, got %q", msg)
	}
	if !strings.Contains(msg, "u1") {
		t.Errorf("expected pk u1 in drift panic, got %q", msg)
	}
}

func TestAdvance_DriftOnRemoveMissingRow_Panics(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	msg := advanceDriftPanic(t, eng, []SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u-ghost", "name": "Ghost"}},
			NextValue:  nil,
		},
	})
	if !strings.Contains(msg, "op=Remove") {
		t.Errorf("expected op=Remove in drift panic, got %q", msg)
	}
}

// Verifies that source state is unchanged after a drift panic: a follow-up
// Add of the same row succeeds (would fail with a dup-Add drift if the
// failed Edit somehow inserted a row). The validation runs BEFORE any
// mutation, so the panic leaves nothing half-applied.
func TestAdvance_DriftLeavesSourceUntouched(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	// First: drift panic on Edit of missing row.
	advanceDriftPanic(t, eng, []SnapshotChange{
		{
			Table:      "users",
			PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
			NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
		},
	})

	// Then: a clean Add for the same row should succeed (source untouched).
	clean := eng.Advance([]SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "Alice"}},
	})
	if len(clean.Changes) != 1 || clean.Changes[0].Type != RowChangeAdd {
		t.Fatalf("expected one ADD, got %+v", clean.Changes)
	}
}

// Verifies the streaming advance's disposition: the drift panic re-raises
// out of AdvanceStream AFTER exactly one empty Final frame ships (the C5
// clean-wire invariant — the TS accumulator sees a terminal frame, then the
// RPC error settles the call).
func TestAdvanceStream_DriftPanicsAfterCleanFinal(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	frames := []AdvanceStreamPartial{}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = eng.AdvanceStream([]SnapshotChange{
			{
				Table:      "users",
				PrevValues: []ivm.Row{{"id": "u1", "name": "Alice"}},
				NextValue:  ivm.Row{"id": "u1", "name": "Alicia"},
			},
		}, func(p AdvanceStreamPartial) {
			frames = append(frames, p)
		})
	}()
	if recovered == nil {
		t.Fatal("expected the drift panic to re-raise out of AdvanceStream")
	}
	err, ok := recovered.(error)
	if !ok || !strings.Contains(err.Error(), "source drift: table=users op=Edit") {
		t.Fatalf("expected the users/Edit source-drift panic, got %T: %v", recovered, recovered)
	}
	if len(frames) != 1 {
		t.Fatalf("expected exactly one Final frame on drift, got %d", len(frames))
	}
	f := frames[0]
	if !f.Final {
		t.Fatalf("expected Final=true, got %+v", f)
	}
	if len(f.Changes) != 0 {
		t.Errorf("expected no Changes on the drift Final frame, got %d", len(f.Changes))
	}
}

// Verifies that a duplicate Add (e.g., retry race) is also caught by the
// pre-push validation — same panic disposition as missing-row Edit/Remove.
func TestAdvance_DriftOnDuplicateAdd(t *testing.T) {
	eng, _ := setupSimpleEngine(t)

	// First Add succeeds.
	eng.Advance([]SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "Alice"}},
	})

	// Re-Add the same row → duplicate → drift panic.
	msg := advanceDriftPanic(t, eng, []SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": "Alice"}},
	})
	if !strings.Contains(msg, "op=Add") {
		t.Errorf("expected op=Add in drift panic, got %q", msg)
	}
}
