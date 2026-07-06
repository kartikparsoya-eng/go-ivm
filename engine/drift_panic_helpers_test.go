package engine

// Helpers for pinning the follow-TS source-drift disposition in engine
// tests: a source-state divergence (dup-Add / missing-row Edit/Remove /
// Take stale-bound) PANICS out of Advance with a plain error — the direct
// twin of TS's assert-throws, which the view-syncer answers with a client
// group teardown. Tests that EXPECT drift recover the panic and assert on
// the message; tests that expect a CLEAN advance simply call Advance — any
// drift panic fails them loudly.

import (
	"strings"
	"testing"
)

// advanceDriftPanic runs eng.Advance(changes) expecting the source-drift
// assert to panic; returns the panic's error message for content asserts.
func advanceDriftPanic(t *testing.T, eng *Engine, changes []SnapshotChange) string {
	t.Helper()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		eng.Advance(changes)
	}()
	if recovered == nil {
		t.Fatal("expected the source-drift assert to panic; Advance returned cleanly")
	}
	err, ok := recovered.(error)
	if !ok {
		t.Fatalf("expected error panic (plain source-drift error), got %T: %v", recovered, recovered)
	}
	if !strings.Contains(err.Error(), "source drift:") {
		t.Fatalf("expected source-drift panic, got: %v", err)
	}
	return err.Error()
}

// advanceScalarResetPanic runs eng.Advance(changes) expecting the companion
// scalar-value-changed check to panic with *ScalarResetError (the twin of
// TS's ResetPipelinesSignal('scalar-subquery') — the sidecar maps it to the
// scalar-reset RPC code, a RESET, not a teardown). Returns the typed error.
func advanceScalarResetPanic(t *testing.T, eng *Engine, changes []SnapshotChange) *ScalarResetError {
	t.Helper()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		eng.Advance(changes)
	}()
	if recovered == nil {
		t.Fatal("expected the scalar-subquery reset to panic; Advance returned cleanly")
	}
	sre, ok := recovered.(*ScalarResetError)
	if !ok {
		t.Fatalf("expected *ScalarResetError panic, got %T: %v", recovered, recovered)
	}
	return sre
}
