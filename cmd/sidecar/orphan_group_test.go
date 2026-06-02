package main

// Regression coverage for the C8 orphan-group leak.
//
// Pre-fix every handler called s.getGroup(cgID) which auto-created a
// fresh empty ClientGroup if the cgID was absent from the map. Combined
// with a concurrent removeGroup, this leaked one ClientGroup + one
// worker goroutine per race occurrence — they sat idle until the
// 30-min reaper finally collected them. Under TS-side churn
// (reconnect storms, network blips), the leak accumulated linearly.
//
// The fix: getGroup now takes a createIfMissing bool. Only handleInit
// passes true. All other handlers pass false and return an error if
// the group is absent, instead of silently spawning an orphan.
//
// These tests pin the new contract.

import (
	"testing"
)

// TestGetGroup_NoCreateWhenAbsent confirms that getGroup with
// createIfMissing=false returns nil instead of spawning a new
// ClientGroup. Pre-fix this would return a fresh empty group.
func TestGetGroup_NoCreateWhenAbsent(t *testing.T) {
	s := NewServer(0, "")
	g := s.getGroup("never-existed", false)
	if g != nil {
		t.Fatalf("expected nil for absent group + createIfMissing=false, got %p", g)
	}
	// And the map should remain empty (no orphan spawned).
	s.mu.RLock()
	n := len(s.groups)
	s.mu.RUnlock()
	if n != 0 {
		t.Fatalf("expected groups map to stay empty, got %d entries", n)
	}
}

// TestGetGroup_CreateWhenAbsent confirms that createIfMissing=true still
// works the way handleInit needs (group + worker spawned).
func TestGetGroup_CreateWhenAbsent(t *testing.T) {
	s := NewServer(0, "")
	g := s.getGroup("first-init", true)
	if g == nil {
		t.Fatal("expected new group with createIfMissing=true, got nil")
	}
	if g.reqC == nil {
		t.Error("expected reqC initialized on new group")
	}
	if g.done == nil {
		t.Error("expected done initialized on new group")
	}
	// Worker is started — calling shutdownGroup must not hang.
	s.shutdownGroup(g)
}

// TestRemoveGroup_NoOrphanAfterConcurrentLookup simulates the race the
// audit identified: handler-mid-flight queries getGroup AFTER
// removeGroup has deleted the entry. Pre-fix this spawned an orphan;
// post-fix it cleanly returns nil and the handler can return an error.
func TestRemoveGroup_NoOrphanAfterConcurrentLookup(t *testing.T) {
	s := NewServer(0, "")

	// Stage: cgID exists.
	const id = "cg-race"
	g := s.getGroup(id, true)
	if g == nil {
		t.Fatal("setup: getGroup(true) should create")
	}

	// removeGroup destroys it.
	s.removeGroup(id)

	// A handler-mid-flight lookup (createIfMissing=false) must NOT spawn
	// a new group.
	again := s.getGroup(id, false)
	if again != nil {
		t.Fatalf("expected nil after removeGroup, got %p (orphan leak)", again)
	}
	s.mu.RLock()
	n := len(s.groups)
	s.mu.RUnlock()
	if n != 0 {
		t.Fatalf("expected groups map empty after remove, got %d", n)
	}
}
