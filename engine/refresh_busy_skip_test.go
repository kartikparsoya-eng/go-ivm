package engine

// Regression test for the mid-batch refresh hazard (full-scale review
// 2026-07-03): the drift audit's RefreshAllSources deliberately bypasses the
// per-CG worker FIFO and used to run fully concurrently with an in-flight
// advance. Between two pushes of one batch the source overlay is nil, so the
// refresh's ROLLBACK discarded the batch's earlier uncommitted writeChange
// rows and re-pinned at post-batch head — false-drifting every subsequent
// Add. The fix: RefreshAllSources TryLocks e.mu and SKIPS (never blocks)
// while an advance/hydrate holds the engine.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// refreshSpySource implements engine.Source plus the RefreshSnapshot
// interface RefreshAllSources probes for, counting refresh calls.
type refreshSpySource struct {
	refreshes atomic.Int32
}

func (s *refreshSpySource) TableName() string                  { return "spy" }
func (s *refreshSpySource) PrimaryKey() []string               { return []string{"id"} }
func (s *refreshSpySource) NormalizeRow(ivm.Row)               {}
func (s *refreshSpySource) Push(ivm.SourceChange) []ivm.Change { return nil }
func (s *refreshSpySource) Close() error                       { return nil }
func (s *refreshSpySource) RefreshSnapshot()                   { s.refreshes.Add(1) }
func (s *refreshSpySource) Connect(ivm.Ordering, *builder.Condition, func(ivm.Row) bool, map[string]bool) ivm.Input {
	return nil
}

func TestRefreshAllSources_SkipsWhileEngineBusy(t *testing.T) {
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	spy := &refreshSpySource{}
	eng.RegisterSource(spy)

	// Simulate an in-flight advance/hydrate: e.mu held.
	eng.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		eng.RefreshAllSources() // must SKIP, not block
	}()
	select {
	case <-done:
		// returned promptly — good
	case <-time.After(2 * time.Second):
		eng.mu.Unlock()
		t.Fatal("RefreshAllSources BLOCKED behind a busy engine — the audit's non-blocking contract is broken")
	}
	if got := spy.refreshes.Load(); got != 0 {
		eng.mu.Unlock()
		t.Fatalf("RefreshAllSources refreshed %d source(s) WHILE the engine was mid-advance — "+
			"this is the destructive mid-batch ROLLBACK the TryLock guard exists to prevent", got)
	}
	eng.mu.Unlock()

	// Engine free → refresh proceeds.
	eng.RefreshAllSources()
	if got := spy.refreshes.Load(); got != 1 {
		t.Fatalf("RefreshAllSources on a free engine refreshed %d time(s), want 1", got)
	}
}
