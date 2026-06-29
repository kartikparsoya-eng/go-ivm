package engine

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// blockingSource wraps a real Source but blocks Push on a channel — used to
// pin e.mu inside Advance so the test can verify concurrent paths don't
// serialize behind it.
type blockingSource struct {
	inner        Source
	gate         chan struct{} // closed by test to release Push
	refreshCount atomic.Int32
}

func (b *blockingSource) TableName() string      { return b.inner.TableName() }
func (b *blockingSource) PrimaryKey() []string   { return b.inner.PrimaryKey() }
func (b *blockingSource) NormalizeRow(r ivm.Row) { b.inner.NormalizeRow(r) }
func (b *blockingSource) Connect(sort ivm.Ordering, filter *builder.Condition, fp func(ivm.Row) bool, sek map[string]bool) ivm.Input {
	return b.inner.Connect(sort, filter, fp, sek)
}
func (b *blockingSource) Push(c ivm.SourceChange) []ivm.Change {
	<-b.gate
	return b.inner.Push(c)
}
func (b *blockingSource) Close() error { return b.inner.Close() }

// RefreshSnapshot is the optional interface RefreshAllSources looks for. We
// don't actually have a snapshot to roll — we just count invocations so the
// test can verify it ran.
func (b *blockingSource) RefreshSnapshot() {
	b.refreshCount.Add(1)
}

// TestRefreshAllSources_NotBlockedByLongAdvance is the H9 regression. Pre-fix,
// RefreshAllSources took e.mu and serialized behind any in-flight Advance —
// so a slow advance would block the drift-audit's refresh call for seconds.
// Post-fix, sources access is via atomic.Pointer + COW and RefreshAllSources
// takes no engine lock, so it proceeds immediately regardless of what other
// handlers are doing.
func TestRefreshAllSources_NotBlockedByLongAdvance(t *testing.T) {
	storagePath := t.TempDir() + "/storage.db"
	eng, err := NewEngine(EngineConfig{StoragePath: storagePath})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	ms := ivm.NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	inner := &memorySourceAdapter{ms: ms}
	gate := make(chan struct{})
	bs := &blockingSource{inner: inner, gate: gate}
	eng.RegisterSource(bs)

	// Start a slow Advance — it'll take e.mu and block on bs.Push.
	// Add a fresh row so we never hit drift validation.
	advanceDone := make(chan struct{})
	go func() {
		defer close(advanceDone)
		eng.Advance([]SnapshotChange{
			{
				Table:      "t",
				PrevValues: nil,
				NextValue:  ivm.Row{"id": "k2"},
			},
		})
	}()

	// Give the advance goroutine a moment to enter Push and pin e.mu.
	time.Sleep(20 * time.Millisecond)

	// RefreshAllSources must return immediately (no e.mu contention).
	refreshStart := time.Now()
	eng.RefreshAllSources()
	refreshElapsed := time.Since(refreshStart)
	if refreshElapsed > 100*time.Millisecond {
		t.Fatalf("RefreshAllSources serialized behind Advance: took %v (expected ~immediate)", refreshElapsed)
	}
	if got := bs.refreshCount.Load(); got != 1 {
		t.Fatalf("RefreshAllSources didn't invoke per-source RefreshSnapshot: count=%d", got)
	}

	// Release the advance so the test cleans up.
	close(gate)
	select {
	case <-advanceDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Advance didn't complete after gate released")
	}
}
