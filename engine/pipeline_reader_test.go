package engine

// Pins for the Option B per-pipeline reader acquisition (acquirePipelineReader
// + the hydrate integration): one exclusive reader per hydrate pipeline,
// acquired at start while holding nothing; queueing past the pool width is
// normal admission behavior; the tripwire (PipelineReaderTripwire) firing is
// a loud PANIC — a wedge signal, never a fallback.

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// stubReaderPool implements pipelineReaderPool with a K-slot semaphore and
// records the acquire/release pairing per queryID. releaseCount is the F2
// progress signal (bumped on every release — or manually by progress tests).
type stubReaderPool struct {
	slots chan struct{}

	mu        sync.Mutex
	acquired  map[string]int
	released  map[string]int
	neverGive bool

	releaseCount atomic.Uint64
}

func newStubReaderPool(k int) *stubReaderPool {
	s := &stubReaderPool{
		slots:    make(chan struct{}, k),
		acquired: map[string]int{},
		released: map[string]int{},
	}
	for i := 0; i < k; i++ {
		s.slots <- struct{}{}
	}
	return s
}

func (s *stubReaderPool) AcquireForPipeline(queryID string, wait time.Duration) (func(), bool) {
	if s.neverGive {
		time.Sleep(wait)
		return nil, false
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-s.slots:
	case <-t.C:
		return nil, false
	}
	s.mu.Lock()
	s.acquired[queryID]++
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		s.released[queryID]++
		s.mu.Unlock()
		s.releaseCount.Add(1)
		s.slots <- struct{}{}
	}, true
}

func (s *stubReaderPool) Releases() uint64 { return s.releaseCount.Load() }

func setPipelineReaderTripwire(t *testing.T, d time.Duration) {
	t.Helper()
	saved := PipelineReaderTripwire
	PipelineReaderTripwire = d
	t.Cleanup(func() { PipelineReaderTripwire = saved })
}

// newUsersEngine builds an engine with one MemorySource users table (the
// MemorySource adapter does NOT implement readerPoolBinder, which is exactly
// right here: these tests exercise the ENGINE's per-pipeline acquire
// discipline, not the leaf's routing — the tablesource package pins that).
func newUsersEngine(t *testing.T) *Engine {
	t.Helper()
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "number", "name": "string"}, []string{"id"})
	users.BulkInsert([]ivm.Row{{"id": float64(1), "name": "alice"}})
	eng.RegisterMemorySource(users)
	return eng
}

// TestAcquirePipelineReader_TripwirePanics: a pool that never grants — and
// shows ZERO release progress — must trip the wire with a loud, distinctive
// panic — never a silent fallback, never an unbounded wait.
func TestAcquirePipelineReader_TripwirePanics(t *testing.T) {
	setPipelineReaderTripwire(t, 100*time.Millisecond)
	pool := newStubReaderPool(0)
	pool.neverGive = true

	done := make(chan any, 1)
	go func() {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_, _ = acquirePipelineReader(pool, "q1", nil)
		}()
		done <- recovered
	}()
	select {
	case recovered := <-done:
		if recovered == nil {
			t.Fatal("tripwire did not fire")
		}
		msg, _ := recovered.(string)
		if !strings.Contains(msg, "reader-pool admission tripwire") {
			t.Fatalf("panic = %v, want the admission-tripwire message", recovered)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("acquirePipelineReader hung past the tripwire")
	}
}

// TestAcquirePipelineReader_ProgressResetsDeadline is the F2 pin
// (parallelism audit 2026-07-10): pool-wide reader releases are PROGRESS —
// a waiter queued behind a busy-but-moving pool must NOT trip, however long
// it waits, because the production pull path runs with timeoutMs=0
// (consumer-driven RPC lifetime) and a wide batch on K readers legitimately
// queues past any fixed wall bound. Pre-F2 the deadline was fixed: this
// exact shape — releases flowing, this waiter never granted — panicked the
// whole batch, the client re-hydrated the same width, and the panic
// repeated (a self-inflicted hydrate storm). Once progress STOPS, the
// tripwire must still fire (the genuine-wedge half).
func TestAcquirePipelineReader_ProgressResetsDeadline(t *testing.T) {
	setPipelineReaderTripwire(t, 250*time.Millisecond)
	pool := newStubReaderPool(0) // never grants to the waiter
	pool.neverGive = true

	// Background "sibling pipelines": releases flow every 50ms — well
	// inside the 250ms tripwire — for ~4× the tripwire window.
	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopProgress:
				return
			case <-ticker.C:
				pool.releaseCount.Add(1)
			}
		}
	}()

	done := make(chan any, 1)
	start := time.Now()
	go func() {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_, _ = acquirePipelineReader(pool, "q-wide-batch", nil)
		}()
		done <- recovered
	}()

	// Phase 1: progress flowing — the waiter must survive well past the
	// tripwire without panicking.
	select {
	case recovered := <-done:
		close(stopProgress)
		<-progressDone
		t.Fatalf("waiter terminated after %v DESPITE pool-wide progress: %v "+
			"(fixed-deadline tripwire — the F2 hydrate-storm shape)",
			time.Since(start), recovered)
	case <-time.After(4 * PipelineReaderTripwire):
		// Survived 4× the tripwire with progress — correct.
	}

	// Phase 2: progress STOPS — now it is a genuine wedge and must trip
	// within roughly one tripwire window (+ slack for the wait slice).
	close(stopProgress)
	<-progressDone
	select {
	case recovered := <-done:
		if recovered == nil {
			t.Fatal("waiter returned without panic after progress froze")
		}
		msg, _ := recovered.(string)
		if !strings.Contains(msg, "ZERO pool-wide reader releases") {
			t.Fatalf("panic = %v, want the zero-progress tripwire message", recovered)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tripwire did not fire after progress froze — the genuine-wedge half broke")
	}
}

// TestAcquirePipelineReader_CancelledWhileQueued: an RPC cancellation while
// queued returns ok=false promptly (the caller abandons the hydrate) — no
// panic, no reader.
func TestAcquirePipelineReader_CancelledWhileQueued(t *testing.T) {
	setPipelineReaderTripwire(t, 30*time.Second) // must NOT be what unblocks
	pool := newStubReaderPool(0)                 // empty: always queues
	var cancelled atomic.Bool

	done := make(chan bool, 1)
	go func() {
		_, ok := acquirePipelineReader(pool, "q1", &cancelled)
		done <- ok
	}()
	time.Sleep(100 * time.Millisecond) // let it park in a wait slice
	cancelled.Store(true)
	select {
	case ok := <-done:
		if ok {
			t.Fatal("cancelled acquire reported ok=true")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled acquire did not return — waiters must notice cancellation between slices")
	}
}

// TestHydrate_AcquiresOneReaderPerPipeline: the streaming hydrate acquires
// exactly ONE reader per query pipeline and releases every one, with
// admission queueing (K=1, 4 queries) serializing but completing the batch.
func TestHydrate_AcquiresOneReaderPerPipeline(t *testing.T) {
	eng := newUsersEngine(t)

	pool := newStubReaderPool(1) // K=1: queries queue at admission
	eng.BindTableSourcesToReaderPool(pool)
	defer eng.UnbindTableSourcesReaderPool()

	const n = 4
	specs := make([]QuerySpec, 0, n)
	for i := 0; i < n; i++ {
		specs = append(specs, QuerySpec{
			QueryID: fmt.Sprintf("q%d", i),
			AST:     builder.AST{Table: "users", OrderBy: ivm.Ordering{{"id", "asc"}}},
		})
	}
	finals := 0
	err := eng.AddQueriesStreamPull(specs, 0, func(r QueryResult) bool {
		if r.Final {
			finals++
		}
		return true
	})
	if err != nil {
		t.Fatalf("AddQueriesStreamPull: %v", err)
	}
	if finals != n {
		t.Fatalf("finals = %d, want %d", finals, n)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	for i := 0; i < n; i++ {
		qid := fmt.Sprintf("q%d", i)
		if pool.acquired[qid] != 1 {
			t.Errorf("pipeline %s acquired %d readers, want exactly 1", qid, pool.acquired[qid])
		}
		if pool.released[qid] != 1 {
			t.Errorf("pipeline %s released %d readers, want exactly 1 (leak)", qid, pool.released[qid])
		}
	}
	if free := len(pool.slots); free != 1 {
		t.Fatalf("pool has %d free slots after the batch, want 1 — a reader leaked", free)
	}
}

// TestUnbindClearsHydratePool: after UnbindTableSourcesReaderPool, hydrates
// must not touch the (possibly closed) pool at all.
func TestUnbindClearsHydratePool(t *testing.T) {
	eng := newUsersEngine(t)

	pool := newStubReaderPool(1)
	eng.BindTableSourcesToReaderPool(pool)
	eng.UnbindTableSourcesReaderPool()

	err := eng.AddQueriesStream([]QuerySpec{{
		QueryID: "q1",
		AST:     builder.AST{Table: "users", OrderBy: ivm.Ordering{{"id", "asc"}}},
	}}, func(QueryResult) {})
	if err != nil {
		t.Fatalf("AddQueriesStream: %v", err)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.acquired) != 0 {
		t.Fatalf("unbound pool was acquired from: %v", pool.acquired)
	}
}
