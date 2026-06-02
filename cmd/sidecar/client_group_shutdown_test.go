package main

// Regression coverage for the per-CG g.mu → done-channel refactor.
//
// Previous design held g.mu during the buffered-channel send in
// trySendReq. shutdownGroup then had to take g.mu to close reqC. Under
// load on a slow handler:
//   1. Reader takes g.mu, blocks on reqC send (buffer full).
//   2. shutdownGroup blocks on g.mu.Lock() until the reader unblocks.
//   3. Reader unblocks when worker drains, but worker is in a slow
//      handler holding g.mu (handleAdvance line 1269 etc.) — adding
//      multi-second latency to shutdown for the duration of the handler.
//
// And cross-CG, while the reader is blocked on one CG's full queue, it
// cannot dispatch to OTHER CGs on the same connection — silent head-of-
// line blocking that the audit overstated as "deadlock" but is a real
// throughput pathology.
//
// The fix uses a done channel for shutdown signalling and select-with-
// done in trySendReq, removing g.mu from the data-channel path entirely.
// These tests pin the new contract.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTrySendReq_ReturnsFalseAfterShutdown verifies that after
// shutdownGroup runs, trySendReq returns false instead of panicking on a
// closed channel. With the old close(reqC) approach plus an unprotected
// send, this scenario panicked the process.
func TestTrySendReq_ReturnsFalseAfterShutdown(t *testing.T) {
	s := NewServer(0, "")
	g := s.getGroup("cg-shutdown-test", true)

	// Pre-shutdown: sends succeed.
	respCh := make(chan RPCResponse, 1)
	if !g.trySendReq(clientGroupReq{
		req:    RPCRequest{ID: 1, Method: "ping"},
		respCh: respCh,
	}) {
		t.Fatal("expected trySendReq to succeed before shutdown")
	}

	s.shutdownGroup(g)

	// Post-shutdown: trySendReq returns false promptly. Without the
	// done-channel fix, this would either (a) panic on send-after-close,
	// or (b) block forever waiting for the closed reqC to drain.
	done := make(chan bool, 1)
	go func() {
		ok := g.trySendReq(clientGroupReq{
			req:    RPCRequest{ID: 2, Method: "ping"},
			respCh: make(chan RPCResponse, 1),
		})
		done <- ok
	}()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected trySendReq to return false after shutdown, got true")
		}
	case <-time.After(time.Second):
		t.Fatal("trySendReq blocked indefinitely after shutdown — done-channel pattern broken")
	}
}

// TestShutdownGroup_Idempotent confirms that concurrent shutdownGroup
// calls don't panic on double-close. The old design guarded with a
// `closed bool` under g.mu; the new design uses sync.Once around
// close(done).
func TestShutdownGroup_Idempotent(t *testing.T) {
	s := NewServer(0, "")
	g := s.getGroup("cg-idempotent", true)

	const N = 16
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("concurrent shutdownGroup panicked: %v", r)
				}
			}()
			s.shutdownGroup(g)
		}()
	}
	wg.Wait()
}

// TestWorker_DrainsBufferedReqsOnShutdown verifies that requests
// already sitting in reqC when done is closed get an error response
// instead of hanging the sender's respCh forever. Without this, a
// shutdown during a request burst would leave senders blocked on
// `respCh <- resp` (the per-request writer goroutine pattern at
// line 1503-1507).
func TestWorker_DrainsBufferedReqsOnShutdown(t *testing.T) {
	s := NewServer(0, "")
	g := s.getGroup("cg-drain", true)

	// Fill the channel with several requests faster than the worker
	// can drain them. The handler for "ping" is fast, but we'll race
	// shutdown against the enqueue burst so some sit in the buffer.
	const N = 32
	respChs := make([]chan RPCResponse, N)
	for i := 0; i < N; i++ {
		respChs[i] = make(chan RPCResponse, 1)
		g.trySendReq(clientGroupReq{
			req:    RPCRequest{ID: float64(i), Method: "ping"},
			respCh: respChs[i],
		})
	}

	// Shut down immediately. Worker may have processed 0-N requests
	// already; the rest must be drained with an error.
	s.shutdownGroup(g)

	// Every respCh must receive a response (either real or drained
	// error) within a reasonable budget. Without the drain, the
	// buffered-but-unprocessed slots would hang their respCh forever.
	var got atomic.Int32
	for i := 0; i < N; i++ {
		select {
		case resp := <-respChs[i]:
			_ = resp
			got.Add(1)
		case <-time.After(2 * time.Second):
			t.Errorf("respCh[%d] hung after shutdown — drain not firing", i)
		}
	}
	if int(got.Load()) != N {
		t.Errorf("expected %d responses (real or drained), got %d", N, got.Load())
	}
}

// TestTrySendReq_NoMutexContention is a minimal liveness check: while
// one goroutine is mid-shutdown, another can call trySendReq and
// return immediately (false) without blocking on any mutex. Confirms
// the data path is decoupled from g.mu.
func TestTrySendReq_NoMutexContention(t *testing.T) {
	s := NewServer(0, "")
	g := s.getGroup("cg-no-contention", true)

	// Pre-fill so the buffer has stuff to drain (gives shutdown some
	// work to do under g.mu in shutdownGroup's engine-cleanup path).
	for i := 0; i < 5; i++ {
		g.trySendReq(clientGroupReq{
			req:    RPCRequest{ID: float64(i), Method: "ping"},
			respCh: make(chan RPCResponse, 1),
		})
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.shutdownGroup(g)
	}()

	// trySendReq should NOT be gated by g.mu (which shutdownGroup may
	// be holding briefly for engine cleanup). It should observe done
	// closed via the select and return false promptly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		done := make(chan bool, 1)
		go func() {
			done <- g.trySendReq(clientGroupReq{
				req:    RPCRequest{ID: 999, Method: "ping"},
				respCh: make(chan RPCResponse, 1),
			})
		}()
		select {
		case <-done:
			// returned promptly — either true (race won) or false (done seen)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("trySendReq blocked on mutex contention with shutdownGroup")
		}
		// Spin a few times to catch any race window.
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
}
