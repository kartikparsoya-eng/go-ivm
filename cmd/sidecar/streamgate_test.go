package main

// Unit tests for streamgate.go (DESIGN-duplex-streaming §6 step 1):
// park/grant/cancel/idle/concurrent semantics, all -race-clean. These run
// with the ordinary test toolchain — no napilib tag, no addon.

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// acquireResult runs gate.acquire on its own goroutine and reports the
// result on a channel, so tests can assert park/unpark timing.
func acquireAsync(g *streamGate) <-chan bool {
	ch := make(chan bool, 1)
	go func() { ch <- g.acquire() }()
	return ch
}

func TestStreamGateAcquireConsumesCredit(t *testing.T) {
	g := newStreamGate(nil)
	g.grant(2)
	if !g.acquire() {
		t.Fatal("acquire with credit available returned false")
	}
	if !g.acquire() {
		t.Fatal("second acquire returned false")
	}
	g.mu.Lock()
	credit := g.credit
	g.mu.Unlock()
	if credit != 0 {
		t.Fatalf("credit = %d after consuming both grants, want 0", credit)
	}
}

func TestStreamGateParksAtZeroAndUnparksOnGrant(t *testing.T) {
	g := newStreamGate(nil)
	ch := acquireAsync(g)
	select {
	case r := <-ch:
		t.Fatalf("acquire returned %v while credit==0; should have parked", r)
	case <-time.After(50 * time.Millisecond):
		// parked, as designed
	}
	g.grant(1)
	select {
	case r := <-ch:
		if !r {
			t.Fatal("acquire returned false after grant; want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquire still parked after grant")
	}
}

func TestStreamGateCancelUnparksWithFalse(t *testing.T) {
	g := newStreamGate(nil)
	ch := acquireAsync(g)
	time.Sleep(20 * time.Millisecond) // let it park
	g.cancel()
	select {
	case r := <-ch:
		if r {
			t.Fatal("acquire returned true after cancel; want false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquire still parked after cancel")
	}
	// Cancelled gate refuses immediately, repeatedly (idempotent).
	if g.acquire() {
		t.Fatal("acquire on cancelled gate returned true")
	}
	g.cancel() // idempotent
	if g.acquire() {
		t.Fatal("acquire after second cancel returned true")
	}
}

func TestStreamGateGrantAfterCancelIsNoop(t *testing.T) {
	g := newStreamGate(nil)
	g.cancel()
	g.grant(5)
	if g.acquire() {
		t.Fatal("grant after cancel resurrected the gate")
	}
	g.mu.Lock()
	credit := g.credit
	g.mu.Unlock()
	if credit != 0 {
		t.Fatalf("credit = %d after post-cancel grant, want 0", credit)
	}
}

func TestStreamGateGrantIgnoresNonPositive(t *testing.T) {
	g := newStreamGate(nil)
	g.grant(0)
	g.grant(-3)
	g.mu.Lock()
	credit := g.credit
	g.mu.Unlock()
	if credit != 0 {
		t.Fatalf("credit = %d after non-positive grants, want 0", credit)
	}
}

func TestStreamGateTouchCalledOnGrant(t *testing.T) {
	var touches atomic.Int64
	g := newStreamGate(func() { touches.Add(1) })
	g.grant(1)
	g.grant(3)
	if got := touches.Load(); got != 2 {
		t.Fatalf("touch called %d times, want 2", got)
	}
	g.cancel()
	g.grant(1) // post-cancel grant must not touch (group may be tearing down)
	if got := touches.Load(); got != 2 {
		t.Fatalf("touch called %d times after post-cancel grant, want 2", got)
	}
}

// TestStreamGateConcurrentAccounting hammers acquire/grant from many
// goroutines: exactly `granted` acquires must succeed (credit conservation)
// and every producer must exit false after cancel. Run with -race.
func TestStreamGateConcurrentAccounting(t *testing.T) {
	g := newStreamGate(nil)
	const producers = 8
	const granted = 1000

	var succeeded atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for g.acquire() {
				succeeded.Add(1)
			}
		}()
	}
	// Grant from several goroutines in uneven chunks.
	var gw sync.WaitGroup
	for i := 0; i < 4; i++ {
		gw.Add(1)
		go func(n int64) {
			defer gw.Done()
			for j := int64(0); j < n; j += 10 {
				g.grant(10)
			}
		}(granted / 4)
	}
	gw.Wait()
	// Wait until all credit is consumed, then cancel to release producers.
	deadline := time.Now().Add(5 * time.Second)
	for {
		g.mu.Lock()
		credit := g.credit
		g.mu.Unlock()
		if credit == 0 && succeeded.Load() == granted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("credit not fully consumed: %d successes, %d credit left",
				succeeded.Load(), credit)
		}
		time.Sleep(time.Millisecond)
	}
	g.cancel()
	wg.Wait()
	if got := succeeded.Load(); got != granted {
		t.Fatalf("successful acquires = %d, want exactly %d", got, granted)
	}
}

func TestStreamGateIdleParked(t *testing.T) {
	g := newStreamGate(nil)
	now := time.Now()

	// No waiters: never idle-parked, no matter how stale lastGrant is.
	if g.idleParked(now.Add(time.Hour), time.Second) {
		t.Fatal("idleParked true with zero waiters")
	}

	ch := acquireAsync(g)
	waitForWaiters(t, g, 1)

	// Parked but within the idle window: not idle.
	if g.idleParked(now, time.Hour) {
		t.Fatal("idleParked true inside the idle window")
	}
	// Parked past the window: idle.
	if !g.idleParked(now.Add(2*time.Hour), time.Hour) {
		t.Fatal("idleParked false past the idle window with a parked producer")
	}
	// A grant refreshes the clock (and releases the waiter).
	g.grant(1)
	if r := <-ch; !r {
		t.Fatal("acquire returned false after grant")
	}
	if g.idleParked(time.Now(), time.Hour) {
		t.Fatal("idleParked true after grant released the waiter")
	}

	// Cancelled gates are never reported idle (already unwinding).
	g2 := newStreamGate(nil)
	ch2 := acquireAsync(g2)
	waitForWaiters(t, g2, 1)
	g2.cancel()
	<-ch2
	if g2.idleParked(time.Now().Add(time.Hour), time.Second) {
		t.Fatal("idleParked true on a cancelled gate")
	}
}

func waitForWaiters(t *testing.T, g *streamGate, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		w := g.waiters
		g.mu.Unlock()
		if w == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiters = %d, want %d", w, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// --- registry ---

func TestStreamGateRegistryRegisterRefusals(t *testing.T) {
	var r streamGateRegistry
	if g := r.register(math.NaN(), "cg", nil); g != nil {
		t.Fatal("register accepted a NaN reqID")
	}
	if g := r.register(1, "cg", nil); g == nil {
		t.Fatal("register refused a valid reqID")
	}
	if g := r.register(1, "cg", nil); g != nil {
		t.Fatal("register accepted a duplicate live reqID")
	}
	if r.size() != 1 {
		t.Fatalf("size = %d, want 1", r.size())
	}
}

func TestStreamGateRegistryGrantCancelUnknownNoop(t *testing.T) {
	var r streamGateRegistry
	r.grant(42, 10) // must not panic
	r.cancel(42)    // must not panic
	if r.size() != 0 {
		t.Fatalf("size = %d, want 0", r.size())
	}
}

func TestStreamGateRegistryGrantAndCancelRoute(t *testing.T) {
	var r streamGateRegistry
	g := r.register(7, "cg", nil)
	ch := acquireAsync(g)
	waitForWaiters(t, g, 1)
	r.grant(7, 1)
	if !<-ch {
		t.Fatal("registry grant did not release the parked producer")
	}
	ch2 := acquireAsync(g)
	waitForWaiters(t, g, 1)
	r.cancel(7)
	if <-ch2 {
		t.Fatal("registry cancel did not fail the parked producer")
	}
	// Entry survives cancel (handler owns removal).
	if r.size() != 1 {
		t.Fatalf("size = %d after cancel, want 1 (unregister is the handler's)", r.size())
	}
}

func TestStreamGateRegistryUnregisterCancelsParked(t *testing.T) {
	var r streamGateRegistry
	g := r.register(3, "cg", nil)
	ch := acquireAsync(g)
	waitForWaiters(t, g, 1)
	r.unregister(3)
	if <-ch {
		t.Fatal("unregister did not cancel the parked producer")
	}
	if r.size() != 0 {
		t.Fatalf("size = %d after unregister, want 0", r.size())
	}
	// reqID is reusable after unregister (fresh RPC, same counter cycle).
	if g2 := r.register(3, "cg", nil); g2 == nil {
		t.Fatal("register refused a reqID freed by unregister")
	}
}

func TestStreamGateRegistryCancelOwnerScoped(t *testing.T) {
	var r streamGateRegistry
	gA1 := r.register(1, "cgA", nil)
	gA2 := r.register(2, "cgA", nil)
	gB := r.register(3, "cgB", nil)

	chA1 := acquireAsync(gA1)
	chA2 := acquireAsync(gA2)
	chB := acquireAsync(gB)
	waitForWaiters(t, gA1, 1)
	waitForWaiters(t, gA2, 1)
	waitForWaiters(t, gB, 1)

	r.cancelOwner("cgA")
	if <-chA1 || <-chA2 {
		t.Fatal("cancelOwner(cgA) failed to cancel a cgA gate")
	}
	select {
	case <-chB:
		t.Fatal("cancelOwner(cgA) cancelled a cgB gate")
	case <-time.After(50 * time.Millisecond):
		// still parked — correct isolation
	}
	r.cancelAll()
	if <-chB {
		t.Fatal("cancelAll did not cancel the cgB gate")
	}
}

// TestStreamGateRegistryCancelOwnerIdentity pins the cross-generation
// safety choice: owners compare by IDENTITY, so tearing down a destroyed
// generation's group never cancels gates of a re-created group that reuses
// the same cgID string.
func TestStreamGateRegistryCancelOwnerIdentity(t *testing.T) {
	type fakeGroup struct{ id string }
	oldGen := &fakeGroup{id: "cg"}
	newGen := &fakeGroup{id: "cg"} // same id, different generation

	var r streamGateRegistry
	gNew := r.register(1, newGen, nil)
	ch := acquireAsync(gNew)
	waitForWaiters(t, gNew, 1)

	r.cancelOwner(oldGen) // old generation's teardown
	select {
	case <-ch:
		t.Fatal("old generation's teardown cancelled the new generation's gate")
	case <-time.After(50 * time.Millisecond):
	}
	r.cancelOwner(newGen)
	if <-ch {
		t.Fatal("cancelOwner(newGen) did not cancel its own gate")
	}
}

func TestStreamGateRegistrySweepIdle(t *testing.T) {
	var r streamGateRegistry
	idle := r.register(1, "cg", nil)
	fresh := r.register(2, "cg", nil)

	chIdle := acquireAsync(idle)
	chFresh := acquireAsync(fresh)
	waitForWaiters(t, idle, 1)
	waitForWaiters(t, fresh, 1)

	// Backdate the idle gate's lastGrant past the window; keep fresh's now.
	idle.mu.Lock()
	idle.lastGrant = time.Now().Add(-time.Hour)
	idle.mu.Unlock()

	n := r.sweepIdle(time.Now(), time.Minute)
	if n != 1 {
		t.Fatalf("sweepIdle cancelled %d gates, want 1", n)
	}
	if <-chIdle {
		t.Fatal("idle gate's producer returned true; want cancelled")
	}
	select {
	case <-chFresh:
		t.Fatal("sweepIdle cancelled a fresh gate")
	case <-time.After(50 * time.Millisecond):
	}
	fresh.cancel()
	<-chFresh
}
