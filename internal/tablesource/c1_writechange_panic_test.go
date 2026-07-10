package tablesource

// C1 regression test: if writeChangeLocked panics (e.g. nil-pointer from a
// cgo crash or an unexpected nil conn), the outer defer in genPushAndWrite
// must be able to re-acquire s.mu to clear the overlay.  Before the fix the
// inline Lock/Unlock pattern skipped Unlock on panic, deadlocking the
// non-reentrant mutex when the defer called Lock again — freezing all
// Push/Fetch/Close on that Source (and the entire engine, since advance
// holds e.mu).

import (
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// sabotageConnsOutput nils both s.externalConn and s.prevConn during fanout
// so that the subsequent writeChangeLocked gets nil from activeConn() →
// nil-pointer dereference in conn.PrepareContext → panic.
//
// The fanout runs WITHOUT s.mu held (genPushAndWrite releases it before
// fanout), so this assignment is safe on the serial path.  The drift check
// has already passed by this point (it used the cached checkExists stmt
// bound to the old conn pointer), so nilling the conns only affects
// writeChangeLocked.
type sabotageConnsOutput struct{ src *Source }

func (s *sabotageConnsOutput) Push(_ ivm.Change, _ ivm.InputBase) {
	s.src.externalConn = nil
	s.src.prevConn = nil
}

func TestC1_WriteChangePanicReleasesMutex(t *testing.T) {
	// Serial fanout so the sabotage runs on the same goroutine (no race).
	withFanoutKnobs(t, false, 0)

	src, db := newUserSource(t)
	defer db.Close()

	// Push #1 (benign): establishes prevConn + caches checkExists and
	// INSERT statements under prevConn's pointer.  The recording output
	// just swallows the fanout.
	in := src.Connect(nil, nil, nil, nil)
	in.SetOutput(&recordingOutput{})
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id":     float64(60),
		"name":   "first",
		"score":  float64(1),
		"active": true,
	}))

	// Point externalConn at the same conn as prevConn so activeConn()
	// returns externalConn (same pointer).  The drift check on the next
	// push will find the cached checkExists stmt under this pointer and
	// succeed without calling PrepareContext.
	src.mu.Lock()
	src.externalConn = src.prevConn
	src.mu.Unlock()

	// Wire the sabotage output for the next push.
	in.SetOutput(&sabotageConnsOutput{src: src})

	// Push #2:
	//  1. ensurePrevTxLocked → returns nil (externalConn non-nil)
	//  2. genPushAndWrite:
	//     a. driftCheckLocked → existsLocked → activeConn() returns
	//        externalConn (valid) → cached checkExists stmt → row doesn't
	//        exist → no drift → PASSES
	//     b. overlay set, s.mu released
	//     c. fanOut → sabotageConnsOutput.Push → nils both conns
	//     d. writeChangeLocked → activeConn() returns nil →
	//        pushStmtLocked(nil, insertSQL) → not cached under nil →
	//        conn.PrepareContext → nil-pointer PANIC
	//     e. closure's defer s.mu.Unlock() runs (THE FIX)
	//     f. outer defer's s.mu.Lock() succeeds (no deadlock)
	//     g. outer defer clears overlay
	var panicked any
	func() {
		defer func() { panicked = recover() }()
		src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
			"id":     float64(999),
			"name":   "c1test",
			"score":  float64(1),
			"active": true,
		}))
	}()

	if panicked == nil {
		t.Fatal("expected writeChangeLocked panic did not propagate")
	}

	// THE FIX: s.mu must be releasable after the panic.
	// With the old code, the outer defer's s.mu.Lock() would deadlock
	// because the panic skipped the inline s.mu.Unlock().
	locked := make(chan struct{})
	go func() {
		src.mu.Lock()
		close(locked)
		src.mu.Unlock()
	}()
	select {
	case <-locked:
	case <-time.After(3 * time.Second):
		t.Fatal("s.mu is still held after writeChangeLocked panic — self-deadlock (C1)")
	}

	// Overlay must be cleared by the outer defer.
	src.mu.Lock()
	stuck := src.overlay != nil
	src.mu.Unlock()
	if stuck {
		t.Fatal("overlay not cleared after writeChangeLocked panic")
	}
}
