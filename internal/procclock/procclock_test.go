package procclock

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// spin burns the calling thread's CPU until its thread-CPU clock has
// advanced by d. Caller must be locked to its OS thread (Begin does this;
// direct callers LockOSThread themselves). Measuring the spin against the
// same clock the assertions read makes the tests deterministic — no
// wall-time assumptions about scheduler fairness.
func spin(d time.Duration) {
	start := ThreadCPUNS()
	if start < 0 {
		return
	}
	for ThreadCPUNS()-start < int64(d) { //nolint:revive // intentional hot loop
	}
}

func TestThreadCPUExcludesSleep(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	c0 := ThreadCPUNS()
	if c0 < 0 {
		t.Skip("CLOCK_THREAD_CPUTIME_ID unavailable on this platform")
	}
	spin(5 * time.Millisecond)
	c1 := ThreadCPUNS()
	if got := c1 - c0; got < int64(5*time.Millisecond) {
		t.Fatalf("5ms spin measured only %v of thread CPU", time.Duration(got))
	}
	// The core contract: blocked time is NOT processing time. Generous
	// upper bound tolerates timer/GC noise on the sleeping thread.
	time.Sleep(60 * time.Millisecond)
	c2 := ThreadCPUNS()
	if got := c2 - c1; got > int64(20*time.Millisecond) {
		t.Fatalf("60ms sleep leaked %v into thread CPU", time.Duration(got))
	}
}

func TestAccumulatorSumsConcurrentBrackets(t *testing.T) {
	var a Accumulator
	var wg sync.WaitGroup
	const workers = 4
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			end := a.Begin()
			defer end()
			spin(3 * time.Millisecond)
		}()
	}
	wg.Wait()
	// Serial-equivalent sum: 4 threads × 3ms of CPU each = ≥12ms even
	// though wall time is ~3ms on a multicore box.
	if got := a.ElapsedNS(); got < int64(workers)*int64(3*time.Millisecond) {
		t.Fatalf("%d×3ms brackets summed to %v", workers, time.Duration(got))
	}
}

func TestAccumulatorCheckpointPublishesMidBracket(t *testing.T) {
	var a Accumulator
	ready := make(chan struct{})
	release := make(chan struct{})
	go func() {
		end := a.Begin()
		defer end()
		spin(3 * time.Millisecond)
		a.Checkpoint()
		close(ready)
		<-release
	}()
	<-ready
	// The bracket is still open; the Checkpoint must have published the
	// spin already (this is what keeps abort checks fresh mid-advance).
	if got := a.ElapsedNS(); got < int64(3*time.Millisecond) {
		t.Fatalf("mid-bracket Checkpoint published only %v", time.Duration(got))
	}
	close(release)
}

func TestAccumulatorSleepInsideBracketNotCounted(t *testing.T) {
	var a Accumulator
	end := a.Begin()
	spin(2 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	end()
	got := a.ElapsedNS()
	if got < int64(2*time.Millisecond) {
		t.Fatalf("bracket lost the spin: %v", time.Duration(got))
	}
	if got > int64(15*time.Millisecond) {
		t.Fatalf("bracket counted sleep as processing: %v", time.Duration(got))
	}
}

func TestAccumulatorNilSafe(t *testing.T) {
	var a *Accumulator
	end := a.Begin()
	a.Checkpoint()
	if a.ElapsedNS() != 0 {
		t.Fatal("nil accumulator must read 0")
	}
	end()
}

func TestAccumulatorSlotExhaustionFallsBack(t *testing.T) {
	var a Accumulator
	const n = maxSlots + 4 // force ≥4 unregistered fallback brackets
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			end := a.Begin()
			defer end()
			<-start // hold every bracket open concurrently
			spin(500 * time.Microsecond)
		}()
	}
	close(start)
	wg.Wait()
	if got := a.ElapsedNS(); got < int64(n)*int64(500*time.Microsecond) {
		t.Fatalf("%d brackets (incl. slot-exhausted fallbacks) summed to %v", n, time.Duration(got))
	}
}
