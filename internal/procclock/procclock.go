// Package procclock measures per-OS-thread CPU time — the Go analog of a
// cooperative "processing time" clock (TS view-syncer Timer laps, which
// exclude event-loop queueing).
//
// Motivation (2026-07-06 ART finding): the sidecar's economic
// advancement-abort ported TS's formula but measured WALL time from handler
// entry. Under production load (8 in-process engines × concurrent CG
// advances per pod) scheduler queueing and GC inflated wall past the 50ms
// floor while each advance did ~15ms of real work — false aborts → reset →
// re-hydrate under the same load → reset storms. Per-thread CPU time is the
// measurement TS's number actually means: work performed, not time spent
// runnable-but-descheduled.
//
// Model: an advance's work happens on (a) the coordinator goroutine (the
// RPC handler driving the changelog feed + engine loop) and (b) bounded
// parallel fanout worker goroutines (tablesource parallel_fanout.go). Each
// participating goroutine registers with an Accumulator:
//
//	end := acc.Begin()   // LockOSThread + register + sample
//	... work ...
//	end()                // publish final delta + unregister + unlock
//
// and any registered thread can publish its in-progress slice with
// Checkpoint() (called by every abort check, so the calling thread's view
// is always fresh). ElapsedNS() is the serial-equivalent sum: scheduler
// wait, GC stop-the-world on other threads, and wg.Wait parking never
// accrue (a parked locked thread consumes no CPU); SQLite reads, operator
// compute, and GC assist on the participating threads do.
//
// Requirements: clock_gettime(CLOCK_THREAD_CPUTIME_ID) — Linux and
// macOS 10.12+ (both production targets; the repo is cgo-everywhere
// already via mattn/go-sqlite3).
package procclock

/*
#include <stdint.h>
#include <pthread.h>
#include <time.h>

static long long goivm_thread_cpu_ns(void) {
	struct timespec ts;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) {
		return -1;
	}
	return (long long)ts.tv_sec * 1000000000LL + (long long)ts.tv_nsec;
}

static unsigned long long goivm_thread_self(void) {
	return (unsigned long long)(uintptr_t)pthread_self();
}
*/
import "C"

import (
	"runtime"
	"sync/atomic"
)

// ThreadCPUNS returns the calling OS thread's consumed CPU time in
// nanoseconds, or a negative value if the clock is unavailable. For deltas
// between two calls to be meaningful the goroutine must be locked to its
// thread (runtime.LockOSThread) for the whole interval — Begin does this.
func ThreadCPUNS() int64 {
	return int64(C.goivm_thread_cpu_ns())
}

// threadSelf returns a comparable identity for the calling OS thread
// (pthread_self as an integer; a TLS read, no syscall). 0 is reserved as
// the free-slot sentinel — a real pthread_t is never 0 on linux/darwin.
func threadSelf() uint64 {
	return uint64(C.goivm_thread_self())
}

// maxSlots bounds concurrently registered threads. Production shape is
// 1 coordinator + GO_IVM_ADVANCE_PARALLELISM fanout workers (default 4); 32 leaves
// generous headroom. Overflow degrades gracefully: Begin falls back to an
// unregistered bracket whose delta still lands at end() — only mid-bracket
// Checkpoint freshness is lost for that thread.
const maxSlots = 32

type clockSlot struct {
	tid  atomic.Uint64 // owning thread identity; 0 = free
	last atomic.Int64  // thread CPU ns at the owner's last publish
}

// Accumulator sums processing CPU across participating threads. The zero
// value is ready to use. All methods are nil-receiver-safe (a nil
// *Accumulator is a no-op clock), so callers on unarmed paths pay nothing.
type Accumulator struct {
	total atomic.Int64
	slots [maxSlots]clockSlot
}

// Begin registers the calling goroutine's OS thread with the accumulator
// and starts accruing its CPU. It locks the goroutine to its thread; the
// returned end func publishes the final delta, unregisters, and unlocks.
// end must be called on the same goroutine (defer it).
func (a *Accumulator) Begin() func() {
	if a == nil {
		return func() {}
	}
	runtime.LockOSThread()
	start := ThreadCPUNS()
	if start < 0 {
		// Clock unavailable: keep the lock/unlock pairing, accrue nothing.
		return runtime.UnlockOSThread
	}
	if tid := threadSelf(); tid != 0 {
		for i := range a.slots {
			if a.slots[i].tid.Load() == 0 && a.slots[i].tid.CompareAndSwap(0, tid) {
				a.slots[i].last.Store(start)
				idx := i
				return func() {
					if end := ThreadCPUNS(); end >= 0 {
						if last := a.slots[idx].last.Load(); end > last {
							a.total.Add(end - last)
						}
					}
					a.slots[idx].tid.Store(0)
					runtime.UnlockOSThread()
				}
			}
		}
	}
	// Slots exhausted (or degenerate thread id): unregistered bracket.
	return func() {
		if end := ThreadCPUNS(); end > start {
			a.total.Add(end - start)
		}
		runtime.UnlockOSThread()
	}
}

// Checkpoint publishes the calling thread's in-progress CPU slice if the
// calling thread has an active Begin; otherwise it is a cheap no-op (one
// TLS read + a bounded scan). Only the owning thread ever rebases its
// slot's `last`, so the slot is single-writer.
func (a *Accumulator) Checkpoint() {
	if a == nil {
		return
	}
	tid := threadSelf()
	if tid == 0 {
		return
	}
	for i := range a.slots {
		if a.slots[i].tid.Load() == tid {
			now := ThreadCPUNS()
			if now < 0 {
				return
			}
			if last := a.slots[i].last.Load(); now > last {
				a.total.Add(now - last)
				a.slots[i].last.Store(now)
			}
			return
		}
	}
}

// ElapsedNS returns the published processing time in nanoseconds.
// Callers wanting the calling thread's in-progress slice included call
// Checkpoint() first. Monotonically non-decreasing.
func (a *Accumulator) ElapsedNS() int64 {
	if a == nil {
		return 0
	}
	return a.total.Load()
}
