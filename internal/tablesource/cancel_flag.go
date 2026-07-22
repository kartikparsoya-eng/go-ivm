package tablesource

/*
#include <stdlib.h>
#include <string.h>

typedef struct sqlite3 sqlite3;

// GOIVM_PROGRESS_N is the opcode interval between progress handler
// invocations. Must match the Go progressN constant. Used in the C
// callback to correctly decrement the budget by the number of opcodes
// actually consumed between callbacks (D1 gas meter fix).
#define GOIVM_PROGRESS_N 4096

// goivm_cancel_flag is the C-allocated per-conn cancel state. It lives
// in C memory (C.malloc'd) to satisfy cgo pointer rules.
typedef struct {
	volatile int cancel;  // set to 1 by Go to request cancellation
	volatile int budget;  // remaining opcode budget (-1 = unlimited)
} goivm_cancel_flag;

// goivm_progress_cb is the C-side progress handler callback registered
// via sqlite3_progress_handler. SQLite invokes it every N VM opcodes
// during sqlite3_step. It reads a C-allocated cancel flag (never crosses
// into Go) and returns nonzero to abort the running statement
// (SQLITE_INTERRUPT).
//
// The flag is per-conn C memory (C.malloc'd, not a Go pointer) so it
// satisfies the cgo rule that C may not retain a Go pointer after a call
// returns. Go writes to it via atomic.StoreInt32; C reads it via a plain
// volatile load — the progress handler runs on the same OS thread as the
// sqlite3_step caller, so there's no need for cross-thread synchronization
// on the read side (the flag is set from a different goroutine, but the
// atomic store is visible to the C load because cgo entersyscall/exitsyscall
// imply the necessary memory barriers).
//
// D1 — gas meter: the same callback also enforces a per-statement opcode
// budget. The remaining budget is decremented on every invocation; when
// it hits zero, the statement aborts identically to a cancel. This makes
// unbounded scans inexpressible — no statement can execute more than
// maxOpcodes VM opcodes, ever, regardless of whether anyone knew to
// cancel. The budget is set per-fetch from the advance's economic budget
// or a generous default for hydrate.
int goivm_progress_cb(void *p) {
	if (p == NULL) return 0;
	goivm_cancel_flag *f = (goivm_cancel_flag*)p;
	// Check cancel flag (volatile read — set by Go via atomic store)
	if (f->cancel) return 1;
	// Check opcode budget (gas meter — D1). The callback fires every
	// GOIVM_PROGRESS_N opcodes, so decrement by that amount to keep the
	// budget in opcode units (not callback-count units). Without this,
	// defaultBudget=50M would bound ~200B opcodes (hours, not ~50s).
	if (f->budget >= 0) {
		if (f->budget < GOIVM_PROGRESS_N) return 1;
		f->budget -= GOIVM_PROGRESS_N;
	}
	return 0;
}

extern void sqlite3_progress_handler(sqlite3*, int, int(*)(void*), void*);
extern void sqlite3_interrupt(sqlite3*);
*/
import "C"

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/mattn/go-sqlite3"
)

// cancelReason classifies WHO set the cancel flag, so the Go-side error
// handling can map SQLITE_INTERRUPT to the right action instead of a
// generic -32000 pipeline reset (C4).
type CancelReason int32

const (
	cancelNone     CancelReason = 0
	CancelStream   CancelReason = 1 // client/stream cancel — quiet unwind
	CancelBudget   CancelReason = 2 // advance budget expired — typed economic abort
	CancelTeardown CancelReason = 3 // CG teardown — teardown path
	CancelWatchdog  CancelReason = 4 // watchdog force-cancel — like client cancel
)

// connCancelFlag wraps a C-allocated goivm_cancel_flag. One per conn
// (poolReader, Source writer conn, snapshotter frame conn). The flag is
// per-conn because conn use is serialized — the only statement that can
// observe a conn's flag is the current owner's. This avoids the lost-cancel
// race a per-CG flag would have (C1).
type connCancelFlag struct {
	cflag *C.goivm_cancel_flag

	// reason records WHO cancelled, for error classification (C4).
	// Written atomically alongside the cancel flag.
	reason atomic.Int32

	// db holds the raw *C.sqlite3 for this conn as a uintptr, accessed
	// atomically. Used to call sqlite3_interrupt on cancel (covers
	// busy-wait sleeps that the progress handler can't reach — C2).
	// L1 fix: atomic to prevent race between setCancel (lock-free) and
	// registerProgressHandler/detachProgressHandler (under s.mu).
	db atomic.Uintptr

	// mu guards the cflag pointer lifetime. setCancel/clearCancel/
	// setBudget/IsCancelled take RLock; Free takes Lock. This prevents
	// the UAF where setCancel reads cflag non-nil, then Free frees it,
	// then setCancel writes to freed C memory (N2 fix).
	mu sync.RWMutex
}

// newConnCancelFlag allocates a C cancel flag. Must be freed via Free.
func newConnCancelFlag() *connCancelFlag {
	cflag := (*C.goivm_cancel_flag)(C.malloc(C.size_t(unsafe.Sizeof(C.goivm_cancel_flag{}))))
	if cflag == nil {
		panic("tablesource: C.malloc failed for connCancelFlag")
	}
	C.memset(unsafe.Pointer(cflag), 0, C.size_t(unsafe.Sizeof(C.goivm_cancel_flag{})))
	cflag.budget = -1 // unlimited by default
	return &connCancelFlag{cflag: cflag}
}

// Free releases the C memory. Safe to call once; caller must ensure no
// in-flight sqlite3_step is using the flag. N2 fix: takes Lock so
// concurrent setCancel/clearCancel/setBudget callers see cflag=nil.
func (f *connCancelFlag) Free() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cflag != nil {
		// Detach the progress handler before freeing so a stale
		// callback can't read freed memory.
		if dbPtr := f.db.Load(); dbPtr != 0 {
			C.sqlite3_progress_handler((*C.sqlite3)(unsafe.Pointer(dbPtr)), 0, nil, nil)
		}
		C.free(unsafe.Pointer(f.cflag))
		f.cflag = nil
	}
}

// setCancel marks the flag as cancelled with the given reason, and
// calls sqlite3_interrupt to break out of busy-wait sleeps (C2).
// Safe to call from any goroutine. N2 fix: RLock synchronizes with Free.
func (f *connCancelFlag) setCancel(reason CancelReason) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.cflag == nil {
		return
	}
	f.reason.Store(int32(reason))
	atomic.StoreInt32((*int32)(unsafe.Pointer(&f.cflag.cancel)), 1)
	if dbPtr := f.db.Load(); dbPtr != 0 {
		C.sqlite3_interrupt((*C.sqlite3)(unsafe.Pointer(dbPtr)))
	}
}

// clearCancel resets the flag for a new operation on this conn.
// Called at acquire time when the conn is bound to a new pipeline.
func (f *connCancelFlag) clearCancel() {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.cflag == nil {
		return
	}
	f.reason.Store(int32(cancelNone))
	atomic.StoreInt32((*int32)(unsafe.Pointer(&f.cflag.cancel)), 0)
}

// setBudget sets the opcode budget (D1 — gas meter). -1 = unlimited.
func (f *connCancelFlag) setBudget(opcodes int32) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.cflag == nil {
		return
	}
	atomic.StoreInt32((*int32)(unsafe.Pointer(&f.cflag.budget)), opcodes)
}

// Reason returns the cancel reason (C4 — error classification).
func (f *connCancelFlag) Reason() CancelReason {
	return CancelReason(f.reason.Load())
}

// IsCancelled returns true if the flag is set.
func (f *connCancelFlag) IsCancelled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.cflag == nil {
		return false
	}
	return atomic.LoadInt32((*int32)(unsafe.Pointer(&f.cflag.cancel))) != 0
}

// registerProgressHandler installs the progress handler on a raw
// *C.sqlite3 conn. Called once when the conn is created/acquired. The
// handler checks the flag every progressN opcodes.
//
// progressN: 4096 is the sweet spot — ~250ns total overhead on a 1M-opcode
// query, cancel latency bounded at ~4096 opcodes (microseconds).
func (f *connCancelFlag) registerProgressHandler(db *C.sqlite3, progressN int) {
	if f.cflag == nil || db == nil {
		return
	}
	f.db.Store(uintptr(unsafe.Pointer(db)))
	C.sqlite3_progress_handler(db, C.int(progressN),
		(*[0]byte)(C.goivm_progress_cb), unsafe.Pointer(f.cflag))
}

// detachProgressHandler removes the progress handler. Called before
// cleanup/recovery SQL so the handler can't abort the cleanup itself (C3).
func (f *connCancelFlag) detachProgressHandler() {
	if f.cflag == nil {
		return
	}
	if dbPtr := f.db.Load(); dbPtr != 0 {
		C.sqlite3_progress_handler((*C.sqlite3)(unsafe.Pointer(dbPtr)), 0, nil, nil)
	}
	f.db.Store(0)
}

// progressN is the opcode interval between progress handler invocations.
// 4096 balances overhead (~250ns per 1M opcodes) against cancel latency
// (~4096 opcodes = microseconds). MUST match the C #define GOIVM_PROGRESS_N.
const progressN = 4096

// defaultBudget is the generous default opcode budget for hydrate-path
// queries. Calibrated to ~30-60s of compute on production hardware.
// The C callback decrements by GOIVM_PROGRESS_N per invocation, so this
// value is in true opcode units: 50M opcodes ≈ 50s at ~1M opcodes/sec.
// -1 means unlimited (only for cleanup/admin queries that must not be
// interrupted).
const defaultBudget int32 = 50_000_000

// IsInterruptError checks if the error is an SQLITE_INTERRUPT error
// (code 9). Used at fetch panic sites to map the cancel reason to the
// appropriate typed error (C4) instead of a generic -32000 panic.
func IsInterruptError(err error) bool {
	if err == nil {
		return false
	}
	if sqliteErr, ok := err.(sqlite3.Error); ok {
		return sqliteErr.Code == sqlite3.ErrInterrupt || sqliteErr.Code == 9
	}
	return false
}

// ErrBudgetCancelled is panicked by fetch sites when the progress handler
// aborts a query due to a CancelBudget reason. The sidecar maps this to
// rpcCodeAdvanceAborted (→ TS ResetPipelinesSignal('advancement-timeout'))
// — the same recovery as the economic advancement-abort.
var ErrBudgetCancelled = errors.New("advance budget cancelled via progress handler")

// ErrStreamCancelledByFlag is panicked by fetch sites when the progress
// handler aborts a query due to CancelStream or CancelTeardown. The
// sidecar maps this to engine.ErrStreamCancelled (→ clean close, no
// teardown).
var ErrStreamCancelledByFlag = errors.New("stream cancelled via progress handler")
