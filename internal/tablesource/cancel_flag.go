package tablesource

/*
#include <stdlib.h>
#include <string.h>

typedef struct sqlite3 sqlite3;

// goivm_cancel_flag is the C-allocated per-conn cancel state. It lives
// in C memory (C.malloc'd) to satisfy cgo pointer rules.
typedef struct {
	volatile int cancel;  // set to 1 by Go to request cancellation
	int budget;            // remaining opcode budget (-1 = unlimited)
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
	// Check opcode budget (gas meter — D1)
	if (f->budget >= 0) {
		if (f->budget == 0) return 1;
		f->budget--;
	}
	return 0;
}

extern void sqlite3_progress_handler(sqlite3*, int, int(*)(void*), void*);
extern void sqlite3_interrupt(sqlite3*);
*/
import "C"

import (
	"sync/atomic"
	"unsafe"
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

	// db, when non-nil, holds the raw *C.sqlite3 for this conn, used
	// to call sqlite3_interrupt on cancel (covers busy-wait sleeps
	// that the progress handler can't reach — C2).
	db unsafe.Pointer
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
// in-flight sqlite3_step is using the flag.
func (f *connCancelFlag) Free() {
	if f.cflag != nil {
		// Detach the progress handler before freeing so a stale
		// callback can't read freed memory.
		if f.db != nil {
			C.sqlite3_progress_handler((*C.sqlite3)(f.db), 0, nil, nil)
		}
		C.free(unsafe.Pointer(f.cflag))
		f.cflag = nil
	}
}

// setCancel marks the flag as cancelled with the given reason, and
// calls sqlite3_interrupt to break out of busy-wait sleeps (C2).
// Safe to call from any goroutine.
func (f *connCancelFlag) setCancel(reason CancelReason) {
	if f.cflag == nil {
		return
	}
	f.reason.Store(int32(reason))
	atomic.StoreInt32((*int32)(unsafe.Pointer(&f.cflag.cancel)), 1)
	if f.db != nil {
		C.sqlite3_interrupt((*C.sqlite3)(f.db))
	}
}

// clearCancel resets the flag for a new operation on this conn.
// Called at acquire time when the conn is bound to a new pipeline.
func (f *connCancelFlag) clearCancel() {
	if f.cflag == nil {
		return
	}
	f.reason.Store(int32(cancelNone))
	atomic.StoreInt32((*int32)(unsafe.Pointer(&f.cflag.cancel)), 0)
}

// setBudget sets the opcode budget (D1 — gas meter). -1 = unlimited.
func (f *connCancelFlag) setBudget(opcodes int32) {
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
	f.db = unsafe.Pointer(db)
	C.sqlite3_progress_handler(db, C.int(progressN),
		(*[0]byte)(C.goivm_progress_cb), unsafe.Pointer(f.cflag))
}

// detachProgressHandler removes the progress handler. Called before
// cleanup/recovery SQL so the handler can't abort the cleanup itself (C3).
func (f *connCancelFlag) detachProgressHandler() {
	if f.cflag == nil || f.db == nil {
		return
	}
	C.sqlite3_progress_handler((*C.sqlite3)(f.db), 0, nil, nil)
	f.db = nil
}

// progressN is the opcode interval between progress handler invocations.
// 4096 balances overhead (~250ns per 1M opcodes) against cancel latency
// (~4096 opcodes = microseconds).
const progressN = 4096

// defaultBudget is the generous default opcode budget for hydrate-path
// queries. Calibrated to ~30-60s of compute on production hardware.
// -1 means unlimited (only for cleanup/admin queries that must not be
// interrupted).
const defaultBudget int32 = 50_000_000 // ~50M opcodes ≈ 30-60s
