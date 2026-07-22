//go:build cgo

package snapshotter

// RegisterProgressHandler installs a sqlite3_progress_handler on the
// snapshotter's frame connection so that stuck snapshotter queries
// (e.g., ChangesSince scanning a very large changeLog) are cancellable
// via sqlite3_interrupt — the same mechanism the reader pool and
// Source prevConn use (see tablesource/cancel_flag.go).
//
// The snapshotter's conn is used for beginAndPin, resetToHead,
// ChangesSince, and GetRow. All are typically instant, but under a
// pathological changeLog (millions of rows changed in one version),
// ChangesSince can be a long scan with no cancellation path.
//
// This file uses the same C-allocated cancel flag pattern as
// tablesource/cancel_flag.go: a malloc'd struct with a cancel int and
// a budget int, checked by a C callback every N opcodes.

/*
#include <stdlib.h>
#include <string.h>

typedef struct sqlite3 sqlite3;

typedef struct {
	volatile int cancel;
	volatile int budget;
} snap_cancel_flag;

#define SNAP_PROGRESS_N 4096

int snap_progress_cb(void *p) {
	if (p == NULL) return 0;
	snap_cancel_flag *f = (snap_cancel_flag*)p;
	if (f->cancel) return 1;
	if (f->budget >= 0) {
		if (f->budget < SNAP_PROGRESS_N) return 1;
		f->budget -= SNAP_PROGRESS_N;
	}
	return 0;
}

extern void sqlite3_progress_handler(sqlite3*, int, int(*)(void*), void*);
extern void sqlite3_interrupt(sqlite3*);
*/
import "C"

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// snapCancelFlag wraps a C-allocated snap_cancel_flag for one
// snapshotter connection. The flag lives in C memory to satisfy cgo
// pointer rules.
type snapCancelFlag struct {
	cflag *C.snap_cancel_flag
	// L1 fix: atomic to prevent race between setCancel and registerOn/Free.
	db atomic.Uintptr
	// N2 fix: RWMutex synchronizes setCancel/clearCancel/setBudget with Free.
	mu sync.RWMutex
}

// newSnapCancelFlag allocates a C cancel flag. Must be freed via Free.
func newSnapCancelFlag() *snapCancelFlag {
	cflag := (*C.snap_cancel_flag)(C.malloc(C.size_t(unsafe.Sizeof(C.snap_cancel_flag{}))))
	if cflag == nil {
		panic("snapshotter: C.malloc failed for snapCancelFlag")
	}
	C.memset(unsafe.Pointer(cflag), 0, C.size_t(unsafe.Sizeof(C.snap_cancel_flag{})))
	cflag.budget = -1
	return &snapCancelFlag{cflag: cflag}
}

// Free releases the C memory and detaches the progress handler.
// N2 fix: takes Lock so concurrent setCancel callers see cflag=nil.
func (f *snapCancelFlag) Free() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cflag == nil {
		return
	}
	if dbPtr := f.db.Load(); dbPtr != 0 {
		C.sqlite3_progress_handler((*C.sqlite3)(unsafe.Pointer(dbPtr)), 0, nil, nil)
	}
	C.free(unsafe.Pointer(f.cflag))
	f.cflag = nil
}

// setCancel sets the cancel flag and calls sqlite3_interrupt.
// N2 fix: RLock synchronizes with Free.
func (f *snapCancelFlag) setCancel() {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.cflag == nil {
		return
	}
	atomic.StoreInt32((*int32)(unsafe.Pointer(&f.cflag.cancel)), 1)
	if dbPtr := f.db.Load(); dbPtr != 0 {
		C.sqlite3_interrupt((*C.sqlite3)(unsafe.Pointer(dbPtr)))
	}
}

// clearCancel resets the flag for a new operation.
func (f *snapCancelFlag) clearCancel() {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.cflag == nil {
		return
	}
	atomic.StoreInt32((*int32)(unsafe.Pointer(&f.cflag.cancel)), 0)
}

// setBudget sets the opcode budget. -1 = unlimited.
func (f *snapCancelFlag) setBudget(opcodes int32) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.cflag == nil {
		return
	}
	atomic.StoreInt32((*int32)(unsafe.Pointer(&f.cflag.budget)), opcodes)
}

// registerOn installs the progress handler on the given *sql.Conn by
// reaching through to mattn's raw *C.sqlite3 handle.
func (f *snapCancelFlag) registerOn(conn *sql.Conn) error {
	return conn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("snapCancelFlag: not a mattn *SQLiteConn (got %T)", driverConn)
		}
		db := (*C.sqlite3)(unsafe.Pointer(c.RawDB()))
		f.db.Store(uintptr(unsafe.Pointer(db)))
		C.sqlite3_progress_handler(db, C.int(C.SNAP_PROGRESS_N),
			(*[0]byte)(C.snap_progress_cb), unsafe.Pointer(f.cflag))
		return nil
	})
}

// defaultSnapBudget is the opcode budget for snapshotter queries.
// Generous — snapshotter queries are typically instant, but
// ChangesSince under a bulk import can be a large scan.
const defaultSnapBudget int32 = 50_000_000
