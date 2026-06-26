//go:build libsqlite3

// Build tag: this file requires SQLite's sqlite3_wal2_coread_* symbols,
// which are conditionally compiled (SQLITE_ENABLE_WAL2_COREAD) and not
// present in mattn/go-sqlite3's default bundled SQLite. Production
// builds (Dockerfile) use `-tags libsqlite3` and link against our
// vendored rocicorp SQLite compiled with -DSQLITE_ENABLE_WAL2_COREAD, so
// the symbols are there. Local builds without the tag fall through to
// coread_stub.go which errors loudly if the caller tries to use coread.

package tablesource

// CoRead: CGO wrapper around SQLite's sqlite3_wal2_coread_* API. This is
// the wal2 counterpart to Snapshot — where sqlite3_snapshot_* is disabled
// in wal2 mode, the coread API lets K connections latch the SAME wal2
// frame an anchor read-transaction holds, eliminating cross-table skew
// without the converge-upward overhead.
//
// The C API:
//   sqlite3_wal2_coread_get(db, schema, **co)  — capture anchor's frame
//   sqlite3_wal2_coread_open(db, schema, *co)  — pin a new tx to that frame
//   sqlite3_wal2_coread_free(*co)              — release the heap-allocated handle
//
// The anchor read transaction MUST remain open for the lifetime of every
// co-located reader. The handle is a heap copy of the wal layer's WalCoRead
// capture; freeing it does NOT release the anchor's pinned frame.
//
// rawSQLiteHandle is defined in snapshot.go (same package, same build tag)
// and is reused here without redefinition.

/*
#cgo CFLAGS: -DSQLITE_ENABLE_WAL2_COREAD
#include <stdlib.h>
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_wal2_coread sqlite3_wal2_coread;
extern int sqlite3_wal2_coread_get(sqlite3*, const char*, sqlite3_wal2_coread**);
extern int sqlite3_wal2_coread_open(sqlite3*, const char*, sqlite3_wal2_coread*);
extern void sqlite3_wal2_coread_free(sqlite3_wal2_coread*);
*/
import "C"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// CoRead is an opaque handle to a wal2 co-located read point captured from
// an anchor read transaction. The originating *sql.Conn is held alive until
// Free so its read-tx (which anchors the WAL frame) doesn't close out from
// under the coread. Free MUST be called exactly once.
//
// mu serializes OpenAtCoRead vs Free so a concurrent Free can't yank the C
// handle out from under an in-flight sqlite3_wal2_coread_open. Without this
// lock the race is a use-after-free at the C boundary.
type CoRead struct {
	mu     sync.Mutex
	handle *C.sqlite3_wal2_coread
	conn   *sql.Conn // originating conn — kept open to anchor the frame
	tx     *sql.Tx   // originating read tx — rolled back on Free
	freed  bool
}

// CaptureCoRead opens a fresh read tx on db, warms it with a SELECT 1 so
// SQLite assigns a wal2 read point, then captures that point as a CoRead
// handle. The returned CoRead owns the conn + tx and releases both on Free.
//
// The caller uses this handle to arm many OTHER connections at the same
// frame via OpenAtCoRead. The originating conn/tx exists only to anchor the
// WAL frame; we do not run queries on it.
func CaptureCoRead(ctx context.Context, db *sql.DB) (*CoRead, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("CaptureCoRead: conn: %w", err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("CaptureCoRead: BeginTx: %w", err)
	}
	// Trigger an actual read so SQLite has assigned a snapshot to this
	// tx — sqlite3_wal2_coread_get returns SQLITE_ERROR otherwise.
	// SELECT 1 is a no-op constant; sqlite_master always exists so this
	// forces a real WAL read-point assignment.
	var warm int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master").Scan(&warm); err != nil {
		tx.Rollback()
		conn.Close()
		return nil, fmt.Errorf("CaptureCoRead: warm tx: %w", err)
	}

	var handle *C.sqlite3_wal2_coread
	err = conn.Raw(func(driverConn any) error {
		raw, err := rawSQLiteHandle(driverConn)
		if err != nil {
			return err
		}
		schema := C.CString("main")
		defer C.free(unsafe.Pointer(schema))
		rc := C.sqlite3_wal2_coread_get(raw, schema, &handle)
		if rc != 0 {
			return fmt.Errorf("sqlite3_wal2_coread_get rc=%d", int(rc))
		}
		return nil
	})
	if err != nil {
		tx.Rollback()
		conn.Close()
		return nil, fmt.Errorf("CaptureCoRead: %w", err)
	}
	return &CoRead{handle: handle, conn: conn, tx: tx}, nil
}

// OpenAtCoRead begins a new deferred read tx on a fresh conn and arms it
// onto cr's wal2 co-located read point. Caller owns the returned tx + conn
// and must release them when done (Rollback the tx, Close the conn).
//
// The coread_open C API requires autoCommit==0 (BEGIN active) and
// txnState==NONE (no read lock taken yet). The deferred BeginTx we issue
// satisfies both: it sets autoCommit=0 without taking a read lock, then
// coread_open arms the capture and begins the read transaction at the
// anchor's frame.
//
// Calling OpenAtCoRead from many goroutines concurrently is safe — each
// call gets its own conn + tx, and the coread handle is read-only to SQLite.
func OpenAtCoRead(ctx context.Context, db *sql.DB, cr *CoRead) (*sql.Tx, *sql.Conn, error) {
	if cr == nil {
		return nil, nil, errors.New("OpenAtCoRead: coread is nil")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("OpenAtCoRead: conn: %w", err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("OpenAtCoRead: BeginTx: %w", err)
	}
	err = conn.Raw(func(driverConn any) error {
		raw, err := rawSQLiteHandle(driverConn)
		if err != nil {
			return err
		}
		schema := C.CString("main")
		defer C.free(unsafe.Pointer(schema))
		// Hold mu across the freed check + C call so a concurrent Free
		// can't release the handle mid-call (use-after-free at the C
		// boundary). The C call itself is fast.
		cr.mu.Lock()
		defer cr.mu.Unlock()
		if cr.freed {
			return errors.New("OpenAtCoRead: coread is freed")
		}
		rc := C.sqlite3_wal2_coread_open(raw, schema, cr.handle)
		if rc != 0 {
			return fmt.Errorf("sqlite3_wal2_coread_open rc=%d", int(rc))
		}
		return nil
	})
	if err != nil {
		tx.Rollback()
		conn.Close()
		return nil, nil, err
	}
	return tx, conn, nil
}

// Free releases the coread handle and the anchoring conn+tx. After Free,
// SQLite is free to checkpoint past the anchor's frame; reads pinned to
// this coread via OpenAtCoRead keep working only because their own
// read-tx holds the frame for them.
//
// Idempotent — second call is a no-op.
func (cr *CoRead) Free() {
	if cr == nil {
		return
	}
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.freed {
		return
	}
	cr.freed = true
	if cr.handle != nil {
		C.sqlite3_wal2_coread_free(cr.handle)
		cr.handle = nil
	}
	if cr.tx != nil {
		_ = cr.tx.Rollback()
		cr.tx = nil
	}
	if cr.conn != nil {
		_ = cr.conn.Close()
		cr.conn = nil
	}
}

// CaptureCoReadFromConn captures a coread handle from an existing conn that
// already has an active read transaction (e.g., the snapshotter's curr conn).
// The returned CoRead does NOT own the conn/tx — Free only frees the C handle.
// The caller (buildReaderPoolLocked) is responsible for ensuring the anchor
// conn stays alive for the lifetime of every co-located reader.
func CaptureCoReadFromConn(conn *sql.Conn) (*CoRead, error) {
	var handle *C.sqlite3_wal2_coread
	err := conn.Raw(func(driverConn any) error {
		raw, err := rawSQLiteHandle(driverConn)
		if err != nil {
			return err
		}
		schema := C.CString("main")
		defer C.free(unsafe.Pointer(schema))
		rc := C.sqlite3_wal2_coread_get(raw, schema, &handle)
		if rc != 0 {
			return fmt.Errorf("sqlite3_wal2_coread_get rc=%d", int(rc))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &CoRead{handle: handle}, nil
}

// armConn arms a conn that has already issued a deferred BEGIN (autoCommit=0,
// txnState=NONE) onto this coread's read point. After this call, the conn's
// read transaction is latched to the anchor's frame. This is the pool-builder
// variant of OpenAtCoRead: it skips BeginTx (the caller issues BEGIN directly
// on the conn, matching NewReaderPool's pattern) so the poolReader can use
// the conn without a *sql.Tx wrapper.
func (cr *CoRead) armConn(conn *sql.Conn) error {
	return conn.Raw(func(driverConn any) error {
		raw, err := rawSQLiteHandle(driverConn)
		if err != nil {
			return err
		}
		schema := C.CString("main")
		defer C.free(unsafe.Pointer(schema))
		cr.mu.Lock()
		defer cr.mu.Unlock()
		if cr.freed {
			return errors.New("armConn: coread is freed")
		}
		rc := C.sqlite3_wal2_coread_open(raw, schema, cr.handle)
		if rc != 0 {
			return fmt.Errorf("sqlite3_wal2_coread_open rc=%d", int(rc))
		}
		return nil
	})
}

// NewCoReadReaderPool opens k read connections from db, all armed onto the
// same wal2 co-located read point (cr). Unlike NewReaderPool (converge-upward),
// this requires no convergence — every reader is latched to the anchor's
// frame by the coread handle. The caller MUST ensure the anchor (whose conn
// produced cr) stays alive for the pool's lifetime.
func NewCoReadReaderPool(ctx context.Context, db *sql.DB, cr *CoRead, k int) (*ReaderPool, error) {
	if db == nil {
		return nil, fmt.Errorf("tablesource.NewCoReadReaderPool: db is nil")
	}
	if k < 1 {
		k = 1
	}

	readers := make([]*poolReader, k)
	var version string
	for i := 0; i < k; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			for j := 0; j < i; j++ {
				readers[j].close(ctx)
			}
			return nil, fmt.Errorf("coread reader pool: acquire conn %d: %w", i, err)
		}
		// Deferred BEGIN: sets autoCommit=0 without taking a read lock
		// (txnState stays NONE), so coread_open can arm + begin the read.
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			_ = conn.Close()
			for j := 0; j < i; j++ {
				readers[j].close(ctx)
			}
			return nil, fmt.Errorf("coread reader pool: BEGIN conn %d: %w", i, err)
		}
		// Arm the conn onto the coread's frame. This latches the read tx
		// at the anchor's frame inside walTryBeginRead.
		if err := cr.armConn(conn); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
			_ = conn.Close()
			for j := 0; j < i; j++ {
				readers[j].close(ctx)
			}
			return nil, fmt.Errorf("coread reader pool: arm conn %d: %w", i, err)
		}
		var ver string
		if err := conn.QueryRowContext(ctx, stateVersionSQL).Scan(&ver); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
			_ = conn.Close()
			for j := 0; j < i; j++ {
				readers[j].close(ctx)
			}
			return nil, fmt.Errorf("coread reader pool: read stateVersion conn %d: %w", i, err)
		}
		readers[i] = &poolReader{conn: conn, stmts: map[string]*sql.Stmt{}}
		if i == 0 {
			version = ver
		} else if ver != version {
			for _, r := range readers {
				if r != nil {
					r.close(ctx)
				}
			}
			return nil, fmt.Errorf("coread reader pool: version mismatch conn %d (%q vs %q)", i, ver, version)
		}
	}
	p := &ReaderPool{
		free:    make(chan *poolReader, k),
		all:     readers,
		version: version,
	}
	for _, r := range readers {
		p.free <- r
	}
	return p, nil
}
