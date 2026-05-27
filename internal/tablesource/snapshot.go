//go:build libsqlite3

// Build tag: this file requires SQLite's sqlite3_snapshot_* symbols,
// which are conditionally compiled (SQLITE_ENABLE_SNAPSHOT) and not
// present in mattn/go-sqlite3's default bundled SQLite. Production
// builds (Dockerfile) use `-tags libsqlite3` and link against our
// vendored rocicorp SQLite compiled with -DSQLITE_ENABLE_SNAPSHOT, so
// the symbols are there. Local builds without the tag fall through to
// snapshot_stub.go which errors loudly if Source tries to capture.

package tablesource

// Snapshot: CGO wrapper around SQLite's sqlite3_snapshot_* API. Lets us
// pin many connections to the same WAL frame so concurrent goroutines
// can read consistent data without sharing a *sql.Tx (which database/sql
// requires us to serialize). The Phase 4 design promised snapshot
// consistency; this wrapper is how we keep that promise while reclaiming
// the goroutine parallelism that the Go sidecar exists for.
//
// The C API:
//   sqlite3_snapshot_get(db, schema, **snap)  — capture current WAL frame
//   sqlite3_snapshot_open(db, schema, *snap)  — pin a new tx to that frame
//   sqlite3_snapshot_free(*snap)              — release; until then SQLite
//                                                cannot checkpoint past
//                                                the named frame
//
// We forward-declare only the symbols we need — sqlite3 and
// sqlite3_snapshot are opaque pointers at this layer. Linking is whatever
// the rest of the binary uses (mattn's bundled SQLite locally, rocicorp's
// patched amalgamation in the production Dockerfile).

/*
#cgo CFLAGS: -DSQLITE_ENABLE_SNAPSHOT
#include <stdlib.h>
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_snapshot sqlite3_snapshot;
extern int sqlite3_snapshot_get(sqlite3*, const char*, sqlite3_snapshot**);
extern int sqlite3_snapshot_open(sqlite3*, const char*, sqlite3_snapshot*);
extern void sqlite3_snapshot_free(sqlite3_snapshot*);
*/
import "C"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// Snapshot is an opaque handle to a pinned SQLite WAL frame. The
// originating *sql.Conn is held alive until Free so its read-tx (which
// is what actually anchors the WAL frame) doesn't close out from under
// the snapshot. Free MUST be called exactly once or the WAL will grow
// without bound — SQLite can't checkpoint past a referenced frame.
type Snapshot struct {
	handle  *C.sqlite3_snapshot
	conn    *sql.Conn // originating conn — kept open to anchor the frame
	tx      *sql.Tx   // originating read tx — committed on Free
	freed   bool
}

// CaptureSnapshot opens a fresh read tx on db, captures the current WAL
// frame as a Snapshot, and returns it. The returned Snapshot owns the
// conn + tx and releases both on Free.
//
// The caller is expected to use this snapshot's handle as the pin point
// for many OTHER connections (via OpenAtSnapshot). The originating
// conn/tx held here exists only to anchor the WAL frame; we do not run
// queries on it.
func CaptureSnapshot(ctx context.Context, db *sql.DB) (*Snapshot, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("CaptureSnapshot: conn: %w", err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("CaptureSnapshot: BeginTx: %w", err)
	}
	// Trigger an actual read so SQLite has assigned a snapshot to this
	// tx — `sqlite3_snapshot_get` returns SQLITE_ERROR otherwise.
	if _, err := tx.ExecContext(ctx, "SELECT 1"); err != nil {
		tx.Rollback()
		conn.Close()
		return nil, fmt.Errorf("CaptureSnapshot: warm tx: %w", err)
	}

	var handle *C.sqlite3_snapshot
	err = conn.Raw(func(driverConn any) error {
		raw, err := rawSQLiteHandle(driverConn)
		if err != nil {
			return err
		}
		schema := C.CString("main")
		defer C.free(unsafe.Pointer(schema))
		rc := C.sqlite3_snapshot_get(raw, schema, &handle)
		if rc != 0 {
			return fmt.Errorf("sqlite3_snapshot_get rc=%d", int(rc))
		}
		return nil
	})
	if err != nil {
		tx.Rollback()
		conn.Close()
		return nil, fmt.Errorf("CaptureSnapshot: %w", err)
	}
	return &Snapshot{handle: handle, conn: conn, tx: tx}, nil
}

// OpenAtSnapshot begins a new read tx on a fresh conn pinned to snap's
// WAL frame. Caller owns the returned tx + conn and must release them
// when done (Rollback the tx, Close the conn).
//
// Calling OpenAtSnapshot from many goroutines concurrently is safe —
// each call gets its own conn + tx (no shared state), and the snap
// handle itself is treated as read-only by SQLite.
func OpenAtSnapshot(ctx context.Context, db *sql.DB, snap *Snapshot) (*sql.Tx, *sql.Conn, error) {
	if snap == nil || snap.freed {
		return nil, nil, errors.New("OpenAtSnapshot: snapshot is nil or freed")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("OpenAtSnapshot: conn: %w", err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("OpenAtSnapshot: BeginTx: %w", err)
	}
	err = conn.Raw(func(driverConn any) error {
		raw, err := rawSQLiteHandle(driverConn)
		if err != nil {
			return err
		}
		schema := C.CString("main")
		defer C.free(unsafe.Pointer(schema))
		rc := C.sqlite3_snapshot_open(raw, schema, snap.handle)
		if rc != 0 {
			return fmt.Errorf("sqlite3_snapshot_open rc=%d", int(rc))
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

// Free releases the snapshot handle and the anchoring conn+tx. After
// Free, SQLite is free to checkpoint past the named frame; reads pinned
// to this snapshot via OpenAtSnapshot keep working only because their
// own read-tx holds the frame for them.
//
// Idempotent — second call is a no-op.
func (s *Snapshot) Free() {
	if s == nil || s.freed {
		return
	}
	s.freed = true
	if s.handle != nil {
		C.sqlite3_snapshot_free(s.handle)
		s.handle = nil
	}
	if s.tx != nil {
		_ = s.tx.Rollback()
		s.tx = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

// rawSQLiteHandle reads mattn's unexported SQLiteConn.db field. mattn
// doesn't export its raw *C.sqlite3 because they want to discourage
// reaching past the driver API; we have to because we need a SQLite C
// API (snapshot_open) that database/sql can't model. Pinned mattn version
// in go.mod keeps the field layout stable across upgrades.
//
// Returns an error rather than panicking if the field isn't there so a
// future mattn breaking-change surfaces as a clean Open failure with a
// diagnostic message, not a runtime crash.
func rawSQLiteHandle(driverConn any) (*C.sqlite3, error) {
	c, ok := driverConn.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, fmt.Errorf("rawSQLiteHandle: not a mattn *SQLiteConn (got %T)", driverConn)
	}
	v := reflect.ValueOf(c).Elem().FieldByName("db")
	if !v.IsValid() {
		return nil, errors.New("rawSQLiteHandle: SQLiteConn.db field not found (mattn struct changed?)")
	}
	// v is a *C.sqlite3 (unexported). Read its pointer value via
	// unsafe; reflect.Value.Pointer() works for pointer-kind values.
	return (*C.sqlite3)(unsafe.Pointer(v.Pointer())), nil
}
