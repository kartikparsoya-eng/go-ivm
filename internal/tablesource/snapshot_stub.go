//go:build !libsqlite3

// Stub Snapshot impl for default builds (no libsqlite3 tag). mattn's
// bundled SQLite doesn't compile with SQLITE_ENABLE_SNAPSHOT, so the
// symbols sqlite3_snapshot_* are unresolved — we can't even link a
// real implementation against it. Default builds therefore use this
// stub, which returns an error from every call. Source's first
// CaptureSnapshot will fail and the caller (handleInit or first Fetch)
// surfaces a clean error.
//
// Production binaries use -tags libsqlite3 + vendored rocicorp SQLite
// compiled with -DSQLITE_ENABLE_SNAPSHOT, which activates snapshot.go.

package tablesource

import (
	"context"
	"database/sql"
	"errors"
)

// Snapshot is the same shape as in the real implementation so callers
// can compile against either. All real state stays nil.
type Snapshot struct{}

var errSnapshotUnavailable = errors.New(
	"tablesource: snapshot API requires the libsqlite3 build tag " +
		"(rocicorp SQLite with SQLITE_ENABLE_SNAPSHOT) — default mattn " +
		"build doesn't expose it",
)

// CaptureSnapshot always errors in the stub.
func CaptureSnapshot(_ context.Context, _ *sql.DB) (*Snapshot, error) {
	return nil, errSnapshotUnavailable
}

// OpenAtSnapshot always errors in the stub. Source's Fetch path checks
// for this and falls back to a non-pinned read on the pool.
func OpenAtSnapshot(_ context.Context, _ *sql.DB, _ *Snapshot) (*sql.Tx, *sql.Conn, error) {
	return nil, nil, errSnapshotUnavailable
}

// Free is a no-op on the stub.
func (s *Snapshot) Free() {}
