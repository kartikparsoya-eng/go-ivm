//go:build !libsqlite3

// Stub CoRead impl for default builds (no libsqlite3 tag). mattn's
// bundled SQLite doesn't compile with SQLITE_ENABLE_WAL2_COREAD, so the
// symbols sqlite3_wal2_coread_* are unresolved — we can't even link a
// real implementation against it. Default builds therefore use this
// stub, which returns an error from every call.
//
// Production binaries use -tags libsqlite3 + vendored rocicorp SQLite
// compiled with -DSQLITE_ENABLE_WAL2_COREAD, which activates coread.go.

package tablesource

import (
	"context"
	"database/sql"
	"errors"
)

// CoRead is the same shape as in the real implementation so callers
// can compile against either. All real state stays nil.
type CoRead struct{}

var errCoReadUnavailable = errors.New(
	"tablesource: coread API requires the libsqlite3 build tag " +
		"(rocicorp SQLite with SQLITE_ENABLE_WAL2_COREAD) — default mattn " +
		"build doesn't expose it",
)

// CaptureCoRead always errors in the stub.
func CaptureCoRead(_ context.Context, _ *sql.DB) (*CoRead, error) {
	return nil, errCoReadUnavailable
}

// OpenAtCoRead always errors in the stub.
func OpenAtCoRead(_ context.Context, _ *sql.DB, _ *CoRead) (*sql.Tx, *sql.Conn, error) {
	return nil, nil, errCoReadUnavailable
}

// Free is a no-op on the stub.
func (cr *CoRead) Free() {}

// CaptureCoReadFromConn always errors in the stub.
func CaptureCoReadFromConn(_ *sql.Conn) (*CoRead, error) {
	return nil, errCoReadUnavailable
}

// NewCoReadReaderPool always errors in the stub.
func NewCoReadReaderPool(_ context.Context, _ *sql.DB, _ *CoRead, _ int) (*ReaderPool, error) {
	return nil, errCoReadUnavailable
}
