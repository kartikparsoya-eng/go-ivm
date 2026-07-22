package sqlite3

import "unsafe"

// RawDB returns the raw *C.sqlite3 handle as a uintptr.
// Callers must not retain the pointer beyond the conn's lifetime.
func (c *SQLiteConn) RawDB() uintptr {
	return uintptr(unsafe.Pointer(c.db))
}

// RawStmt returns the raw *C.sqlite3_stmt handle as a uintptr.
// Callers must not retain the pointer beyond the stmt's lifetime.
func (s *SQLiteStmt) RawStmt() uintptr {
	return uintptr(unsafe.Pointer(s.s))
}

// RawStmtFromRows returns the raw *C.sqlite3_stmt handle from a SQLiteRows
// as a uintptr. The stmt belongs to the rows' parent SQLiteStmt and is valid
// until the rows are closed.
func (r *SQLiteRows) RawStmt() uintptr {
	if r.s == nil {
		return 0
	}
	return uintptr(unsafe.Pointer(r.s.s))
}
