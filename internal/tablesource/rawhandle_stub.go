//go:build !libsqlite3

// rawSQLiteHandle for default (non-libsqlite3) builds. snapshot.go
// (libsqlite3 tag) has the primary definition; this stub lets the
// cancel_flag.go progress handler get the raw *C.sqlite3 without
// requiring the libsqlite3 build tag.
//
// The reflection trick (reading mattn's unexported SQLiteConn.db field)
// works identically in both builds — the mattn driver is always linked,
// only the wal2/snapshot symbols differ.

package tablesource

/*
#include <stdlib.h>
typedef struct sqlite3 sqlite3;
*/
import "C"

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/mattn/go-sqlite3"
)

// rawSQLiteHandle is duplicated here for non-libsqlite3 builds. The
// function is identical to the one in snapshot.go — both read mattn's
// unexported SQLiteConn.db field via reflect.
func rawSQLiteHandle(driverConn any) (*C.sqlite3, error) {
	c, ok := driverConn.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, fmt.Errorf("rawSQLiteHandle: not a mattn *SQLiteConn (got %T)", driverConn)
	}
	v := reflect.ValueOf(c).Elem().FieldByName("db")
	if !v.IsValid() {
		return nil, errors.New("rawSQLiteHandle: SQLiteConn.db field not found (mattn struct changed?)")
	}
	return (*C.sqlite3)(unsafe.Pointer(v.Pointer())), nil
}
