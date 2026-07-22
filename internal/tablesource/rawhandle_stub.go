//go:build !libsqlite3

package tablesource

/*
#include <stdlib.h>
typedef struct sqlite3 sqlite3;
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/mattn/go-sqlite3"
)

func rawSQLiteHandle(driverConn any) (*C.sqlite3, error) {
	c, ok := driverConn.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, fmt.Errorf("rawSQLiteHandle: not a mattn *SQLiteConn (got %T)", driverConn)
	}
	return (*C.sqlite3)(unsafe.Pointer(c.RawDB())), nil
}
