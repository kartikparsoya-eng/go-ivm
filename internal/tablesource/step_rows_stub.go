//go:build !cgo

package tablesource

import (
	"database/sql"
	"fmt"
)

func stepRowsShimAvailable() bool { return false }

func stepRowsShim(_ *sql.Conn, _ string, _ []any, _ func(colNames []string, rowVals []any) bool) error {
	return fmt.Errorf("stepRowsShim: not available (non-cgo build)")
}
