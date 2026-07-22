//go:build cgo

package tablesource

/*
#include <stdlib.h>
#include <string.h>

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;

extern int sqlite3_step(sqlite3_stmt*);
extern int sqlite3_column_count(sqlite3_stmt*);
extern int sqlite3_column_type(sqlite3_stmt*, int);
extern long long sqlite3_column_int64(sqlite3_stmt*, int);
extern double sqlite3_column_double(sqlite3_stmt*, int);
extern const void* sqlite3_column_text(sqlite3_stmt*, int);
extern const void* sqlite3_column_blob(sqlite3_stmt*, int);
extern int sqlite3_column_bytes(sqlite3_stmt*, int);
extern int sqlite3_reset(sqlite3_stmt*);
extern int sqlite3_clear_bindings(sqlite3_stmt*);
extern const char* sqlite3_column_name(sqlite3_stmt*, int);
extern int sqlite3_bind_int64(sqlite3_stmt*, int, long long);
extern int sqlite3_bind_double(sqlite3_stmt*, int, double);
extern int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int, void(*)(void*));
extern int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int, void(*)(void*));
extern int sqlite3_bind_null(sqlite3_stmt*, int);
extern int sqlite3_bind_zeroblob(sqlite3_stmt*, int, int);

#define GOIVM_SQLITE_ROW   100
#define GOIVM_SQLITE_DONE  101

typedef struct {
	int typ;
	long long i64;
	double f64;
	int stroff;
	int n;
} goivm_col;

typedef struct {
	int nrows;
	int done;
	int errcode;
} goivm_step_result;

// goivm_step_rows steps up to nrows_req rows from stmt, extracting all
// columns into colbuf and copying TEXT/BLOB data into strbuf.
// Returns a result with nrows (rows stepped), done (1 if SQLITE_DONE),
// and errcode (non-zero on error).
//
// If strbuf is too small, returns with nrows < nrows_req, done=0,
// errcode=0, and *strbuflen set to the required size. The caller should
// grow strbuf and retry.
static goivm_step_result
goivm_step_rows(sqlite3_stmt *stmt, goivm_col *colbuf, int ncol,
                int nrows_req, char *strbuf, int *strbuflen)
{
	goivm_step_result res = {0, 0, 0};
	int stroff = 0;
	int strcap = *strbuflen;
	for (int r = 0; r < nrows_req; r++) {
		int rc = sqlite3_step(stmt);
		if (rc == GOIVM_SQLITE_DONE) { res.done = 1; break; }
		if (rc != GOIVM_SQLITE_ROW) { res.errcode = rc; break; }
		goivm_col *row = &colbuf[r * ncol];
		for (int c = 0; c < ncol; c++) {
			goivm_col *col = &row[c];
			int typ = sqlite3_column_type(stmt, c);
			col->typ = typ;
			col->stroff = 0;
			col->n = 0;
			switch (typ) {
			case 1: col->i64 = sqlite3_column_int64(stmt, c); break;
			case 2: col->f64 = sqlite3_column_double(stmt, c); break;
			case 3: case 4: {
				int n = sqlite3_column_bytes(stmt, c);
				const void *p = (typ == 3) ?
					(void*)sqlite3_column_text(stmt, c) :
					sqlite3_column_blob(stmt, c);
				if (n > 0 && p != NULL) {
					if (stroff + n > strcap) {
						*strbuflen = stroff + n;
						res.nrows = r;
						return res;
					}
					memcpy(strbuf + stroff, p, n);
					col->stroff = stroff;
					col->n = n;
					stroff += n;
				}
				break;
			}
			}
		}
		res.nrows++;
	}
	*strbuflen = stroff;
	return res;
}
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"unsafe"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	shimBatchSize     = 1024
	shimStrBufInitial = 512 * 1024
)

var shimBufPool = sync.Pool{
	New: func() any {
		return &shimBufs{
			colbuf: make([]C.goivm_col, 20*shimBatchSize),
			strbuf: make([]byte, shimStrBufInitial),
		}
	},
}

type shimBufs struct {
	colbuf []C.goivm_col
	strbuf []byte
}

func stepRowsShimAvailable() bool { return true }

// stepRowsShim executes a query via the mattn driver interface (which handles
// prepare + bind correctly), then takes over the step loop using a C shim
// that batches up to 1024 rows per CGO crossing.
//
// The entire operation runs inside conn.Raw so the conn lock is held
// throughout — no other goroutine can use this conn while we're stepping.
func stepRowsShim(
	conn *sql.Conn,
	sqlText string,
	args []any,
	onRow func(colNames []string, rowVals []any) bool,
) error {
	return conn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("stepRowsShim: not a mattn *SQLiteConn (got %T)", driverConn)
		}

		// Prepare via driver.Conn.Prepare (mattn handles SQL parsing)
		driverStmt, err := c.Prepare(sqlText)
		if err != nil {
			return fmt.Errorf("stepRowsShim: prepare: %w", err)
		}
		defer driverStmt.Close()

		ss, ok := driverStmt.(*sqlite3.SQLiteStmt)
		if !ok {
			return fmt.Errorf("stepRowsShim: not a *SQLiteStmt (got %T)", driverStmt)
		}

		// Use mattn's Query to bind args correctly (handles all Go types)
		dArgs := make([]driver.Value, len(args))
		for i, a := range args {
			switch v := a.(type) {
			case int:
				dArgs[i] = int64(v)
			default:
				dArgs[i] = a
			}
		}
		driverRows, err := ss.Query(dArgs)
		if err != nil {
			return fmt.Errorf("stepRowsShim: query: %w", err)
		}
		defer driverRows.Close()

		// Extract the *C.sqlite3_stmt from SQLiteRows via the exported accessor.
		sr, ok := driverRows.(*sqlite3.SQLiteRows)
		if !ok {
			return fmt.Errorf("stepRowsShim: not *SQLiteRows (got %T)", driverRows)
		}
		cstmt := (*C.sqlite3_stmt)(unsafe.Pointer(sr.RawStmt()))
		if cstmt == nil {
			return fmt.Errorf("stepRowsShim: SQLiteRows has nil stmt")
		}

		// Get column count and names from the stmt
		ncol := int(C.sqlite3_column_count(cstmt))
		colNames := make([]string, ncol)
		for i := 0; i < ncol; i++ {
			colNames[i] = C.GoString(C.sqlite3_column_name(cstmt, C.int(i)))
		}

		// Get reusable buffers from pool
		bufs := shimBufPool.Get().(*shimBufs)
		defer shimBufPool.Put(bufs)
		if len(bufs.colbuf) < ncol*shimBatchSize {
			bufs.colbuf = make([]C.goivm_col, ncol*shimBatchSize)
		}
		colbuf := bufs.colbuf
		strbuf := bufs.strbuf

		// Batch-step using the C shim

		for {
			strLen := C.int(len(strbuf))
			res := C.goivm_step_rows(
				cstmt,
				(*C.goivm_col)(unsafe.Pointer(&colbuf[0])),
				C.int(ncol),
				C.int(shimBatchSize),
				(*C.char)(unsafe.Pointer(&strbuf[0])),
				&strLen,
			)
			stepped := int(res.nrows)
			done := res.done != 0
			errcode := int(res.errcode)

			// If strbuf overflowed, deliver the rows we have, then grow and continue
			if stepped > 0 && !done && errcode == 0 && int(strLen) > len(strbuf) {
				// Decode and deliver partial rows first
				partialVals := make([]any, ncol)
				for r := 0; r < stepped; r++ {
					for c := 0; c < ncol; c++ {
						col := &colbuf[r*ncol+c]
						switch col.typ {
						case 1:
							partialVals[c] = int64(col.i64)
						case 2:
							partialVals[c] = float64(col.f64)
						case 3:
							if col.n > 0 {
								partialVals[c] = string(strbuf[col.stroff : int(col.stroff)+int(col.n)])
							} else {
								partialVals[c] = ""
							}
						case 4:
							if col.n > 0 {
								b := make([]byte, col.n)
								copy(b, strbuf[col.stroff:int(col.stroff)+int(col.n)])
								partialVals[c] = b
							} else {
								partialVals[c] = []byte{}
							}
						default:
							partialVals[c] = nil
						}
					}
					if !onRow(colNames, partialVals) {
						return nil
					}
				}
				strbuf = make([]byte, int(strLen)*2)
				continue
			}
			if errcode != 0 {
				return fmt.Errorf("stepRowsShim: step error rc=%d", errcode)
			}

			// Decode rows
			rowVals := make([]any, ncol)
			for r := 0; r < stepped; r++ {
				for c := 0; c < ncol; c++ {
					col := &colbuf[r*ncol+c]
					switch col.typ {
					case 1:
						rowVals[c] = int64(col.i64)
					case 2:
						rowVals[c] = float64(col.f64)
					case 3:
						if col.n > 0 {
							rowVals[c] = string(strbuf[col.stroff : int(col.stroff)+int(col.n)])
						} else {
							rowVals[c] = ""
						}
					case 4:
						if col.n > 0 {
							b := make([]byte, col.n)
							copy(b, strbuf[col.stroff:int(col.stroff)+int(col.n)])
							rowVals[c] = b
						} else {
							rowVals[c] = []byte{}
						}
					default:
						rowVals[c] = nil
					}
				}
				if !onRow(colNames, rowVals) {
					return nil
				}
			}

			if done {
				return nil
			}
		}
	})
}
