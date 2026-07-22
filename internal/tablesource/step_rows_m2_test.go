//go:build cgo

package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// TestStepRowsShim_PanicInOnRowPreservesConn is the M2 regression guard.
//
// The onRow callback runs inside StepRowsShim's conn.Raw. If a panic (the
// typed advance-abort, or a FromSQLiteType/invalidColumnPanic DataError)
// escaped the callback, database/sql marks the driver conn ErrBadConn and
// DISCARDS it — silently closing the Source's prev/external conn (in drive
// mode, the snapshotter's leapfrog conn). scanRowsShim avoids this by
// recovering inside onRow, stopping the scan cleanly so Raw releases the conn
// healthy, and re-raising from scanRowsShim. This test verifies the conn
// survives WITH that recover, and (negative control) is discarded WITHOUT it.
func TestStepRowsShim_PanicInOnRowPreservesConn(t *testing.T) {
	db, err := sql.Open("sqlite3",
		fmt.Sprintf("file:m2_%d?mode=memory&cache=shared", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// --- WITH the scanRowsShim recover pattern: conn must survive. ---
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rowPanic any
	err = stepRowsShim(conn, "SELECT +\"v\" AS \"v\" FROM t ORDER BY id", nil,
		func(colNames []string, vals []any) (keepGoing bool) {
			defer func() {
				if r := recover(); r != nil {
					rowPanic = r
					keepGoing = false
				}
			}()
			panic("simulated abort/DataError from onRow")
		})
	if err != nil {
		t.Fatalf("stepRowsShim returned error, expected clean stop: %v", err)
	}
	if rowPanic == nil {
		t.Fatal("expected to capture the onRow panic")
	}
	// The conn must still be usable — not ErrBadConn/ErrConnDone.
	var n int
	if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
		t.Fatalf("M2 regression: conn discarded after recovered onRow panic: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("conn.Close after recovered panic: %v", err)
	}

	// --- Negative control: WITHOUT the recover, the panic escapes through
	// conn.Raw and the conn is discarded. ---
	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() { _ = recover() }() // catch Raw's re-panic
		_ = stepRowsShim(conn2, "SELECT +\"v\" AS \"v\" FROM t ORDER BY id", nil,
			func(colNames []string, vals []any) bool {
				panic("escaping panic")
			})
	}()
	if err := conn2.QueryRowContext(ctx, "SELECT 1").Scan(&n); err == nil {
		t.Fatal("sanity: expected conn2 to be discarded when the panic escapes Raw")
	}
	_ = conn2.Close()
}
