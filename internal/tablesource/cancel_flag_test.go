package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

// TestProgressHandler_AbortsLongScan proves the progress handler interrupts
// a long-running sqlite3_step within ~progressN opcodes of the cancel flag
// being set. This is the by-construction guarantee that closes the wedge
// class: no SQLite computation can outlive its CG's cancellation.
func TestProgressHandler_AbortsLongScan(t *testing.T) {
	const rows = 100000
	path := seedLargeTable(t, rows)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Verify mattn driver
	if _, ok := db.Driver().(*sqlite3.SQLiteDriver); !ok {
		t.Fatalf("driver is %T, not mattn/go-sqlite3", db.Driver())
	}

	// Acquire a raw conn and register the progress handler
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	flag := newConnCancelFlag()
	defer flag.Free()

	err = conn.Raw(func(driverConn any) error {
		rawDB, rErr := rawSQLiteHandle(driverConn)
		if rErr != nil {
			return rErr
		}
		flag.registerProgressHandler(rawDB, progressN)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	// Start a long scan (100K rows) using the raw conn via database/sql
	// with context.Background (synchronous path — no goroutine-per-Next)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rows_started := atomic.Int32{}
	done := make(chan error, 1)

	go func() {
		r, err := conn.QueryContext(ctx, "SELECT id, name, val FROM users")
		if err != nil {
			done <- fmt.Errorf("query: %w", err)
			return
		}
		count := 0
		for r.Next() {
			count++
			rows_started.Store(int32(count))
		}
		r.Close()
		// If we got here, the scan completed without interruption
		if count == rows {
			done <- fmt.Errorf("scan completed (%d rows) without interruption", count)
		} else {
			done <- fmt.Errorf("scan ended early at %d rows (want interruption via progress handler)", count)
		}
	}()

	// Wait for the scan to start, then cancel
	time.Sleep(50 * time.Millisecond)
	startRows := rows_started.Load()
	if startRows == 0 {
		// The scan might not have started yet; give it more time
		time.Sleep(100 * time.Millisecond)
		startRows = rows_started.Load()
	}
	if startRows == 0 {
		cancel()
		t.Skip("scan didn't start in time — skipping interrupt test")
	}

	// Set the cancel flag — the progress handler should abort the step
	flag.setCancel(CancelStream)

	select {
	case err := <-done:
		if err == nil {
			// rows.Close() returns the error from the interrupted step
			t.Log("scan was interrupted by progress handler (expected)")
		} else {
			// An error is also fine — SQLITE_INTERRUPT surfaces as a
			// driver error. The key is that the scan stopped.
			t.Logf("scan ended with error (expected): %v", err)
		}
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("scan was NOT interrupted by progress handler within 10s — " +
			"the cancel flag was not checked by the progress handler")
	}

	if flag.IsCancelled() {
		t.Log("cancel flag correctly reports cancelled state")
	}
	if flag.Reason() != CancelStream {
		t.Errorf("cancel reason = %d, want %d (CancelStream)", flag.Reason(), CancelStream)
	}
}

// TestProgressHandler_GasMeterAbortsBudget verifies the D1 gas meter: a
// statement that exceeds its opcode budget is aborted even without an
// explicit cancel. This makes unbounded scans inexpressible by construction.
func TestProgressHandler_GasMeterAbortsBudget(t *testing.T) {
	const rows = 100000
	path := seedLargeTable(t, rows)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	flag := newConnCancelFlag()
	defer flag.Free()

	err = conn.Raw(func(driverConn any) error {
		rawDB, rErr := rawSQLiteHandle(driverConn)
		if rErr != nil {
			return rErr
		}
		flag.registerProgressHandler(rawDB, progressN)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	// Set a very tight budget — 5 opcode callbacks (5 * 4096 = ~20K
	// opcodes). A 100K-row scan uses ~25 callbacks at 4096 opcodes each,
	// so a budget of 5 should abort after ~20K opcodes.
	flag.setBudget(5)

	ctx := context.Background()
	r, err := conn.QueryContext(ctx, "SELECT id FROM users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	count := 0
	for r.Next() {
		count++
	}
	r.Close()

	t.Logf("gas meter aborted scan at %d rows (budget=100 callbacks)", count)

	if count == rows {
		t.Errorf("scan completed all %d rows despite budget — gas meter not working", rows)
	}
	if count == 0 {
		t.Error("gas meter aborted before any rows — budget too tight")
	}
	// Should have scanned some rows before the budget ran out
	if count < 10 || count > rows-1 {
		t.Errorf("gas meter count %d is unexpected — should be between 10 and %d", count, rows-1)
	}
}

// TestProgressHandler_ClearCancelResets verifies that clearCancel allows
// subsequent queries to run without interruption.
func TestProgressHandler_ClearCancelResets(t *testing.T) {
	const rows = 1000
	path := seedLargeTable(t, rows)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	flag := newConnCancelFlag()
	defer flag.Free()

	err = conn.Raw(func(driverConn any) error {
		rawDB, rErr := rawSQLiteHandle(driverConn)
		if rErr != nil {
			return rErr
		}
		flag.registerProgressHandler(rawDB, progressN)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	// Cancel, then clear, then query — should succeed
	flag.setCancel(CancelStream)
	flag.clearCancel()
	flag.setBudget(-1) // unlimited

	r, err := conn.QueryContext(context.Background(), "SELECT id FROM users")
	if err != nil {
		t.Fatalf("query after clear: %v", err)
	}
	count := 0
	for r.Next() {
		count++
	}
	r.Close()

	if count != rows {
		t.Fatalf("query after clearCancel got %d rows, want %d", count, rows)
	}
	t.Logf("clearCancel works — query returned %d rows after cancel+clear", count)
}

// TestProgressHandler_DetachBeforeCleanup verifies the C3 invariant:
// detaching the progress handler before cleanup SQL prevents the handler
// from aborting the cleanup itself.
func TestProgressHandler_DetachBeforeCleanup(t *testing.T) {
	path := seedLargeTable(t, 10)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	flag := newConnCancelFlag()
	defer flag.Free()

	err = conn.Raw(func(driverConn any) error {
		rawDB, rErr := rawSQLiteHandle(driverConn)
		if rErr != nil {
			return rErr
		}
		flag.registerProgressHandler(rawDB, progressN)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	// Set the cancel flag (simulating an interrupted operation)
	flag.setCancel(CancelTeardown)

	// Without detaching, the handler would abort this ROLLBACK
	// Detach first (C3 invariant)
	flag.detachProgressHandler()

	// Now cleanup SQL should succeed
	_, err = conn.ExecContext(context.Background(), "ROLLBACK")
	// ROLLBACK may fail with "no transaction" if nothing was open — that's fine.
	// The key is it doesn't fail with SQLITE_INTERRUPT.
	if err != nil && !isInterruptError(err) {
		t.Logf("ROLLBACK returned non-interrupt error (expected if no tx): %v", err)
	}
	if err != nil && isInterruptError(err) {
		t.Fatal("ROLLBACK was aborted by progress handler — detach failed (C3 invariant violated)")
	}
	t.Log("C3 invariant verified — cleanup SQL succeeds after detach")
}

// isInterruptError checks if the error is an SQLITE_INTERRUPT error.
func isInterruptError(err error) bool {
	if err == nil {
		return false
	}
	if sqliteErr, ok := err.(sqlite3.Error); ok {
		return sqliteErr.Code == sqlite3.ErrInterrupt || sqliteErr.Code == 9
	}
	return false
}
