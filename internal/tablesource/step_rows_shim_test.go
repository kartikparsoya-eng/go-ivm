//go:build cgo

package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestStepRowsShim_FatRow verifies S1/S2: a row with a TEXT value larger
// than the initial strbuf (512KB) doesn't cause an infinite hang, and the
// overflow row is not silently dropped.
func TestStepRowsShim_FatRow(t *testing.T) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:fatrow_%d?mode=memory&cache=shared", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table with a TEXT column and insert one fat row (>512KB)
	fatValue := strings.Repeat("X", 600*1024)
	_, err = db.Exec("CREATE TABLE fat(id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO fat VALUES(1, ?)", fatValue)
	if err != nil {
		t.Fatal(err)
	}
	// Also insert a normal row after the fat one
	_, err = db.Exec("INSERT INTO fat VALUES(2, 'small')")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var rows []string
	err = stepRowsShim(conn, "SELECT data FROM fat ORDER BY id", nil, func(colNames []string, rowVals []any) bool {
		if s, ok := rowVals[0].(string); ok {
			rows = append(rows, s)
		}
		return true
	})
	if err != nil {
		t.Fatalf("stepRowsShim failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if len(rows[0]) != 600*1024 {
		t.Errorf("fat row: expected %d bytes, got %d", 600*1024, len(rows[0]))
	}
	if rows[0] != fatValue {
		t.Error("fat row value mismatch")
	}
	if rows[1] != "small" {
		t.Errorf("small row: expected 'small', got %q", rows[1])
	}
}

// TestStepRowsShim_EarlyStop verifies N1: onRow returning false stops the
// scan immediately, not reading the entire result set.
func TestStepRowsShim_EarlyStop(t *testing.T) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:earlystop_%d?mode=memory&cache=shared", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE estop(id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO estop VALUES(?, ?)", i, fmt.Sprintf("row-%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	count := 0
	err = stepRowsShim(conn, "SELECT val FROM estop ORDER BY id", nil, func(colNames []string, rowVals []any) bool {
		count++
		return count < 5 // stop after 5 rows
	})
	if err != nil {
		t.Fatalf("stepRowsShim failed: %v", err)
	}

	if count != 5 {
		t.Errorf("expected scan to stop at 5 rows, got %d", count)
	}
}

// TestCancelFlag_ConcurrentFree verifies N2: calling setCancel concurrently
// with Free doesn't cause a use-after-free. Run with -race to catch issues.
func TestCancelFlag_ConcurrentFree(t *testing.T) {
	for i := 0; i < 100; i++ {
		flag := newConnCancelFlag()
		flag.setBudget(defaultBudget)

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: repeatedly call setCancel
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				flag.setCancel(CancelStream)
			}
		}()

		// Goroutine 2: Free after a tiny delay
		go func() {
			defer wg.Done()
			time.Sleep(time.Microsecond)
			flag.Free()
		}()

		wg.Wait()
	}
}
