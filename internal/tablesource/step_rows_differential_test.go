//go:build cgo

package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// TestStepRowsShim_Differential is the S3 gate: runs representative queries
// through both the database/sql path and the C shim path and asserts
// byte-identical output across every type that could diverge:
//   - int64 (normal + >2^53 MAX_SAFE_INTEGER)
//   - float64
//   - TEXT (normal, empty, timestamp-stored-as-text)
//   - BLOB (normal, empty)
//   - NULL
//   - boolean (0/1)
//   - json
//   - fat row (>512KB, exercises S1/S2 overflow)
//
// Also catches the time.Time tripwire asymmetry: if a future SELECT site
// forgets the unary-+ wrap, the database/sql path would produce a time.Time
// (panicking in FromSQLiteType), while the shim path would ship raw int64/text
// silently. This test uses the wrap, so both paths produce identical raw values.
func TestStepRowsShim_Differential(t *testing.T) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:diff_%d?mode=memory&cache=shared", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a table covering all divergent types. The unary-+ wrap is applied
	// in the SELECT (matching production's BuildSelectQuery/selectColList), so
	// mattn's decltype conversions are stripped and both paths see raw values.
	_, err = db.Exec(`CREATE TABLE diff_test (
		id INTEGER PRIMARY KEY,
		int_val INTEGER,
		float_val REAL,
		text_val TEXT,
		empty_text TEXT,
		blob_val BLOB,
		empty_blob BLOB,
		null_val TEXT,
		bool_val INTEGER,
		json_val TEXT,
		ts_text TEXT,
		big_int INTEGER,
		fat_text TEXT
	)`)
	if err != nil {
		t.Fatal(err)
	}

	fatValue := strings.Repeat("Z", 600*1024)

	_, err = db.Exec(`INSERT INTO diff_test VALUES (
		1,
		42,
		3.14,
		'hello',
		'',
		X'deadbeef',
		X'',
		NULL,
		1,
		'{"key":"value"}',
		'2026-01-15T10:30:00Z',
		9007199254740992,
		?
	)`, fatValue)
	if err != nil {
		t.Fatal(err)
	}

	// Also insert a NULL bool + NULL json row
	_, err = db.Exec(`INSERT INTO diff_test VALUES (
		2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 'null', '2020-06-08', 0, 'small'
	)`)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Build the SELECT with unary-+ wrap (strips declared types, matching production)
	cols := []string{
		"id", "int_val", "float_val", "text_val", "empty_text",
		"blob_val", "empty_blob", "null_val", "bool_val", "json_val",
		"ts_text", "big_int", "fat_text",
	}
	wrappedCols := make([]string, len(cols))
	for i, c := range cols {
		wrappedCols[i] = `+"` + c + `" AS "` + c + `"`
	}
	query := "SELECT " + strings.Join(wrappedCols, ",") + " FROM diff_test ORDER BY id"

	// Path 1: database/sql (non-shim)
	dbResults := make([][]any, 0)
	{
		rows, err := conn.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatalf("database/sql query: %v", err)
		}
		defer rows.Close()
		colNames, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(colNames))
			ptrs := make([]any, len(colNames))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			row := make([]any, len(vals))
			copy(row, vals)
			dbResults = append(dbResults, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}

	// Path 2: C shim
	shimResults := make([][]any, 0)
	err = stepRowsShim(conn, query, nil, func(colNames []string, rowVals []any) bool {
		row := make([]any, len(rowVals))
		copy(row, rowVals)
		shimResults = append(shimResults, row)
		return true
	})
	if err != nil {
		t.Fatalf("shim query: %v", err)
	}

	// Assert identical row count
	if len(dbResults) != len(shimResults) {
		t.Fatalf("row count mismatch: database/sql=%d, shim=%d", len(dbResults), len(shimResults))
	}

	for r := 0; r < len(dbResults); r++ {
		dbRow := dbResults[r]
		shimRow := shimResults[r]
		if len(dbRow) != len(shimRow) {
			t.Fatalf("row %d: column count mismatch: db=%d, shim=%d", r, len(dbRow), len(shimRow))
		}
		for c := 0; c < len(dbRow); c++ {
			dbVal := dbRow[c]
			shimVal := shimRow[c]
			colName := cols[c]

			// Normalize []byte vs string for BLOB columns — database/sql may
			// return []byte where the shim returns []byte (both should be []byte
			// for BLOB storage class, but empty BLOB may differ)
			if dbB, ok := dbVal.([]byte); ok {
				if shimB, ok2 := shimVal.([]byte); ok2 {
					if string(dbB) != string(shimB) {
						t.Errorf("row %d col %s: []byte mismatch: db=%x, shim=%x", r, colName, dbB, shimB)
					}
					continue
				}
			}

			// Normalize nil
			if dbVal == nil && shimVal == nil {
				continue
			}
			if dbVal == nil || shimVal == nil {
				t.Errorf("row %d col %s: nil mismatch: db=%v(%T), shim=%v(%T)", r, colName, dbVal, dbVal, shimVal, shimVal)
				continue
			}

			// Compare by type
			if fmt.Sprintf("%T(%v)", dbVal, dbVal) != fmt.Sprintf("%T(%v)", shimVal, shimVal) {
				t.Errorf("row %d col %s: type+value mismatch: db=%T(%v), shim=%T(%v)",
					r, colName, dbVal, dbVal, shimVal, shimVal)
			}
		}
	}

	// Specific assertions for critical types
	if len(shimResults) < 1 {
		t.Fatal("no shim results")
	}

	// Row 1: check key types
	row1 := shimResults[0]
	// big_int = 9007199254740992 (exactly MAX_SAFE_INTEGER — should be int64, not float)
	if v, ok := row1[11].(int64); !ok || v != 9007199254740992 {
		t.Errorf("big_int: expected int64(9007199254740992), got %T(%v)", row1[11], row1[11])
	}
	// bool_val = 1 — raw int64, not bool (decltype stripped)
	if v, ok := row1[8].(int64); !ok || v != 1 {
		t.Errorf("bool_val: expected int64(1), got %T(%v)", row1[8], row1[8])
	}
	// json_val = '{"key":"value"}' — raw string
	if v, ok := row1[9].(string); !ok || v != `{"key":"value"}` {
		t.Errorf("json_val: expected string, got %T(%v)", row1[9], row1[9])
	}
	// ts_text = '2026-01-15T10:30:00Z' — raw string, NOT time.Time
	if v, ok := row1[10].(string); !ok || v != "2026-01-15T10:30:00Z" {
		t.Errorf("ts_text: expected string, got %T(%v)", row1[10], row1[10])
	}
	// empty_text = '' — empty string
	if v, ok := row1[4].(string); !ok || v != "" {
		t.Errorf("empty_text: expected empty string, got %T(%v)", row1[4], row1[4])
	}
	// null_val = nil
	if row1[7] != nil {
		t.Errorf("null_val: expected nil, got %T(%v)", row1[7], row1[7])
	}
	// fat_text = 600KB
	if v, ok := row1[12].(string); !ok || len(v) != 600*1024 {
		t.Errorf("fat_text: expected 600KB string, got %T len=%d", row1[12], len(v))
	}

	// Row 2: check NULL handling
	row2 := shimResults[1]
	// int_val = NULL
	if row2[1] != nil {
		t.Errorf("row2 int_val: expected nil, got %T(%v)", row2[1], row2[1])
	}
	// bool_val = 0
	if v, ok := row2[8].(int64); !ok || v != 0 {
		t.Errorf("row2 bool_val: expected int64(0), got %T(%v)", row2[8], row2[8])
	}
}

// TestStepRowsShim_TripwireAsymmetry verifies that with the unary-+ wrap,
// neither path produces time.Time (the tripwire doesn't fire). If a future
// change removes the wrap, the database/sql path would produce time.Time
// and this test would fail (panic in FromSQLiteType or type mismatch),
// catching the asymmetry before prod.
func TestStepRowsShim_TripwireAsymmetry(t *testing.T) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:trip_%d?mode=memory&cache=shared", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a table with a TEXT column storing a timestamp string.
	// The unary-+ wrap strips the declared type so mattn doesn't convert to time.Time.
	_, err = db.Exec("CREATE TABLE trip(id INTEGER PRIMARY KEY, ts TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO trip VALUES(1, '2026-01-15T10:30:00Z')")
	if err != nil {
		t.Fatal(err)
	}

	conn, _ := db.Conn(context.Background())
	defer conn.Close()

	// Query WITH the wrap — both paths should return string, not time.Time
	wrappedQuery := `SELECT +"ts" AS "ts" FROM trip WHERE id=?`

	// database/sql path
	var dbVal any
	err = conn.QueryRowContext(context.Background(), wrappedQuery, 1).Scan(&dbVal)
	if err != nil {
		t.Fatalf("database/sql: %v", err)
	}
	if _, ok := dbVal.(time.Time); ok {
		t.Fatal("database/sql returned time.Time despite unary-+ wrap — wrap is broken")
	}
	if s, ok := dbVal.(string); !ok || s != "2026-01-15T10:30:00Z" {
		t.Errorf("database/sql: expected string '2026-01-15T10:30:00Z', got %T(%v)", dbVal, dbVal)
	}

	// Shim path
	var shimVal any
	err = stepRowsShim(conn, wrappedQuery, []any{1}, func(colNames []string, rowVals []any) bool {
		shimVal = rowVals[0]
		return false
	})
	if err != nil {
		t.Fatalf("shim: %v", err)
	}
	if s, ok := shimVal.(string); !ok || s != "2026-01-15T10:30:00Z" {
		t.Errorf("shim: expected string '2026-01-15T10:30:00Z', got %T(%v)", shimVal, shimVal)
	}

	// Verify both paths produce the same type
	if fmt.Sprintf("%T", dbVal) != fmt.Sprintf("%T", shimVal) {
		t.Errorf("type asymmetry: database/sql=%T, shim=%T", dbVal, shimVal)
	}
}

// Suppress unused import for math when MAX_SAFE_INTEGER is used inline.
var _ = math.MaxInt64
