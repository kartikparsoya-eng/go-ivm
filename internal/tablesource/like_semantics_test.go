package tablesource

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// newWALFixture creates a WAL-mode SQLite file the pools will accept.
func newWALFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "like.db")
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("set WAL: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return path
}

// TestOpenLikeSemantics pins the zero-1.7.0 connection contract
// (zqlite/db.ts): every replica connection runs `case_sensitive_like = ON`,
// and lower() is a full-Unicode case mapping (TS gets ICU from
// @rocicorp/zero-sqlite3; we override mattn's ASCII-only built-in via the
// driver ConnectHook). Pre-port, 'ABC' LIKE 'abc' returned 1 (SQLite's
// case-insensitive default) and lower('É') returned 'É' unchanged.
func TestOpenLikeSemantics(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(string) (*sql.DB, error)
	}{
		{"Open", func(p string) (*sql.DB, error) { return Open(p, OpenOptions{}) }},
		{"OpenWritable", func(p string) (*sql.DB, error) { return OpenWritable(p, OpenOptions{}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := tc.open(newWALFixture(t))
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			defer pool.Close()

			// case_sensitive_like=ON: bare LIKE is case-sensitive (Postgres).
			var caseMatch int
			if err := pool.QueryRow(`SELECT 'ABC' LIKE 'abc'`).Scan(&caseMatch); err != nil {
				t.Fatalf("LIKE probe: %v", err)
			}
			if caseMatch != 0 {
				t.Fatalf("'ABC' LIKE 'abc' = %d; want 0 (case_sensitive_like=ON not applied)", caseMatch)
			}

			// Unicode lower(): must lowercase non-ASCII like ICU / JS
			// toLowerCase, including Greek final sigma.
			var lowered string
			if err := pool.QueryRow(`SELECT lower('ÉΟΔΟΣ')`).Scan(&lowered); err != nil {
				t.Fatalf("lower probe: %v", err)
			}
			if lowered != "éοδος" {
				t.Fatalf("lower('ÉΟΔΟΣ') = %q; want %q (Unicode lower() override not applied)", lowered, "éοδος")
			}

			// End-to-end ILIKE shape as query_builder emits it.
			var ilikeMatch int
			if err := pool.QueryRow(`SELECT lower('CAFÉ') LIKE lower('caf_') ESCAPE '\'`).Scan(&ilikeMatch); err != nil {
				t.Fatalf("ILIKE-shape probe: %v", err)
			}
			if ilikeMatch != 1 {
				t.Fatalf("lower('CAFÉ') LIKE lower('caf_') = %d; want 1", ilikeMatch)
			}

			// ESCAPE '\': backslash escapes % — literal-percent match.
			var escMatch, escNonMatch int
			if err := pool.QueryRow(`SELECT '50% off' LIKE '%\%%' ESCAPE '\'`).Scan(&escMatch); err != nil {
				t.Fatalf("escape probe: %v", err)
			}
			if err := pool.QueryRow(`SELECT 'path\to' LIKE '%\%%' ESCAPE '\'`).Scan(&escNonMatch); err != nil {
				t.Fatalf("escape probe 2: %v", err)
			}
			if escMatch != 1 || escNonMatch != 0 {
				t.Fatalf(`ESCAPE '\' semantics: '50%% off'→%d (want 1), 'path\to'→%d (want 0)`, escMatch, escNonMatch)
			}
		})
	}
}
