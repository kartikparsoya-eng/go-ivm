package tablesource

import (
	"database/sql"
	"math"
	"math/rand"
	"testing"
)

// newNullableFixture builds a WAL replica-shaped file with a nullable text
// column containing NULL, matching, and non-matching rows.
func newNullableFixture(t *testing.T) string {
	t.Helper()
	path := newWALFixture(t)
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(
		`INSERT INTO t (id, s) VALUES (1, 'Hello World'), (2, NULL), (3, 'goodbye')`,
	); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	return path
}

// TestLowerNullILIKEShape pins C1 from the napi-path hostile review: the
// lower() override was registered as func(string) string, and mattn's
// callbackArgString rejects SQLITE_NULL ("argument must be BLOB or TEXT").
// Every `nullable_col ILIKE ?` hydrate runs the query_builder shape
// `lower(col) LIKE lower(?) ESCAPE '\'`; the moment the scan reached a NULL
// row the statement errored mid-scan, which panics at all three
// Source.Fetch paths (source.go rows.Err() checks) → hydrate-failure loop.
// TS's ICU lower() (ext/icu icuCaseFunc16) passes NULL through: NULL LIKE
// pattern is NULL → row simply doesn't match.
func TestLowerNullILIKEShape(t *testing.T) {
	pool, err := Open(newNullableFixture(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer pool.Close()

	// Exact ILIKE SQL shape emitted by sqlite/query_builder.go.
	rows, err := pool.Query(
		`SELECT id FROM t WHERE lower(s) LIKE lower(?) ESCAPE '\'`, "%hello%")
	if err != nil {
		t.Fatalf("ILIKE query: %v", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	// Pre-fix: rows.Err() = "argument must be BLOB or TEXT" once the scan
	// hits the NULL row — the exact error Source.Fetch panics on.
	if err := rows.Err(); err != nil {
		t.Fatalf("ILIKE scan over NULL row errored (C1 regression): %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("ILIKE matches = %v; want [1]", ids)
	}

	// lower(NULL) → NULL, same as SQLite built-in / ICU lower().
	var lowered sql.NullString
	if err := pool.QueryRow(`SELECT lower(NULL)`).Scan(&lowered); err != nil {
		t.Fatalf("lower(NULL): %v", err)
	}
	if lowered.Valid {
		t.Fatalf("lower(NULL) = %q; want NULL", lowered.String)
	}
}

// TestLowerCoercionParity pins the non-TEXT coercion contract: SQLite's
// built-in and ICU lower() coerce INTEGER/FLOAT/BLOB args through
// sqlite3_value_text (vdbeMemStringify: Int64ToText for ints, %!.17g for
// reals, bytes-as-text for blobs). Our override receives the typed Go value
// from mattn and must produce byte-identical text. Oracle: CAST(?1 AS TEXT)
// runs the very MemStringify the ICU lower would — digits never case-fold,
// so lower(?1) must equal it exactly.
//
// Known residual (documented, untestable through binds): integral REALs in
// [1e17, 2^63) read from int-serial-encoded records surface as MEM_IntReal
// and stringify as '...0.0' inside SQLite, but mattn collapses the arg to a
// plain double before our code runs, so we render '1.0e+17' style. zql only
// ILIKEs string columns, so this is unreachable via replication.
func TestLowerCoercionParity(t *testing.T) {
	pool, err := Open(newWALFixture(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer pool.Close()

	stmt, err := pool.Prepare(`SELECT lower(?1), CAST(?1 AS TEXT)`)
	if err != nil {
		t.Fatalf("prepare parity probe: %v", err)
	}
	defer stmt.Close()

	check := func(v any) {
		t.Helper()
		var got, want string
		if err := stmt.QueryRow(v).Scan(&got, &want); err != nil {
			t.Fatalf("parity probe %v (%T): %v", v, v, err)
		}
		if got != want {
			t.Fatalf("lower(%v %T) = %q; CAST oracle says %q", v, v, got, want)
		}
	}

	// Integers.
	for _, v := range []int64{0, 1, -1, 42, math.MaxInt64, math.MinInt64} {
		check(v)
	}

	// Adversarial floats: '.0' rule, exp/decimal thresholds, double-rounding
	// (1/3 renders as 0.33333333333333332 — SQLite rounds 18 digits then
	// re-rounds at 17), reduction heuristics (49.47), subnormals, -0.0.
	for _, v := range []float64{
		0.0, math.Copysign(0, -1), 1.0, -1.0, 0.1, -0.1, 1.0 / 3.0,
		49.47, 100.0, 123456789.0, 1e15, 1e16, 1e17, -1e17, 1e18,
		1e-4, 1e-5, -1e-5, 5e-324, math.MaxFloat64, -math.MaxFloat64,
		math.SmallestNonzeroFloat64, 9.109383632e-31, 6.02214076e23,
		2.5, 0.5, 1.5e300, 4.9e-300, 99999999999999999.0, 0.30000000000000004,
	} {
		check(v)
	}

	// Fuzz: random bit patterns (finite only — NaN binds become NULL and
	// Inf can't ride database/sql), plus random decimal-ish values.
	rng := rand.New(rand.NewSource(1))
	fuzzed := 0
	for fuzzed < 20000 {
		var v float64
		if fuzzed%2 == 0 {
			v = math.Float64frombits(rng.Uint64())
		} else {
			v = (rng.Float64() - 0.5) * math.Pow(10, float64(rng.Intn(40)-20))
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		check(v)
		fuzzed++
	}

	// Random int64 fuzz.
	for i := 0; i < 2000; i++ {
		check(rng.Int63() - rng.Int63())
	}

	// BLOB: bytes-as-text then lowercase (value_text semantics); empty blob
	// stays '' (distinguished from NULL by mattn's non-nil empty slice).
	var got string
	if err := pool.QueryRow(`SELECT lower(X'41424321')`).Scan(&got); err != nil {
		t.Fatalf("lower(blob): %v", err)
	}
	if want := "abc!"; got != want {
		t.Fatalf("lower(X'41424321') = %q; want %q", got, want)
	}
	if err := pool.QueryRow(`SELECT lower(X'') || 'ok'`).Scan(&got); err != nil {
		t.Fatalf("lower(empty blob): %v", err)
	}
	if got != "ok" {
		t.Fatalf("lower(X'') || 'ok' = %q; want %q (empty blob must not become NULL)", got, "ok")
	}
}
