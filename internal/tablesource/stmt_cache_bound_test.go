package tablesource

// Regression: the per-conn stmt cache must be BOUNDED (scale review).
// Pre-fix it grew one compiled sqlite3_stmt (C heap) per distinct SQL text
// for the conn's whole life — and the long-lived conns (prevConn, the
// Snapshotter's leapfrog conns) are torn down only at CG teardown, so a
// long-lived CG with query churn ratcheted RSS without bound. Post-fix,
// inserting past stmtCachePerConnCap evicts the least-recently-returned
// quarter, and hot (recently-returned) entries survive.

import (
	"fmt"
	"testing"
)

func TestStmtCacheBoundedPerConn(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	src.mu.Lock()
	defer src.mu.Unlock()
	if err := src.ensurePrevTxLocked(); err != nil {
		t.Fatalf("ensurePrevTx: %v", err)
	}
	conn := src.activeConn()

	// cap mirrors stmtCachePerConnCap (kept literal so this test compiles —
	// and demonstrably FAILS — against the pre-fix tree, which had no cap).
	const cap = 512

	checkoutReturn := func(q string) {
		t.Helper()
		st, err := src.checkoutSelectLocked(conn, q)
		if err != nil {
			t.Fatalf("checkout %q: %v", q, err)
		}
		src.returnSelectStmtLocked(conn, q, st)
	}

	// A hot shape, re-touched throughout: must survive every eviction.
	hotSQL := "SELECT id FROM users WHERE id = 1 /* hot */"
	checkoutReturn(hotSQL)

	// Simulate a CG lifetime of query churn: 3x cap distinct SQL shapes.
	for i := 0; i < 3*cap; i++ {
		checkoutReturn(fmt.Sprintf("SELECT id FROM users WHERE id = 1 /* shape %d */", i))
		if i%16 == 0 {
			checkoutReturn(hotSQL) // keep the hot entry recent
		}
	}

	n := len(src.stmtCache[conn])
	if n > cap {
		t.Fatalf("stmt cache unbounded: %d entries after churn (cap %d) — C-heap RSS ratchet", n, cap)
	}
	if n == 0 {
		t.Fatal("stmt cache empty after churn — eviction is wiping the working set")
	}
	if _, ok := src.stmtCache[conn][hotSQL]; !ok {
		t.Fatal("hot (recently-returned) entry was evicted — eviction must be coldest-first")
	}
}
