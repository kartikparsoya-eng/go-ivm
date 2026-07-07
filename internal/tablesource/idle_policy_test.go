package tablesource

// Pins for the keep-warm idle-conn policy (2026-07-07 latency forensics):
// MaxIdleConns unset defaults to MaxOpenConns, explicit values clamp to
// ≤ MaxOpenConns. The old small idle cap (32) made every pool-demand burst
// churn fresh SQLite opens against the replica — observed as read-pool
// acquisition stalls (70s cumulative wait per 10s window at sampled in-use
// 0–25/128), a uniform steady-latency tax on the warm hydrate/advance path.

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func drainAndCount(t *testing.T, db *sql.DB, n int) sql.DBStats {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conns := make([]*sql.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("acquire %d/%d: %v", i+1, n, err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		_ = c.Close()
	}
	return db.Stats()
}

// TestOpen_IdleDefaultsToMaxOpen: with MaxIdleConns unset, releasing a burst
// of conns must keep them ALL idle (idle == open cap), not discard all but a
// small default's worth. Open cap and burst size are both deliberately ABOVE
// the old defaultMaxIdleConns=32 — under the old policy this drain would end
// with Idle=32 and MaxIdleClosed=16, so the test discriminates old vs new.
func TestOpen_IdleDefaultsToMaxOpen(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	st := drainAndCount(t, db, 48)
	if st.Idle < 48 {
		t.Fatalf("after releasing 48 conns: idle=%d (MaxIdleClosed=%d) — keep-warm default not applied",
			st.Idle, st.MaxIdleClosed)
	}
	if st.MaxIdleClosed != 0 {
		t.Fatalf("MaxIdleClosed=%d — burst conns were churned instead of kept warm", st.MaxIdleClosed)
	}
}

// TestOpen_IdleClampsToMaxOpen: an explicit MaxIdleConns above MaxOpenConns
// must not error and must behave as idle == open.
func TestOpen_IdleClampsToMaxOpen(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "0000000001")
	db, err := Open(path, OpenOptions{MaxOpenConns: 4, MaxIdleConns: 100})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	st := drainAndCount(t, db, 4)
	if st.Idle != 4 {
		t.Fatalf("idle=%d, want 4 (clamped to open cap)", st.Idle)
	}
}
