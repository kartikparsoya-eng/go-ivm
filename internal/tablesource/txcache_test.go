package tablesource

import (
	"context"
	"sync"
	"testing"
)

func TestTxCacheSameEpochReusesTx(t *testing.T) {
	db, err := Open(seedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cache := NewTxCache(db)
	defer cache.Close()
	ctx := context.Background()

	tx1, rel1, err := cache.Acquire(ctx, "cg-a", 1)
	if err != nil {
		t.Fatalf("Acquire #1: %v", err)
	}
	rel1()

	tx2, rel2, err := cache.Acquire(ctx, "cg-a", 1)
	if err != nil {
		t.Fatalf("Acquire #2: %v", err)
	}
	rel2()

	if tx1 != tx2 {
		t.Fatalf("expected same tx pointer for same (cg, epoch); got distinct")
	}
}

func TestTxCacheNewEpochRollsTx(t *testing.T) {
	db, err := Open(seedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cache := NewTxCache(db)
	defer cache.Close()
	ctx := context.Background()

	tx1, rel1, err := cache.Acquire(ctx, "cg-a", 1)
	if err != nil {
		t.Fatalf("Acquire epoch=1: %v", err)
	}
	rel1()

	tx2, rel2, err := cache.Acquire(ctx, "cg-a", 2)
	if err != nil {
		t.Fatalf("Acquire epoch=2: %v", err)
	}
	rel2()

	if tx1 == tx2 {
		t.Fatalf("expected fresh tx after epoch bump; got same pointer")
	}
}

func TestTxCacheDifferentCGsIndependent(t *testing.T) {
	db, err := Open(seedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cache := NewTxCache(db)
	defer cache.Close()
	ctx := context.Background()

	txA, relA, err := cache.Acquire(ctx, "cg-a", 1)
	if err != nil {
		t.Fatalf("Acquire cg-a: %v", err)
	}
	defer relA()
	txB, relB, err := cache.Acquire(ctx, "cg-b", 1)
	if err != nil {
		t.Fatalf("Acquire cg-b: %v", err)
	}
	defer relB()

	if txA == txB {
		t.Fatalf("expected distinct txs across CGs; got same pointer")
	}
}

func TestTxCacheDropEndsTx(t *testing.T) {
	db, err := Open(seedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cache := NewTxCache(db)
	defer cache.Close()
	ctx := context.Background()

	_, rel, err := cache.Acquire(ctx, "cg-a", 1)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	rel()

	if err := cache.Drop("cg-a"); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	// Drop on unknown CG is a no-op (no error).
	if err := cache.Drop("cg-unknown"); err != nil {
		t.Fatalf("Drop unknown: %v", err)
	}

	// Re-acquiring should produce a fresh tx (the old one is gone).
	tx, rel2, err := cache.Acquire(ctx, "cg-a", 1)
	if err != nil {
		t.Fatalf("Acquire after drop: %v", err)
	}
	defer rel2()
	if tx == nil {
		t.Fatalf("expected non-nil tx after drop+acquire")
	}
}

func TestTxCacheAfterCloseErrors(t *testing.T) {
	db, err := Open(seedReplica(t), OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cache := NewTxCache(db)
	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, _, err := cache.Acquire(context.Background(), "cg-a", 1); err == nil {
		t.Fatalf("Acquire after Close succeeded; expected error")
	}
}

// TestTxCacheConcurrentSameCGSerializes is the constraint check: with one
// tx shared across queries in a CG, multiple goroutines must serialize
// through the per-CG mutex without deadlock or visibility errors.
func TestTxCacheConcurrentSameCGSerializes(t *testing.T) {
	db, err := Open(seedReplica(t), OpenOptions{MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cache := NewTxCache(db)
	defer cache.Close()
	ctx := context.Background()

	const goroutines = 4
	const iters = 100
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				tx, rel, err := cache.Acquire(ctx, "cg-shared", 1)
				if err != nil {
					errs <- err
					return
				}
				var n int
				err = tx.QueryRow("SELECT COUNT(*) FROM t").Scan(&n)
				rel()
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent same-CG query failed: %v", err)
	}
}

// TestTxCacheConcurrentDifferentCGsParallel: distinct CGs should NOT
// serialize on each other. We don't measure wall-time here (flaky), but
// we verify correctness under concurrent multi-CG load.
func TestTxCacheConcurrentDifferentCGsParallel(t *testing.T) {
	db, err := Open(seedReplica(t), OpenOptions{MaxOpenConns: 8})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	cache := NewTxCache(db)
	defer cache.Close()
	ctx := context.Background()

	const cgs = 8
	const iters = 50
	var wg sync.WaitGroup
	errs := make(chan error, cgs)

	for c := 0; c < cgs; c++ {
		cgID := "cg-" + string(rune('a'+c))
		wg.Add(1)
		go func(cgID string) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				tx, rel, err := cache.Acquire(ctx, cgID, 1)
				if err != nil {
					errs <- err
					return
				}
				var n int
				err = tx.QueryRow("SELECT COUNT(*) FROM t").Scan(&n)
				rel()
				if err != nil {
					errs <- err
					return
				}
			}
		}(cgID)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("multi-CG concurrent query failed: %v", err)
	}
}
