package main

// M5 regression test: deliverTimeoutDefault must sit BELOW the advance
// budget (advanceBudgetMs, default 60s) so a parked advance producer
// can't hold a WAL pin past the budget.  Pre-fix deliverTimeoutDefault
// was 150s — 2.5× the 60s budget.

import (
	"testing"
	"time"
)

func TestM5_DeliverTimeoutBelowAdvanceBudget(t *testing.T) {
	// deliverTimeoutDefault must be below advanceBudgetMs.
	budget := time.Duration(advanceBudgetMs) * time.Millisecond
	if deliverTimeoutDefault >= budget {
		t.Fatalf("deliverTimeoutDefault (%v) >= advanceBudgetMs (%v) — "+
			"parked advance producer can hold WAL pin past budget (M5)",
			deliverTimeoutDefault, budget)
	}
	// Sanity: the timeout must still be above the longest observed
	// recoverable JS-loop stall (43-46s).
	if deliverTimeoutDefault < 46*time.Second {
		t.Fatalf("deliverTimeoutDefault (%v) < 46s — too short, would "+
			"fire on recoverable JS stalls (M5)", deliverTimeoutDefault)
	}
}
