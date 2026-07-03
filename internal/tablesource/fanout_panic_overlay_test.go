package tablesource

// Regression tests: a panic raised inside the Push fanout (operator drift,
// DataError from a poison value, programmer bug) must NOT leave s.overlay
// set. genPushAndWrite used to clear the overlay only on the success path;
// the fanout re-raises recovered panics on the calling goroutine
// (parallel_fanout.go), so the unwind skipped the clear and the overlay
// stuck until the next Push overwrote it or the CG was destroyed. While
// stuck:
//
//   - OnAdvanceEnd early-returned (overlay guard), so the drift path's
//     rollback+re-pin contract (advance_drift_stale_bound_test.go) was
//     silently defeated — the prev tx kept the aborted batch's earlier
//     writeChanges;
//   - RefreshSnapshot (drift audit) skipped for the same reason;
//   - fetches on epoch-current connections spliced the FAILED change into
//     results — a phantom row visible to any hydrate landing before the
//     TS reset, drive mode included.
//
// Both fanout paths are pinned (the panic reaches genPushAndWrite via
// different routes): parallel (goroutine recover + re-raise) and serial
// (pushGroup panics on the caller directly).

import (
	"slices"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// bombOutput panics when pushed a change whose row id equals armedID and
// records nothing otherwise (downstream changes are irrelevant here).
type bombOutput struct{ armedID float64 }

func (b *bombOutput) Push(c ivm.Change, _ ivm.InputBase) []ivm.Change {
	if c.Node.Row["id"] == b.armedID {
		panic("fanout bomb")
	}
	return nil
}

func TestFanoutPanicClearsOverlay(t *testing.T) {
	for _, tc := range []struct {
		name     string
		parallel bool
	}{
		// Two conns in two groups either way; the knob alone selects the
		// fanOut path (parallel goroutines vs the strict serial loop).
		{"parallelPath", true},
		{"serialPath", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withFanoutKnobs(t, tc.parallel, 4)
			src, db := newUserSource(t)
			defer db.Close()

			// conn A: healthy, registered FIRST so its group completes (or
			// precedes the bomb serially) — its lastPushedEpoch reaches the
			// failed epoch, satisfying the overlay splice gate on fetch.
			src.SetNextConnectGroup("qa")
			inA := src.Connect(nil, nil, nil, nil)
			inA.SetOutput(&recordingOutput{})

			// conn B: panics when the id=50 change is fanned out.
			src.SetNextConnectGroup("qb")
			inB := src.Connect(nil, nil, nil, nil)
			inB.SetOutput(&bombOutput{armedID: float64(50)})

			ids := func() []float64 {
				var out []float64
				for _, n := range slices.Collect(inA.Fetch(ivm.FetchRequest{})) {
					out = append(out, n.Row["id"].(float64))
				}
				return out
			}

			// Push #1 (benign): fans out cleanly and lands its writeChange in
			// the prev tx — the state OnAdvanceEnd must later roll back.
			src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
				"id": float64(60), "name": "wc", "score": float64(1), "active": true,
			}))

			// Push #2: the bomb fires mid-fanout and the panic must reach us.
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
					"id": float64(50), "name": "boom", "score": float64(2), "active": true,
				}))
			}()
			if recovered == nil {
				t.Fatal("fanout panic did not propagate to the Push caller")
			}

			// THE FIX: the overlay must be cleared on the unwind.
			src.mu.Lock()
			stuck := src.overlay != nil
			src.mu.Unlock()
			if stuck {
				t.Fatal("overlay stuck after fanout panic — OnAdvanceEnd/RefreshSnapshot are wedged and fetches will splice the failed change")
			}

			// No phantom row: conn A is epoch-current, so a stuck overlay
			// would splice id=50 into this fetch. Push #1's write (id=60) IS
			// visible — it's in the prev tx until the rollback below.
			got := ids()
			if slices.Contains(got, float64(50)) {
				t.Fatalf("failed change spliced into fetch (phantom row): ids=%v", got)
			}
			if !slices.Contains(got, float64(60)) {
				t.Fatalf("sanity: push #1's prev-tx write should be visible before rollback: ids=%v", got)
			}

			// Drift-path contract: the engine's signalAdvanceEnd (recover
			// path) calls OnAdvanceEnd, which must ROLL BACK the aborted
			// batch's writes — not early-return on the stuck overlay.
			src.OnAdvanceEnd()
			got = ids()
			if slices.Contains(got, float64(60)) {
				t.Fatalf("OnAdvanceEnd did not roll back the prev tx after the fanout panic: ids=%v", got)
			}
			want := []float64{1, 2, 3}
			if !slices.Equal(got, want) {
				t.Fatalf("post-rollback fetch = %v, want seed rows %v", got, want)
			}
		})
	}
}
