package ivm

// The key insight: during genPush, the overlay is read-only by all connections.
// Each connection's pipeline is independent — no shared mutable state between
// pipelines during a single push. This makes parallelization semantically safe.
//
// This file provides GenPushParallel as an alternative to the sequential genPush.
// Enable it by setting MemorySource.Parallel = true.

import "sync"

// GenPushParallel pushes a change to all connections concurrently.
// Each connection's pipeline runs in its own goroutine.
// Results are collected and returned in connection order (deterministic).
func (ms *MemorySource) GenPushParallel(change SourceChange) []Change {
	// Validate (same as sequential). Typed *DriftError panics so
	// engine.Advance can recover them without conflating with programmer
	// bugs (which still abort the process). See ivm/drift.go.
	switch change.Type {
	case ChangeTypeAdd:
		if ms.has(change.Row) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.Row[c]
			}
			panic(&DriftError{Table: ms.tableName, Op: "Add", PK: pk, HasCount: len(ms.data)})
		}
	case ChangeTypeRemove:
		if !ms.has(change.Row) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.Row[c]
			}
			panic(&DriftError{Table: ms.tableName, Op: "Remove", PK: pk, HasCount: len(ms.data)})
		}
	case ChangeTypeEdit:
		if !ms.has(change.OldRow) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.OldRow[c]
			}
			panic(&DriftError{Table: ms.tableName, Op: "Edit", PK: pk, HasCount: len(ms.data)})
		}
	}

	ms.pushEpoch++
	epoch := ms.pushEpoch

	// Snapshot connections under RLock (see MemorySource.connsMu doc).
	ms.connsMu.RLock()
	var activeConns []*Connection
	for _, conn := range ms.connections {
		if conn.Output != nil {
			conn.LastPushedEpoch = epoch
			activeConns = append(activeConns, conn)
		}
	}
	ms.connsMu.RUnlock()

	if len(activeConns) == 0 {
		return nil
	}

	// Atomic Store of overlay establishes a happens-before edge to every
	// subsequent atomic Load in the goroutines spawned below. The `go` spawn
	// itself also establishes happens-before, so this is belt+suspenders.
	ms.overlay.Store(&Overlay{Epoch: epoch, Change: change})
	defer ms.overlay.Store(nil)

	// For single connection, skip goroutine overhead
	if len(activeConns) == 1 {
		conn := activeConns[0]
		outputChange := ms.sourceChangeToChange(change)
		return FilterPush(outputChange, conn.Output, conn.Input, conn.FilterPredicate)
	}

	// Fan-out to goroutines. wg.Wait happens-before the read of `ordered`
	// and `panics`, so direct-slot writes from each goroutine are safe
	// without a channel.
	//
	// Per-goroutine recover is load-bearing: a panic on a spawned goroutine
	// terminates the entire Go runtime (panic-on-goroutine is fatal — no
	// outer caller's recover can catch it). Without this, a Take stale-bound
	// *DriftError panic — which engine.Advance's outer recover is designed
	// to catch and translate into a drift result — would crash the shared
	// sidecar process instead, taking down every CG. See e9d4946 (Take
	// stale-bound → DriftError) which introduced the recoverable panic
	// contract that this site has to honor.
	//
	// We split captured panics by type:
	//   - *DriftError → routed to engine.Advance's recover (self-healable)
	//   - everything else → re-raised so programmer-bug signals still abort
	//
	// Priority on re-raise: non-Drift > Drift. If any connection saw a real
	// programmer bug it must surface, even if other connections concurrently
	// raised drift on the same change.
	ordered := make([][]Change, len(activeConns))
	panics := make([]goroutinePanic, len(activeConns))
	var wg sync.WaitGroup
	for i, conn := range activeConns {
		wg.Add(1)
		go func(idx int, c *Connection) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					if d, ok := r.(*DriftError); ok {
						panics[idx].drift = d
					} else {
						panics[idx].other = r
					}
				}
			}()
			outputChange := ms.sourceChangeToChange(change)
			ordered[idx] = FilterPush(outputChange, c.Output, c.Input, c.FilterPredicate)
		}(i, conn)
	}
	wg.Wait()

	// Re-raise on the caller's goroutine so engine.Advance's recover (or the
	// process-abort path for programmer bugs) sees the panic in the right
	// scope. defer ms.overlay.Store(nil) above still runs.
	for _, p := range panics {
		if p.other != nil {
			panic(p.other)
		}
	}
	for _, p := range panics {
		if p.drift != nil {
			panic(p.drift)
		}
	}

	var allResults []Change
	for _, changes := range ordered {
		allResults = append(allResults, changes...)
	}
	return allResults
}

// goroutinePanic stores a recovered panic from a fan-out goroutine so the
// caller can re-raise it in its own scope after wg.Wait. drift and other
// are mutually exclusive per goroutine.
type goroutinePanic struct {
	drift *DriftError
	other any
}

// SetParallel enables or disables parallel push on this source.
// threshold is the minimum number of connections to trigger parallel fan-out.
func (ms *MemorySource) SetParallel(enabled bool, threshold ...int) {
	ms.parallel = enabled
	ms.parallelThreshold = 2 // default
	if len(threshold) > 0 && threshold[0] > 0 {
		ms.parallelThreshold = threshold[0]
	}
}

// PushWithMode applies a source change using parallel or sequential mode.
// Deprecated: Use Push() directly — it now checks the parallel flag internally.
func (ms *MemorySource) PushWithMode(change SourceChange) []Change {
	return ms.Push(change)
}

func (ms *MemorySource) genPushAndWriteParallel(change SourceChange) []Change {
	// Handle split-edit same as sequential.
	// connsMu guards the slice header against Connect/Disconnect
	// torn-reads — see genPushAndWriteWithSplitEdit for the full
	// rationale on why the engine.mu invariant isn't sufficient on its
	// own. GenPushParallel's own iteration already takes RLock when it
	// reaches its own activeConns snapshot; this pre-scan was missed
	// in the original parallelism patch.
	shouldSplit := false
	if change.Type == ChangeTypeEdit {
		ms.connsMu.RLock()
		for _, conn := range ms.connections {
			if conn.SplitEditKeys != nil {
				for key := range conn.SplitEditKeys {
					if !ValuesEqual(change.Row[key], change.OldRow[key]) {
						shouldSplit = true
						break
					}
				}
			}
			if shouldSplit {
				break
			}
		}
		ms.connsMu.RUnlock()
	}

	if change.Type == ChangeTypeEdit && shouldSplit {
		var results []Change
		r1 := ms.GenPushParallel(MakeSourceChangeRemove(change.OldRow))
		ms.writeChange(MakeSourceChangeRemove(change.OldRow))
		results = append(results, r1...)
		r2 := ms.GenPushParallel(MakeSourceChangeAdd(change.Row))
		ms.writeChange(MakeSourceChangeAdd(change.Row))
		results = append(results, r2...)
		return results
	}

	results := ms.GenPushParallel(change)
	ms.writeChange(change)
	return results
}
