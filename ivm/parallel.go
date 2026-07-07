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
func (ms *MemorySource) GenPushParallel(change SourceChange) {
	// Validate (same as sequential) — the panic propagates out of the
	// engine unrecovered (TS assert-throw → teardown disposition).
	switch change.Type {
	case ChangeTypeAdd:
		if ms.has(change.Row) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.Row[c]
			}
			panic(SourceDriftError(ms.tableName, "Add", pk, len(ms.data)))
		}
	case ChangeTypeRemove:
		if !ms.has(change.Row) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.Row[c]
			}
			panic(SourceDriftError(ms.tableName, "Remove", pk, len(ms.data)))
		}
	case ChangeTypeEdit:
		if !ms.has(change.OldRow) {
			pk := make(map[string]Value, len(ms.primaryKey))
			for _, c := range ms.primaryKey {
				pk[c] = change.OldRow[c]
			}
			panic(SourceDriftError(ms.tableName, "Edit", pk, len(ms.data)))
		}
	}

	ms.pushEpoch++
	epoch := ms.pushEpoch

	// Snapshot connections under RLock (see MemorySource.connsMu doc).
	// LastPushedEpoch is NOT bumped here: TS bumps it per connection
	// immediately before that connection's push (memory-source.ts:625-629),
	// so an unpushed connection must never look pushed — pre-bumping made a
	// concurrent fetch through a not-yet-pushed connection wrongly apply the
	// overlay (computeOverlays: LastPushedEpoch >= overlay.Epoch ⇒ splice).
	// Each goroutine bumps its own connection right before FilterPush.
	ms.connsMu.RLock()
	var activeConns []*Connection
	for _, conn := range ms.connections {
		if conn.Output != nil {
			activeConns = append(activeConns, conn)
		}
	}
	ms.connsMu.RUnlock()

	if len(activeConns) == 0 {
		return
	}

	// Atomic Store of overlay establishes a happens-before edge to every
	// subsequent atomic Load in the goroutines spawned below. The `go` spawn
	// itself also establishes happens-before, so this is belt+suspenders.
	ms.overlay.Store(&Overlay{Epoch: epoch, Change: change})
	defer ms.overlay.Store(nil)

	// For single connection, skip goroutine overhead
	if len(activeConns) == 1 {
		conn := activeConns[0]
		conn.LastPushedEpoch.Store(int64(epoch))
		outputChange := ms.sourceChangeToChange(change)
		FilterPush(outputChange, conn.Output, conn.Input, conn.FilterPredicate)
		return
	}

	// Fan-out to goroutines. wg.Wait happens-before the read of `panics`,
	// so direct-slot writes from each goroutine are safe without a channel.
	//
	// Per-goroutine recover is load-bearing: a panic on a spawned goroutine
	// terminates the entire Go runtime (panic-on-goroutine is fatal — no
	// outer caller's recover can catch it). Capture per slot and re-raise
	// the first (connection order) on the caller's goroutine so the panic
	// unwinds through the engine in the ordinary single-goroutine scope.
	panics := make([]any, len(activeConns))
	var wg sync.WaitGroup
	for i, conn := range activeConns {
		wg.Add(1)
		go func(idx int, c *Connection) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics[idx] = r
				}
			}()
			c.LastPushedEpoch.Store(int64(epoch))
			outputChange := ms.sourceChangeToChange(change)
			FilterPush(outputChange, c.Output, c.Input, c.FilterPredicate)
		}(i, conn)
	}
	wg.Wait()

	// Re-raise on the caller's goroutine so the caller's (or the sidecar
	// handler's) recover sees the panic in the right scope.
	// defer ms.overlay.Store(nil) above still runs.
	for _, p := range panics {
		if p != nil {
			panic(p)
		}
	}
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
func (ms *MemorySource) PushWithMode(change SourceChange) {
	ms.Push(change)
}

func (ms *MemorySource) genPushAndWriteParallel(change SourceChange) {
	// Resolve intra-batch staleness BEFORE the split decision — same order
	// as the sequential path (genPushAndWriteWithSplitEdit): TS's split
	// logic sees the Edit with the lazily-read OldRow, so the split-key
	// comparison must run against the batch-current value. See
	// resolveBatchChange (source.go) for the case table.
	var skip bool
	if change, skip = ms.resolveBatchChange(change); skip {
		return
	}
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
		// The Edit was resolved above, so OldRow is the batch-current prev
		// value — the Remove leg can never target an already-removed row
		// (that case resolved to Add, which does not split).
		ms.GenPushParallel(MakeSourceChangeRemove(change.OldRow))
		ms.writeChange(MakeSourceChangeRemove(change.OldRow))
		ms.GenPushParallel(MakeSourceChangeAdd(change.Row))
		ms.writeChange(MakeSourceChangeAdd(change.Row))
		return
	}

	ms.GenPushParallel(change)
	ms.writeChange(change)
}
