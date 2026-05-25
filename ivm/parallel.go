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
	// Validate (same as sequential)
	switch change.Type {
	case ChangeTypeAdd:
		if ms.has(change.Row) {
			panic("Row already exists")
		}
	case ChangeTypeRemove:
		if !ms.has(change.Row) {
			panic("Row not found")
		}
	case ChangeTypeEdit:
		if !ms.has(change.OldRow) {
			panic("Row not found")
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

	// Fan-out to goroutines. wg.Wait happens-before the read of `ordered`,
	// so direct-slot writes from each goroutine are safe without a channel.
	ordered := make([][]Change, len(activeConns))
	var wg sync.WaitGroup
	for i, conn := range activeConns {
		wg.Add(1)
		go func(idx int, c *Connection) {
			defer wg.Done()
			outputChange := ms.sourceChangeToChange(change)
			ordered[idx] = FilterPush(outputChange, c.Output, c.Input, c.FilterPredicate)
		}(i, conn)
	}
	wg.Wait()

	var allResults []Change
	for _, changes := range ordered {
		allResults = append(allResults, changes...)
	}
	return allResults
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
	// Handle split-edit same as sequential
	shouldSplit := false
	if change.Type == ChangeTypeEdit {
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
