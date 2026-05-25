/**
 * integration-example.ts — Shows how to wire GoComputeBackend into PipelineDriver.
 *
 * The Go sidecar is a COMPUTE BACKEND — it does NOT replace PipelineDriver.
 * PipelineDriver keeps all its state and logic. The Go backend only handles
 * the two CPU-bound operations: advance (genPush) and hydrate (fetch).
 *
 * Feature flag: ZERO_USE_GO_SIDECAR=true|false (default: false)
 * Fallback: If Go throws, disable it for this group and continue with TS.
 */

// ============================================================
// STEP 1: At zero-cache startup, start the sidecar process
// ============================================================

/*
import {SidecarManager, isGoSidecarEnabled} from '@zero/go-ivm-client';

// In the service-runner / main entry point:
let sidecarManager: SidecarManager | null = null;

if (isGoSidecarEnabled()) {
  sidecarManager = new SidecarManager({
    binaryPath: process.env['GO_IVM_BINARY_PATH'] ?? '/usr/local/bin/go-ivm-sidecar',
    maxRestarts: 3,
  });
  await sidecarManager.start();
}
*/

// ============================================================
// STEP 2: In PipelineDriver constructor, create the backend
// ============================================================

/*
// pipeline-driver.ts — add these changes:

import {createGoComputeBackend, isGoSidecarEnabled, GoComputeBackend} from '@zero/go-ivm-client';

export class PipelineDriver {
  // ... existing fields ...
  #goBackend: GoComputeBackend | null = null;

  constructor(...args, sidecarManager?: SidecarManager) {
    // ... existing constructor logic ...

    // NEW: create Go compute backend (returns null if flag off or sidecar unhealthy)
    if (sidecarManager && isGoSidecarEnabled()) {
      this.#goBackend = createGoComputeBackend(sidecarManager, clientGroupID);
    }
  }
*/

// ============================================================
// STEP 3: In PipelineDriver.init(), init the Go engine
// ============================================================

/*
  init(clientSchema: ClientSchema) {
    // ... existing: snapshotter.init(), set up tables ...

    // NEW: initialize Go engine with same SQLite + schema
    if (this.#goBackend) {
      const dbPath = this.#snapshotter.current().db.db.name;
      const tables = Object.fromEntries(
        [...this.#tables.entries()].map(([name, ts]) => [name, {
          columns: ts.columns,
          primaryKey: ts.primaryKey,
        }])
      );
      await this.#goBackend.initEngine(dbPath, tables);
    }
  }
*/

// ============================================================
// STEP 4: In PipelineDriver.addQuery(), use Go for hydration
// ============================================================

/*
  *addQuery(transformationHash, queryID, query, timer) {
    // ... existing: build TS pipeline for getRow/permissions ...

    if (this.#goBackend) {
      // Go handles hydration (parallel fetch across all operator trees)
      try {
        const rowChanges = await this.#goBackend.hydrate(queryID, query.ast);
        for (const rc of rowChanges) {
          // Feed into existing rowState XOR tracking
          this.#updateRowState(rc);
          yield rc;
        }
        return;
      } catch (err) {
        this.#lc.error?.('Go hydrate failed, falling back to TS', err);
        this.#goBackend = null; // disable for this group
      }
    }

    // Existing TS hydration path (unchanged, used as fallback)
    yield* hydrateInternal(...);
  }
*/

// ============================================================
// STEP 5: In PipelineDriver.advance(), use Go for push
// ============================================================

/*
  *advance(timer: Timer) {
    const {prev, curr} = this.#snapshotter.advance();
    const snapshotDiff = computeDiff(prev, curr);  // existing logic

    if (this.#goBackend) {
      // Go handles the genPush loop (parallel across pipelines).
      // The `await` below yields the event loop — other groups fire their
      // RPCs concurrently while Go computes on separate cores (Option 3).
      try {
        const rowChanges = await this.#goBackend.advance(snapshotDiff);
        // Process batch with cooperative yielding (same as TS generator path)
        for (let i = 0; i < rowChanges.length; i++) {
          const rc = rowChanges[i];
          this.#updateRowState(rc);
          yield rc;
          // Yield event loop periodically so other groups can process too
          if (i % 1000 === 999) yield 'yield' as const;
        }
        return;
      } catch (err) {
        this.#lc.error?.('Go advance failed, falling back to TS', err);
        this.#goBackend = null; // disable for this group
        // Fall through to TS path below
      }
    }

    // Existing TS advance path (unchanged, used as fallback)
    for (const change of snapshotDiff) {
      const source = this.#tables.get(change.table);
      yield* source.genPush(change);
    }
  }
*/

// ============================================================
// STEP 6: In PipelineDriver.removeQuery(), mirror to Go
// ============================================================

/*
  removeQuery(queryID: string) {
    // ... existing TS cleanup ...
    this.#goBackend?.removeQuery(queryID);  // fire-and-forget
  }
*/

// ============================================================
// STEP 7: In PipelineDriver.destroy(), destroy Go engine
// ============================================================

/*
  destroy() {
    // ... existing TS cleanup ...
    this.#goBackend?.destroy();  // fire-and-forget
    this.#goBackend = null;
  }
*/

// ============================================================
// KEY DESIGN DECISIONS
// ============================================================

/*
1. PipelineDriver keeps ALL state — nothing migrates to Go:
   - Snapshotter (version management)
   - #tables (TableSource instances for getRow)
   - #rowState (XOR signature tracking)
   - #pipelines (query registry)
   - permissions, timers, logging

2. Go only does COMPUTE — receives data, pushes through operators, returns results:
   - advance(snapshotDiff) → RowChange[]
   - hydrate(queryID, ast) → RowChange[]
   - No version tracking, no persistence, no auth

3. Fallback is PER-GROUP — if Go fails for one group, only that group reverts to TS.
   Other groups keep using Go. No global fallback.

4. RowState tracking stays in TS — PipelineDriver updates #rowState from Go's
   RowChanges the same way it does from TS's yielded changes. The XOR hash
   stays consistent regardless of backend.

5. getRow() stays in TS — uses TableSource (SQLite) directly. Fast enough for
   single-row lookups. No need to round-trip to Go for this.

6. Snapshotter stays in TS — manages SQLite file lifecycle, WAL checkpoints,
   version tracking. Go reads from the same SQLite file (read-only, WAL mode).

7. Go must see the SAME data as TS — both read from the same SQLite snapshot file.
   Go reads it at initEngine() time into MemorySources. On advance(), Go receives
   the same snapshotDiff that TS's Snapshotter computed. Correctness is guaranteed
   because both see identical inputs.
*/

export {};
