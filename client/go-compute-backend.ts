/**
 * go-compute-backend.ts — Go sidecar as a compute backend for PipelineDriver.
 *
 * This does NOT replace PipelineDriver. It sits INSIDE PipelineDriver and only
 * handles the two CPU-bound operations:
 *
 *   1. advance (genPush loop) — push snapshot diffs through operator trees
 *   2. hydrate (fetch) — initial query hydration
 *
 * Everything else stays in TS PipelineDriver:
 *   - Snapshotter (version management, SQLite snapshot lifecycle)
 *   - getRow() (single-row lookup for CVR)
 *   - rowSetSignature() (XOR hash tracking)
 *   - queries() map
 *   - currentPermissions()
 *   - replicaVersion / currentVersion
 *   - time budgeting / yield logic
 *   - destroy / cleanup
 *
 * Feature flag: ZERO_USE_GO_SIDECAR=true|false (default: false)
 *
 * Integration point in PipelineDriver:
 *
 *   // In PipelineDriver constructor:
 *   this.#goBackend = isGoSidecarEnabled()
 *     ? new GoComputeBackend(sidecarClient, clientGroupID)
 *     : null;
 *
 *   // In PipelineDriver.#advance():
 *   if (this.#goBackend) {
 *     const rowChanges = await this.#goBackend.advance(snapshotDiff);
 *     for (const rc of rowChanges) yield rc;
 *   } else {
 *     // existing TS genPush loop (unchanged)
 *   }
 *
 *   // In PipelineDriver.addQuery():
 *   if (this.#goBackend) {
 *     const rowChanges = await this.#goBackend.hydrate(queryID, ast);
 *     for (const rc of rowChanges) yield rc;
 *   } else {
 *     // existing TS hydration (unchanged)
 *   }
 *
 * Architecture:
 *   PipelineDriver (TS — unchanged, owns all state)
 *     ├── Snapshotter, queries, permissions, getRow, versions
 *     └── GoComputeBackend (optional, behind flag)
 *           └── GoIVMClient → Unix socket → Go sidecar
 *                                             └── Engine (parallel operator trees)
 */

import type {GoIVMClient, RowChange, SnapshotChange, TableData} from './go-ivm-client.ts';
import type {SidecarManager} from './sidecar-manager.ts';

export type QueryAST = unknown;

export interface TableSchemaSpec {
  columns: Record<string, {type: 'boolean' | 'number' | 'string' | 'null' | 'json'; optional?: boolean | undefined}>;
  primaryKey: string[];
}

/**
 * GoComputeBackend — handles only the CPU-intensive IVM compute.
 *
 * Sits inside PipelineDriver. PipelineDriver calls:
 *   - initEngine() once after Snapshotter.init()
 *   - hydrate() inside addQuery() instead of fetch + yield
 *   - advance() inside #advance() instead of genPush loop
 *   - removeQuery() when pipeline is destroyed
 *   - destroy() when PipelineDriver is destroyed
 *
 * All other PipelineDriver state/logic is unaffected.
 */
export class GoComputeBackend {
  readonly #client: GoIVMClient;
  readonly #clientGroupID: string;
  #initialized = false;

  constructor(client: GoIVMClient, clientGroupID: string) {
    this.#client = client;
    this.#clientGroupID = clientGroupID;
  }

  get initialized(): boolean {
    return this.#initialized;
  }

  /**
   * Initialize the Go engine with pre-loaded table data.
   * Called once from PipelineDriver.init(), after Snapshotter is ready.
   * TS reads rows from SQLite (WAL2-compatible) and sends them to Go.
   *
   * @param tables - Table schemas with pre-loaded rows
   */
  async initEngine(
    tables: Record<string, TableData>,
  ): Promise<void> {
    await this.#client.init(this.#clientGroupID, {tables});
    this.#initialized = true;
  }

  /**
   * Re-initialize after snapshot swap (e.g., version mismatch reset).
   * Called from PipelineDriver.reset().
   */
  async resetEngine(
    tables: Record<string, TableData>,
  ): Promise<void> {
    await this.#client.destroy(this.#clientGroupID);
    this.#initialized = false;
    await this.initEngine(tables);
  }

  /**
   * Hydrate a query — build pipeline in Go and return initial state as ADDs.
   *
   * Replaces the TS hydration path:
   *   pipeline.input.fetch() → yield nodes as RowChanges
   *
   * Called from PipelineDriver.addQuery() when Go backend is active.
   */
  async hydrate(queryID: string, ast: QueryAST): Promise<RowChange[]> {
    return this.#client.addQuery(this.#clientGroupID, queryID, ast);
  }

  /**
   * Advance — push snapshot diff through all pipelines in parallel.
   *
   * Replaces the TS advance path:
   *   for (change of snapshotDiff) { yield* tableSource.genPush(change) }
   *
   * Called from PipelineDriver.advance() when Go backend is active.
   * Returns flat RowChanges that PipelineDriver yields to ViewSyncer.
   *
   * The Go sidecar pushes changes through ALL pipelines for this client group
   * concurrently (one goroutine per pipeline fan-out). This is where the
   * parallelism happens — multiple pipelines execute on multiple cores.
   */
  async advance(changes: SnapshotChange[]): Promise<RowChange[]> {
    return this.#client.advance(this.#clientGroupID, changes);
  }

  /**
   * Remove a query's pipeline from the Go engine.
   * Called from PipelineDriver.removeQuery().
   */
  async removeQuery(queryID: string): Promise<void> {
    await this.#client.removeQuery(this.#clientGroupID, queryID);
  }

  /**
   * Destroy the Go engine for this client group.
   * Called from PipelineDriver.destroy().
   */
  async destroy(): Promise<void> {
    if (this.#initialized) {
      await this.#client.destroy(this.#clientGroupID);
      this.#initialized = false;
    }
  }
}

// --- Feature flags ---

/**
 * Check if the Go compute backend is enabled.
 *
 * Set ZERO_USE_GO_SIDECAR=true to enable.
 * Default: false (TS operator trees run inline, single-threaded).
 */
export function isGoSidecarEnabled(): boolean {
  return process.env['ZERO_USE_GO_SIDECAR'] === 'true';
}

/**
 * Shadow mode: run BOTH Go and TS paths, compare results, log mismatches.
 * TS is the source of truth. Go results are discarded after comparison.
 *
 * Set ZERO_GO_SHADOW_MODE=true to enable.
 * Requires ZERO_USE_GO_SIDECAR=true as well.
 */
export function isGoShadowMode(): boolean {
  return process.env['ZERO_GO_SHADOW_MODE'] === 'true';
}

/**
 * Create a GoComputeBackend instance for a PipelineDriver.
 *
 * Call this in the PipelineDriver constructor:
 *
 * ```typescript
 * this.#goBackend = isGoSidecarEnabled()
 *   ? createGoComputeBackend(sidecarManager, this.#clientGroupID)
 *   : null;
 * ```
 *
 * If sidecarManager is unavailable or unhealthy, returns null (fallback to TS).
 */
export function createGoComputeBackend(
  sidecarManager: SidecarManager,
  clientGroupID: string,
): GoComputeBackend | null {
  try {
    if (sidecarManager.status !== 'running') return null;
    const client = sidecarManager.getClient();
    return new GoComputeBackend(client, clientGroupID);
  } catch {
    return null;
  }
}
