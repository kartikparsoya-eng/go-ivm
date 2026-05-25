/**
 * go-pipeline-driver.ts — Drop-in replacement for PipelineDriver that routes
 * IVM operations to the Go sidecar process.
 *
 * Feature flag: ZERO_USE_GO_SIDECAR=true|false (default: false)
 *
 * This module provides:
 * - GoPipelineDriver: wraps GoIVMClient with the same interface shape as PipelineDriver
 * - createPipelineDriver(): factory that returns either TS PipelineDriver or Go driver based on flag
 *
 * Fallback behavior:
 * - If the sidecar is not running or crashes, operations throw and the caller
 *   (ViewSyncerService) should catch and fall back to TS PipelineDriver.
 * - The SidecarManager auto-restarts up to N times before marking status='failed'.
 * - When status='failed', the factory returns TS PipelineDriver for new instances.
 *
 * Architecture:
 *   ViewSyncerService
 *     └── GoPipelineDriver (one per client group)
 *           └── GoIVMClient (shared, connected to sidecar socket)
 *                 └── Go sidecar process (multi-engine, one Engine per clientGroupID)
 */

import type {GoIVMClient, RowChange, SnapshotChange, InitParams} from './go-ivm-client.ts';
import type {SidecarManager} from './sidecar-manager.ts';

// --- Types matching PipelineDriver's public interface ---

export type QueryAST = unknown; // builder.AST from zero-protocol

export interface TableSchema {
  columns: Record<string, {type: string; optional?: boolean}>;
  primaryKey: string[];
}

/**
 * GoPipelineDriver — routes IVM operations to the Go sidecar.
 *
 * One instance per client group. All operations include the clientGroupID
 * so the sidecar routes to the correct Engine.
 *
 * Thread-safety: the Go sidecar serializes operations within a group via
 * per-group mutex. The TS side sends requests; the sidecar handles ordering.
 */
export class GoPipelineDriver {
  readonly #client: GoIVMClient;
  readonly #clientGroupID: string;
  #initialized = false;
  #replicaVersion: string | null = null;
  #queries = new Map<string, {ast: QueryAST}>();

  constructor(client: GoIVMClient, clientGroupID: string) {
    this.#client = client;
    this.#clientGroupID = clientGroupID;
  }

  get initialized(): boolean {
    return this.#initialized;
  }

  get replicaVersion(): string | null {
    return this.#replicaVersion;
  }

  get queries(): ReadonlyMap<string, {ast: QueryAST}> {
    return this.#queries;
  }

  /**
   * Initialize the Go engine for this client group.
   * Loads data from the SQLite snapshot into MemorySources.
   */
  async init(params: InitParams & {replicaVersion: string}): Promise<void> {
    await this.#client.init(this.#clientGroupID, {
      dbPath: params.dbPath,
      storagePath: params.storagePath,
      tables: params.tables,
    });
    this.#initialized = true;
    this.#replicaVersion = params.replicaVersion;
  }

  /**
   * Reset the engine (e.g., after schema change or corruption).
   * Destroys and re-inits.
   */
  async reset(params: InitParams & {replicaVersion: string}): Promise<void> {
    await this.#client.destroy(this.#clientGroupID);
    this.#queries.clear();
    this.#initialized = false;
    await this.init(params);
  }

  /**
   * Add a query pipeline. Returns hydration RowChanges.
   */
  async addQuery(queryID: string, ast: QueryAST): Promise<RowChange[]> {
    const changes = await this.#client.addQuery(this.#clientGroupID, queryID, ast);
    this.#queries.set(queryID, {ast});
    return changes;
  }

  /**
   * Add multiple queries. Returns all hydration RowChanges combined.
   */
  async addQueries(
    queries: Array<{queryID: string; ast: QueryAST}>,
  ): Promise<RowChange[]> {
    const allChanges: RowChange[] = [];
    for (const q of queries) {
      const changes = await this.addQuery(q.queryID, q.ast);
      allChanges.push(...changes);
    }
    return allChanges;
  }

  /**
   * Remove a query pipeline.
   */
  async removeQuery(queryID: string): Promise<void> {
    await this.#client.removeQuery(this.#clientGroupID, queryID);
    this.#queries.delete(queryID);
  }

  /**
   * Advance: push snapshot changes through all pipelines.
   * Returns incremental RowChanges for all affected queries.
   */
  async advance(changes: SnapshotChange[]): Promise<{
    changes: RowChange[];
    numChanges: number;
  }> {
    const rowChanges = await this.#client.advance(this.#clientGroupID, changes);
    return {
      changes: rowChanges,
      numChanges: changes.length,
    };
  }

  /**
   * Destroy this client group's engine in the sidecar.
   * Call when the ViewSyncer is destroyed (client disconnects).
   */
  async destroy(): Promise<void> {
    await this.#client.destroy(this.#clientGroupID);
    this.#queries.clear();
    this.#initialized = false;
  }
}

// --- Feature flag and factory ---

/**
 * Check if the Go sidecar feature flag is enabled.
 * Follows the same pattern as ZQLITE_RS_USE_STREAMING_CONSUMER.
 *
 * Set ZERO_USE_GO_SIDECAR=true to enable.
 * Default: false (use TS/Rust PipelineDriver).
 */
export function isGoSidecarEnabled(): boolean {
  return process.env['ZERO_USE_GO_SIDECAR'] === 'true';
}

/**
 * Factory to create a GoPipelineDriver for a client group.
 *
 * Usage in the ViewSyncer/service-runner layer:
 *
 * ```typescript
 * import {isGoSidecarEnabled, createGoPipelineDriver} from './go-pipeline-driver';
 *
 * if (isGoSidecarEnabled() && sidecarManager.status === 'running') {
 *   const goDriver = createGoPipelineDriver(sidecarManager, clientGroupID);
 *   // Use goDriver instead of PipelineDriver
 * } else {
 *   // Fall back to TS PipelineDriver (existing code path)
 * }
 * ```
 */
export function createGoPipelineDriver(
  sidecarManager: SidecarManager,
  clientGroupID: string,
): GoPipelineDriver {
  const client = sidecarManager.getClient();
  return new GoPipelineDriver(client, clientGroupID);
}
