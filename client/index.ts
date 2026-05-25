/**
 * go-ivm client library — production integration for Zero's Go IVM sidecar.
 *
 * The Go sidecar is a COMPUTE BACKEND that sits inside PipelineDriver.
 * It only handles the CPU-bound hot path (advance + hydrate).
 * PipelineDriver keeps all state: Snapshotter, versions, getRow, permissions, etc.
 *
 * Feature flag: ZERO_USE_GO_SIDECAR=true (default: false)
 *
 * Components:
 * - GoIVMClient: Low-level JSON-RPC client over Unix socket
 * - SidecarManager: Process lifecycle (spawn, health, restart)
 * - GoComputeBackend: The integration point inside PipelineDriver
 * - isGoSidecarEnabled(): Check the feature flag
 * - createGoComputeBackend(): Factory with null fallback
 */

export {GoIVMClient} from './go-ivm-client.ts';
export type {
  ColumnSchema,
  TableSchema,
  InitParams,
  SnapshotChange,
  RowChange,
} from './go-ivm-client.ts';

export {SidecarManager} from './sidecar-manager.ts';
export type {SidecarConfig, SidecarStatus} from './sidecar-manager.ts';

export {
  GoComputeBackend,
  isGoSidecarEnabled,
  createGoComputeBackend,
} from './go-compute-backend.ts';
export type {TableSchemaSpec} from './go-compute-backend.ts';
