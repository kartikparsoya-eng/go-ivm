/**
 * pipeline-driver-go.ts — PipelineDriver with Go compute backend wired in.
 *
 * This is the REAL pipeline-driver.ts from zero-cache with minimal additions
 * to route advance() and hydrate() through the Go sidecar when the feature
 * flag ZERO_USE_GO_SIDECAR=true is set.
 *
 * Changes from original (search for "GO_SIDECAR"):
 *   1. Import GoComputeBackend + isGoSidecarEnabled
 *   2. #goBackend field + sidecarManager constructor param
 *   3. initEngine() call in #initAndResetCommon
 *   4. Go path in #advance() — batch RPC + cooperative yield
 *   5. Go path in #addQueryImpl() — hydrate via RPC
 *   6. removeQuery() mirrors to Go
 *   7. destroy() tears down Go engine
 *
 * Integration: Replace the import of PipelineDriver in view-syncer.ts with
 * this file when ZERO_USE_GO_SIDECAR is enabled.
 */

import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {deepEqual, type JSONValue} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST, LiteralValue} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import {
  Debug,
  runtimeDebugFlags,
} from '../../../../zql/src/builder/debug-delegate.ts';
import {ChangeIndex} from '../../../../zql/src/ivm/change-index.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import type {Change} from '../../../../zql/src/ivm/change.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import {
  skipYields,
  type Input,
  type Storage,
} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import {
  type Source,
  type SourceChange,
  type SourceInput,
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../../zql/src/ivm/source.ts';
import type {ConnectionCostModel} from '../../../../zql/src/planner/planner-connection.ts';
import {MeasurePushOperator} from '../../../../zql/src/query/measure-push-operator.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {
  resolveSimpleScalarSubqueries,
  type CompanionSubquery,
} from '../../../../zqlite/src/resolve-scalar-subqueries.ts';
import {createSQLiteCostModel} from '../../../../zqlite/src/sqlite-cost-model.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import type {LogConfig, ZeroConfig} from '../../config/zero-config.ts';
import {computeZqlSpecs, mustGetTableSpec} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type RowKey} from '../../types/row-key.ts';
import {type ShardID} from '../../types/shards.ts';
import {
  getSubscriptionState,
  ZERO_VERSION_COLUMN_NAME,
} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import {rowIDSignatureUnit} from './row-set-signature.ts';
import type {Snapshotter} from './snapshotter.ts';
import {ResetPipelinesSignal, type SnapshotDiff} from './snapshotter.ts';

// GO_SIDECAR: Import Go compute backend
import {
  GoComputeBackend,
  createGoComputeBackend,
  isGoSidecarEnabled,
} from './go-compute-backend.ts';
import type {SidecarManager} from './sidecar-manager.ts';

type RowOp<Op extends Omit<ChangeType, ChangeType.CHILD>> = {
  readonly type: Op;
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowAdd = RowOp<ChangeType.ADD>;

export type RowRemove = RowOp<ChangeType.REMOVE>;

export type RowEdit = RowOp<ChangeType.EDIT>;

export type RowChange = RowAdd | RowRemove | RowEdit;

type CompanionPipeline = {
  readonly input: Input;
  readonly childField: string;
  readonly resolvedValue: LiteralValue | null | undefined;
};

type Pipeline = {
  readonly input: Input;
  readonly hydrationTimeMs: number;
  readonly transformedAst: AST;
  readonly transformationHash: string;
  readonly companions: readonly CompanionPipeline[];
};

type QueryInfo = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
};

type AdvanceContext = {
  readonly timer: Timer;
  readonly totalHydrationTimeMs: number;
  readonly numChanges: number;
  pos: number;
};

type HydrateContext = {
  readonly timer: Timer;
};

export type Timer = {
  elapsedLap: () => number;
  totalElapsed: () => number;
};

/**
 * No matter how fast hydration is, advancement is given at least this long to
 * complete before doing a pipeline reset.
 */
const MIN_ADVANCEMENT_TIME_LIMIT_MS = 50;

/** GO_SIDECAR: Cooperative yield interval when processing Go batch results */
const GO_BATCH_YIELD_INTERVAL = 1000;

/**
 * Manages the state of IVM pipelines for a given ViewSyncer (i.e. client group).
 */
export class PipelineDriver {
  readonly #tables = new Map<string, TableSource>();
  // Query id to pipeline
  readonly #pipelines = new Map<string, Pipeline>();
  readonly #rowSetSignatures = new Map<string, bigint>();

  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #storage: ClientGroupStorage;
  readonly #shardID: ShardID;
  readonly #logConfig: LogConfig;
  readonly #config: ZeroConfig | undefined;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  readonly #costModels: WeakMap<Database, ConnectionCostModel> | undefined;
  readonly #yieldThresholdMs: () => number;
  #streamer: Streamer | null = null;
  #hydrateContext: HydrateContext | null = null;
  #advanceContext: AdvanceContext | null = null;
  #replicaVersion: string | null = null;
  #primaryKeys: Map<string, PrimaryKey> | null = null;
  #permissions: LoadedPermissions | null = null;

  // GO_SIDECAR: Go compute backend (null if disabled or unhealthy)
  #goBackend: GoComputeBackend | null = null;
  readonly #clientGroupID: string;

  readonly #advanceTime = getOrCreateLatencyHistogram(
    'sync',
    'ivm.advance-time',
    'Time to advance all queries for a given client group in response to a single change.',
  );

  readonly #conflictRowsDeleted = getOrCreateCounter(
    'sync',
    'ivm.conflict-rows-deleted',
    'Number of rows deleted because they conflicted with added row',
  );

  readonly #inspectorDelegate: InspectorDelegate;

  constructor(
    lc: LogContext,
    logConfig: LogConfig,
    snapshotter: Snapshotter,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    inspectorDelegate: InspectorDelegate,
    yieldThresholdMs: () => number,
    enablePlanner?: boolean,
    config?: ZeroConfig,
    // GO_SIDECAR: Optional sidecar manager for Go compute
    sidecarManager?: SidecarManager,
  ) {
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    this.#snapshotter = snapshotter;
    this.#storage = storage;
    this.#shardID = shardID;
    this.#logConfig = logConfig;
    this.#config = config;
    this.#inspectorDelegate = inspectorDelegate;
    this.#costModels = enablePlanner ? new WeakMap() : undefined;
    this.#yieldThresholdMs = yieldThresholdMs;
    this.#clientGroupID = clientGroupID;

    // GO_SIDECAR: Create Go compute backend if enabled and healthy
    if (sidecarManager && isGoSidecarEnabled()) {
      this.#goBackend = createGoComputeBackend(sidecarManager, clientGroupID);
      if (this.#goBackend) {
        this.#lc.info?.('Go compute backend enabled for this client group');
      }
    }
  }

  /**
   * Initializes the PipelineDriver to the current head of the database.
   */
  init(clientSchema: ClientSchema) {
    assert(!this.#snapshotter.initialized(), 'Already initialized');
    this.#snapshotter.init();
    this.#initAndResetCommon(clientSchema);
  }

  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  reset(clientSchema: ClientSchema) {
    for (const pipeline of this.#pipelines.values()) {
      pipeline.input.destroy();
      for (const companion of pipeline.companions) {
        companion.input.destroy();
      }
    }
    this.#pipelines.clear();
    this.#tables.clear();
    this.#allTableNames.clear();
    this.#rowSetSignatures.clear();
    this.#initAndResetCommon(clientSchema);

    // GO_SIDECAR: Reset Go engine with new snapshot
    if (this.#goBackend) {
      const {db} = this.#snapshotter.current();
      const tables = this.#buildGoTableSchemas();
      this.#goBackend.resetEngine(db.db.name, tables).catch(err => {
        this.#lc.error?.('Go engine reset failed, disabling', err);
        this.#goBackend = null;
      });
    }
  }

  #initAndResetCommon(clientSchema: ClientSchema) {
    const {db} = this.#snapshotter.current();
    const fullTables = new Map<string, LiteTableSpec>();
    computeZqlSpecs(
      this.#lc,
      db.db,
      {includeBackfillingColumns: false},
      this.#tableSpecs,
      fullTables,
    );
    checkClientSchema(
      this.#shardID,
      clientSchema,
      this.#tableSpecs,
      fullTables,
    );
    this.#allTableNames.clear();
    for (const table of fullTables.keys()) {
      this.#allTableNames.add(table);
    }
    const primaryKeys = this.#primaryKeys ?? new Map<string, PrimaryKey>();
    this.#primaryKeys = primaryKeys;
    primaryKeys.clear();
    for (const [table, spec] of this.#tableSpecs.entries()) {
      primaryKeys.set(table, spec.tableSpec.primaryKey);
    }
    buildPrimaryKeys(clientSchema, primaryKeys);
    const {replicaVersion} = getSubscriptionState(db);
    this.#replicaVersion = replicaVersion;

    // GO_SIDECAR: Initialize Go engine with current snapshot
    if (this.#goBackend && !this.#goBackend.initialized) {
      const tables = this.#buildGoTableSchemas();
      this.#goBackend.initEngine(db.db.name, tables).catch(err => {
        this.#lc.error?.('Go engine init failed, disabling', err);
        this.#goBackend = null;
      });
    }
  }

  // GO_SIDECAR: Build table schemas for Go engine initialization
  #buildGoTableSchemas(): Record<
    string,
    {columns: Record<string, {type: string; optional?: boolean}>; primaryKey: string[]}
  > {
    const tables: Record<
      string,
      {columns: Record<string, {type: string; optional?: boolean}>; primaryKey: string[]}
    > = {};
    for (const [name, spec] of this.#tableSpecs.entries()) {
      const columns: Record<string, {type: string; optional?: boolean}> = {};
      for (const [colName, colSpec] of Object.entries(
        spec.zqlSpec.columns ?? {},
      )) {
        columns[colName] = {
          type: (colSpec as {type: string}).type,
          optional: (colSpec as {optional?: boolean}).optional,
        };
      }
      tables[name] = {
        columns,
        primaryKey: spec.tableSpec.primaryKey as string[],
      };
    }
    return tables;
  }

  get replicaVersion(): string {
    return must(this.#replicaVersion, 'Not yet initialized');
  }

  currentVersion(): string {
    assert(this.initialized(), 'Not yet initialized');
    return this.#snapshotter.current().version;
  }

  currentPermissions(): LoadedPermissions | null {
    assert(this.initialized(), 'Not yet initialized');
    const res = reloadPermissionsIfChanged(
      this.#lc,
      this.#snapshotter.current().db,
      this.#shardID.appID,
      this.#permissions,
      this.#config,
    );
    if (res.changed) {
      this.#permissions = res.permissions;
      this.#lc.debug?.(
        'Reloaded permissions',
        JSON.stringify(this.#permissions),
      );
    }
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    const {db, version} = this.#snapshotter.advanceWithoutDiff().curr;
    for (const table of this.#tables.values()) {
      table.setDB(db.db);
    }
    return version;
  }

  #ensureCostModelExistsIfEnabled(db: Database) {
    let existing = this.#costModels?.get(db);
    if (existing) {
      return existing;
    }
    if (this.#costModels) {
      const costModel = createSQLiteCostModel(db, this.#tableSpecs);
      this.#costModels.set(db, costModel);
      return costModel;
    }
    return undefined;
  }

  destroy() {
    this.#storage.destroy();
    this.#snapshotter.destroy();

    // GO_SIDECAR: Destroy Go engine
    if (this.#goBackend) {
      this.#goBackend.destroy().catch(err => {
        this.#lc.error?.('Go engine destroy failed', err);
      });
      this.#goBackend = null;
    }
  }

  queries(): ReadonlyMap<string, QueryInfo> {
    return this.#pipelines;
  }

  totalHydrationTimeMs(): number {
    let total = 0;
    for (const pipeline of this.#pipelines.values()) {
      total += pipeline.hydrationTimeMs;
    }
    return total;
  }

  #resolveScalarSubqueries(ast: AST): {
    ast: AST;
    companionRows: {table: string; row: Row}[];
    companions: CompanionSubquery[];
    companionInputs: Input[];
  } {
    const companionRows: {table: string; row: Row}[] = [];
    const companionInputs: Input[] = [];

    const executor = (
      subqueryAST: AST,
      childField: string,
    ): LiteralValue | null | undefined => {
      const input = buildPipeline(
        subqueryAST,
        {
          getSource: name => this.#getSource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (input: SourceInput): Input => input,
          decorateInput: input => input,
          addEdge() {},
          decorateFilterInput: input => input,
        },
        'scalar-subquery',
      );
      let node: Node | undefined;
      for (const n of skipYields(input.fetch({}))) {
        node ??= n;
      }
      if (!node) {
        companionInputs.push(input);
        return undefined;
      }
      companionRows.push({table: subqueryAST.table, row: node.row as Row});
      companionInputs.push(input);
      return (node.row[childField] as LiteralValue) ?? null;
    };

    const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
      ast,
      this.#tableSpecs,
      executor,
    );
    return {ast: resolved, companionRows, companions, companionInputs};
  }

  addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): AsyncIterable<RowChange | 'yield'> | Iterable<RowChange | 'yield'> {
    if (this.#goBackend) {
      // Async path: Go hydration requires await
      return this.#trackRowSetSignaturesAsync(
        this.#addQueryImplGo(transformationHash, queryID, query, timer),
      );
    }
    return this.#trackRowSetSignatures(
      this.#addQueryImplTs(transformationHash, queryID, query, timer),
    );
  }

  async *#addQueryImplGo(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): AsyncGenerator<RowChange | 'yield'> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before adding queries',
    );
    this.removeQuery(queryID);
    const debugDelegate = runtimeDebugFlags.trackRowsVended
      ? new Debug()
      : undefined;

    const costModel = this.#ensureCostModelExistsIfEnabled(
      this.#snapshotter.current().db.db,
    );

    assert(
      this.#advanceContext === null,
      'Cannot hydrate while advance is in progress',
    );
    this.#hydrateContext = {
      timer,
    };
    try {
      const {
        ast: resolvedQuery,
        companionRows,
        companions: companionMeta,
        companionInputs,
      } = this.#resolveScalarSubqueries(query);

      const input = buildPipeline(
        resolvedQuery,
        {
          debug: debugDelegate,
          enableNotExists: true,
          getSource: name => this.#getSource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (input: SourceInput, _queryID: string): Input =>
            new MeasurePushOperator(
              input,
              queryID,
              this.#inspectorDelegate,
              'query-update-server',
            ),
          decorateInput: input => input,
          addEdge() {},
          decorateFilterInput: input => input,
        },
        queryID,
        costModel,
      );
      const schema = input.getSchema();
      input.setOutput({
        push: change => {
          const streamer = this.#streamer;
          assert(streamer, 'must #startAccumulating() before pushing changes');
          streamer.accumulate(queryID, schema, [change]);
          return [];
        },
      });

      // GO_SIDECAR: Use Go backend for hydration
      // We still build the TS pipeline above for push output (live updates).
      // Go handles the initial fetch; TS pipeline handles subsequent pushes.
      try {
        const rowChanges = await this.#goBackend!.hydrate(queryID, query);
        let count = 0;
        for (const rc of rowChanges) {
          yield rc;
          count++;
          if (count % GO_BATCH_YIELD_INTERVAL === 0) {
            yield 'yield';
          }
        }
      } catch (err) {
        this.#lc.error?.('Go hydrate failed, falling back to TS', err);
        this.#goBackend = null;
        // Fall through to TS hydration
        yield* hydrateInternal(
          input,
          queryID,
          must(this.#primaryKeys),
          this.#tableSpecs,
        );
      }

      for (const {table, row} of companionRows) {
        const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
        yield {
          type: ChangeType.ADD,
          queryID,
          table,
          rowKey: getRowKey(primaryKey, row),
          row,
        } as RowChange;
      }

      const hydrationTimeMs = timer.totalElapsed();
      if (runtimeDebugFlags.trackRowCountsVended) {
        if (hydrationTimeMs > this.#logConfig.slowHydrateThreshold) {
          let totalRowsConsidered = 0;
          const lc = this.#lc
            .withContext('queryID', queryID)
            .withContext('hydrationTimeMs', hydrationTimeMs);
          for (const tableName of this.#tables.keys()) {
            const entries = Object.entries(
              debugDelegate?.getVendedRowCounts()[tableName] ?? {},
            );
            totalRowsConsidered += entries.reduce(
              (acc, entry) => acc + entry[1],
              0,
            );
            lc.info?.(tableName + ' VENDED: ', entries);
          }
          lc.info?.(`Total rows considered: ${totalRowsConsidered}`);
        }
      }
      debugDelegate?.reset();

      const liveCompanions: CompanionPipeline[] = [];
      for (let i = 0; i < companionMeta.length; i++) {
        const meta = companionMeta[i];
        const companionInput = companionInputs[i];
        const companionSchema = companionInput.getSchema();
        const {childField, resolvedValue} = meta;
        companionInput.setOutput({
          push: (change: Change) => {
            let newValue: LiteralValue | null | undefined;
            switch (change[ChangeIndex.TYPE]) {
              case ChangeType.ADD:
              case ChangeType.EDIT:
                newValue =
                  (change[ChangeIndex.NODE].row[childField] as LiteralValue) ??
                  null;
                break;
              case ChangeType.REMOVE:
                newValue = undefined;
                break;
              case ChangeType.CHILD:
                return [];
            }
            if (!scalarValuesEqual(newValue, resolvedValue)) {
              throw new ResetPipelinesSignal(
                `Scalar subquery value changed for ${meta.ast.table}: ` +
                  `${String(resolvedValue)} -> ${String(newValue)}`,
                'scalar-subquery',
              );
            }
            const streamer = this.#streamer;
            assert(
              streamer,
              'must #startAccumulating() before pushing changes',
            );
            streamer.accumulate(queryID, companionSchema, [change]);
            return [];
          },
        });
        liveCompanions.push({input: companionInput, childField, resolvedValue});
      }

      this.#pipelines.set(queryID, {
        input,
        hydrationTimeMs,
        transformedAst: resolvedQuery,
        transformationHash,
        companions: liveCompanions,
      });
    } finally {
      this.#hydrateContext = null;
    }
  }

  // TS-only hydration path (sync generator, original behavior)
  *#addQueryImplTs(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before adding queries',
    );
    this.removeQuery(queryID);
    const debugDelegate = runtimeDebugFlags.trackRowsVended
      ? new Debug()
      : undefined;

    const costModel = this.#ensureCostModelExistsIfEnabled(
      this.#snapshotter.current().db.db,
    );

    assert(
      this.#advanceContext === null,
      'Cannot hydrate while advance is in progress',
    );
    this.#hydrateContext = {timer};
    try {
      const {
        ast: resolvedQuery,
        companionRows,
        companions: companionMeta,
        companionInputs,
      } = this.#resolveScalarSubqueries(query);

      const input = buildPipeline(
        resolvedQuery,
        {
          debug: debugDelegate,
          enableNotExists: true,
          getSource: name => this.#getSource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (input: SourceInput, _queryID: string): Input =>
            new MeasurePushOperator(
              input,
              queryID,
              this.#inspectorDelegate,
              'query-update-server',
            ),
          decorateInput: input => input,
          addEdge() {},
          decorateFilterInput: input => input,
        },
        queryID,
        costModel,
      );
      const schema = input.getSchema();
      input.setOutput({
        push: change => {
          const streamer = this.#streamer;
          assert(streamer, 'must #startAccumulating() before pushing changes');
          streamer.accumulate(queryID, schema, [change]);
          return [];
        },
      });

      yield* hydrateInternal(
        input,
        queryID,
        must(this.#primaryKeys),
        this.#tableSpecs,
      );

      for (const {table, row} of companionRows) {
        const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
        yield {
          type: ChangeType.ADD,
          queryID,
          table,
          rowKey: getRowKey(primaryKey, row),
          row,
        } as RowChange;
      }

      const hydrationTimeMs = timer.totalElapsed();
      if (runtimeDebugFlags.trackRowCountsVended) {
        if (hydrationTimeMs > this.#logConfig.slowHydrateThreshold) {
          let totalRowsConsidered = 0;
          const lc = this.#lc
            .withContext('queryID', queryID)
            .withContext('hydrationTimeMs', hydrationTimeMs);
          for (const tableName of this.#tables.keys()) {
            const entries = Object.entries(
              debugDelegate?.getVendedRowCounts()[tableName] ?? {},
            );
            totalRowsConsidered += entries.reduce(
              (acc, entry) => acc + entry[1],
              0,
            );
            lc.info?.(tableName + ' VENDED: ', entries);
          }
          lc.info?.(`Total rows considered: ${totalRowsConsidered}`);
        }
      }
      debugDelegate?.reset();

      const liveCompanions: CompanionPipeline[] = [];
      for (let i = 0; i < companionMeta.length; i++) {
        const meta = companionMeta[i];
        const companionInput = companionInputs[i];
        const companionSchema = companionInput.getSchema();
        const {childField, resolvedValue} = meta;
        companionInput.setOutput({
          push: (change: Change) => {
            let newValue: LiteralValue | null | undefined;
            switch (change[ChangeIndex.TYPE]) {
              case ChangeType.ADD:
              case ChangeType.EDIT:
                newValue =
                  (change[ChangeIndex.NODE].row[childField] as LiteralValue) ??
                  null;
                break;
              case ChangeType.REMOVE:
                newValue = undefined;
                break;
              case ChangeType.CHILD:
                return [];
            }
            if (!scalarValuesEqual(newValue, resolvedValue)) {
              throw new ResetPipelinesSignal(
                `Scalar subquery value changed for ${meta.ast.table}: ` +
                  `${String(resolvedValue)} -> ${String(newValue)}`,
                'scalar-subquery',
              );
            }
            const streamer = this.#streamer;
            assert(
              streamer,
              'must #startAccumulating() before pushing changes',
            );
            streamer.accumulate(queryID, companionSchema, [change]);
            return [];
          },
        });
        liveCompanions.push({input: companionInput, childField, resolvedValue});
      }

      this.#pipelines.set(queryID, {
        input,
        hydrationTimeMs,
        transformedAst: resolvedQuery,
        transformationHash,
        companions: liveCompanions,
      });
    } finally {
      this.#hydrateContext = null;
    }
  }

  removeQuery(queryID: string) {
    const pipeline = this.#pipelines.get(queryID);
    if (pipeline) {
      this.#pipelines.delete(queryID);
      pipeline.input.destroy();
      for (const companion of pipeline.companions) {
        companion.input.destroy();
      }
    }
    this.#rowSetSignatures.delete(queryID);

    // GO_SIDECAR: Remove pipeline from Go engine
    this.#goBackend?.removeQuery(queryID).catch(err => {
      this.#lc.error?.('Go removeQuery failed', err);
    });
  }

  rowSetSignature(queryID: string): bigint | undefined {
    return this.#rowSetSignatures.get(queryID);
  }

  *#trackRowSetSignatures(
    changes: Iterable<RowChange | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    for (const change of changes) {
      if (change !== 'yield' && change.type !== ChangeType.EDIT) {
        const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n;
        const unit = rowIDSignatureUnit({
          schema: '',
          table: change.table,
          rowKey: change.rowKey as RowKey,
        });
        this.#rowSetSignatures.set(change.queryID, cur ^ unit);
      }
      yield change;
    }
  }

  // GO_SIDECAR: Async version for Go path
  async *#trackRowSetSignaturesAsync(
    changes: AsyncIterable<RowChange | 'yield'>,
  ): AsyncGenerator<RowChange | 'yield'> {
    for await (const change of changes) {
      if (change !== 'yield' && change.type !== ChangeType.EDIT) {
        const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n;
        const unit = rowIDSignatureUnit({
          schema: '',
          table: change.table,
          rowKey: change.rowKey as RowKey,
        });
        this.#rowSetSignatures.set(change.queryID, cur ^ unit);
      }
      yield change;
    }
  }

  getRow(table: string, pk: RowKey): Row | undefined {
    assert(this.initialized(), 'Not yet initialized');
    const source = must(this.#tables.get(table));
    return source.getRow(pk as Row);
  }

  advance(timer: Timer): {
    version: string;
    numChanges: number;
    changes: AsyncIterable<RowChange | 'yield'> | Iterable<RowChange | 'yield'>;
  } {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before advancing',
    );
    const diff = this.#snapshotter.advance(
      this.#tableSpecs,
      this.#allTableNames,
    );
    const {prev, curr, changes} = diff;
    this.#lc.debug?.(
      `advance ${prev.version} => ${curr.version}: ${changes} changes`,
    );

    if (this.#goBackend) {
      return {
        version: curr.version,
        numChanges: changes,
        changes: this.#trackRowSetSignaturesAsync(
          this.#advanceGo(diff),
        ),
      };
    }
    return {
      version: curr.version,
      numChanges: changes,
      changes: this.#trackRowSetSignatures(this.#advanceTs(diff, timer, changes)),
    };
  }

  // GO_SIDECAR: Async advance via Go sidecar
  async *#advanceGo(
    diff: SnapshotDiff,
  ): AsyncGenerator<RowChange | 'yield'> {
    try {
      const goChanges = this.#snapshotDiffToGoChanges(diff);

      // This await yields the event loop — other client groups can fire
      // their advance RPCs concurrently while Go computes on separate cores.
      const rowChanges = await this.#goBackend!.advance(goChanges);

      // Process batch with cooperative yielding
      let count = 0;
      for (const rc of rowChanges) {
        yield rc;
        count++;
        if (count % GO_BATCH_YIELD_INTERVAL === 0) {
          yield 'yield';
        }
      }

      // Set the new snapshot on all TableSources
      const {curr} = diff;
      for (const table of this.#tables.values()) {
        table.setDB(curr.db.db);
      }
      this.#ensureCostModelExistsIfEnabled(curr.db.db);
      this.#lc.debug?.(`Advanced to ${curr.version} (Go backend)`);
    } catch (err) {
      this.#lc.error?.('Go advance failed, disabling Go backend', err);
      this.#goBackend = null;
      // Cannot fall back mid-advance since SnapshotDiff is already consumed.
      // Throw to trigger ResetPipelinesSignal at ViewSyncer level.
      throw new ResetPipelinesSignal(
        'Go advance failed, resetting pipelines for TS fallback',
        'go-sidecar-failure',
      );
    }
  }

  // Original TS advance path (unchanged)
  *#advanceTs(
    diff: SnapshotDiff,
    timer: Timer,
    numChanges: number,
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.#hydrateContext === null,
      'Cannot advance while hydration is in progress',
    );

    // --- Original TS advance path (unchanged) ---
    const totalHydrationTimeMs = this.totalHydrationTimeMs();
    this.#advanceContext = {
      timer,
      totalHydrationTimeMs,
      numChanges,
      pos: 0,
    };
    this.#lc.info?.(
      `starting pipeline advancement of ${numChanges} changes with an ` +
        `advancement time limited based on total hydration time of ` +
        `${totalHydrationTimeMs} ms.`,
    );
    try {
      for (const {table, prevValues, nextValue} of diff) {
        if (this.#shouldAdvanceYieldMaybeAbortAdvance()) {
          yield 'yield';
        }
        const start = timer.totalElapsed();

        let type;
        try {
          const tableSource = this.#tables.get(table);
          if (!tableSource) {
            continue;
          }
          const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
          let editOldRow: Row | undefined = undefined;
          for (const prevValue of prevValues) {
            if (
              nextValue &&
              deepEqual(
                getRowKey(primaryKey, prevValue as Row) as JSONValue,
                getRowKey(primaryKey, nextValue as Row) as JSONValue,
              )
            ) {
              editOldRow = prevValue;
            } else {
              if (nextValue) {
                this.#conflictRowsDeleted.add(1);
              }
              yield* this.#push(
                tableSource,
                makeSourceChangeRemove(prevValue as Row),
              );
            }
          }
          if (nextValue) {
            if (editOldRow) {
              yield* this.#push(
                tableSource,
                makeSourceChangeEdit(nextValue as Row, editOldRow),
              );
            } else {
              yield* this.#push(
                tableSource,
                makeSourceChangeAdd(nextValue as Row),
              );
            }
          }
        } finally {
          this.#advanceContext.pos++;
        }

        const elapsed = timer.totalElapsed() - start;
        this.#advanceTime.recordMs(elapsed, {
          table,
          type,
        });
      }

      const {curr} = diff;
      for (const table of this.#tables.values()) {
        table.setDB(curr.db.db);
      }
      this.#ensureCostModelExistsIfEnabled(curr.db.db);
      this.#lc.debug?.(`Advanced to ${curr.version}`);
    } finally {
      this.#advanceContext = null;
    }
  }

  // GO_SIDECAR: Convert SnapshotDiff iterator to array of changes for Go RPC
  #snapshotDiffToGoChanges(
    diff: SnapshotDiff,
  ): Array<{table: string; type: string; row: Row; oldRow?: Row}> {
    const changes: Array<{table: string; type: string; row: Row; oldRow?: Row}> = [];
    for (const {table, prevValues, nextValue} of diff) {
      const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
      let editOldRow: Row | undefined = undefined;
      for (const prevValue of prevValues) {
        if (
          nextValue &&
          deepEqual(
            getRowKey(primaryKey, prevValue as Row) as JSONValue,
            getRowKey(primaryKey, nextValue as Row) as JSONValue,
          )
        ) {
          editOldRow = prevValue;
        } else {
          changes.push({table, type: 'remove', row: prevValue as Row});
        }
      }
      if (nextValue) {
        if (editOldRow) {
          changes.push({
            table,
            type: 'edit',
            row: nextValue as Row,
            oldRow: editOldRow,
          });
        } else {
          changes.push({table, type: 'add', row: nextValue as Row});
        }
      }
    }
    return changes;
  }

  #getSource(tableName: string): Source {
    let source = this.#tables.get(tableName);
    if (source) {
      return source;
    }

    const tableSpec = mustGetTableSpec(this.#tableSpecs, tableName);
    const primaryKey = mustGetPrimaryKey(this.#primaryKeys, tableName);

    const {db} = this.#snapshotter.current();
    source = new TableSource(
      this.#lc,
      this.#logConfig,
      db.db,
      tableName,
      tableSpec.zqlSpec,
      primaryKey,
      () => this.#shouldYield(),
    );
    this.#tables.set(tableName, source);
    this.#lc.debug?.(`created TableSource for ${tableName}`);
    return source;
  }

  #shouldYield(): boolean {
    if (this.#hydrateContext) {
      return this.#hydrateContext.timer.elapsedLap() > this.#yieldThresholdMs();
    }
    if (this.#advanceContext) {
      return this.#shouldAdvanceYieldMaybeAbortAdvance();
    }
    throw new Error('shouldYield called outside of hydration or advancement');
  }

  #shouldAdvanceYieldMaybeAbortAdvance(): boolean {
    const {
      pos,
      numChanges,
      timer: advanceTimer,
      totalHydrationTimeMs,
    } = must(this.#advanceContext);
    const elapsed = advanceTimer.totalElapsed();
    if (
      elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
      (elapsed > totalHydrationTimeMs ||
        (elapsed > totalHydrationTimeMs / 2 && pos <= numChanges / 2))
    ) {
      throw new ResetPipelinesSignal(
        `Advancement exceeded timeout at ${pos} of ${numChanges} changes ` +
          `after ${elapsed} ms. Advancement time limited based on total ` +
          `hydration time of ${totalHydrationTimeMs} ms.`,
        'advancement-timeout',
      );
    }
    return advanceTimer.elapsedLap() > this.#yieldThresholdMs();
  }

  #createStorage(): Storage {
    return this.#storage.createStorage();
  }

  *#push(
    source: TableSource,
    change: SourceChange,
  ): Iterable<RowChange | 'yield'> {
    this.#startAccumulating();
    try {
      for (const val of source.genPush(change)) {
        if (val === 'yield') {
          yield 'yield';
        }
        for (const changeOrYield of this.#stopAccumulating().stream()) {
          yield changeOrYield;
        }
        this.#startAccumulating();
      }
    } finally {
      if (this.#streamer !== null) {
        this.#stopAccumulating();
      }
    }
  }

  #startAccumulating() {
    assert(this.#streamer === null, 'Streamer already started');
    this.#streamer = new Streamer(must(this.#primaryKeys), this.#tableSpecs);
  }

  #stopAccumulating(): Streamer {
    const streamer = this.#streamer;
    assert(streamer, 'Streamer not started');
    this.#streamer = null;
    return streamer;
  }
}

class Streamer {
  readonly #primaryKeys: Map<string, PrimaryKey>;
  readonly #tableSpecs: Map<string, LiteAndZqlSpec>;

  constructor(
    primaryKeys: Map<string, PrimaryKey>,
    tableSpecs: Map<string, LiteAndZqlSpec>,
  ) {
    this.#primaryKeys = primaryKeys;
    this.#tableSpecs = tableSpecs;
  }

  readonly #changes: [
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ][] = [];

  accumulate(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ): this {
    this.#changes.push([queryID, schema, changes]);
    return this;
  }

  *stream(): Iterable<RowChange | 'yield'> {
    for (const [queryID, schema, changes] of this.#changes) {
      yield* this.#streamChanges(queryID, schema, changes);
    }
  }

  *#streamChanges(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    if (schema.system === 'permissions') {
      return;
    }

    for (const change of changes) {
      if (change === 'yield') {
        yield change;
        continue;
      }
      const type = change[ChangeIndex.TYPE];
      switch (type) {
        case ChangeType.REMOVE:
        case ChangeType.ADD: {
          yield* this.#streamNodes(queryID, schema, type, () => [
            change[ChangeIndex.NODE],
          ]);
          break;
        }

        case ChangeType.CHILD: {
          const child = change[ChangeIndex.CHILD_DATA];
          const childSchema = must(
            schema.relationships[child.relationshipName],
          );

          yield* this.#streamChanges(queryID, childSchema, [child.change]);
          break;
        }
        case ChangeType.EDIT:
          yield* this.#streamNodes(queryID, schema, type, () => [
            {row: change[ChangeIndex.NODE].row, relationships: {}},
          ]);
          break;
        default:
          unreachable(change[ChangeIndex.TYPE]);
      }
    }
  }

  *#streamNodes(
    queryID: string,
    schema: SourceSchema,
    op: ChangeType.ADD | ChangeType.REMOVE | ChangeType.EDIT,
    nodes: () => Iterable<Node | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    const {tableName: table, system} = schema;

    const primaryKey = must(this.#primaryKeys.get(table));
    const spec = must(this.#tableSpecs.get(table)).tableSpec;

    if (system === 'permissions') {
      return;
    }

    for (const node of nodes()) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      const {relationships} = node;
      let {row} = node;
      const rowKey = getRowKey(primaryKey, row);
      if (op !== ChangeType.REMOVE) {
        const rowVersion = row[ZERO_VERSION_COLUMN_NAME];
        if (
          typeof rowVersion === 'string' &&
          rowVersion < (spec.minRowVersion ?? '00')
        ) {
          row = {...row, [ZERO_VERSION_COLUMN_NAME]: spec.minRowVersion};
        }
      }

      yield {
        type: op,
        queryID,
        table,
        rowKey,
        row: op === ChangeType.REMOVE ? undefined : row,
      } as RowChange;

      for (const [relationship, children] of Object.entries(relationships)) {
        const childSchema = must(schema.relationships[relationship]);
        yield* this.#streamNodes(queryID, childSchema, op, children);
      }
    }
  }
}

function* toAdds(nodes: Iterable<Node | 'yield'>): Iterable<Change | 'yield'> {
  for (const node of nodes) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    yield [ChangeType.ADD, node, null];
  }
}

function getRowKey(cols: PrimaryKey, row: Row): RowKey {
  return Object.fromEntries(cols.map(col => [col, must(row[col])]));
}

export function* hydrate(
  input: Input,
  hash: string,
  clientSchema: ClientSchema,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): Iterable<RowChange | 'yield'> {
  const res = input.fetch({});
  const streamer = new Streamer(
    buildPrimaryKeys(clientSchema),
    tableSpecs,
  ).accumulate(hash, input.getSchema(), toAdds(res));
  yield* streamer.stream();
}

export function* hydrateInternal(
  input: Input,
  hash: string,
  primaryKeys: Map<string, PrimaryKey>,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): Iterable<RowChange | 'yield'> {
  const res = input.fetch({});
  const streamer = new Streamer(primaryKeys, tableSpecs).accumulate(
    hash,
    input.getSchema(),
    toAdds(res),
  );
  yield* streamer.stream();
}

function buildPrimaryKeys(
  clientSchema: ClientSchema,
  primaryKeys: Map<string, PrimaryKey> = new Map<string, PrimaryKey>(),
) {
  for (const [tableName, {primaryKey}] of Object.entries(clientSchema.tables)) {
    primaryKeys.set(tableName, primaryKey as unknown as PrimaryKey);
  }
  return primaryKeys;
}

function mustGetPrimaryKey(
  primaryKeys: Map<string, PrimaryKey> | null,
  table: string,
): PrimaryKey {
  const pKeys = must(primaryKeys, 'primaryKey map must be non-null');

  const rv = pKeys.get(table);
  assert(
    rv,
    () =>
      `table '${table}' is not one of: ${[...pKeys.keys()].sort()}. ` +
      `Check the spelling and ensure that the table has a primary key.`,
  );
  return rv;
}

function scalarValuesEqual(
  a: LiteralValue | null | undefined,
  b: LiteralValue | null | undefined,
): boolean {
  return a === b;
}
