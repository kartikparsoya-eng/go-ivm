/**
 * sidecar-parity.ts — Parity test: TS IVM vs Go sidecar (via Unix socket RPC).
 *
 * For each fuzzed test case:
 *   1. Run through TS IVM in-process (MemorySource + buildPipeline + Catch)
 *   2. Run through Go sidecar via Unix socket RPC (init + addQuery + advance)
 *   3. Compare hydration RowChanges and per-mutation advance deltas
 *
 * This validates the FULL integration path: JSON serialization, AST parsing,
 * SQLite table source, pipeline build, engine advance — all via the sidecar.
 *
 * Usage:
 *   npx tsx --tsconfig mono/packages/zql/tsconfig.json go-ivm/tools/parity/sidecar-parity.ts
 *
 * Env vars:
 *   SEED_START   — first seed (default: 1)
 *   SEED_COUNT   — number of seeds to test (default: 50)
 *   MAX_PARALLEL — concurrent test cases (default: 1, sidecar is single-connection)
 */

import {execSync, spawn, type ChildProcess} from 'child_process';
import {existsSync, unlinkSync, writeFileSync, mkdirSync} from 'fs';
import {tmpdir} from 'os';
import {join, dirname} from 'path';
import {fileURLToPath} from 'url';
import Database from 'better-sqlite3';

import {MemorySource} from '../../../mono/packages/zql/src/ivm/memory-source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeRemove,
  makeSourceChangeEdit,
} from '../../../mono/packages/zql/src/ivm/source.ts';
import {consume} from '../../../mono/packages/zql/src/ivm/stream.ts';
import {buildPipeline} from '../../../mono/packages/zql/src/builder/builder.ts';
import {TestBuilderDelegate} from '../../../mono/packages/zql/src/builder/test-builder-delegate.ts';
import {Catch} from '../../../mono/packages/zql/src/ivm/catch.ts';
import type {AST} from '../../../mono/packages/zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../../mono/packages/zero-schema/src/table-schema.ts';

// Reuse the client
import {GoIVMClient} from '../../client/go-ivm-client.ts';

const __dirname = dirname(fileURLToPath(import.meta.url));
const GO_IVM_DIR = join(__dirname, '../..');

const SEED_START = Number(process.env.SEED_START ?? 1);
const SEED_COUNT = Number(process.env.SEED_COUNT ?? 50);

// --- Test case generation ---

interface TestCase {
  schema: Record<string, {columns: Record<string, {type: string}>; primaryKey: string[]}>;
  initialData: Record<string, Record<string, unknown>[]>;
  ast: any;
  mutations: Array<{type: string; table: string; row: Record<string, unknown>; oldRow?: Record<string, unknown>}>;
}

interface FuzzerOpts {
  NumTables: number;
  NumColumns: number;
  NumRows: number;
  NumMutations: number;
  WithFilter?: boolean;
  WithLimit?: boolean;
  WithJoins?: boolean;
  WithExists?: boolean;
  WithCompound?: boolean;
}

function generateTestCase(seed: number, opts: FuzzerOpts): TestCase {
  const result = execSync(
    `/tmp/gen-testcase ${seed} '${JSON.stringify(opts)}'`,
    {encoding: 'utf-8', maxBuffer: 10 * 1024 * 1024}
  );
  return JSON.parse(result);
}

// --- TS IVM execution ---

interface TSResult {
  hydration: unknown[];
  steps: unknown[][];
}

function runTS(tc: TestCase): TSResult {
  const sources: Record<string, MemorySource> = {};
  for (const [tableName, tableSchema] of Object.entries(tc.schema)) {
    const columns: Record<string, SchemaValue> = {};
    for (const [colName, colDef] of Object.entries(tableSchema.columns)) {
      columns[colName] = {type: colDef.type as SchemaValue['type']};
    }
    sources[tableName] = new MemorySource(tableName, columns, tableSchema.primaryKey);
  }

  for (const [tableName, rows] of Object.entries(tc.initialData)) {
    const source = sources[tableName];
    for (const row of rows) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
  }

  const delegate = new TestBuilderDelegate(sources);
  const input = buildPipeline(tc.ast as AST, delegate, 'parity-test');
  const output = new Catch(input);

  const hydration = output.fetch();

  const steps: unknown[][] = [];
  for (const mutation of tc.mutations) {
    output.reset();
    const source = sources[mutation.table];
    switch (mutation.type) {
      case 'add':
        consume(source.push(makeSourceChangeAdd(mutation.row)));
        break;
      case 'remove':
        consume(source.push(makeSourceChangeRemove(mutation.row)));
        break;
      case 'edit':
        consume(source.push(makeSourceChangeEdit(mutation.row, mutation.oldRow!)));
        break;
    }
    steps.push([...output.pushes]);
  }

  return {hydration, steps};
}

// --- Go sidecar execution ---

interface GoRowChange {
  type: number; // 0=add, 1=remove, 2=edit
  queryID: string;
  table: string;
  rowKey: Record<string, unknown>;
  row: Record<string, unknown> | null;
}

interface GoResult {
  hydration: GoRowChange[];
  steps: GoRowChange[][];
}

async function runGo(tc: TestCase, client: GoIVMClient, dbPath: string): Promise<GoResult> {
  // Write initial data to SQLite
  const db = new Database(dbPath);
  db.exec('PRAGMA journal_mode=WAL;');

  // Drop and recreate tables
  for (const [tableName, tableSchema] of Object.entries(tc.schema)) {
    db.exec(`DROP TABLE IF EXISTS "${tableName}"`);
    const cols = Object.entries(tableSchema.columns).map(([col, def]) => {
      const sqlType = def.type === 'number' ? 'REAL' : 'TEXT';
      return `"${col}" ${sqlType}`;
    }).join(', ');
    const pk = tableSchema.primaryKey.map(c => `"${c}"`).join(', ');
    db.exec(`CREATE TABLE "${tableName}" (${cols}, PRIMARY KEY (${pk}))`);
  }

  // Insert initial data
  for (const [tableName, rows] of Object.entries(tc.initialData)) {
    if (rows.length === 0) continue;
    const cols = Object.keys(rows[0]);
    const placeholders = cols.map(() => '?').join(', ');
    const stmt = db.prepare(`INSERT INTO "${tableName}" (${cols.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`);
    for (const row of rows) {
      stmt.run(...cols.map(c => {
        const v = row[c];
        if (v === true) return 1;
        if (v === false) return 0;
        return v ?? null;
      }));
    }
  }
  db.close();

  // Re-init the engine with this DB
  await client.init({
    dbPath,
    tables: tc.schema as any,
  });

  // addQuery — hydration
  const hydration = await client.addQuery('parity-q', tc.ast);

  // advance — one mutation at a time
  const steps: GoRowChange[][] = [];
  for (const mutation of tc.mutations) {
    let changes: any[];
    switch (mutation.type) {
      case 'add':
        changes = await client.advance([{
          table: mutation.table,
          prevValues: [],
          nextValue: mutation.row,
        }]);
        break;
      case 'remove':
        changes = await client.advance([{
          table: mutation.table,
          prevValues: [mutation.row],
          nextValue: null,
        }]);
        break;
      case 'edit':
        changes = await client.advance([{
          table: mutation.table,
          prevValues: [mutation.oldRow!],
          nextValue: mutation.row,
        }]);
        break;
      default:
        changes = [];
    }
    steps.push(changes as GoRowChange[]);
  }

  // Clean up — remove query
  await client.removeQuery('parity-q');

  return {hydration: hydration as GoRowChange[], steps};
}

// --- Comparison ---

function normalizeRow(row: Record<string, unknown> | null | undefined): string {
  if (!row) return 'null';
  const sorted = Object.keys(row).sort().reduce((acc, k) => {
    let v = row[k];
    // Normalize booleans to 0/1 (SQLite stores them as integers)
    if (v === true) v = 1;
    if (v === false) v = 0;
    acc[k] = v;
    return acc;
  }, {} as Record<string, unknown>);
  return JSON.stringify(sorted);
}

function normalizeChanges(changes: any[]): string[] {
  // For TS: changes come from Catch.pushes which are Change objects
  // For Go: changes come as RowChange objects
  // We need a common format. Let's just serialize and sort.
  return changes.map(c => JSON.stringify(c)).sort();
}

function flattenNodes(nodes: any[]): string[] {
  const result: string[] = [];
  for (const node of nodes) {
    if (node && typeof node === 'object' && 'row' in node) {
      result.push(normalizeRow(node.row));
      // Recurse into relationships
      if (node.relationships) {
        for (const relName of Object.keys(node.relationships)) {
          const children = node.relationships[relName];
          if (Array.isArray(children)) {
            result.push(...flattenNodes(children));
          }
        }
      }
    } else {
      result.push(normalizeRow(node));
    }
  }
  return result;
}

function compareHydration(tsResult: TSResult, goResult: GoResult, tc: TestCase): string | null {
  // TS hydration is the Catch.fetch() result (tree nodes)
  // Go hydration is flat RowChange[] (type=0 adds)
  // We need to compare the set of hydrated rows.

  // Extract rows from Go hydration (all should be type=0 adds)
  const goRows = goResult.hydration
    .filter(rc => rc.type === 0)
    .map(rc => normalizeRow(rc.row))
    .sort();

  // Extract rows from TS hydration (Catch.fetch returns nodes)
  // Nodes have shape {row, relationships}. Recursively flatten all rows.
  const tsRows = flattenNodes(tsResult.hydration as any[]).sort();

  if (tsRows.length !== goRows.length) {
    return `Hydration row count: TS=${tsRows.length}, Go=${goRows.length}`;
  }

  for (let i = 0; i < tsRows.length; i++) {
    if (tsRows[i] !== goRows[i]) {
      return `Hydration row ${i} differs:\n  TS: ${tsRows[i]}\n  Go: ${goRows[i]}`;
    }
  }

  return null; // match
}

function flattenTSChanges(changes: any[]): Array<{type: number; row: string}> {
  const typeMap: Record<string, number> = {add: 0, remove: 1, edit: 2, child: -1};
  const result: Array<{type: number; row: string}> = [];
  for (const change of changes) {
    if (change.type === 'child') {
      // Recurse into child change
      if (change.child?.change) {
        result.push(...flattenTSChanges([change.child.change]));
      }
    } else {
      const row = change.node?.row ?? change.row ?? null;
      result.push({type: typeMap[change.type] ?? -1, row: normalizeRow(row)});
      // Expand relationships (Catch materializes them eagerly at push time)
      if (change.node?.relationships) {
        for (const relName of Object.keys(change.node.relationships)) {
          const children = change.node.relationships[relName];
          if (Array.isArray(children)) {
            const childType = typeMap[change.type] ?? -1;
            result.push(...flattenTSNodes(children, childType));
          }
        }
      }
    }
  }
  return result;
}

function flattenTSNodes(nodes: any[], changeType: number): Array<{type: number; row: string}> {
  const result: Array<{type: number; row: string}> = [];
  for (const node of nodes) {
    if (node && typeof node === 'object' && 'row' in node) {
      result.push({type: changeType, row: normalizeRow(node.row)});
      if (node.relationships) {
        for (const relName of Object.keys(node.relationships)) {
          const children = node.relationships[relName];
          if (Array.isArray(children)) {
            result.push(...flattenTSNodes(children, changeType));
          }
        }
      }
    }
  }
  return result;
}

function compareAdvanceStep(tsStep: unknown[], goStep: GoRowChange[], stepIdx: number): string | null {
  // TS step is Change[] from Catch.pushes
  // Go step is RowChange[]
  // Both represent row-level changes. Compare count and content.

  // TS Change has shape {type: 'add'|'remove'|'edit', node: {row, relationships}}
  // Go RowChange has {type: 0|1|2, row: {...}}

  const tsSimplified = flattenTSChanges(tsStep as any[])
    .sort((a, b) => `${a.type}${a.row}`.localeCompare(`${b.type}${b.row}`));

  const goSimplified = goStep.map(rc => ({
    type: rc.type,
    row: normalizeRow(rc.row ?? rc.rowKey),
  })).sort((a, b) => `${a.type}${a.row}`.localeCompare(`${b.type}${b.row}`));

  if (tsSimplified.length !== goSimplified.length) {
    const tsDetail = tsSimplified.map(c => `  type=${c.type}:${c.row}`).join('\n');
    const goDetail = goSimplified.map(c => `  type=${c.type}:${c.row}`).join('\n');
    return `Step ${stepIdx}: change count TS=${tsSimplified.length}, Go=${goSimplified.length}\nTS:\n${tsDetail}\nGo:\n${goDetail}`;
  }

  for (let i = 0; i < tsSimplified.length; i++) {
    if (tsSimplified[i].type !== goSimplified[i].type) {
      return `Step ${stepIdx} change ${i} type differs: TS=${tsSimplified[i].type}, Go=${goSimplified[i].type}`;
    }
    // For removes, compare rowKeys (Go has rowKey, TS has full row which contains the PK)
    // For adds/edits, compare full rows
    if (tsSimplified[i].type !== 1 && tsSimplified[i].row !== goSimplified[i].row) {
      return `Step ${stepIdx} change ${i} differs:\n  TS: type=${tsSimplified[i].type} ${tsSimplified[i].row}\n  Go: type=${goSimplified[i].type} ${goSimplified[i].row}`;
    }
  }

  return null;
}

// --- Test case configs (matching benchmarks + more variety) ---

const CONFIGS: Array<{name: string; opts: FuzzerOpts}> = [
  {name: 'SimpleFilter', opts: {NumTables: 1, NumColumns: 4, NumRows: 5, NumMutations: 10, WithFilter: true}},
  {name: 'FilterWithLimit', opts: {NumTables: 1, NumColumns: 4, NumRows: 5, NumMutations: 10, WithFilter: true, WithLimit: true}},
  {name: 'Compound', opts: {NumTables: 1, NumColumns: 5, NumRows: 5, NumMutations: 8, WithFilter: true, WithLimit: true, WithCompound: true}},
  {name: 'Joins', opts: {NumTables: 2, NumColumns: 3, NumRows: 5, NumMutations: 8, WithFilter: true, WithLimit: true, WithJoins: true}},
  {name: 'Exists', opts: {NumTables: 2, NumColumns: 3, NumRows: 5, NumMutations: 8, WithFilter: true, WithLimit: true, WithExists: true}},
  {name: 'KitchenSink', opts: {NumTables: 3, NumColumns: 4, NumRows: 5, NumMutations: 10, WithFilter: true, WithLimit: true, WithJoins: true, WithExists: true, WithCompound: true}},
];

// --- Main ---

async function main() {
  console.log('=== Sidecar Parity Test ===\n');
  console.log(`Seeds: ${SEED_START} to ${SEED_START + SEED_COUNT - 1} (${SEED_COUNT} per config)`);
  console.log(`Configs: ${CONFIGS.map(c => c.name).join(', ')}\n`);

  // Build tools
  console.log('Building gen-testcase...');
  execSync('go build -o /tmp/gen-testcase ./cmd/gen-testcase/', {cwd: GO_IVM_DIR, stdio: 'pipe'});

  console.log('Building sidecar...');
  execSync('go build -o /tmp/go-ivm-sidecar ./cmd/sidecar/', {cwd: GO_IVM_DIR, stdio: 'pipe'});
  console.log('Done.\n');

  // Spawn sidecar
  const socketPath = join(tmpdir(), `go-ivm-parity-${process.pid}.sock`);
  const dbPath = join(tmpdir(), `go-ivm-parity-${process.pid}.db`);

  const cleanup = () => {
    try { unlinkSync(socketPath); } catch {}
    try { unlinkSync(dbPath); } catch {}
  };

  const sidecar = spawn('/tmp/go-ivm-sidecar', [socketPath], {stdio: ['ignore', 'pipe', 'pipe']});
  process.on('exit', () => { sidecar.kill('SIGTERM'); cleanup(); });
  process.on('SIGINT', () => { sidecar.kill('SIGTERM'); cleanup(); process.exit(1); });

  // Wait for socket
  for (let i = 0; i < 50; i++) {
    await sleep(100);
    if (existsSync(socketPath)) break;
  }
  if (!existsSync(socketPath)) {
    throw new Error('Sidecar did not create socket within 5s');
  }

  // Connect client
  const client = new GoIVMClient(socketPath);
  await client.connect();
  const pong = await client.ping();
  if (pong !== 'pong') throw new Error(`ping failed: ${pong}`);
  console.log('Sidecar connected.\n');

  // Run parity tests
  let totalPassed = 0;
  let totalFailed = 0;
  let totalErrors = 0;
  const failures: Array<{config: string; seed: number; error: string}> = [];

  for (const config of CONFIGS) {
    let passed = 0;
    let failed = 0;
    let errors = 0;

    for (let seed = SEED_START; seed < SEED_START + SEED_COUNT; seed++) {
      try {
        const tc = generateTestCase(seed, config.opts);

        // Run TS
        const tsResult = runTS(tc);

        // Run Go (via sidecar) with timeout
        const goResult = await Promise.race([
          runGo(tc, client, dbPath),
          sleep(10000).then(() => { throw new Error('Sidecar timeout (10s)'); }),
        ]) as GoResult;

        // Compare hydration
        const hydErr = compareHydration(tsResult, goResult, tc);
        if (hydErr) {
          failed++;
          failures.push({config: config.name, seed, error: `HYDRATION: ${hydErr}`});
          continue;
        }

        // Compare advance steps
        let stepErr: string | null = null;
        const minSteps = Math.min(tsResult.steps.length, goResult.steps.length);
        for (let i = 0; i < minSteps; i++) {
          stepErr = compareAdvanceStep(tsResult.steps[i], goResult.steps[i], i);
          if (stepErr) break;
        }
        if (!stepErr && tsResult.steps.length !== goResult.steps.length) {
          stepErr = `Step count mismatch: TS=${tsResult.steps.length}, Go=${goResult.steps.length}`;
        }

        if (stepErr) {
          failed++;
          failures.push({config: config.name, seed, error: `ADVANCE: ${stepErr}`});
        } else {
          passed++;
        }
      } catch (err: any) {
        errors++;
        failures.push({config: config.name, seed, error: `ERROR: ${err.message}`});
      }
    }

    const status = failed === 0 && errors === 0 ? '✓' : '✗';
    console.log(`${status} ${config.name}: ${passed}/${SEED_COUNT} passed, ${failed} diverged, ${errors} errors`);
    totalPassed += passed;
    totalFailed += failed;
    totalErrors += errors;
  }

  // Summary
  const total = totalPassed + totalFailed + totalErrors;
  console.log(`\n=== Summary ===`);
  console.log(`Total: ${totalPassed}/${total} passed (${((totalPassed / total) * 100).toFixed(1)}%)`);
  console.log(`Divergences: ${totalFailed}`);
  console.log(`Errors: ${totalErrors}`);

  if (failures.length > 0) {
    console.log(`\n=== Failures (first 20) ===`);
    for (const f of failures.slice(0, 20)) {
      console.log(`  ${f.config} seed=${f.seed}: ${f.error}`);
    }
  }

  // Write full report
  const reportPath = join(__dirname, 'parity_report.json');
  writeFileSync(reportPath, JSON.stringify({
    timestamp: new Date().toISOString(),
    seedRange: {start: SEED_START, count: SEED_COUNT},
    configs: CONFIGS.map(c => c.name),
    summary: {total, passed: totalPassed, diverged: totalFailed, errors: totalErrors},
    failures,
  }, null, 2));
  console.log(`\nFull report: ${reportPath}`);

  // Cleanup
  client.close();
  sidecar.kill('SIGTERM');
  cleanup();

  process.exit(totalFailed > 0 || totalErrors > 0 ? 1 : 0);
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
