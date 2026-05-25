/**
 * ts-benchmark.ts — Benchmark the TS IVM on the same workloads as Go benchmarks.
 *
 * Generates test cases using the Go gen-testcase tool, then runs them in-process
 * through the TS IVM (MemorySource + buildPipeline + Catch).
 *
 * Run: npx tsx --tsconfig mono/packages/zql/tsconfig.json go-ivm/testharness/ts-benchmark.ts
 */

import {MemorySource} from '../../mono/packages/zql/src/ivm/memory-source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeRemove,
  makeSourceChangeEdit,
} from '../../mono/packages/zql/src/ivm/source.ts';
import {consume} from '../../mono/packages/zql/src/ivm/stream.ts';
import {buildPipeline} from '../../mono/packages/zql/src/builder/builder.ts';
import {TestBuilderDelegate} from '../../mono/packages/zql/src/builder/test-builder-delegate.ts';
import {Catch} from '../../mono/packages/zql/src/ivm/catch.ts';
import type {AST} from '../../mono/packages/zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../mono/packages/zero-schema/src/table-schema.ts';
import {execSync} from 'child_process';
import {join, dirname} from 'path';
import {fileURLToPath} from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const GO_IVM_DIR = join(__dirname, '..');

interface TestCase {
  schema: Record<string, {columns: Record<string, {type: string}>; primaryKey: string[]}>;
  initialData: Record<string, Record<string, unknown>[]>;
  ast: AST;
  mutations: Array<{type: string; table: string; row: Record<string, unknown>; oldRow?: Record<string, unknown>}>;
}

function generateTestCase(seed: number, opts: Record<string, unknown>): TestCase {
  const optsJSON = JSON.stringify(opts);
  const result = execSync(
    `/tmp/gen-testcase ${seed} '${optsJSON}'`,
    {encoding: 'utf-8', maxBuffer: 10 * 1024 * 1024}
  );
  return JSON.parse(result);
}

function runTestCase(tc: TestCase): void {
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
  const input = buildPipeline(tc.ast as AST, delegate, 'bench');
  const output = new Catch(input);

  output.fetch();

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
  }
}

interface BenchConfig {
  name: string;
  seed: number;
  opts: Record<string, unknown>;
  iterations: number;
}

const benchmarks: BenchConfig[] = [
  {
    name: 'SimpleFilter',
    seed: 100,
    opts: {NumTables: 1, NumColumns: 4, NumRows: 10, NumMutations: 20, WithFilter: true},
    iterations: 1000,
  },
  {
    name: 'FilterWithLimit',
    seed: 101,
    opts: {NumTables: 1, NumColumns: 4, NumRows: 10, NumMutations: 20, WithFilter: true, WithLimit: true},
    iterations: 1000,
  },
  {
    name: 'Compound',
    seed: 105,
    opts: {NumTables: 1, NumColumns: 5, NumRows: 8, NumMutations: 15, WithFilter: true, WithLimit: true, WithCompound: true},
    iterations: 1000,
  },
  {
    name: 'Joins',
    seed: 200,
    opts: {NumTables: 2, NumColumns: 3, NumRows: 5, NumMutations: 10, WithFilter: true, WithLimit: true, WithJoins: true},
    iterations: 500,
  },
  {
    name: 'Exists',
    seed: 300,
    opts: {NumTables: 2, NumColumns: 3, NumRows: 5, NumMutations: 10, WithFilter: true, WithLimit: true, WithExists: true},
    iterations: 500,
  },
  {
    name: 'KitchenSink',
    seed: 1000,
    opts: {NumTables: 3, NumColumns: 4, NumRows: 5, NumMutations: 15, WithFilter: true, WithLimit: true, WithJoins: true, WithExists: true, WithCompound: true},
    iterations: 500,
  },
];

async function main() {
  console.log('=== TS IVM Benchmark ===\n');

  // Build gen-testcase
  console.log('Building gen-testcase tool...');
  execSync('go build -o /tmp/gen-testcase ./cmd/gen-testcase/', {cwd: GO_IVM_DIR, stdio: 'pipe'});
  console.log('Done.\n');

  const results: Array<{name: string; avgUs: number; iterations: number}> = [];

  for (const bench of benchmarks) {
    process.stdout.write(`${bench.name}: generating... `);
    const tc = generateTestCase(bench.seed, bench.opts);
    process.stdout.write(`running ${bench.iterations} iterations... `);

    // Warmup (5 iterations)
    for (let i = 0; i < 5; i++) {
      runTestCase(tc);
    }

    // Timed runs
    const start = performance.now();
    for (let i = 0; i < bench.iterations; i++) {
      runTestCase(tc);
    }
    const elapsed = performance.now() - start;
    const avgUs = (elapsed * 1000) / bench.iterations;

    console.log(`${avgUs.toFixed(0)} ns/op (${(elapsed / bench.iterations).toFixed(3)} ms/op)`);
    results.push({name: bench.name, avgUs, iterations: bench.iterations});
  }

  console.log('\n=== Results (compare with Go: go test -bench=. ./testharness/) ===');
  console.log('| Benchmark       |   TS ns/op |');
  console.log('|-----------------|------------|');
  for (const r of results) {
    console.log(`| ${r.name.padEnd(15)} | ${r.avgUs.toFixed(0).padStart(10)} |`);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
