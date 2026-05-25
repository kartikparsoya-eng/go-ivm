/**
 * ts-harness.ts — Differential parity testing harness for TS IVM.
 *
 * Reads a JSON test case from stdin, runs it through the TS IVM pipeline
 * (MemorySource + buildPipeline + Catch), and outputs JSON results to stdout.
 *
 * Protocol:
 *   Input:  { schema, initialData, ast, mutations }
 *   Output: { hydration: CaughtNode[], steps: CaughtChange[][] }
 *
 * Run: tsx --tsconfig mono/packages/zql/tsconfig.json go-ivm/testharness/ts-harness.ts < test-case.json
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
import {Catch, expandNode} from '../../mono/packages/zql/src/ivm/catch.ts';
import type {AST} from '../../mono/packages/zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../mono/packages/zero-schema/src/table-schema.ts';

// --- Types for test case protocol ---

interface TestCase {
  schema: Record<
    string,
    {
      columns: Record<string, {type: string}>;
      primaryKey: string[];
    }
  >;
  initialData: Record<string, Record<string, unknown>[]>;
  ast: AST;
  mutations: Mutation[];
}

type Mutation =
  | {type: 'add'; table: string; row: Record<string, unknown>}
  | {type: 'remove'; table: string; row: Record<string, unknown>}
  | {
      type: 'edit';
      table: string;
      row: Record<string, unknown>;
      oldRow: Record<string, unknown>;
    };

// --- Main ---

async function main() {
  const input = await readStdin();
  const testCase: TestCase = JSON.parse(input);

  // Create sources
  const sources: Record<string, MemorySource> = {};
  for (const [tableName, tableSchema] of Object.entries(testCase.schema)) {
    const columns: Record<string, SchemaValue> = {};
    for (const [colName, colDef] of Object.entries(tableSchema.columns)) {
      columns[colName] = {type: colDef.type} as SchemaValue;
    }
    sources[tableName] = new MemorySource(
      tableName,
      columns,
      tableSchema.primaryKey,
    );
  }

  // Populate initial data
  for (const [tableName, rows] of Object.entries(testCase.initialData)) {
    const source = sources[tableName];
    for (const row of rows) {
      consume(source.push(makeSourceChangeAdd(row as Record<string, unknown>)));
    }
  }

  // Build pipeline
  const delegate = new TestBuilderDelegate(sources);
  const pipeline = buildPipeline(testCase.ast, delegate, 'test-query');

  // Attach Catch to collect output
  const sink = new Catch(pipeline);

  // Hydration: fetch initial state
  const hydration = sink.fetch();

  // Process mutations one at a time, collecting pushes after each
  const steps: unknown[][] = [];
  for (const mutation of testCase.mutations) {
    sink.reset();
    const source = sources[mutation.table];
    switch (mutation.type) {
      case 'add':
        consume(source.push(makeSourceChangeAdd(mutation.row)));
        break;
      case 'remove':
        consume(source.push(makeSourceChangeRemove(mutation.row)));
        break;
      case 'edit':
        consume(source.push(makeSourceChangeEdit(mutation.row, mutation.oldRow)));
        break;
    }
    steps.push([...sink.pushes]);
  }

  // Output
  const output = {hydration, steps};
  process.stdout.write(JSON.stringify(output, null, 2) + '\n');
}

function readStdin(): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf-8');
    process.stdin.on('data', chunk => (data += chunk));
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
