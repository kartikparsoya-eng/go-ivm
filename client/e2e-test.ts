/**
 * e2e-test.ts — End-to-end integration test for Go IVM sidecar.
 *
 * 1. Builds the sidecar binary
 * 2. Creates a test SQLite DB with sample data
 * 3. Spawns the sidecar process
 * 4. Connects via GoIVMClient
 * 5. Tests: init → addQuery (hydration) → advance (incremental changes)
 *
 * Run: npx tsx go-ivm/client/e2e-test.ts
 */

import {execSync, spawn, type ChildProcess} from 'child_process';
import {unlinkSync, existsSync} from 'fs';
import {tmpdir} from 'os';
import {join} from 'path';
import {GoIVMClient} from './go-ivm-client.js';

const SOCKET_PATH = join(tmpdir(), `go-ivm-test-${process.pid}.sock`);
const DB_PATH = join(tmpdir(), `go-ivm-test-${process.pid}.db`);
const GO_IVM_DIR = join(import.meta.dirname, '..');

let sidecar: ChildProcess | null = null;

function cleanup() {
  sidecar?.kill('SIGTERM');
  try { unlinkSync(SOCKET_PATH); } catch {}
  try { unlinkSync(DB_PATH); } catch {}
}

process.on('exit', cleanup);
process.on('SIGINT', () => { cleanup(); process.exit(1); });

async function sleep(ms: number) {
  return new Promise(r => setTimeout(r, ms));
}

async function main() {
  console.log('=== Go IVM Sidecar E2E Test ===\n');

  // 1. Build sidecar
  console.log('1. Building sidecar...');
  execSync('go build -o /tmp/go-ivm-sidecar ./cmd/sidecar/', {cwd: GO_IVM_DIR, stdio: 'pipe'});
  console.log('   OK\n');

  // 2. Create test database
  console.log('2. Creating test SQLite database...');
  const Database = (await import('better-sqlite3')).default;
  const db = new Database(DB_PATH);
  db.exec(`
    CREATE TABLE issues (
      id TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      priority INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'open'
    );
    INSERT INTO issues VALUES ('i1', 'Fix bug', 1, 'open');
    INSERT INTO issues VALUES ('i2', 'Add feature', 2, 'open');
    INSERT INTO issues VALUES ('i3', 'Write docs', 3, 'closed');
  `);
  db.close();
  console.log(`   Created ${DB_PATH} with 3 rows\n`);

  // 3. Spawn sidecar
  console.log('3. Spawning sidecar...');
  sidecar = spawn('/tmp/go-ivm-sidecar', [SOCKET_PATH], {stdio: ['ignore', 'pipe', 'pipe']});
  sidecar.stderr?.on('data', d => process.stderr.write(`   [sidecar] ${d}`));

  // Wait for socket to appear
  for (let i = 0; i < 50; i++) {
    await sleep(100);
    if (existsSync(SOCKET_PATH)) break;
  }
  if (!existsSync(SOCKET_PATH)) {
    throw new Error('Sidecar did not create socket');
  }
  console.log(`   Listening on ${SOCKET_PATH}\n`);

  // 4. Connect client
  console.log('4. Connecting client...');
  const client = new GoIVMClient(SOCKET_PATH);
  await client.connect();
  const pong = await client.ping();
  assert(pong === 'pong', `ping returned: ${pong}`);
  console.log('   Connected, ping OK\n');

  const CG = 'e2e-group';

  // 5. Init
  console.log('5. Initializing engine...');
  // Read rows from SQLite and send to Go (sidecar no longer opens SQLite directly).
  const Database2 = (await import('better-sqlite3')).default;
  const db2 = new Database2(DB_PATH);
  const issueRows = db2.prepare('SELECT * FROM issues').all() as Record<string, unknown>[];
  db2.close();

  await client.init(CG, {
    tables: {
      issues: {
        columns: {
          id: {type: 'string'},
          title: {type: 'string'},
          priority: {type: 'number'},
          status: {type: 'string'},
        },
        primaryKey: ['id'],
        rows: issueRows,
      },
    },
  });
  console.log('   Engine initialized\n');

  // 6. addQuery — hydration
  console.log('6. Adding query (SELECT * FROM issues WHERE status = "open" ORDER BY priority)...');
  const hydration = await client.addQuery(CG, 'q1', {
    table: 'issues',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'status'},
      op: '=',
      right: {type: 'literal', value: 'open'},
    },
    orderBy: [['priority', 'asc']],
  });
  console.log(`   Hydration: ${hydration.length} rows`);
  for (const rc of hydration) {
    console.log(`     ${rc.type === 0 ? 'ADD' : rc.type === 1 ? 'REM' : 'EDIT'} ${JSON.stringify(rc.row)}`);
  }
  assert(hydration.length === 2, `Expected 2 hydration rows, got ${hydration.length}`);
  console.log('   OK\n');

  // 7. advance — insert a new matching row
  console.log('7. Advancing (INSERT new open issue)...');
  const advResult1 = await client.advance(CG, [{
    table: 'issues',
    prevValues: [],
    nextValue: {id: 'i4', title: 'New task', priority: 0, status: 'open'},
  }]);
  console.log(`   Changes: ${advResult1.length}`);
  for (const rc of advResult1) {
    console.log(`     ${rc.type === 0 ? 'ADD' : rc.type === 1 ? 'REM' : 'EDIT'} ${JSON.stringify(rc.row)}`);
  }
  assert(advResult1.length >= 1, `Expected at least 1 change, got ${advResult1.length}`);
  assert(advResult1.some(c => c.type === 0), 'Expected an ADD change');
  console.log('   OK\n');

  // 8. advance — update row to no longer match filter
  console.log('8. Advancing (UPDATE i1 status → closed)...');
  const advResult2 = await client.advance(CG, [{
    table: 'issues',
    prevValues: [{id: 'i1', title: 'Fix bug', priority: 1, status: 'open'}],
    nextValue: {id: 'i1', title: 'Fix bug', priority: 1, status: 'closed'},
  }]);
  console.log(`   Changes: ${advResult2.length}`);
  for (const rc of advResult2) {
    console.log(`     ${rc.type === 0 ? 'ADD' : rc.type === 1 ? 'REM' : 'EDIT'} ${JSON.stringify(rc.row)}`);
  }
  assert(advResult2.length >= 1, `Expected at least 1 change, got ${advResult2.length}`);
  assert(advResult2.some(c => c.type === 1), 'Expected a REMOVE change');
  console.log('   OK\n');

  // 9. removeQuery
  console.log('9. Removing query...');
  await client.removeQuery(CG, 'q1');
  console.log('   OK\n');

  // Done
  client.close();
  console.log('=== ALL TESTS PASSED ===');
  cleanup();
  process.exit(0);
}

function assert(cond: boolean, msg: string) {
  if (!cond) {
    console.error(`ASSERTION FAILED: ${msg}`);
    cleanup();
    process.exit(1);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  cleanup();
  process.exit(1);
});
