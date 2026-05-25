/**
 * go-ivm-client.ts — TypeScript client for the Go IVM sidecar.
 *
 * Wire format: MessagePack over Unix domain socket with 4-byte big-endian
 * length-prefix framing. Each frame is one RPC message (request or response).
 *
 * Drop-in replacement for the hot path in PipelineDriver:
 *   - init(clientGroupID, dbPath, tables) → registers schema + loads data
 *   - addQuery(clientGroupID, queryID, ast) → hydration RowChanges
 *   - removeQuery(clientGroupID, queryID)
 *   - advance(clientGroupID, changes) → incremental RowChanges
 *   - destroy(clientGroupID) → tear down engine for a group
 *
 * Multi-engine architecture: each clientGroupID maps to its own Engine
 * in the Go sidecar. Different groups run in parallel on multiple cores.
 *
 * Usage:
 *   const client = new GoIVMClient('/tmp/go-ivm.sock');
 *   await client.connect();
 *   await client.init('group-1', { dbPath: '/path/to/db', tables: {...} });
 *   const hydration = await client.addQuery('group-1', 'q1', ast);
 *   const changes = await client.advance('group-1', snapshotChanges);
 *   await client.destroy('group-1');
 *   client.close();
 */

import {createConnection, type Socket} from 'net';
import {Packr, Unpackr} from 'msgpackr';

// msgpackr emits several JS-specific extensions by default that Go's
// vmihailenco/msgpack decoder cannot understand. We opt out of all of them:
//
//   useRecords: false        — disables the structure-sharing ext type 0xd4
//                              (would otherwise reference repeated object shapes)
//   encodeUndefinedAsNil:true— encodes JS `undefined` as msgpack nil (0xc0)
//                              instead of fixext1 type=0 (0xd4 0x00 0x00).
//                              Triggered by ASTs with `related: undefined` etc.
//   useBigIntExtension:false — encodes BigInt as msgpack int instead of an ext
//   mapsAsObjects:true       — decodes msgpack maps as plain objects (not Map)
//
// The result is plain msgpack on the wire, which vmihailenco speaks natively.
const codec = {
  packr: new Packr({
    useRecords: false,
    encodeUndefinedAsNil: true,
    mapsAsObjects: true,
    useBigIntExtension: false,
  }),
  unpackr: new Unpackr({
    useRecords: false,
    mapsAsObjects: true,
    useBigIntExtension: false,
  }),
};
const pack = (v: unknown) => codec.packr.pack(v);
const unpack = (buf: Buffer | Uint8Array) => codec.unpackr.unpack(buf as Buffer);

// --- Types ---

export type ColumnSchema = {
  type: 'boolean' | 'number' | 'string' | 'null' | 'json';
  optional?: boolean;
};

export type TableSchema = {
  columns: Record<string, ColumnSchema>;
  primaryKey: string[];
};

export type TableData = {
  columns: Record<string, ColumnSchema>;
  primaryKey: string[];
  rows: Record<string, unknown>[];
};

export type InitParams = {
  dbPath?: string;
  storagePath?: string;
  tables: Record<string, TableData>;
};

export type SnapshotChange = {
  table: string;
  prevValues: Record<string, unknown>[];
  nextValue: Record<string, unknown> | null;
};

export type RowChange = {
  type: 0 | 1 | 2; // add, remove, edit
  queryID: string;
  table: string;
  rowKey: Record<string, unknown>;
  row: Record<string, unknown> | null;
};

// --- RPC (msgpack) ---

type RPCRequest = {
  jsonrpc: '2.0';
  method: string;
  params?: unknown;
  id: number;
};

type RPCResponse = {
  jsonrpc: '2.0';
  result?: unknown;
  error?: {code: number; message: string};
  id: number;
};

const MAX_FRAME_SIZE = 64 * 1024 * 1024; // 64MB — must match Go side

// --- Client ---

export class GoIVMClient {
  readonly #socketPath: string;
  #socket: Socket | null = null;
  #nextID = 1;
  #pending = new Map<number, {resolve: (v: unknown) => void; reject: (e: Error) => void}>();

  // Incoming-byte accumulator for length-prefix framing.
  #recvBuf: Buffer = Buffer.alloc(0);

  constructor(socketPath = '/tmp/go-ivm.sock') {
    this.#socketPath = socketPath;
  }

  /** Connect to the Go sidecar. Resolves when connected. */
  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = createConnection(this.#socketPath, () => {
        this.#socket = socket;
        socket.on('data', chunk => this.#onData(chunk));
        resolve();
      });
      socket.on('error', err => {
        if (!this.#socket) {
          reject(err);
        }
      });
      socket.on('close', () => {
        // Reject all pending requests
        for (const [, p] of this.#pending) {
          p.reject(new Error('Connection closed'));
        }
        this.#pending.clear();
      });
    });
  }

  /** Close the connection. */
  close(): void {
    this.#socket?.destroy();
    this.#socket = null;
    this.#recvBuf = Buffer.alloc(0);
  }

  /**
   * Initialize an engine for a client group.
   * Creates MemorySources from the SQLite DB and registers them.
   */
  async init(clientGroupID: string, params: InitParams): Promise<void> {
    await this.#call('init', {clientGroupID, ...params});
  }

  /**
   * Add a query pipeline to a client group's engine.
   * Returns initial hydration RowChanges (current state as ADDs).
   */
  async addQuery(clientGroupID: string, queryID: string, ast: unknown): Promise<RowChange[]> {
    const result = (await this.#call('addQuery', {clientGroupID, queryID, ast})) as {
      changes: RowChange[];
    };
    return result.changes ?? [];
  }

  /** Remove a query pipeline from a client group's engine. */
  async removeQuery(clientGroupID: string, queryID: string): Promise<void> {
    await this.#call('removeQuery', {clientGroupID, queryID});
  }

  /**
   * Advance: push snapshot changes through a client group's engine.
   * Returns incremental RowChanges (deltas for all affected pipelines).
   */
  async advance(clientGroupID: string, changes: SnapshotChange[]): Promise<RowChange[]> {
    const result = (await this.#call('advance', {clientGroupID, changes})) as {
      changes: RowChange[];
    };
    return result.changes ?? [];
  }

  /**
   * Destroy a client group's engine.
   * Call when the client disconnects to free memory.
   */
  async destroy(clientGroupID: string): Promise<void> {
    await this.#call('destroy', {clientGroupID});
  }

  /** Ping the sidecar. */
  async ping(): Promise<string> {
    return (await this.#call('ping', undefined)) as string;
  }

  // --- Private ---

  #call(method: string, params: unknown): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const socket = this.#socket;
      if (!socket) {
        reject(new Error('Not connected'));
        return;
      }

      const id = this.#nextID++;
      const req: RPCRequest = {jsonrpc: '2.0', method, params, id};
      this.#pending.set(id, {resolve, reject});

      const payload = pack(req);
      const frame = Buffer.allocUnsafe(4 + payload.length);
      frame.writeUInt32BE(payload.length, 0);
      payload.copy(frame, 4);
      socket.write(frame);
    });
  }

  #onData(chunk: Buffer): void {
    // Append new bytes to the accumulator.
    this.#recvBuf = this.#recvBuf.length === 0 ? chunk : Buffer.concat([this.#recvBuf, chunk]);

    // Drain as many complete frames as we can.
    while (this.#recvBuf.length >= 4) {
      const len = this.#recvBuf.readUInt32BE(0);
      if (len > MAX_FRAME_SIZE) {
        // Protocol error — frame too large. Reject all pending and close.
        const err = new Error(`Frame too large: ${len}`);
        for (const [, p] of this.#pending) p.reject(err);
        this.#pending.clear();
        this.close();
        return;
      }
      if (this.#recvBuf.length < 4 + len) {
        // Wait for more bytes.
        return;
      }

      const payload = this.#recvBuf.subarray(4, 4 + len);
      this.#recvBuf = this.#recvBuf.subarray(4 + len);

      let resp: RPCResponse;
      try {
        resp = unpack(payload) as RPCResponse;
      } catch (e) {
        // Bad frame — skip it.
        continue;
      }

      // Coerce id to Number: msgpackr decodes Go's uint64 (9-byte non-compact
      // encoding) as BigInt, which won't match the Number key stored in
      // #pending when the request was sent.
      const respId = typeof resp.id === 'bigint' ? Number(resp.id) : resp.id;
      const pending = this.#pending.get(respId);
      if (!pending) continue;
      this.#pending.delete(respId);

      if (resp.error) {
        pending.reject(new Error(`RPC error ${resp.error.code}: ${resp.error.message}`));
      } else {
        pending.resolve(resp.result);
      }
    }
  }
}
