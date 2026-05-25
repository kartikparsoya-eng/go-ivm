/**
 * sidecar-manager.ts — Manages the Go IVM sidecar process lifecycle.
 *
 * Responsibilities:
 * - Spawn the Go sidecar binary
 * - Health checks (ping with retry)
 * - Automatic restart on crash
 * - Graceful shutdown
 * - Connection pooling (one socket per ViewSyncer instance)
 *
 * Usage:
 *   const manager = new SidecarManager({ binaryPath: '/usr/local/bin/go-ivm-sidecar' });
 *   await manager.start();
 *   const client = manager.getClient(); // shared GoIVMClient instance
 *   // ... use client for init/addQuery/advance/destroy ...
 *   await manager.stop();
 */

import {spawn, type ChildProcess} from 'child_process';
import {existsSync, unlinkSync} from 'fs';
import {tmpdir} from 'os';
import {join} from 'path';
import {GoIVMClient} from './go-ivm-client.ts';

export type SidecarConfig = {
  /** Path to the compiled go-ivm-sidecar binary */
  binaryPath: string;
  /** Unix socket path (default: /tmp/go-ivm-<pid>.sock) */
  socketPath?: string;
  /** Max restart attempts before giving up (default: 3) */
  maxRestarts?: number;
  /** Delay between restart attempts in ms (default: 1000) */
  restartDelayMs?: number;
  /** Timeout for health check ping in ms (default: 5000) */
  healthCheckTimeoutMs?: number;
  /** Whether to log sidecar stdout/stderr (default: true) */
  verbose?: boolean;
};

export type SidecarStatus = 'stopped' | 'starting' | 'running' | 'restarting' | 'failed';

export class SidecarManager {
  readonly #config: Required<SidecarConfig>;
  #proc: ChildProcess | null = null;
  #client: GoIVMClient | null = null;
  #status: SidecarStatus = 'stopped';
  #restartCount = 0;
  #shutdownRequested = false;

  constructor(config: SidecarConfig) {
    this.#config = {
      binaryPath: config.binaryPath,
      socketPath: config.socketPath ?? join(tmpdir(), `go-ivm-${process.pid}.sock`),
      maxRestarts: config.maxRestarts ?? 3,
      restartDelayMs: config.restartDelayMs ?? 1000,
      healthCheckTimeoutMs: config.healthCheckTimeoutMs ?? 5000,
      verbose: config.verbose ?? true,
    };
  }

  get status(): SidecarStatus {
    return this.#status;
  }

  get socketPath(): string {
    return this.#config.socketPath;
  }

  /**
   * Start the sidecar process and wait until it's ready.
   * Throws if the binary doesn't exist or the process fails to start.
   */
  async start(): Promise<void> {
    if (this.#status === 'running') return;

    if (!existsSync(this.#config.binaryPath)) {
      throw new Error(
        `Go IVM sidecar binary not found at: ${this.#config.binaryPath}. ` +
          `Build it with: cd go-ivm && go build -o ${this.#config.binaryPath} ./cmd/sidecar/`,
      );
    }

    this.#shutdownRequested = false;
    this.#status = 'starting';
    await this.#spawn();
  }

  /**
   * Get the shared GoIVMClient instance (connected to the running sidecar).
   * Throws if the sidecar is not running.
   */
  getClient(): GoIVMClient {
    if (!this.#client || this.#status !== 'running') {
      throw new Error(`Sidecar is not running (status: ${this.#status})`);
    }
    return this.#client;
  }

  /**
   * Stop the sidecar process gracefully.
   */
  async stop(): Promise<void> {
    this.#shutdownRequested = true;
    this.#status = 'stopped';

    if (this.#client) {
      this.#client.close();
      this.#client = null;
    }

    if (this.#proc) {
      this.#proc.kill('SIGTERM');
      // Wait for exit (max 5s)
      await new Promise<void>(resolve => {
        const timeout = setTimeout(() => {
          this.#proc?.kill('SIGKILL');
          resolve();
        }, 5000);
        this.#proc?.on('exit', () => {
          clearTimeout(timeout);
          resolve();
        });
      });
      this.#proc = null;
    }

    // Clean up socket file
    try {
      unlinkSync(this.#config.socketPath);
    } catch {
      // ignore
    }
  }

  // --- Private ---

  async #spawn(): Promise<void> {
    // Clean up stale socket
    try {
      unlinkSync(this.#config.socketPath);
    } catch {
      // ignore
    }

    const proc = spawn(this.#config.binaryPath, [this.#config.socketPath], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    this.#proc = proc;

    if (this.#config.verbose) {
      proc.stdout?.on('data', (data: Buffer) => {
        process.stdout.write(`[go-ivm] ${data.toString()}`);
      });
      proc.stderr?.on('data', (data: Buffer) => {
        process.stderr.write(`[go-ivm:err] ${data.toString()}`);
      });
    }

    // Handle unexpected exit
    proc.on('exit', (code, signal) => {
      if (this.#shutdownRequested) return;

      console.error(
        `[go-ivm] Sidecar exited unexpectedly (code=${code}, signal=${signal})`,
      );

      if (this.#restartCount < this.#config.maxRestarts) {
        this.#restartCount++;
        this.#status = 'restarting';
        console.error(
          `[go-ivm] Restarting (attempt ${this.#restartCount}/${this.#config.maxRestarts})...`,
        );
        setTimeout(() => this.#spawn().catch(console.error), this.#config.restartDelayMs);
      } else {
        this.#status = 'failed';
        console.error(
          `[go-ivm] Max restarts exceeded. Sidecar is DOWN. Falling back to TS IVM.`,
        );
      }
    });

    // Wait for the socket to appear and become connectable
    await this.#waitForReady();

    // Connect the client
    this.#client = new GoIVMClient(this.#config.socketPath);
    await this.#client.connect();

    // Verify with ping
    const pong = await this.#client.ping();
    if (pong !== 'pong') {
      throw new Error(`Sidecar health check failed: expected 'pong', got '${pong}'`);
    }

    this.#status = 'running';
    this.#restartCount = 0; // Reset on successful start
  }

  async #waitForReady(): Promise<void> {
    const deadline = Date.now() + this.#config.healthCheckTimeoutMs;
    const pollMs = 50;

    while (Date.now() < deadline) {
      if (existsSync(this.#config.socketPath)) {
        return;
      }
      await new Promise(resolve => setTimeout(resolve, pollMs));
    }

    throw new Error(
      `Sidecar did not create socket at ${this.#config.socketPath} within ${this.#config.healthCheckTimeoutMs}ms`,
    );
  }
}
