# Go-IVM — Build, Test, and Deploy Guide

## Repos

| Repo | Path | Branch |
|------|------|--------|
| go-ivm | `/Users/kartik.parsoya/Documents/Go-RS/go-ivm` | `feat/napi-transport` |
| mono | `/Users/kartik.parsoya/Documents/Go-RS/mono` | `feat/napi-transport` |

## Architecture

The Go IVM engine runs **in-process** via a NAPI c-shared library (`libgoivm.so`).
Each zero-cache syncer worker dlopens the library and calls into it through
the ABI surface (`cmd/sidecar/napi_lib.go`). There is no socket transport and
no separate sidecar process.

- **Transport**: NAPI in-process (`ZERO_GO_SIDECAR_NAPI_LIB_PATH=/opt/go-ivm/libgoivm.so`)
- **Mode**: Go-primary (Go derives its own diff via `advanceToHeadStream`)
- **SQLite**: rocicorp wal2 fork (3.51.0) with `SQLITE_ENABLE_WAL2_COREAD`,
  vendored in `c/sqlite3/`, compiled as a static library and linked via
  mattn/go-sqlite3 with `-tags libsqlite3`
- **Protocol revision**: 12
- **ABI version**: 5

## Build

### Local native (macOS arm64)

```bash
# Build the c-shared library for local testing
cd /Users/kartik.parsoya/Documents/Go-RS/go-ivm
CGO_ENABLED=1 go build -tags "libsqlite3 sqlite_omit_load_extension osusergo netgo napilib" \
  -ldflags="-s -w" -trimpath -buildmode=c-shared -o libgoivm.so ./cmd/sidecar
```

### Docker images

```bash
# go-ivm artifact carrier (libgoivm.so only)
cd /Users/kartik.parsoya/Documents/Go-RS/go-ivm
docker build --platform linux/arm64 -t go-ivm:local-arm64 \
  --build-arg GO_IVM_BUILD_SHA=$(git rev-parse --short HEAD) \
  --build-arg GO_IVM_BUILD_REF=$(git rev-parse --abbrev-ref HEAD) .

# zero-cache + go (pulls go-ivm image, builds addon, installs deps)
cd /Users/kartik.parsoya/Documents/Go-RS/mono
docker build --platform linux/arm64 -f packages/zero-cache/Dockerfile.go-ivm \
  --build-arg GO_IVM_IMAGE=go-ivm:local-arm64 \
  -t zero-cache-go:local-arm64-napi .
```

### CI

Both repos have GitHub Actions on `feat/napi-transport`:
- **go-ivm**: `.github/workflows/build-image.yml` — builds `libgoivm.so`, publishes to `ghcr.io/kartikparsoya-eng/go-ivm`
- **mono**: `.github/workflows/build-zero-cache-go.yml` — pulls latest go-ivm image, builds zero-cache+go image, runs no-pg tests

```bash
gh run list --branch feat/napi-transport --limit 5
```

## Testing

```bash
# Go race tests (key packages)
cd /Users/kartik.parsoya/Documents/Go-RS/go-ivm
go test -race -count=1 ./internal/tablesource/...
go test -race -count=1 ./cmd/sidecar/...
go test -race -count=1 ./engine/...
go test -race -count=1 ./internal/snapshotter/...

# Go vet + gofmt
go vet ./cmd/sidecar/... ./engine/... ./internal/...
gofmt -l cmd/sidecar/ engine/ internal/

# Mono no-pg tests (1750+ tests)
cd /Users/kartik.parsoya/Documents/Go-RS/mono
pnpm --filter zero-cache run test -- --no-pg
pnpm --filter zero-cache run check-types
```

## Key Environment Variables

### Go engine (set in Dockerfile.go-ivm, overridable at deploy)

| Variable | Default | Purpose |
|----------|---------|---------|
| `GO_IVM_HYDRATE_PARALLELISM` | 4 | Hydrate lane count (readers = 2× lanes) |
| `GO_IVM_PARALLEL_ADVANCE` | false | Advance-fanout master switch — SERIAL by default (code `== "true"`; `Dockerfile.go-ivm` bakes `false`). Advance push is TS-faithful serial in prod; the parallel `fanOut` path is shadow-only. |
| `GO_IVM_ADVANCE_PARALLELISM` | 1 | Advance fanout worker count. NO-OP while `GO_IVM_PARALLEL_ADVANCE=false` (the master switch gates it). Code default 1. |
| `GO_IVM_MAX_OPEN_CONNS` | 1024 | Per-worker SQLite connection pool ceiling |
| `GO_IVM_MAX_IDLE_CONNS` | 1024 | Idle connection cap (self-clamps to MAX_OPEN) |
| `GO_IVM_CONN_CACHE_KB` | 1024 | Per-conn SQLite page cache (C-side malloc) |
| `GO_IVM_GOMEMLIMIT_PERCENT` | 40 | Soft Go heap ceiling as % of container memory |
| `GO_IVM_ADVANCE_BUDGET_MS` | 60000 | Wall-clock advance backstop (CPU abort is separate) |
| `GO_IVM_DELIVER_TIMEOUT_SEC` | 55 | Row-plane park deadline (M5 fix: was 150s) |
| `GO_IVM_WEDGE_WATCHDOG_SEC` | 90 | CG handler wedge detection threshold |
| `GO_IVM_CHUNK_SOFT_BYTES` | 8388608 | Soft byte budget per streamed partial frame |
| `GO_IVM_REAPER_IDLE_SEC` | 900 (15 min) | Idle CG reaping timeout |
| `GO_IVM_REAPER_INTERVAL_SEC` | 300 (5 min) | Reaper scan interval |

### TS side (set in zero-config.ts / Dockerfile.go-ivm)

| Variable | Default | Purpose |
|----------|---------|---------|
| `ZERO_GO_SIDECAR_ENABLED` | true | Enable Go IVM in-process engine |
| `ZERO_GO_SIDECAR_NAPI_LIB_PATH` | /opt/go-ivm/libgoivm.so | Path to c-shared library |
| `ZERO_NUM_SYNC_WORKERS` | 8 | V8 worker count (each hosts a Go engine) |
| `ZERO_CURSOR_PAGE_SIZE` | 100 | View-syncer poke cadence |

### Sandbox override (docker-compose.override.yml)

The local sandbox uses reduced settings for 125 CGs / 4 workers / 8GB:
- `GO_IVM_MAX_OPEN_CONNS=128` (32 per worker)
- `GO_IVM_HYDRATE_PARALLELISM=2` (4 readers)
- `GO_IVM_ADVANCE_PARALLELISM=4` (worker count only — inert unless `GO_IVM_PARALLEL_ADVANCE=true`; advance is serial by default)
- `GO_IVM_GOMEMLIMIT_PERCENT=75`

## Local Sandbox

```bash
cd /Users/kartik.parsoya/Documents/xy-repo/xyne-spaces/.sandboxes/rust-test
docker compose up -d zero-cache
docker logs -f xyne-sandbox-rust-test-zero-cache
```

### Oracle suite

```bash
cd /Users/kartik.parsoya/Documents/xyne-art
./run-art-local.sh --oracle --negative --mutation-matrix \
  --connections 125 --duration 600 --clean
```

Gates: G1-G3 (connectivity/errors/protocol), G8 (diff oracle vs TS mirror),
G11 (negative suite), G13 (log health), G15 (mutation matrix).

## Git Workflow

- Pushes require `--no-verify` (GitGuardian pre-commit hooks)
- Branch: `feat/napi-transport` for both repos

```bash
git push --no-verify origin feat/napi-transport
```

## Current State (as of 2026-07-11)

- Go-primary mode active via NAPI in-process transport
- Advance push SERIAL (`GO_IVM_PARALLEL_ADVANCE=false`, TS-faithful); parallel fanout is shadow-only
- Advance leaf fetch is lazy (`fetchDuringPushStream`, `iter.Seq` — TS `statement.iterate()` semantics, unconditional)
- Tiered reset circuit breaker (view-syncer.ts): economic-class resets
  (advancement-timeout) never trip the breaker; deterministic, transient,
  and lawful classes have per-class thresholds
- suppressAbort escalation: after 3 consecutive abort-doubling cycles,
  the next advance sends `suppressAbort=true` for an un-abortable catch-up
- Oracle suite: G8/G11/G15 PASS (correctness), G13 WATCH (log health,
  down from FAIL — 5 resets/0.3min vs prior 692/37.6min)
- Known M6/M7 divergences from TS are intentional bug fixes (see
  DRIFT-LEDGER.md)
