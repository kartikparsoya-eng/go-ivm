# Phase 7 — SQLite `snapshot_open` for concurrent pinned reads

**Goal.** Eliminate the per-CG `*sql.Tx` mutex contention surfaced by the
Phase 6 profile, without sacrificing the snapshot consistency Phase 4
established as a hard requirement. Architecturally, this is the move
from "shared tx, serialized goroutines" to "multiple txs, shared
snapshot, independent goroutines."

## What the profile said

CPU profile during a 4-min 5-CG soak with the existing Phase 4–6 code:

- **CPU 0.2% busy** — sidecar wasn't doing work, it was waiting.
- **69 / 110 goroutines** parked in `database/sql.(*Tx).awaitDone`.
- **2.93 s / 3.05 s mutex-delay** in `tablesource.(*Source).fetchForConn`
  — almost the entire contention budget is in our `TxCache` mutex.
- User-facing symptom: `addQueriesStream` RPCs timing out at 30s / 120s.

The pinned read tx model from Phase 4 means every concurrent goroutine
trying to Fetch the same Source serializes through one `*sql.Tx`. Under
`addQueriesStream` (which fans queries out across goroutines within a
single CG) the serialization is catastrophic. The hot loop isn't slow
per-row — it's a queue of goroutines waiting their turn.

## Why TS doesn't have this problem

TS's view-syncer uses `@rocicorp/zero-sqlite3` (better-sqlite3 fork).
better-sqlite3 is **single-threaded per Database instance**. There's
exactly one connection per CG and the IVM operators call into it
synchronously. No goroutines, no parallelism, no need for snapshot
pinning beyond "the next call sees the same state as the previous one"
— guaranteed for free by single-threaded execution.

We made a different bet: parallelize within a CG (per-query goroutines
during hydrate, parallel-push fanout above `GO_IVM_PARALLEL_THRESHOLD`).
That bet is the whole reason the Go sidecar exists. To keep it, we need
per-goroutine independent reads that still see a consistent snapshot.

## SQLite's `snapshot_open` API

Since SQLite 3.10, three C functions let one snapshot be read by many
connections:

```c
int sqlite3_snapshot_get(sqlite3 *db, const char *zSchema, sqlite3_snapshot **ppSnapshot);
int sqlite3_snapshot_open(sqlite3 *db, const char *zSchema, sqlite3_snapshot *pSnapshot);
void sqlite3_snapshot_free(sqlite3_snapshot *pSnapshot);
```

Workflow:

1. On one connection in a `BEGIN`-ed read tx: `snapshot_get` returns a
   handle that names the current WAL frame.
2. On any other connection (also in a `BEGIN`-ed read tx): `snapshot_open`
   pins that tx to the named frame. Both connections now see identical
   data.
3. When the snapshot is no longer needed, `snapshot_free` releases the
   handle. Without this, SQLite can't checkpoint past the named frame —
   WAL grows.

Each connection's `*sql.Tx` is independent → goroutine-safe to use
from different goroutines. Same snapshot → consistent. That's the
combination Go's pure database/sql can't give us; the C API does.

## Implementation

### Package shape

```
internal/tablesource/
  snapshot.go        // CGO wrapper around the 3 sqlite3_snapshot_* calls
  snapshot_test.go   // verifies multi-conn pinning
  source.go          // drop TxCache, hold a current *Snapshot, fan reads
  ...
```

### New types

```go
// Snapshot wraps a *C.sqlite3_snapshot pointer + the conn that begat
// it. The conn must stay alive while any other conn is pinned to this
// snapshot — closing it would release the WAL frame.
type Snapshot struct {
    handle  *C.sqlite3_snapshot
    ownConn *sql.Conn           // the originating conn, held until Free
    epoch   uint64              // for diagnostics + drift-audit correlation
}

func (s *Snapshot) Free()        // sqlite3_snapshot_free + close ownConn
```

### Source changes

```go
type Source struct {
    db         *sql.DB
    tableName  string
    // ... unchanged ...

    snapMu   sync.RWMutex
    current  *Snapshot   // shared by all read goroutines
    overlay  *ivm.Overlay
}
```

Fetch flow:

```go
func (s *Source) fetchForConn(...) []ivm.Node {
    s.snapMu.RLock()
    snap := s.current
    s.snapMu.RUnlock()

    if snap == nil {
        // First Fetch ever, or post-RefreshSnapshot. Create one.
        snap = s.acquireFreshSnapshot()
    }

    conn, _ := s.db.Conn(ctx)
    defer conn.Close()

    tx, _ := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
    if err := openOnSnapshot(conn, snap.handle); err != nil { ... }
    defer tx.Commit() // closes tx, releases conn back to pool

    rows, _ := tx.Query(...)
    // ... scan + overlay logic as today ...
}
```

The R-lock around `s.current` is read-mostly; only Push and
RefreshSnapshot acquire the W-lock. The actual reads run under
RLock-held + their own conn — no mutual exclusion between goroutines.

### Push changes

Push's deferred cleanup replaces `snapshotEpoch++` with:

```go
defer func() {
    s.mu.Lock()
    s.overlay = nil
    s.mu.Unlock()

    s.snapMu.Lock()
    old := s.current
    s.current = nil  // next Fetch will re-acquire from the live state
    s.snapMu.Unlock()
    if old != nil {
        old.Free()
    }
}()
```

After Push, the cached snapshot is gone; the next Fetch creates a fresh
one (pinning to the now-post-replicator-commit frame).

### RefreshSnapshot changes

```go
func (s *Source) RefreshSnapshot() {
    s.snapMu.Lock()
    if s.overlay != nil { s.snapMu.Unlock(); return }
    old := s.current
    s.current = nil
    s.snapMu.Unlock()
    if old != nil { old.Free() }
}
```

Same idea — invalidate the cache, let next Fetch re-acquire.

### CGO wrapper

```go
// snapshot.go
package tablesource

/*
#include "../../c/sqlite3/sqlite3.h"
*/
import "C"
import (
    "database/sql"
    "errors"
    "unsafe"

    sqlite3 "github.com/mattn/go-sqlite3"
)

// snapshotGet runs sqlite3_snapshot_get on the conn's raw *C.sqlite3
// handle. mattn lets us reach it via Raw().
func snapshotGet(ctx context.Context, conn *sql.Conn) (*C.sqlite3_snapshot, error) {
    var handle *C.sqlite3_snapshot
    err := conn.Raw(func(driverConn any) error {
        c, ok := driverConn.(*sqlite3.SQLiteConn)
        if !ok { return errors.New("not a sqlite3 conn") }
        rc := C.sqlite3_snapshot_get(
            (*C.sqlite3)(unsafe.Pointer(c)),    // approximate; see note
            cStr("main"),
            &handle,
        )
        if rc != C.SQLITE_OK { return errors.New("sqlite3_snapshot_get failed") }
        return nil
    })
    return handle, err
}
```

(The exact `unsafe.Pointer` cast depends on how mattn lays out
`SQLiteConn`. Worst case we reach the C handle by reflection of an
unexported field; mattn's tests and its own snapshot-API users have
precedent for this pattern.)

### Pool sizing

Today's default `MaxOpenConns=64` was sized for "occasional concurrent
reads." With this change, EVERY concurrent Fetch consumes one conn for
its lifetime. A CG with 14 concurrent queries on the same Source uses
14 conns. Multi-CG workloads multiply.

New default: `MaxOpenConns = 256`, `MaxIdleConns = 32`. Bench-tunable.
SQLite WAL readers don't block writes, so the only cost is FD/socket
usage — fine at this scale.

## Risks

- **WAL growth from leaked snapshots.** Every `snapshot_get` MUST pair
  with `snapshot_free` or SQLite refuses to checkpoint. Lifecycle bugs
  here cause disk usage to climb until OOM/disk-full. Mitigation:
  Snapshot has a finalizer as belt-and-suspenders; Push and
  RefreshSnapshot are the only owners; explicit Free in both paths.
- **mattn's internal struct layout.** We're reaching into mattn for the
  raw `*C.sqlite3` pointer. If mattn bumps a major and renames fields,
  build breaks loudly (not silently — type assertion fails). Pinned
  go.mod version is the fix.
- **`snapshot_open` requires a fresh `BEGIN`.** Each Fetch's tx must be
  newly begun on a fresh conn; can't reuse an already-running tx. The
  pool gives us a fresh conn per Fetch anyway, so this is naturally
  handled.
- **Tests.** Need a fixture that opens 16 goroutines concurrently and
  verifies (a) no mutex contention via timing, (b) consistent snapshot
  semantics across an external writer commit.

## Phasing within Phase 7

7a — CGO wrapper (`snapshot.go`) + unit tests verifying basic
     get/open/free against an in-memory file.
7b — Source refactor: drop TxCache, hold `*Snapshot`, refactor
     fetchForConn for per-call conn acquisition.
7c — Push + RefreshSnapshot wire the snapshot rotation.
7d — Bench under the same workload that triggered the timeouts;
     verify mutex profile shows no `fetchForConn` contention.

Each phase ends green tests + at least the existing 30+ tablesource
tests still passing.

## What stays

Everything from Phases 0–6 except the per-Source shared `*sql.Tx` /
`TxCache`. The overlay model, the engine wiring, the snapshot-aware
drift audit on the TS side, the FIFO bypass — all still relevant and
unchanged.
