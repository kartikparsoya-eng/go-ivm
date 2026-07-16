# Lean Models

This directory contains small Lean models for the Go IVM work.

## Fanout Serializability

`FanoutSerializability.lean` models the Go IVM parallel fanout claim:

```txt
Under contention, parallel fanout may serialize or interleave at lock/output
boundaries, but every completed execution must preserve each query group's
local order and is observationally equivalent to serial execution for
query-local consumers.
```

It deliberately does not prove global wire-order equality with TS. The Go
implementation permits cross-query interleaving; the correctness surface is
per-query ordering after demultiplexing by `queryID`.

Run:

```sh
cd go-ivm/specs/lean
lake build
```

Main theorem:

```lean
query_local_consumer_same
```

Meaning: for any consumer that processes one query group's projected trace, a
completed parallel trace and a completed serial trace from the same program
produce identical consumer input.

## Worker Sizing

`WorkerSizing.lean` models the sync-worker sizing argument:

```txt
workers are safe when both resident CG demand and active CG drain demand fit
inside the measured per-worker comfort envelope.
```

Lean proves the inequalities and monotonicity. It does not derive constants
such as "100 resident CGs per worker" or "4 active CGs per worker"; those come
from production metrics and ladder experiments.

Main theorems:

```lean
comfortable_mono_workers
not_comfortable_if_active_over
all_workers_safe_of_each_bound
```

Meaning: adding workers preserves an already comfortable envelope; if active
CGs exceed `workers * activePerWorker`, the configuration is outside the
comfort envelope; and if routing keeps every worker under the local resident
and active bounds, every worker is safe.
