package engine

import (
	"fmt"
	"os"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// QueryFailureLoggingOperator is a transparent ivm.Output decorator that
// catches panics from Push, logs them with query context (queryID, queryHash,
// transformationHash, queryName), and re-panics. Mirrors TS's
// QueryFailureLoggingOperator (pipeline-driver.ts:1068-1124), which wraps the
// pipeline's output to log per-query errors before they propagate to the
// view-syncer's teardown machinery.
//
// Expected control-flow panics are filtered and re-panicked WITHOUT logging:
//
//   - *ScalarResetError: the Go twin of TS's ResetPipelinesSignal
//     ('scalar-subquery'). A scalar-subquery value change is a planned
//     reset + re-hydrate, not a failure — logging it would drown the signal
//     in noise (the same way TS's logQueryFailure returns early on
//     ResetPipelinesSignal at pipeline-driver.ts:1132-1134).
//
// All other panics (source drift, nil-PK, join mis-construction, etc.) are
// logged at ERROR level with the query's identifying context and then
// re-panicked, preserving the engine's fail-fast contract. The re-panic
// ensures the sidecar's RPC-level recover still maps the panic to its
// appropriate RPC code — the decorator is observability-only, never a
// swallow.
type QueryFailureLoggingOperator struct {
	output ivm.Output

	// Query context for failure attribution. queryID is the user-facing
	// identifier; queryHash and transformationHash are the TS-side digest
	// pair (pipeline-driver.ts:1079-1081); queryName is the optional
	// human-readable label.
	queryID            string
	queryHash          string
	transformationHash string
	queryName          string
}

// NewQueryFailureLoggingOperator wraps output with panic-logging
// instrumentation. All four query-context fields are included in every
// failure log line; queryName may be empty when the query has no
// user-assigned name (TS passes undefined, rendered as an absent
// queryName context key — pipeline-driver.ts:1140-1142).
func NewQueryFailureLoggingOperator(
	output ivm.Output,
	queryID, queryHash, transformationHash, queryName string,
) *QueryFailureLoggingOperator {
	return &QueryFailureLoggingOperator{
		output:             output,
		queryID:            queryID,
		queryHash:          queryHash,
		transformationHash: transformationHash,
		queryName:          queryName,
	}
}

// Push forwards the change to the wrapped output. If the downstream push
// panics, the panic is recovered, classified, and either silently re-panicked
// (ScalarResetError — expected control flow) or logged and re-panicked
// (everything else — genuine failure). The re-panic preserves the original
// panic value and stack so the sidecar's outer recover sees the same type
// and message it would without the decorator.
func (q *QueryFailureLoggingOperator) Push(change ivm.Change, pusher ivm.InputBase) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}

		// ScalarResetError is the Go twin of TS's ResetPipelinesSignal —
		// a planned reset + re-hydrate, not a failure. Re-panic without
		// logging, matching TS's logQueryFailure early-return on
		// `error instanceof ResetPipelinesSignal` (pipeline-driver.ts:1132).
		if _, ok := r.(*ScalarResetError); ok {
			panic(r)
		}

		// Genuine failure: log with query context before re-panicking so
		// the operator can attribute the crash to a specific query even
		// when the sidecar's outer recover flattens the stack.
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][QUERY-FAILURE] query pipeline failed: "+
				"queryID=%s queryHash=%s transformationHash=%s queryName=%s error=%v\n",
			q.queryID, q.queryHash, q.transformationHash, q.queryName, r)
		panic(r)
	}()
	q.output.Push(change, pusher)
}
