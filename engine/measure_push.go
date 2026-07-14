package engine

import (
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// MeasurePushOperator is a transparent ivm.Output decorator that times each
// Push call and reports the elapsed duration via a callback. Mirrors TS's
// MeasurePushOperator (measure-push-operator.ts), which wraps pipeline push
// operations and reports per-push latency to a MetricsDelegate via
// performance.now() deltas.
//
// In the TS pipeline, the operator wraps the source input and sits between the
// source and the downstream pipeline: the source pushes to MeasurePushOperator,
// which forwards to its wrapped output and records the wall time of the entire
// downstream push. In Go, it wraps any ivm.Output — the caller inserts it at
// the desired point in the Output chain and the operator remains invisible to
// the pipeline (same ivm.Output interface, same call semantics).
//
// onPush is invoked AFTER the wrapped Push returns, so the timing includes the
// full downstream propagation (join flattening, streamer accumulation, chunk
// flushing). A nil onPush makes the operator a zero-cost pass-through — useful
// for feature-flag gating without changing call sites.
type MeasurePushOperator struct {
	output  ivm.Output
	queryID string
	onPush  func(queryID string, d time.Duration)
}

// NewMeasurePushOperator wraps output with push-timing instrumentation. onPush
// receives the queryID and the wall-clock duration of each Push; pass nil to
// disable recording (the operator forwards pushes with no overhead beyond the
// function call).
func NewMeasurePushOperator(output ivm.Output, queryID string, onPush func(string, time.Duration)) *MeasurePushOperator {
	return &MeasurePushOperator{
		output:  output,
		queryID: queryID,
		onPush:  onPush,
	}
}

// Push forwards the change to the wrapped output and records the elapsed time.
// The timing covers the entire downstream push — every operator in the chain
// below this decorator executes within the measured window, matching TS's
// performance.now() bracketing of this.#output.push.
func (m *MeasurePushOperator) Push(change ivm.Change, pusher ivm.InputBase) {
	start := time.Now()
	m.output.Push(change, pusher)
	if m.onPush != nil {
		m.onPush(m.queryID, time.Since(start))
	}
}
