package ivm

import "fmt"

// DataError marks a DETERMINISTIC, NON-RETRYABLE failure that a full pipeline
// rebuild cannot fix — the input (replica data OR query AST) is fixed, so a
// reset re-reads/re-builds the same input and re-panics. Two sources:
//   1. Replica data that can't be represented in the JS value model: a
//      non-JSON string in a json/array column, an integer beyond JS
//      MAX_SAFE_INTEGER, a cross-type comparison.
//   2. An unsupported query shape the builder can't compile: an unknown
//      condition type / operator, or a table with no source.
// These panics are recovered by the sidecar and mapped to RPC code -32102
// (RPC_CODE_DATA_ERROR on the TS side) so the view-syncer TEARS DOWN the
// offending client group — like TS-native's UnsupportedValueError throw —
// instead of escalating to a pipeline reset (which would loop forever).
//
// Why the distinction matters: a reset re-hydrates and re-reads the SAME bad
// row, which re-panics, which resets again → an infinite reset storm that also
// re-pays every CG's hydrate cost. A data error is permanent; retrying cannot
// fix it, so it must fail once and tear down, never loop.
//
// The formatted message is preserved verbatim, so existing panic-message
// expectations, tests, and logs are unchanged — only the panic VALUE's type
// changes (string -> *DataError), which the recover sites type-assert on.
type DataError struct{ msg string }

func (e *DataError) Error() string { return e.msg }

// NewDataError builds a *DataError with a formatted message.
func NewDataError(format string, args ...any) *DataError {
	return &DataError{msg: fmt.Sprintf(format, args...)}
}
