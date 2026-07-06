package main

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// The panic→RPC-error classification is the Go half of the TS recovery
// ladder (view-syncer.ts run() / #advancePipelines / go-ivm-client.ts
// RPC_CODE_DATA_ERROR / RPC_CODE_SCALAR_RESET):
//   -32102 (*ivm.DataError)         → TS tears the CG down (poison input; a
//                                     reset would re-read the same bad row
//                                     and loop)
//   -32105 (*engine.ScalarResetError) → TS resets + re-hydrates
//                                     (ResetPipelinesSignal 'scalar-subquery')
//   -32000 (everything else)        → TS classifies 'unclassified' → rethrow
//                                     → CG teardown (follow-TS failure model)
// Getting a DataError misclassified as -32000 loses the poison-row
// attribution; getting a scalar reset misclassified as -32000 turns TS's
// reset into a teardown. Nothing covered this seam before.

func TestPanicErrorCode_Classification(t *testing.T) {
	cases := []struct {
		name string
		r    any
		want int
	}{
		{"DataError → teardown code", ivm.NewDataError("no source for table %q", "ghosts"), rpcCodeDataError},
		{"plain string → generic", "boom", -32000},
		{"error value → generic", errFake{}, -32000},
		// The source-drift asserts panic with a plain error — 'unclassified'
		// → rethrow → teardown, exactly TS's disposition for its own asserts.
		{"source-drift error → generic (teardown)", ivm.SourceDriftError("users", "Edit", nil, -1), -32000},
		// The companion scalar reset is TS's ResetPipelinesSignal
		// ('scalar-subquery') — a RESET, so it must NOT ride -32000.
		{"ScalarResetError → scalar-reset code", &engine.ScalarResetError{Table: "users", Resolved: "Alice", New: "Alicia"}, rpcCodeScalarReset},
	}
	for _, c := range cases {
		if got := panicErrorCode(c.r); got != c.want {
			t.Errorf("%s: panicErrorCode(%T) = %d, want %d", c.name, c.r, got, c.want)
		}
	}

	// The scalar reset's wire message must be the TS signal text VERBATIM
	// (no "panic: " prefix) — TS surfaces it as the ResetPipelinesSignal
	// message.
	sre := &engine.ScalarResetError{Table: "users", Resolved: "Alice", New: "Alicia"}
	if got := panicErrorMessage(sre); got != "Scalar subquery value changed for users: Alice -> Alicia" {
		t.Errorf("panicErrorMessage(ScalarResetError) = %q, want the TS signal text", got)
	}
}

type errFake struct{}

func (errFake) Error() string { return "fake" }

// handleStreamWithRecover is the only recover between a streaming handler's
// panic and process abort (streaming handlers run past handleRequest's
// recover). Pin: panic → error RPCResponse with the request's ID (so the TS
// client rejects THAT call instead of orphaning it into a 60s timeout) and
// the classification code from panicErrorCode.
func TestHandleStreamWithRecover_ConvertsPanicToRPCError(t *testing.T) {
	s := &Server{}
	req := RPCRequest{JSONRPC: "2.0", Method: "advanceToHeadStream", ID: 42}
	noopStream := streamWriter(func(interface{}, interface{}) {})

	t.Run("generic panic → -32000", func(t *testing.T) {
		resp := s.handleStreamWithRecover(req, noopStream, func(RPCRequest, streamWriter) RPCResponse {
			panic("write to broken pipe")
		})
		if resp.Error == nil {
			t.Fatal("want error response, got none")
		}
		if resp.Error.Code != -32000 {
			t.Fatalf("code = %d, want -32000", resp.Error.Code)
		}
		if resp.ID != 42 {
			t.Fatalf("response ID = %v, want the request ID 42 (else the call orphans)", resp.ID)
		}
		if !strings.Contains(resp.Error.Message, "broken pipe") {
			t.Fatalf("panic message lost: %q", resp.Error.Message)
		}
	})

	t.Run("DataError panic → -32102", func(t *testing.T) {
		resp := s.handleStreamWithRecover(req, noopStream, func(RPCRequest, streamWriter) RPCResponse {
			panic(ivm.NewDataError("unsupported json column"))
		})
		if resp.Error == nil || resp.Error.Code != rpcCodeDataError {
			t.Fatalf("want error code %d, got %+v", rpcCodeDataError, resp.Error)
		}
	})

	t.Run("clean handler passes through", func(t *testing.T) {
		want := RPCResponse{JSONRPC: "2.0", Result: "done", ID: 42}
		resp := s.handleStreamWithRecover(req, noopStream, func(RPCRequest, streamWriter) RPCResponse {
			return want
		})
		if resp.Error != nil || resp.Result != "done" {
			t.Fatalf("clean handler mangled: %+v", resp)
		}
	})
}

// Same contract for the non-streaming dispatch path.
func TestHandleRequest_PanicRecoverAndUnknownMethod(t *testing.T) {
	s := &Server{}

	// Unknown method → -32601, never a panic/crash.
	resp := s.handleRequest(RPCRequest{JSONRPC: "2.0", Method: "no-such-method", ID: 7})
	if resp.Error == nil || resp.Error.Code != -32601 {
		t.Fatalf("unknown method: want -32601, got %+v", resp.Error)
	}
	if resp.ID != 7 {
		t.Fatalf("unknown method: response ID = %v, want 7", resp.ID)
	}
}
