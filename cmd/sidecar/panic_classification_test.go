package main

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// The panic→RPC-error classification is the Go half of the TS recovery
// ladder (view-syncer.ts run() / #advancePipelines / go-ivm-client.ts
// RPC_CODE_DATA_ERROR):
//   -32102 (*ivm.DataError)  → TS tears the CG down (poison input; a reset
//                              would re-read the same bad row and loop)
//   -32000 (everything else) → TS classifies 'unclassified' → pipeline reset
//                              (transient; re-hydrate heals)
// Getting a DataError misclassified as -32000 recreates the pre-prod reset
// storm; getting a transient panic misclassified as -32102 tears down CGs
// needlessly. Nothing covered this seam before.

func TestPanicErrorCode_Classification(t *testing.T) {
	cases := []struct {
		name string
		r    any
		want int
	}{
		{"DataError → teardown code", ivm.NewDataError("no source for table %q", "ghosts"), rpcCodeDataError},
		{"plain string → generic", "boom", -32000},
		{"error value → generic", errFake{}, -32000},
		// DriftError normally never reaches the handler recover (AdvanceStream
		// converts it in-band), but if one ever escapes it must classify as
		// transient/reset — NOT teardown.
		{"DriftError → generic (reset, not teardown)", &ivm.DriftError{Table: "users", Op: "Edit"}, -32000},
	}
	for _, c := range cases {
		if got := panicErrorCode(c.r); got != c.want {
			t.Errorf("%s: panicErrorCode(%T) = %d, want %d", c.name, c.r, got, c.want)
		}
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
	req := RPCRequest{JSONRPC: "2.0", Method: "advanceStream", ID: 42}
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
