package main

// Parity test: a hydrate-time *ivm.DataError must surface with the SAME
// JSON-RPC code (rpcCodeDataError, -32102) as the advance path emits via
// panicErrorCode — so the TS client classifies a bad replica value
// (unsafe int / non-JSON in a json column) identically whether it is first
// read during hydrate or during advance. Before hydrateErrorResponse, a
// hydrate DataError degraded to a generic -32000 (TS: transient → reset)
// while the same value in advance produced -32102 (TS: permanent → clean
// teardown).

import (
	"errors"
	"fmt"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestHydrateErrorResponse_DataErrorMapsToDataCode(t *testing.T) {
	// firstHydratePanic wraps the lane panic with %w; simulate that exactly.
	dataErr := ivm.NewDataError("FromSQLiteType(number): int64 exceeds MAX_SAFE_INTEGER")
	wrapped := fmt.Errorf("hydrate panic (query q1): %w", dataErr)

	resp := hydrateErrorResponse(float64(7), "addQueriesStream: ", wrapped)
	if resp.Error == nil {
		t.Fatal("expected an error response")
	}
	if resp.Error.Code != rpcCodeDataError {
		t.Fatalf("DataError must map to rpcCodeDataError (%d), got %d — parity with the advance path lost",
			rpcCodeDataError, resp.Error.Code)
	}
	// Sanity: this is the same code panicErrorCode gives the advance path for
	// the identical underlying error.
	if panicErrorCode(dataErr) != resp.Error.Code {
		t.Fatalf("hydrate code %d != advance code %d for the same DataError",
			resp.Error.Code, panicErrorCode(dataErr))
	}
}

func TestHydrateErrorResponse_GenericStaysMinus32000(t *testing.T) {
	// A plain (non-DataError) hydrate failure keeps -32000 (transient → TS
	// reset), unchanged from before.
	wrapped := fmt.Errorf("hydrate panic (query q1): %w", errors.New("boom"))
	resp := hydrateErrorResponse(float64(9), "addQueries: ", wrapped)
	if resp.Error == nil || resp.Error.Code != -32000 {
		t.Fatalf("generic hydrate error should stay -32000, got %+v", resp.Error)
	}
}
