package main

// RPC-surface pin for the 2026-07-06 ART incident fix: init against an
// exhausted replica read pool must fail FAST with a diagnosable rpcError
// (tablesource's bounded presence probe), not wedge the CG worker until the
// TS-side 120s RPC timeout and leak the goroutine. TS's init-failure path
// (fall back to TS-native, retry Go later) only engages if the error
// actually arrives.

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
)

func TestHandleInit_FailsFastWhenReadPoolExhausted(t *testing.T) {
	t.Setenv("GO_IVM_MAX_OPEN_CONNS", "2")
	t.Setenv("GO_IVM_MAX_IDLE_CONNS", "2")
	savedTimeout := tablesource.PoolAcquireTimeout
	tablesource.PoolAcquireTimeout = 300 * time.Millisecond
	defer func() { tablesource.PoolAcquireTimeout = savedTimeout }()

	path, _ := makeReplica(t)
	srv := NewServer(path)
	t.Cleanup(srv.closeAll)

	// Open the pools (lazy) so we can exhaust the read pool.
	rdb, err := srv.getReplicaDB()
	if err != nil {
		t.Fatalf("getReplicaDB: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var held []*sql.Conn
	for i := 0; i < 2; i++ {
		c, cerr := rdb.Conn(ctx)
		if cerr != nil {
			t.Fatalf("hold conn %d: %v", i, cerr)
		}
		held = append(held, c)
	}
	releaseHeld := func() {
		for _, c := range held {
			_ = c.Close()
		}
		held = nil
	}
	defer releaseHeld()

	initReq := RPCRequest{Method: "init", ID: 1, Params: mustMarshal(t, issueInitParams("cg-exhausted"))}
	start := time.Now()
	resp := srv.handleInit(initReq)
	elapsed := time.Since(start)

	if resp.Error == nil {
		t.Fatalf("init succeeded against a fully-held read pool: %+v", resp.Result)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("init blocked %v — pre-fix this wedged until the TS 120s timeout", elapsed)
	}
	if !strings.Contains(resp.Error.Message, "presence probe timed out") {
		t.Fatalf("error %q should carry the probe-timeout diagnostic", resp.Error.Message)
	}

	// Recovery: same CG, pool freed → init must succeed (nothing leaked,
	// group not poisoned by the failed attempt).
	releaseHeld()
	resp = srv.handleInit(RPCRequest{Method: "init", ID: 2, Params: mustMarshal(t, issueInitParams("cg-exhausted"))})
	if resp.Error != nil {
		t.Fatalf("post-release init failed: %+v", resp.Error)
	}
}
