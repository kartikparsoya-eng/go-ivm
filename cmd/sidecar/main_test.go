package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestMultiGroupParallel verifies that multiple client groups can
// init and destroy concurrently through the in-process ABI host without races
// or corruption.
func TestMultiGroupParallel(t *testing.T) {
	server := NewServer(makeReplicaPathOnly(t))
	router := newResponseRouter()
	host := startABIHostWithServer(server, router.sink, nil)
	defer host.Shutdown()

	numGroups := 8
	var wg sync.WaitGroup

	for i := 0; i < numGroups; i++ {
		wg.Add(1)
		go func(groupIdx int) {
			defer wg.Done()

			cgID := fmt.Sprintf("group-%d", groupIdx)
			initID := float64(groupIdx*10 + 1)
			destroyID := float64(groupIdx*10 + 2)

			initP := map[string]interface{}{
				"clientGroupID": cgID,
				"dbPath":        ":memory:",
				"storagePath":   ":memory:",
				"tables":        map[string]interface{}{},
			}
			if err := host.Send(encodeReq(t, "init", initID, initP)); err != nil {
				t.Errorf("group %d init send: %v", groupIdx, err)
				return
			}
			resp := router.wait(t, initID)
			if resp.Error != nil {
				t.Errorf("group %d init: %s", groupIdx, resp.Error.Message)
				return
			}

			// Destroy — must send the matching initEpoch so the epoch guard
			// accepts the call (D2 fix).
			g := server.getGroup(cgID, false)
			var epoch uint64
			if g != nil {
				g.mu.Lock()
				epoch = g.initEpoch.Load()
				g.mu.Unlock()
			}
			destroyP := map[string]interface{}{
				"clientGroupID": cgID,
				"initEpoch":     epoch,
			}
			if err := host.Send(encodeReq(t, "destroy", destroyID, destroyP)); err != nil {
				t.Errorf("group %d destroy send: %v", groupIdx, err)
				return
			}
			resp = router.wait(t, destroyID)
			if resp.Error != nil {
				t.Errorf("group %d destroy: %s", groupIdx, resp.Error.Message)
			}
		}(i)
	}

	wg.Wait()
}

type responseRouter struct {
	mu    sync.Mutex
	chans map[int]chan RPCResponse
	errs  chan error
}

func newResponseRouter() *responseRouter {
	return &responseRouter{
		chans: make(map[int]chan RPCResponse),
		errs:  make(chan error, 16),
	}
}

func (r *responseRouter) channel(id int) chan RPCResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := r.chans[id]
	if ch == nil {
		ch = make(chan RPCResponse, 8)
		r.chans[id] = ch
	}
	return ch
}

func (r *responseRouter) sink(kind int32, payload []byte) int32 {
	if kind != abiKindFrame {
		return deliverOK
	}
	var resp RPCResponse
	if err := mpUnmarshal(payload, &resp); err != nil {
		r.errs <- err
		return deliverOK
	}
	id, ok := toFloat(resp.ID)
	if !ok {
		r.errs <- fmt.Errorf("non-numeric response ID %#v", resp.ID)
		return deliverOK
	}
	r.channel(int(id)) <- resp
	return deliverOK
}

func (r *responseRouter) wait(t *testing.T, id float64) RPCResponse {
	t.Helper()
	select {
	case resp := <-r.channel(int(id)):
		return resp
	case err := <-r.errs:
		t.Fatalf("decode response: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for response id %v", id)
	}
	return RPCResponse{}
}
