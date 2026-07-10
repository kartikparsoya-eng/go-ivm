package main

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/vmihailenco/msgpack/v5"
)

func encodeReq(t *testing.T, method string, id float64, params interface{}) []byte {
	t.Helper()
	rawParams, err := mpMarshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	data, err := mpMarshal(RPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  msgpack.RawMessage(rawParams),
		ID:      id,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return data
}

func collectAdvanceToHeadProdFrames(t *testing.T, srv *Server, reqID float64) (streamWriter, *[]advanceToHeadStreamPartial) {
	t.Helper()
	frames := &[]advanceToHeadStreamPartial{}
	appendPartial := func(partial interface{}) {
		if p, ok := partial.(advanceToHeadStreamPartial); ok {
			*frames = append(*frames, p)
		}
	}
	w := streamWriter(func(_ interface{}, partial interface{}) {
		appendPartial(partial)
	})

	origDeliver := srv.abiDeliver
	col := newSinkCollector()
	defs := map[uint32]decodedGroupDef{}
	nextRowChunkIndex := 0
	srv.abiDeliver = func(kind int32, payload []byte) int32 {
		code := col.sink(kind, payload)
		if kind == abiKindGroupDef {
			def := decodeGroupDef(t, payload)
			if def.reqID == reqID {
				defs[def.groupID] = def
			}
			return code
		}
		if kind == abiKindRow {
			rr := &recReader{buf: payload}
			rowReqID := rr.f64()
			groupID := rr.u32()
			changeType := int(rr.u8())
			if rowReqID != reqID {
				return code
			}
			def, ok := defs[groupID]
			if !ok {
				t.Fatalf("row record for unknown groupID=%d", groupID)
			}
			nvalues := len(def.cols)
			if changeType == engine.RowChangeRemove {
				nvalues = len(def.pk)
			}
			row := decodeRowRecord(t, payload, nvalues)
			rowKey := map[string]interface{}{}
			var fullRow ivm.Row
			if row.changeType == engine.RowChangeRemove {
				for i, pk := range def.pk {
					rowKey[pk] = row.values[i]
				}
			} else {
				fullRow = ivm.Row{}
				for i, col := range def.cols {
					fullRow[col] = row.values[i]
				}
				for _, pk := range def.pk {
					rowKey[pk] = fullRow[pk]
				}
			}
			pc := toPositional([]engine.RowChange{{
				Type:    row.changeType,
				QueryID: def.queryID,
				Table:   def.table,
				RowKey:  rowKey,
				Row:     fullRow,
			}})
			*frames = append(*frames, advanceToHeadStreamPartial{
				Dict:       pc.Dict,
				Rows:       pc.Rows,
				ChunkIndex: nextRowChunkIndex,
				Final:      false,
			})
			nextRowChunkIndex++
			return code
		}
		if kind != abiKindFrame {
			return code
		}
		resp := decodeResp(t, payload)
		if id, ok := toFloat(resp.ID); !ok || id != reqID || resp.Error != nil {
			return code
		}
		if _, ok := resp.Result.(string); ok {
			return code
		}
		p, ok := advancePartialFromResult(resp.Result)
		if !ok {
			t.Fatalf("decode advance partial result: %#v", resp.Result)
		}
		*frames = append(*frames, p)
		return code
	}
	t.Cleanup(func() { srv.abiDeliver = origDeliver })
	return w, frames
}

func prodAdvanceParams(cgID string, initEpoch uint64) advanceToHeadParams {
	return advanceToHeadParams{
		ClientGroupID: cgID,
		InitEpoch:     initEpoch,
		RowMode:       true,
		PullMode:      true,
		PullWindow:    1024,
	}
}

func advancePartialFromResult(result interface{}) (advanceToHeadStreamPartial, bool) {
	m, ok := result.(map[string]interface{})
	if !ok {
		return advanceToHeadStreamPartial{}, false
	}
	var p advanceToHeadStreamPartial
	if v, ok := toFloat(m["chunkIndex"]); ok {
		p.ChunkIndex = int(v)
	}
	if v, ok := m["final"].(bool); ok {
		p.Final = v
	}
	if v, ok := m["header"].(bool); ok {
		p.Header = v
	}
	if v, ok := m["version"].(string); ok {
		p.Version = v
	}
	if v, ok := toFloat(m["numChanges"]); ok {
		p.NumChanges = int(v)
	}
	if reset, ok := m["reset"].(map[string]interface{}); ok {
		p.Reset = &resetWire{}
		if v, ok := reset["reason"].(string); ok {
			p.Reset.Reason = v
		}
		if v, ok := reset["msg"].(string); ok {
			p.Reset.Msg = v
		}
	}
	if rows, ok := m["r"].([]interface{}); ok {
		for _, row := range rows {
			if vals, ok := row.([]interface{}); ok {
				p.Rows = append(p.Rows, vals)
			}
		}
	}
	return p, true
}
