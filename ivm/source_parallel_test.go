package ivm_test

// results to sequential fan-out. This is the core correctness guarantee.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// collectOutput is a simple output that records all pushed changes.
type collectOutput struct {
	changes []ivm.Change
	input   ivm.InputBase
}

func (c *collectOutput) Push(change ivm.Change, pusher ivm.InputBase) []ivm.Change {
	c.changes = append(c.changes, change)
	c.input = pusher
	return nil
}

func newTestSource() *ivm.MemorySource {
	return ivm.NewMemorySource("test", map[string]string{
		"id":   "string",
		"name": "string",
		"age":  "number",
	}, []string{"id"})
}

func TestParallelFanOutMatchesSequential(t *testing.T) {
	// Setup: create source with initial data
	seqSource := newTestSource()
	parSource := newTestSource()

	rows := []ivm.Row{
		{"id": "1", "name": "alice", "age": float64(30)},
		{"id": "2", "name": "bob", "age": float64(25)},
		{"id": "3", "name": "carol", "age": float64(35)},
	}

	for _, row := range rows {
		seqSource.Push(ivm.MakeSourceChangeAdd(row))
		parSource.Push(ivm.MakeSourceChangeAdd(row))
	}

	// Connect multiple outputs to both sources
	seqOutputs := make([]*collectOutput, 3)
	parOutputs := make([]*collectOutput, 3)

	for i := 0; i < 3; i++ {
		seqInput := seqSource.Connect(nil, nil, nil)
		seqOutputs[i] = &collectOutput{}
		seqInput.SetOutput(seqOutputs[i])

		parInput := parSource.Connect(nil, nil, nil)
		parOutputs[i] = &collectOutput{}
		parInput.SetOutput(parOutputs[i])
	}

	parSource.SetParallel(true)

	// Push same change to both
	change := ivm.MakeSourceChangeAdd(ivm.Row{"id": "4", "name": "dave", "age": float64(28)})
	seqResults := seqSource.Push(change)
	parResults := parSource.PushWithMode(change)

	// Compare results
	if len(seqResults) != len(parResults) {
		t.Fatalf("result count mismatch: seq=%d par=%d", len(seqResults), len(parResults))
	}

	// Compare per-output changes
	for i := 0; i < 3; i++ {
		if len(seqOutputs[i].changes) != len(parOutputs[i].changes) {
			t.Errorf("output[%d] change count mismatch: seq=%d par=%d",
				i, len(seqOutputs[i].changes), len(parOutputs[i].changes))
		}
	}
}

func TestParallelFanOutRemove(t *testing.T) {
	seqSource := newTestSource()
	parSource := newTestSource()

	row := ivm.Row{"id": "1", "name": "alice", "age": float64(30)}
	seqSource.Push(ivm.MakeSourceChangeAdd(row))
	parSource.Push(ivm.MakeSourceChangeAdd(row))

	// Connect outputs
	seqOutputs := make([]*collectOutput, 4)
	parOutputs := make([]*collectOutput, 4)
	for i := 0; i < 4; i++ {
		si := seqSource.Connect(nil, nil, nil)
		seqOutputs[i] = &collectOutput{}
		si.SetOutput(seqOutputs[i])

		pi := parSource.Connect(nil, nil, nil)
		parOutputs[i] = &collectOutput{}
		pi.SetOutput(parOutputs[i])
	}

	parSource.SetParallel(true)

	change := ivm.MakeSourceChangeRemove(row)
	seqSource.Push(change)
	parSource.PushWithMode(change)

	for i := 0; i < 4; i++ {
		if len(seqOutputs[i].changes) != len(parOutputs[i].changes) {
			t.Errorf("output[%d] mismatch: seq=%d par=%d",
				i, len(seqOutputs[i].changes), len(parOutputs[i].changes))
		}
		for j := range seqOutputs[i].changes {
			if seqOutputs[i].changes[j].Type != parOutputs[i].changes[j].Type {
				t.Errorf("output[%d] change[%d] type mismatch", i, j)
			}
		}
	}
}

func TestParallelFanOutEdit(t *testing.T) {
	seqSource := newTestSource()
	parSource := newTestSource()

	row := ivm.Row{"id": "1", "name": "alice", "age": float64(30)}
	seqSource.Push(ivm.MakeSourceChangeAdd(row))
	parSource.Push(ivm.MakeSourceChangeAdd(row))

	seqOutputs := make([]*collectOutput, 3)
	parOutputs := make([]*collectOutput, 3)
	for i := 0; i < 3; i++ {
		si := seqSource.Connect(nil, nil, nil)
		seqOutputs[i] = &collectOutput{}
		si.SetOutput(seqOutputs[i])

		pi := parSource.Connect(nil, nil, nil)
		parOutputs[i] = &collectOutput{}
		pi.SetOutput(parOutputs[i])
	}

	parSource.SetParallel(true)

	newRow := ivm.Row{"id": "1", "name": "alice_updated", "age": float64(31)}
	change := ivm.MakeSourceChangeEdit(newRow, row)
	seqSource.Push(change)
	parSource.PushWithMode(change)

	for i := 0; i < 3; i++ {
		if len(seqOutputs[i].changes) != len(parOutputs[i].changes) {
			t.Errorf("output[%d] mismatch: seq=%d par=%d",
				i, len(seqOutputs[i].changes), len(parOutputs[i].changes))
		}
	}
}

func TestParallelFanOutWithFilter(t *testing.T) {
	seqSource := newTestSource()
	parSource := newTestSource()

	rows := []ivm.Row{
		{"id": "1", "name": "alice", "age": float64(30)},
		{"id": "2", "name": "bob", "age": float64(25)},
	}
	for _, row := range rows {
		seqSource.Push(ivm.MakeSourceChangeAdd(row))
		parSource.Push(ivm.MakeSourceChangeAdd(row))
	}

	// Connect with different filter predicates
	ageOver28 := func(row ivm.Row) bool {
		age, ok := row["age"].(float64)
		return ok && age > 28
	}
	nameStartsB := func(row ivm.Row) bool {
		name, ok := row["name"].(string)
		return ok && len(name) > 0 && name[0] == 'b'
	}

	si1 := seqSource.Connect(nil, ageOver28, nil)
	so1 := &collectOutput{}
	si1.SetOutput(so1)
	si2 := seqSource.Connect(nil, nameStartsB, nil)
	so2 := &collectOutput{}
	si2.SetOutput(so2)

	pi1 := parSource.Connect(nil, ageOver28, nil)
	po1 := &collectOutput{}
	pi1.SetOutput(po1)
	pi2 := parSource.Connect(nil, nameStartsB, nil)
	po2 := &collectOutput{}
	pi2.SetOutput(po2)

	parSource.SetParallel(true)

	// Add a row that matches filter 1 but not filter 2
	change := ivm.MakeSourceChangeAdd(ivm.Row{"id": "3", "name": "carol", "age": float64(40)})
	seqSource.Push(change)
	parSource.PushWithMode(change)

	if len(so1.changes) != len(po1.changes) {
		t.Errorf("filter1 output mismatch: seq=%d par=%d", len(so1.changes), len(po1.changes))
	}
	if len(so2.changes) != len(po2.changes) {
		t.Errorf("filter2 output mismatch: seq=%d par=%d", len(so2.changes), len(po2.changes))
	}

	// filter1 should have 1 change (age 40 > 28), filter2 should have 0 (carol doesn't start with b)
	if len(so1.changes) != 1 {
		t.Errorf("expected 1 change for ageOver28 filter, got %d", len(so1.changes))
	}
	if len(so2.changes) != 0 {
		t.Errorf("expected 0 changes for nameStartsB filter, got %d", len(so2.changes))
	}
}

// BenchmarkParallelVsSequential measures the speedup from parallel fan-out.
func BenchmarkSequentialFanOut(b *testing.B) {
	benchFanOut(b, false)
}

func BenchmarkParallelFanOut(b *testing.B) {
	benchFanOut(b, true)
}

func benchFanOut(b *testing.B, parallel bool) {
	source := newTestSource()

	// Seed data
	for i := 0; i < 100; i++ {
		source.Push(ivm.MakeSourceChangeAdd(ivm.Row{
			"id":   string(rune('a'+i%26)) + string(rune('0'+i/26)),
			"name": "user",
			"age":  float64(i),
		}))
	}

	// Connect 8 outputs (simulating 8 pipelines)
	for i := 0; i < 8; i++ {
		si := source.Connect(nil, nil, nil)
		si.SetOutput(&collectOutput{})
	}

	source.SetParallel(parallel)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := ivm.Row{"id": "new", "name": "bench", "age": float64(i)}
		source.PushWithMode(ivm.MakeSourceChangeAdd(row))
		// Remove it so next iteration can add again
		source.PushWithMode(ivm.MakeSourceChangeRemove(row))
	}
}
