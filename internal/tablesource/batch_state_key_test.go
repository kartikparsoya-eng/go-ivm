package tablesource

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestBatchStatePKKeyIsTypedAndUnambiguous(t *testing.T) {
	src := &Source{primaryKey: []string{"a", "b"}}

	withSeparator := src.pkKey(ivm.Row{"a": "x\x00y", "b": "z"})
	shiftedSeparator := src.pkKey(ivm.Row{"a": "x", "b": "y\x00z"})
	if withSeparator == shiftedSeparator {
		t.Fatalf("PK key collision across tuple boundary: %q", withSeparator)
	}

	stringOne := src.pkKey(ivm.Row{"a": "1", "b": "x"})
	numberOne := src.pkKey(ivm.Row{"a": float64(1), "b": "x"})
	if stringOne == numberOne {
		t.Fatalf("PK key collision across value types: %q", stringOne)
	}
}
