package sqlite

import (
	"os"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestDatabaseStorage(t *testing.T) {
	tmpFile := t.TempDir() + "/test-storage.db"
	defer os.Remove(tmpFile)

	ds, err := NewDatabaseStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewDatabaseStorage: %v", err)
	}
	defer ds.Close()

	cgs := ds.CreateClientGroupStorage("cg1")

	t.Run("OperatorStorage get/set/del", func(t *testing.T) {
		store := cgs.CreateStorage()

		// Initially empty
		_, ok := store.Get("foo")
		if ok {
			t.Fatal("expected not found")
		}

		// Set and get
		store.Set("foo", []byte(`"bar"`))
		val, ok := store.Get("foo")
		if !ok || string(val) != `"bar"` {
			t.Fatalf("expected \"bar\", got %s (ok=%v)", val, ok)
		}

		// Del
		store.Del("foo")
		_, ok = store.Get("foo")
		if ok {
			t.Fatal("expected deleted")
		}
	})

	t.Run("OperatorStorage scan", func(t *testing.T) {
		store := cgs.CreateStorage()
		store.Set("item:1", []byte(`1`))
		store.Set("item:2", []byte(`2`))
		store.Set("other:1", []byte(`3`))

		results := store.Scan("item:")
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		if results[0][0] != "item:1" || results[1][0] != "item:2" {
			t.Fatalf("unexpected keys: %v", results)
		}
	})

	t.Run("SQLiteTakeStorage", func(t *testing.T) {
		takeStore := cgs.CreateTakeStorage()

		// No state initially
		if state := takeStore.GetTakeState("partition:x"); state != nil {
			t.Fatal("expected nil state")
		}
		if bound := takeStore.GetMaxBound(); bound != nil {
			t.Fatal("expected nil bound")
		}

		// Set take state
		takeStore.SetTakeState("partition:x", ivm.TakeState{Size: 5, Bound: ivm.Row{"id": 10}})
		state := takeStore.GetTakeState("partition:x")
		if state == nil || state.Size != 5 {
			t.Fatalf("expected size 5, got %+v", state)
		}

		// Set max bound
		takeStore.SetMaxBound(ivm.Row{"id": 99})
		bound := takeStore.GetMaxBound()
		if bound == nil {
			t.Fatal("expected non-nil bound")
		}
		// JSON roundtrip converts int to float64
		if bound["id"] != float64(99) {
			t.Fatalf("expected id=99, got %v", bound["id"])
		}

		// Del
		takeStore.Del("partition:x")
		if state := takeStore.GetTakeState("partition:x"); state != nil {
			t.Fatal("expected nil after del")
		}
	})

	t.Run("separate client groups isolated", func(t *testing.T) {
		cgs2 := ds.CreateClientGroupStorage("cg2")
		store1 := cgs.CreateStorage()
		store2 := cgs2.CreateStorage()

		store1.Set("key", []byte(`"from-cg1"`))
		store2.Set("key", []byte(`"from-cg2"`))

		val1, _ := store1.Get("key")
		val2, _ := store2.Get("key")
		if string(val1) != `"from-cg1"` || string(val2) != `"from-cg2"` {
			t.Fatalf("isolation failed: val1=%s val2=%s", val1, val2)
		}
	})
}
