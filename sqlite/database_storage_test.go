package sqlite

import (
	"fmt"
	"os"
	"strings"
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

// newTakeStore builds a SQLiteTakeStorage with an explicit cache cap so
// eviction is deterministic (the production cap is takeStateCacheMax).
func newTakeStore(t *testing.T, maxStates int) (*DatabaseStorage, *SQLiteTakeStorage) {
	t.Helper()
	ds, err := NewDatabaseStorage(t.TempDir() + "/take.db")
	if err != nil {
		t.Fatalf("NewDatabaseStorage: %v", err)
	}
	cgs := ds.CreateClientGroupStorage("cg")
	return ds, &SQLiteTakeStorage{storage: cgs.CreateStorage(), maxStates: maxStates}
}

// The cache must never exceed maxStates, and an evicted key must still read
// back identically — SQLite is authoritative, so eviction is transparent.
func TestSQLiteTakeStorageLRUEviction(t *testing.T) {
	ds, s := newTakeStore(t, 3)
	defer ds.Close()

	for i := 0; i < 5; i++ {
		s.SetTakeState(fmt.Sprintf("k%d", i), ivm.TakeState{Size: i, Bound: ivm.Row{"id": i}})
	}

	if got := len(s.states); got != 3 {
		t.Fatalf("map size: got %d, want 3", got)
	}
	if got := s.lru.Len(); got != 3 {
		t.Fatalf("lru size: got %d, want 3 (map and lru must stay in lock-step)", got)
	}

	// k0 and k1 are the two least-recently-used → evicted from the cache.
	for _, k := range []string{"k0", "k1"} {
		if _, ok := s.states[k]; ok {
			t.Fatalf("%s should have been evicted from the in-memory cache", k)
		}
	}
	// ...yet still readable, faithfully, from authoritative SQLite.
	st := s.GetTakeState("k0")
	if st == nil || st.Size != 0 || st.Bound["id"] != float64(0) {
		t.Fatalf("evicted key k0 not re-read identically from SQLite: %+v", st)
	}
}

// A recently-used key must survive eviction in favor of a colder one.
func TestSQLiteTakeStorageLRUKeepsHot(t *testing.T) {
	ds, s := newTakeStore(t, 3)
	defer ds.Close()

	s.SetTakeState("a", ivm.TakeState{Size: 1})
	s.SetTakeState("b", ivm.TakeState{Size: 2})
	s.SetTakeState("c", ivm.TakeState{Size: 3})
	_ = s.GetTakeState("a") // touch "a" → most-recently-used
	s.SetTakeState("d", ivm.TakeState{Size: 4})

	if _, ok := s.states["a"]; !ok {
		t.Fatal("recently-used key a was wrongly evicted")
	}
	if _, ok := s.states["b"]; ok {
		t.Fatal("LRU victim b should have been evicted, not a")
	}
}

// Negative ("known absent") entries count toward the bound too, or a scan of
// many missing partition keys would leak unboundedly.
func TestSQLiteTakeStorageNegativeEntriesBounded(t *testing.T) {
	ds, s := newTakeStore(t, 2)
	defer ds.Close()

	for i := 0; i < 10; i++ {
		if st := s.GetTakeState(fmt.Sprintf("missing-%d", i)); st != nil {
			t.Fatalf("expected nil for missing key, got %+v", st)
		}
	}
	if got := len(s.states); got > 2 {
		t.Fatalf("negative entries unbounded: cache size %d > 2", got)
	}
}

// maxStates == 0 is the explicit "unbounded" escape hatch (old behavior).
func TestSQLiteTakeStorageUnbounded(t *testing.T) {
	ds, s := newTakeStore(t, 0)
	defer ds.Close()

	const n = 200
	for i := 0; i < n; i++ {
		s.SetTakeState(fmt.Sprintf("k%d", i), ivm.TakeState{Size: i})
	}
	if got := len(s.states); got != n {
		t.Fatalf("unbounded cache should retain all %d entries, got %d", n, got)
	}
}

func TestReadTakeStateCacheMax(t *testing.T) {
	const env = "GO_IVM_TAKE_STATE_CACHE_MAX"
	cases := []struct {
		name string
		set  bool
		val  string
		want int
	}{
		{name: "unset → default", set: false, want: defaultTakeStateCacheMax},
		{name: "explicit 0 → unbounded", set: true, val: "0", want: 0},
		{name: "positive honored", set: true, val: "500", want: 500},
		{name: "negative → default", set: true, val: "-1", want: defaultTakeStateCacheMax},
		{name: "malformed → default", set: true, val: "abc", want: defaultTakeStateCacheMax},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(env, tc.val)
			} else {
				os.Unsetenv(env)
			}
			if got := readTakeStateCacheMax(); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// pageCount reads PRAGMA page_count on the rolling tx (the single
// connection); test-only helper for the compaction assertions.
func pageCount(t *testing.T, ds *DatabaseStorage) int64 {
	t.Helper()
	var n int64
	if err := ds.tx.QueryRow("PRAGMA page_count").Scan(&n); err != nil {
		t.Fatalf("PRAGMA page_count: %v", err)
	}
	return n
}

// Fresh storage databases must be created with auto_vacuum=INCREMENTAL
// (TS: database-storage.ts:59) or freed pages can never be returned to the
// OS by Destroy-time compaction. Pre-fix this read 0 (NONE).
func TestDatabaseStorageAutoVacuumIncremental(t *testing.T) {
	ds, err := NewDatabaseStorage(t.TempDir() + "/av.db")
	if err != nil {
		t.Fatalf("NewDatabaseStorage: %v", err)
	}
	defer ds.Close()

	var mode int64
	if err := ds.tx.QueryRow("PRAGMA auto_vacuum").Scan(&mode); err != nil {
		t.Fatalf("PRAGMA auto_vacuum: %v", err)
	}
	if mode != autoVacuumIncremental {
		t.Fatalf("auto_vacuum mode: got %d, want %d (INCREMENTAL)", mode, autoVacuumIncremental)
	}
}

// Destroy must reclaim the deleted client group's pages once the freeable
// bytes exceed the compaction threshold (TS: database-storage.ts:164-169 +
// db.ts:82-121). Pre-fix the storage file never shrank: without
// auto_vacuum=INCREMENTAL and the incremental_vacuum call, deleted pages sat
// on the freelist for the life of the worker.
func TestDatabaseStorageDestroyCompacts(t *testing.T) {
	ds, err := NewDatabaseStorage(t.TempDir() + "/compact.db")
	if err != nil {
		t.Fatalf("NewDatabaseStorage: %v", err)
	}
	defer ds.Close()
	// Production threshold is 50MB (defaultCompactionThresholdBytes); drop it
	// to one page so a ~1MB test payload triggers compaction — the same knob
	// TS injects via DatabaseStorage.create's options parameter.
	ds.compactionThresholdBytes = ds.pageSize

	cgs := ds.CreateClientGroupStorage("cg-compact")
	store := cgs.CreateStorage()
	val := []byte(`"` + strings.Repeat("x", 1024) + `"`)
	for i := 0; i < 1000; i++ {
		store.Set(fmt.Sprintf("k%04d", i), val)
	}

	before := pageCount(t, ds)
	cgs.Destroy()
	after := pageCount(t, ds)

	if after >= before {
		t.Fatalf("Destroy did not compact: page_count before=%d after=%d", before, after)
	}
}

// Below the threshold Destroy must NOT vacuum — the gate is the whole point
// of compactionThresholdBytes (TS: db.ts:87-93): tiny groups churn constantly
// and re-truncating the file for a few KB each time would be pure overhead.
func TestDatabaseStorageDestroyBelowThresholdSkipsCompaction(t *testing.T) {
	ds, err := NewDatabaseStorage(t.TempDir() + "/nocompact.db")
	if err != nil {
		t.Fatalf("NewDatabaseStorage: %v", err)
	}
	defer ds.Close()
	// Default 50MB threshold stays in force; ~1MB of freeable pages is
	// far below it.
	cgs := ds.CreateClientGroupStorage("cg-small")
	store := cgs.CreateStorage()
	val := []byte(`"` + strings.Repeat("x", 1024) + `"`)
	for i := 0; i < 1000; i++ {
		store.Set(fmt.Sprintf("k%04d", i), val)
	}

	before := pageCount(t, ds)
	cgs.Destroy()
	after := pageCount(t, ds)

	if after != before {
		t.Fatalf("sub-threshold Destroy should leave pages on the freelist: before=%d after=%d", before, after)
	}
}
