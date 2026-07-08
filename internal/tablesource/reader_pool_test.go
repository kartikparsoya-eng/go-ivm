package tablesource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// seedReplicaWithStateVersion seeds a WAL replica that includes the
// _zero.replicationState table the reader pool validates against, plus a typed
// users table. Returns the file path and the seeded stateVersion.
func seedReplicaWithStateVersion(t *testing.T, stateVersion string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL)`,
		`INSERT INTO "_zero.replicationState" VALUES ('` + stateVersion + `')`,
		`CREATE TABLE users (
			id     INTEGER PRIMARY KEY,
			name   TEXT,
			score  INTEGER,
			active INTEGER
		)`,
		`INSERT INTO users VALUES
			(1, 'alice', 90, 1),
			(2, 'bob',   80, 0),
			(3, 'carol', 70, 1),
			(4, 'dave',  60, 1),
			(5, 'eve',   50, 0)`,
	}
	for _, s := range stmts {
		if _, err := w.Exec(s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	return path
}

func newUserSourceAt(t *testing.T, path string) *Source {
	t.Helper()
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb := openWritableForTest(t, path)
	t.Cleanup(func() { wdb.Close() })
	src, err := New(db, wdb, "users", userSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return src
}

func nodeIDs(nodes []ivm.Node) []int64 {
	out := make([]int64, 0, len(nodes))
	for _, n := range nodes {
		switch v := n.Row["id"].(type) {
		case int64:
			out = append(out, v)
		case float64:
			out = append(out, int64(v))
		}
	}
	return out
}

// rawScalarIntErr runs a single-value query on a poolReader's raw conn and
// returns the first column of the first row as int64.
func rawScalarIntErr(r *poolReader, query string) (int64, error) {
	st, err := r.checkoutStmt(context.Background(), query)
	if err != nil {
		return 0, fmt.Errorf("prepare %q: %w", query, err)
	}
	defer r.returnStmt(query, st, true)
	rows, err := queryStmt(context.Background(), st, nil)
	if err != nil {
		return 0, fmt.Errorf("query %q: %w", query, err)
	}
	defer rows.Close()
	dest := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(dest); err != nil {
		return 0, fmt.Errorf("next %q: %w", query, err)
	}
	n, _ := dest[0].(int64)
	return n, nil
}

// rawScalarInt is rawScalarIntErr with t.Fatalf on error.
func rawScalarInt(t *testing.T, r *poolReader, query string) int64 {
	t.Helper()
	n, err := rawScalarIntErr(r, query)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// rawScalarString runs a single-value query and returns the first column of
// the first row as a string.
func rawScalarString(t *testing.T, r *poolReader, query string) string {
	t.Helper()
	st, err := r.checkoutStmt(context.Background(), query)
	if err != nil {
		t.Fatalf("prepare %q: %v", query, err)
	}
	defer r.returnStmt(query, st, true)
	rows, err := queryStmt(context.Background(), st, nil)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	dest := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(dest); err != nil {
		t.Fatalf("next %q: %v", query, err)
	}
	switch v := dest[0].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		t.Fatalf("scalar %q has type %T, want string", query, dest[0])
		return ""
	}
}

// rawCountUsers runs `SELECT count(*) FROM users` on a poolReader's raw
// conn — the test-side stand-in for a leaf read on the reader.
func rawCountUsers(t *testing.T, r *poolReader) int64 {
	t.Helper()
	st, err := r.checkoutStmt(context.Background(), "SELECT count(*) FROM users")
	if err != nil {
		t.Fatalf("rawCountUsers prepare: %v", err)
	}
	defer r.returnStmt("SELECT count(*) FROM users", st, true)
	rows, err := queryStmt(context.Background(), st, nil)
	if err != nil {
		t.Fatalf("rawCountUsers query: %v", err)
	}
	defer rows.Close()
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("rawCountUsers next: %v", err)
	}
	n, _ := dest[0].(int64)
	// Drain to EOF so the stmt resets cleanly on rows.Close.
	for {
		if err := rows.Next(make([]driver.Value, 1)); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("rawCountUsers drain: %v", err)
		}
	}
	return n
}

// TestReaderPool_PinsAllToVersion: a pool built at the seeded version opens K
// readers, each validated to that version, each able to query.
func TestReaderPool_PinsAllToVersion(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v42")
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const k = 6
	pool, err := NewReaderPool(context.Background(), db, "v42", k)
	if err != nil {
		t.Fatalf("NewReaderPool: %v", err)
	}
	defer pool.Close()

	if pool.Version() != "v42" {
		t.Fatalf("pool.Version() = %q, want v42", pool.Version())
	}
	if len(pool.all) != k {
		t.Fatalf("pool has %d readers, want %d", len(pool.all), k)
	}
	// Borrow every reader at once via the per-pipeline admission gate
	// (proves K independent raw conns) and read on each.
	releases := make([]func(), 0, k)
	for i := 0; i < k; i++ {
		group := fmt.Sprintf("q%d", i)
		release, ok := pool.AcquireForPipeline(group, time.Second)
		if !ok {
			t.Fatalf("AcquireForPipeline %d did not grant", i)
		}
		releases = append(releases, release)
		r := pool.readerFor(group)
		if r == nil {
			t.Fatalf("readerFor(%q) = nil after acquire", group)
		}
		if n := rawCountUsers(t, r); n != 5 {
			t.Fatalf("reader %d saw %d users, want 5", i, n)
		}
	}
	for _, release := range releases {
		release()
	}
}

// TestReaderPool_ConvergesToHead_IgnoresStaleTarget: the pool converges to the
// replica's actual head regardless of the (now-ignored) wantVersion parameter.
// The old code rejected a stale target; the converge-upward strategy simply
// pins all readers at whatever head they land on.
func TestReaderPool_ConvergesToHead_IgnoresStaleTarget(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v42")
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Pass a stale/wrong version — the pool should ignore it and converge
	// to whatever the replica is actually at ("v42").
	pool, err := NewReaderPool(context.Background(), db, "v999", 4)
	if err != nil {
		t.Fatalf("NewReaderPool: %v", err)
	}
	defer pool.Close()

	if pool.Version() != "v42" {
		t.Fatalf("pool.Version() = %q, want v42 (actual head)", pool.Version())
	}
}

// TestSourceFetch_PoolEqualsSingleConn: the bound-reader path returns exactly
// the same Nodes as the locked single-conn path, across plain / sorted /
// reverse / filtered fetches. This is the correctness oracle: the pool is
// pure parallelism, output must be byte-identical.
func TestSourceFetch_PoolEqualsSingleConn(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v1")

	type tc struct {
		name string
		sort ivm.Ordering
		pred func(ivm.Row) bool
		req  ivm.FetchRequest
	}
	asInt := func(v any) int64 {
		switch x := v.(type) {
		case int64:
			return x
		case float64:
			return int64(x)
		}
		return 0
	}
	cases := []tc{
		{name: "plain"},
		{name: "sorted_score_desc", sort: ivm.Ordering{{"score", "desc"}, {"id", "asc"}}},
		{name: "reverse", req: ivm.FetchRequest{Reverse: true}},
		{name: "filtered_active", pred: func(r ivm.Row) bool { return asInt(r["active"]) == 1 }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Single-conn (no pool) reference.
			srcA := newUserSourceAt(t, path)
			inA := srcA.Connect(c.sort, nil, c.pred, nil)
			want := nodeIDs(slices.Collect(inA.Fetch(c.req)))

			// Bound-reader path: the connection's pipeline group holds an
			// exclusive reader (the engine's hydrateOne discipline).
			srcB := newUserSourceAt(t, path)
			poolDB, err := Open(path, OpenOptions{})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer poolDB.Close()
			pool, err := NewReaderPool(context.Background(), poolDB, "v1", 4)
			if err != nil {
				t.Fatalf("NewReaderPool: %v", err)
			}
			defer pool.Close()
			srcB.BindReaderPool(pool)
			srcB.SetNextConnectGroup("q1")
			inB := srcB.Connect(c.sort, nil, c.pred, nil)

			// Pool bound but pipeline NOT acquired: the fetch must route to
			// the serial bound conn (build-phase shape) and still be correct.
			serialGot := nodeIDs(slices.Collect(inB.Fetch(c.req)))
			if !slices.Equal(serialGot, want) {
				t.Fatalf("unacquired-group fetch = %v, want %v (serial route broken)", serialGot, want)
			}

			release, ok := pool.AcquireForPipeline("q1", time.Second)
			if !ok {
				t.Fatal("AcquireForPipeline did not grant")
			}
			got := nodeIDs(slices.Collect(inB.Fetch(c.req)))
			release()

			if len(got) != len(want) {
				t.Fatalf("pool path len=%d ids=%v, single-conn len=%d ids=%v",
					len(got), got, len(want), want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("row %d: pool id=%d, single-conn id=%d (full pool=%v want=%v)",
						i, got[i], want[i], got, want)
				}
			}

			// Unbind reverts to the locked path and must still work.
			srcB.UnbindReaderPool()
			again := nodeIDs(slices.Collect(inB.Fetch(c.req)))
			if len(again) != len(want) {
				t.Fatalf("post-unbind len=%d, want %d", len(again), len(want))
			}
		})
	}
}

// TestSourceFetch_PoolConcurrent: many pipelines fetching the SAME source
// through the pool concurrently all get the full correct result. 32
// pipelines against K=8 exercises admission queueing (each waits holding
// nothing until a reader frees). Run under -race to prove the lock-free read
// path has no data races on Source state.
func TestSourceFetch_PoolConcurrent(t *testing.T) {
	path := seedReplicaWithStateVersion(t, "v7")
	src := newUserSourceAt(t, path)
	poolDB, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer poolDB.Close()
	pool, err := NewReaderPool(context.Background(), poolDB, "v7", 8)
	if err != nil {
		t.Fatalf("NewReaderPool: %v", err)
	}
	defer pool.Close()
	src.BindReaderPool(pool)

	// Distinct connections (distinct pipelines), like distinct queries in one
	// hydrate batch, all reading this source concurrently — each holding its
	// OWN exclusive reader for its whole drain.
	const goroutines = 32
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for g := 0; g < goroutines; g++ {
		group := fmt.Sprintf("q%d", g)
		src.SetNextConnectGroup(group)
		in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
		wg.Add(1)
		go func(idx int, group string, input ivm.Input) {
			defer wg.Done()
			var release func()
			for {
				var ok bool
				release, ok = pool.AcquireForPipeline(group, time.Second)
				if ok {
					break
				}
			}
			defer release()
			ids := nodeIDs(slices.Collect(input.Fetch(ivm.FetchRequest{})))
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			want := []int64{1, 2, 3, 4, 5}
			ok := len(ids) == len(want)
			for i := 0; ok && i < len(want); i++ {
				ok = ids[i] == want[i]
			}
			if !ok {
				errs[idx] = fmt.Errorf("goroutine %d got %v, want %v", idx, ids, want)
			}
		}(g, group, in)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
}
