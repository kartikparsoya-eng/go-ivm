package engine

import (
	"database/sql"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestLazySourceFactoryCreatesOnlyRequestedSources(t *testing.T) {
	var created []string
	eng, err := NewEngine(EngineConfig{
		StoragePath: tempStoragePath(t),
		SourceFactory: func(tableName string) (Source, error) {
			created = append(created, tableName)
			ms := ivm.NewMemorySource(tableName, map[string]string{"id": "string"}, []string{"id"})
			return &memorySourceAdapter{ms: ms}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	if got := len(eng.sourcesView()); got != 0 {
		t.Fatalf("sources before query = %d, want 0", got)
	}

	rows, _, err := eng.AddQuery("q1", builder.AST{Table: "users"})
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("hydrate rows = %d, want 0", len(rows))
	}
	if len(created) != 1 || created[0] != "users" {
		t.Fatalf("created sources = %v, want [users]", created)
	}
	if _, ok := eng.sourcesView()["users"]; !ok {
		t.Fatal("users source was not registered")
	}
	if _, ok := eng.sourcesView()["projects"]; ok {
		t.Fatal("unused projects source was registered")
	}
}

type bindingStubSource struct {
	table string
	conn  *sql.Conn
	pool  any
}

func (s *bindingStubSource) TableName() string       { return s.table }
func (s *bindingStubSource) PrimaryKey() []string    { return []string{"id"} }
func (s *bindingStubSource) NormalizeRow(ivm.Row)    {}
func (s *bindingStubSource) Push(ivm.SourceChange)   {}
func (s *bindingStubSource) Close() error            { return nil }
func (s *bindingStubSource) BindConn(conn *sql.Conn) { s.conn = conn }
func (s *bindingStubSource) UnbindConn()             { s.conn = nil }
func (s *bindingStubSource) BindReaderPool(pool any) { s.pool = pool }
func (s *bindingStubSource) UnbindReaderPool()       { s.pool = nil }
func (s *bindingStubSource) Connect(ivm.Ordering, *builder.Condition, func(ivm.Row) bool, map[string]bool) ivm.Input {
	return nil
}

type bindingStubPool struct{}

func (bindingStubPool) AcquireForPipeline(string, time.Duration) (func(), bool) {
	return func() {}, true
}
func (bindingStubPool) Releases() uint64 { return 0 }

func TestLazySourceFactoryInheritsActiveBindings(t *testing.T) {
	conn := new(sql.Conn)
	pool := bindingStubPool{}
	eng, err := NewEngine(EngineConfig{
		StoragePath: tempStoragePath(t),
		SourceFactory: func(tableName string) (Source, error) {
			return &bindingStubSource{table: tableName}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	eng.BindTableSourcesToConn(conn)
	eng.BindTableSourcesToReaderPool(pool)

	eng.mu.Lock()
	src, ok := eng.ensureSourceLocked("users")
	eng.mu.Unlock()
	if !ok {
		t.Fatal("lazy source was not created")
	}
	stub := src.(*bindingStubSource)
	if stub.conn != conn {
		t.Fatalf("lazy source conn binding = %p, want %p", stub.conn, conn)
	}
	if stub.pool != pool {
		t.Fatalf("lazy source pool binding = %#v, want %#v", stub.pool, pool)
	}
}

func TestAdvanceTimingIsPerSnapshotChange(t *testing.T) {
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	ms := ivm.NewMemorySource("users", map[string]string{"id": "string"}, []string{"id"})
	ms.BulkInsert([]ivm.Row{{"id": "old"}})
	eng.RegisterMemorySource(ms)
	if _, _, err := eng.AddQuery("q1", builder.AST{Table: "users"}); err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	result := eng.Advance([]SnapshotChange{{
		Table:      "users",
		PrevValues: []ivm.Row{{"id": "old"}},
		NextValue:  ivm.Row{"id": "new"},
	}})
	if len(result.Timings) != 1 {
		t.Fatalf("timings = %d, want 1 per snapshot diff entry: %#v", len(result.Timings), result.Timings)
	}
	if result.Timings[0].Table != "users" || result.Timings[0].ChangeT != int(ivm.ChangeTypeAdd) {
		t.Fatalf("timing = %#v, want users/add for final operation", result.Timings[0])
	}
}
