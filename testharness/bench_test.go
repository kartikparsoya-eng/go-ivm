package testharness

// Measures: hydration + N mutations for various workload profiles.
// Run: go test -bench=. -benchmem ./testharness/

import (
	"sync"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// benchSource constructs a MemorySource using the same converter the
// production sidecar uses, so benchmark numbers reflect the real
// normalization cost (REVIEW-final MED-PORT-1).
func benchSource(name string, columns map[string]string, pk []string) *ivm.MemorySource {
	return ivm.NewMemorySourceWithConverter(name, columns, pk,
		func(v interface{}, colType string) ivm.Value {
			return sqlite.FromSQLiteType(v, colType)
		},
	)
}

// BenchmarkSimpleFilter: 1 table, 10 rows, simple filter, 20 mutations
func BenchmarkSimpleFilter(b *testing.B) {
	fuzzer := NewFuzzer(100)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    1,
		NumColumns:   4,
		NumRows:      10,
		NumMutations: 20,
		WithFilter:   true,
		WithLimit:    false,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := RunTestCase(tc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFilterWithLimit: 1 table, 10 rows, filter + LIMIT, 20 mutations
func BenchmarkFilterWithLimit(b *testing.B) {
	fuzzer := NewFuzzer(101)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    1,
		NumColumns:   4,
		NumRows:      10,
		NumMutations: 20,
		WithFilter:   true,
		WithLimit:    true,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := RunTestCase(tc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompound: 1 table, 8 rows, AND/OR + limit, 15 mutations
func BenchmarkCompound(b *testing.B) {
	fuzzer := NewFuzzer(105)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    1,
		NumColumns:   5,
		NumRows:      8,
		NumMutations: 15,
		WithFilter:   true,
		WithLimit:    true,
		WithCompound: true,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := RunTestCase(tc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkJoins: 2 tables, 5 rows each, Related join, 10 mutations
func BenchmarkJoins(b *testing.B) {
	fuzzer := NewFuzzer(200)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    2,
		NumColumns:   3,
		NumRows:      5,
		NumMutations: 10,
		WithFilter:   true,
		WithLimit:    true,
		WithJoins:    true,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := RunTestCase(tc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExists: 2 tables, 5 rows each, EXISTS subquery, 10 mutations
func BenchmarkExists(b *testing.B) {
	fuzzer := NewFuzzer(300)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    2,
		NumColumns:   3,
		NumRows:      5,
		NumMutations: 10,
		WithFilter:   true,
		WithLimit:    true,
		WithExists:   true,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := RunTestCase(tc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkKitchenSink: 3 tables, 5 rows each, joins+exists+filter+limit, 15 mutations
func BenchmarkKitchenSink(b *testing.B) {
	fuzzer := NewFuzzer(1000)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    3,
		NumColumns:   4,
		NumRows:      5,
		NumMutations: 15,
		WithFilter:   true,
		WithLimit:    true,
		WithJoins:    true,
		WithExists:   true,
		WithCompound: true,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := RunTestCase(tc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHighFanOut: 1 table, 10 rows, 20 pipelines receiving same changes, 10 mutations
func BenchmarkHighFanOut(b *testing.B) {
	fuzzer := NewFuzzer(102)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    1,
		NumColumns:   4,
		NumRows:      10,
		NumMutations: 10,
		WithFilter:   true,
		WithLimit:    true,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sources := make(map[string]*ivm.MemorySource)
		for tableName, schema := range tc.Schema {
			columns := make(map[string]string)
			for colName, colDef := range schema.Columns {
				columns[colName] = colDef.Type
			}
			sources[tableName] = benchSource(tableName, columns, schema.PrimaryKey)
		}

		// Populate
		for tableName, rows := range tc.InitialData {
			source := sources[tableName]
			for _, row := range rows {
				source.Push(ivm.MakeSourceChangeAdd(row))
			}
		}

		// Build 20 pipelines from the same AST
		delegate := &memoryDelegate{sources: sources}
		for p := 0; p < 20; p++ {
			pipeline := builder.BuildPipeline(tc.AST, delegate)
			collector := &changeCollector{}
			pipeline.Input.SetOutput(collector)
			pipeline.Input.Fetch(ivm.FetchRequest{})
		}

		// Push mutations through all 20 pipelines
		for _, mutation := range tc.Mutations {
			source := sources[mutation.Table]
			switch mutation.Type {
			case "add":
				source.Push(ivm.MakeSourceChangeAdd(mutation.Row))
			case "remove":
				source.Push(ivm.MakeSourceChangeRemove(mutation.Row))
			case "edit":
				source.Push(ivm.MakeSourceChangeEdit(mutation.Row, mutation.OldRow))
			}
		}
	}
}

// =============================================================================
// Multi-Engine Parallel Benchmarks
// Simulates N client groups each running IVM (hydrate + advance) concurrently.
// This measures the REAL parallelism win: independent engines on separate cores.
// =============================================================================

// BenchmarkMultiEngine_Sequential: N engines run one after another (baseline)
func BenchmarkMultiEngine_1_Sequential(b *testing.B)  { benchMultiEngine(b, 1, false) }
func BenchmarkMultiEngine_2_Sequential(b *testing.B)  { benchMultiEngine(b, 2, false) }
func BenchmarkMultiEngine_4_Sequential(b *testing.B)  { benchMultiEngine(b, 4, false) }
func BenchmarkMultiEngine_8_Sequential(b *testing.B)  { benchMultiEngine(b, 8, false) }
func BenchmarkMultiEngine_16_Sequential(b *testing.B) { benchMultiEngine(b, 16, false) }

// BenchmarkMultiEngine_Parallel: N engines run concurrently (the win)
func BenchmarkMultiEngine_1_Parallel(b *testing.B)  { benchMultiEngine(b, 1, true) }
func BenchmarkMultiEngine_2_Parallel(b *testing.B)  { benchMultiEngine(b, 2, true) }
func BenchmarkMultiEngine_4_Parallel(b *testing.B)  { benchMultiEngine(b, 4, true) }
func BenchmarkMultiEngine_8_Parallel(b *testing.B)  { benchMultiEngine(b, 8, true) }
func BenchmarkMultiEngine_16_Parallel(b *testing.B) { benchMultiEngine(b, 16, true) }

func benchMultiEngine(b *testing.B, numEngines int, parallel bool) {
	// Generate N different test cases (different seeds = different data/queries)
	testCases := make([]TestCase, numEngines)
	for i := 0; i < numEngines; i++ {
		fuzzer := NewFuzzer(int64(5000 + i))
		testCases[i] = fuzzer.generateTestCaseStruct(FuzzOpts{
			NumTables:    2,
			NumColumns:   4,
			NumRows:      10,
			NumMutations: 20,
			WithFilter:   true,
			WithLimit:    true,
			WithJoins:    true,
		})
	}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		if parallel {
			// All engines run concurrently — this is the production scenario
			var wg sync.WaitGroup
			for i := 0; i < numEngines; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					RunTestCase(testCases[idx])
				}(i)
			}
			wg.Wait()
		} else {
			// Sequential baseline — like TS single-threaded event loop
			for i := 0; i < numEngines; i++ {
				RunTestCase(testCases[i])
			}
		}
	}
}

// BenchmarkHighFanOutJoins: 2 tables, joins+filter+limit, 20 pipelines, sequential
func BenchmarkHighFanOutJoins(b *testing.B) {
	benchFanOutJoins(b, false, 20)
}

// BenchmarkHighFanOutJoinsParallel: same with parallel
func BenchmarkHighFanOutJoinsParallel(b *testing.B) {
	benchFanOutJoins(b, true, 20)
}

// BenchmarkFanOut4Joins: 4 connections (threshold boundary)
func BenchmarkFanOut4Joins(b *testing.B) {
	benchFanOutJoins(b, false, 4)
}

func BenchmarkFanOut4JoinsParallel(b *testing.B) {
	benchFanOutJoins(b, true, 4)
}

// BenchmarkFanOut8Joins: 8 connections
func BenchmarkFanOut8Joins(b *testing.B) {
	benchFanOutJoins(b, false, 8)
}

func BenchmarkFanOut8JoinsParallel(b *testing.B) {
	benchFanOutJoins(b, true, 8)
}

// BenchmarkFanOut50Joins: 50 connections (production-like)
func BenchmarkFanOut50Joins(b *testing.B) {
	benchFanOutJoins(b, false, 50)
}

func BenchmarkFanOut50JoinsParallel(b *testing.B) {
	benchFanOutJoins(b, true, 50)
}

func benchFanOutJoins(b *testing.B, parallel bool, numPipelines int) {
	fuzzer := NewFuzzer(200)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    2,
		NumColumns:   4,
		NumRows:      10,
		NumMutations: 10,
		WithFilter:   true,
		WithLimit:    true,
		WithJoins:    true,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sources := make(map[string]*ivm.MemorySource)
		for tableName, schema := range tc.Schema {
			columns := make(map[string]string)
			for colName, colDef := range schema.Columns {
				columns[colName] = colDef.Type
			}
			src := benchSource(tableName, columns, schema.PrimaryKey)
			if parallel {
				src.SetParallel(true, 2)
			}
			sources[tableName] = src
		}

		for tableName, rows := range tc.InitialData {
			source := sources[tableName]
			for _, row := range rows {
				source.Push(ivm.MakeSourceChangeAdd(row))
			}
		}

		delegate := &memoryDelegate{sources: sources}
		for p := 0; p < numPipelines; p++ {
			pipeline := builder.BuildPipeline(tc.AST, delegate)
			collector := &changeCollector{}
			pipeline.Input.SetOutput(collector)
			pipeline.Input.Fetch(ivm.FetchRequest{})
		}

		for _, mutation := range tc.Mutations {
			source := sources[mutation.Table]
			switch mutation.Type {
			case "add":
				source.Push(ivm.MakeSourceChangeAdd(mutation.Row))
			case "remove":
				source.Push(ivm.MakeSourceChangeRemove(mutation.Row))
			case "edit":
				source.Push(ivm.MakeSourceChangeEdit(mutation.Row, mutation.OldRow))
			}
		}
	}
}

// BenchmarkHighFanOutParallel: same as HighFanOut but with parallel enabled
func BenchmarkHighFanOutParallel(b *testing.B) {
	fuzzer := NewFuzzer(102)
	tc := fuzzer.generateTestCaseStruct(FuzzOpts{
		NumTables:    1,
		NumColumns:   4,
		NumRows:      10,
		NumMutations: 10,
		WithFilter:   true,
		WithLimit:    true,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sources := make(map[string]*ivm.MemorySource)
		for tableName, schema := range tc.Schema {
			columns := make(map[string]string)
			for colName, colDef := range schema.Columns {
				columns[colName] = colDef.Type
			}
			src := benchSource(tableName, columns, schema.PrimaryKey)
			src.SetParallel(true)
			sources[tableName] = src
		}

		// Populate
		for tableName, rows := range tc.InitialData {
			source := sources[tableName]
			for _, row := range rows {
				source.Push(ivm.MakeSourceChangeAdd(row))
			}
		}

		// Build 20 pipelines from the same AST
		delegate := &memoryDelegate{sources: sources}
		for p := 0; p < 20; p++ {
			pipeline := builder.BuildPipeline(tc.AST, delegate)
			collector := &changeCollector{}
			pipeline.Input.SetOutput(collector)
			pipeline.Input.Fetch(ivm.FetchRequest{})
		}

		// Push mutations through all 20 pipelines (in parallel)
		for _, mutation := range tc.Mutations {
			source := sources[mutation.Table]
			switch mutation.Type {
			case "add":
				source.Push(ivm.MakeSourceChangeAdd(mutation.Row))
			case "remove":
				source.Push(ivm.MakeSourceChangeRemove(mutation.Row))
			case "edit":
				source.Push(ivm.MakeSourceChangeEdit(mutation.Row, mutation.OldRow))
			}
		}
	}
}
