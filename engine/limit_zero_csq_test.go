package engine

// Regression for the limit-0 short-circuit gap closed alongside the
// permissions-limit fix:
//   G2: csqConditions loop now skips Join construction when
//       subquery.limit == 0 (TS builder.ts:621 fromCondition path)
//   G3: applyCorrelatedSubqueryCondition emits a constant-false Filter
//       for EXISTS limit:0, constant-true for NOT EXISTS limit:0
//       (TS builder.ts:659-668)
//
// Without these, Go builds a useless Join + Exists check against an
// empty relationship. The Exists naturally fails so the end result
// happens to match, but the operator graph is heavier and a subtle
// push-time bug could diverge — TS shortcuts both paths.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestAddQuery_LimitZeroExists_NoParentEmits(t *testing.T) {
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	users.BulkInsert([]ivm.Row{
		{"id": "u1", "name": "Alice"},
		{"id": "u2", "name": "Bob"},
	})
	posts := ivm.NewMemorySource("posts",
		map[string]string{"id": "string", "userId": "string"},
		[]string{"id"})
	posts.BulkInsert([]ivm.Row{
		{"id": "p1", "userId": "u1"},
		{"id": "p2", "userId": "u2"},
	})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(posts)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetTableUniqueKeys("posts", [][]string{{"id"}})

	zero := 0
	ast := builder.AST{
		Table: "users",
		Where: &builder.Condition{
			Type: "correlatedSubquery",
			Op:   "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id"},
					ChildField:  []string{"userId"},
				},
				Subquery: builder.AST{
					Table: "posts",
					Alias: "zsubq_posts",
					Limit: &zero,
				},
			},
		},
	}

	changes, _, err := eng.AddQuery("q-exists-limit-0", ast)
	if err != nil {
		t.Fatal(err)
	}

	if len(changes) != 0 {
		t.Errorf("EXISTS with subquery.limit=0 must never match any parent; got %d changes:", len(changes))
		for _, c := range changes {
			t.Logf("  %s rowKey=%v", c.Table, c.RowKey)
		}
	}
}

func TestAddQuery_LimitZeroNotExists_AllParentsEmit(t *testing.T) {
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	users.BulkInsert([]ivm.Row{
		{"id": "u1", "name": "Alice"},
		{"id": "u2", "name": "Bob"},
		{"id": "u3", "name": "Carol"},
	})
	posts := ivm.NewMemorySource("posts",
		map[string]string{"id": "string", "userId": "string"},
		[]string{"id"})
	posts.BulkInsert([]ivm.Row{
		// Plenty of posts — but the limit:0 subquery must still produce
		// a constant-true predicate so every user passes NOT EXISTS.
		{"id": "p1", "userId": "u1"},
		{"id": "p2", "userId": "u2"},
		{"id": "p3", "userId": "u3"},
	})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(posts)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetTableUniqueKeys("posts", [][]string{{"id"}})

	zero := 0
	ast := builder.AST{
		Table: "users",
		Where: &builder.Condition{
			Type: "correlatedSubquery",
			Op:   "NOT EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id"},
					ChildField:  []string{"userId"},
				},
				Subquery: builder.AST{
					Table: "posts",
					Alias: "zsubq_posts",
					Limit: &zero,
				},
			},
		},
	}

	changes, _, err := eng.AddQuery("q-notexists-limit-0", ast)
	if err != nil {
		t.Fatal(err)
	}

	userCount := 0
	for _, c := range changes {
		if c.Table == "users" {
			userCount++
		}
	}
	if userCount != 3 {
		t.Errorf("NOT EXISTS with subquery.limit=0 must match every parent; got %d users emit", userCount)
	}
}
