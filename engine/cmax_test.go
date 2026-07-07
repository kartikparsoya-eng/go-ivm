package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestConservativeHydrateCmax_SimpleQuery verifies that a single-table query
// with no joins or EXISTS subqueries yields Cmax=1.
func TestConservativeHydrateCmax_SimpleQuery(t *testing.T) {
	asts := []builder.AST{
		{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}},
	}
	cmax := ConservativeHydrateCmax(asts)
	if cmax != 1 {
		t.Errorf("simple query: got Cmax=%d, want 1", cmax)
	}
}

// TestConservativeHydrateCmax_OneRelated verifies that a query with one
// related subquery (a 2-table join) yields Cmax=2.
func TestConservativeHydrateCmax_OneRelated(t *testing.T) {
	asts := []builder.AST{
		{
			Table: "issue",
			Related: []builder.CorrelatedSubquery{
				{
					Correlation: builder.Correlation{
						ParentField: []string{"id"},
						ChildField:  []string{"issueId"},
					},
					Subquery: builder.AST{
						Table:   "comment",
						OrderBy: ivm.Ordering{{"id", "asc"}},
					},
				},
			},
		},
	}
	cmax := ConservativeHydrateCmax(asts)
	if cmax != 2 {
		t.Errorf("one related: got Cmax=%d, want 2", cmax)
	}
}

// TestConservativeHydrateCmax_TwoDeepRelated verifies that a 3-deep join
// (issue → comment → reaction) yields Cmax=3.
func TestConservativeHydrateCmax_TwoDeepRelated(t *testing.T) {
	asts := []builder.AST{
		{
			Table: "issue",
			Related: []builder.CorrelatedSubquery{
				{
					Correlation: builder.Correlation{
						ParentField: []string{"id"},
						ChildField:  []string{"issueId"},
					},
					Subquery: builder.AST{
						Table: "comment",
						Related: []builder.CorrelatedSubquery{
							{
								Correlation: builder.Correlation{
									ParentField: []string{"id"},
									ChildField:  []string{"commentId"},
								},
								Subquery: builder.AST{
									Table:   "reaction",
									OrderBy: ivm.Ordering{{"id", "asc"}},
								},
							},
						},
					},
				},
			},
		},
	}
	cmax := ConservativeHydrateCmax(asts)
	if cmax != 3 {
		t.Errorf("two-deep related: got Cmax=%d, want 3", cmax)
	}
}

// TestConservativeHydrateCmax_ExistsSubquery verifies that an EXISTS
// correlated subquery in the WHERE clause contributes its sources.
func TestConservativeHydrateCmax_ExistsSubquery(t *testing.T) {
	asts := []builder.AST{
		{
			Table: "issue",
			Where: &builder.Condition{
				Type: "correlatedSubquery",
				Op:   "EXISTS",
				Related: &builder.CorrelatedSubquery{
					Correlation: builder.Correlation{
						ParentField: []string{"id"},
						ChildField:  []string{"issueId"},
					},
					Subquery: builder.AST{
						Table:   "comment",
						OrderBy: ivm.Ordering{{"id", "asc"}},
					},
				},
			},
		},
	}
	cmax := ConservativeHydrateCmax(asts)
	if cmax != 2 {
		t.Errorf("EXISTS subquery: got Cmax=%d, want 2", cmax)
	}
}

// TestConservativeHydrateCmax_MixedRelatedAndExists verifies that a query
// with BOTH a related subquery AND an EXISTS subquery counts both.
func TestConservativeHydrateCmax_MixedRelatedAndExists(t *testing.T) {
	asts := []builder.AST{
		{
			Table: "issue",
			Related: []builder.CorrelatedSubquery{
				{
					Correlation: builder.Correlation{
						ParentField: []string{"id"},
						ChildField:  []string{"issueId"},
					},
					Subquery: builder.AST{
						Table:   "comment",
						OrderBy: ivm.Ordering{{"id", "asc"}},
					},
				},
			},
			Where: &builder.Condition{
				Type: "correlatedSubquery",
				Op:   "EXISTS",
				Related: &builder.CorrelatedSubquery{
					Correlation: builder.Correlation{
						ParentField: []string{"id"},
						ChildField:  []string{"issueId"},
					},
					Subquery: builder.AST{
						Table:   "label",
						OrderBy: ivm.Ordering{{"id", "asc"}},
					},
				},
			},
		},
	}
	cmax := ConservativeHydrateCmax(asts)
	if cmax != 3 {
		t.Errorf("mixed related+EXISTS: got Cmax=%d, want 3 (issue + comment + label)", cmax)
	}
}

// TestConservativeHydrateCmax_BatchMax verifies that a batch of queries
// returns the MAX cmax across all queries.
func TestConservativeHydrateCmax_BatchMax(t *testing.T) {
	asts := []builder.AST{
		{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}}, // Cmax=1
		{
			Table: "issue",
			Related: []builder.CorrelatedSubquery{
				{
					Correlation: builder.Correlation{
						ParentField: []string{"id"},
						ChildField:  []string{"issueId"},
					},
					Subquery: builder.AST{
						Table:   "comment",
						OrderBy: ivm.Ordering{{"id", "asc"}},
					},
				},
			},
		}, // Cmax=2
	}
	cmax := ConservativeHydrateCmax(asts)
	if cmax != 2 {
		t.Errorf("batch max: got Cmax=%d, want 2", cmax)
	}
}

// TestConservativeHydrateCmaxForSpecs verifies the QuerySpec-based variant.
func TestConservativeHydrateCmaxForSpecs(t *testing.T) {
	specs := []QuerySpec{
		{QueryID: "q1", AST: builder.AST{Table: "issue"}},
		{
			QueryID: "q2",
			AST: builder.AST{
				Table: "issue",
				Related: []builder.CorrelatedSubquery{
					{
						Correlation: builder.Correlation{
							ParentField: []string{"id"},
							ChildField:  []string{"issueId"},
						},
						Subquery: builder.AST{
							Table:   "comment",
							OrderBy: ivm.Ordering{{"id", "asc"}},
						},
					},
				},
			},
		},
	}
	cmax := ConservativeHydrateCmaxForSpecs(specs)
	if cmax != 2 {
		t.Errorf("specs batch: got Cmax=%d, want 2", cmax)
	}
}

// TestConservativeHydrateCmax_NestedAndOrCondition verifies that EXISTS
// subqueries nested inside AND/OR condition trees are found.
func TestConservativeHydrateCmax_NestedAndOrCondition(t *testing.T) {
	asts := []builder.AST{
		{
			Table: "issue",
			Where: &builder.Condition{
				Type: "and",
				Conditions: []builder.Condition{
					{
						Type: "correlatedSubquery",
						Op:   "EXISTS",
						Related: &builder.CorrelatedSubquery{
							Correlation: builder.Correlation{
								ParentField: []string{"id"},
								ChildField:  []string{"issueId"},
							},
							Subquery: builder.AST{
								Table:   "comment",
								OrderBy: ivm.Ordering{{"id", "asc"}},
							},
						},
					},
					{
						Type: "or",
						Conditions: []builder.Condition{
							{
								Type: "correlatedSubquery",
								Op:   "NOT EXISTS",
								Related: &builder.CorrelatedSubquery{
									Correlation: builder.Correlation{
										ParentField: []string{"id"},
										ChildField:  []string{"issueId"},
									},
									Subquery: builder.AST{
										Table:   "label",
										OrderBy: ivm.Ordering{{"id", "asc"}},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	cmax := ConservativeHydrateCmax(asts)
	if cmax != 3 {
		t.Errorf("nested AND/OR with EXISTS: got Cmax=%d, want 3 (issue + comment + label)", cmax)
	}
}

// Ensure builder.Pipeline is used (import guard)
var _ = builder.Pipeline{}
