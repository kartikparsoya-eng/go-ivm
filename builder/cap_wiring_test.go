package builder

// Cap wiring tests — port of the TS 'Cap wiring' describe block
// (mono/packages/zql/src/ivm/cap.push.test.ts:1210-1563) plus the
// unordered-connect half of buildPipelineInternal (builder.ts:293-383):
//
//   - non-flipped EXISTS children connect UNORDERED and end in Cap
//   - flipped EXISTS children stay ordered (FlippedJoin path, no Cap)
//   - a flipped subquery anywhere in the child's WHERE falls back to
//     ordered + Take (UnionFanIn requires sorted input)
//   - EXISTS children must not carry start/related (asserts)
//   - permissions-system EXISTS uses cap limit 1
//   - related (non-EXISTS) children keep Take

import (
	"slices"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// capRecordSource wraps ONE shared MemorySource per table (so joins and
// hydrates read real data) and records the Sort of every Connect.
type capRecordSource struct {
	ms    *ivm.MemorySource
	sorts *[]ivm.Ordering
}

func (s *capRecordSource) Connect(opts ConnectOptions) ivm.Input {
	*s.sorts = append(*s.sorts, opts.Sort)
	return s.ms.Connect(opts.Sort, opts.FilterPredicate, opts.SplitEditKeys)
}

func (s *capRecordSource) PrimaryKey() []string     { return s.ms.PrimaryKey() }
func (s *capRecordSource) NormalizeRow(row ivm.Row) { s.ms.NormalizeRow(row) }

// capRecordDelegate records every storage the builder creates, by name.
type capRecordDelegate struct {
	sources     map[string]*capRecordSource
	takeNames   []string
	capNames    []string
	capStorages map[string]*ivm.MemoryCapStorage
}

func newCapRecordDelegate(tables map[string]struct {
	cols map[string]string
	pk   []string
	rows []ivm.Row
}) *capRecordDelegate {
	d := &capRecordDelegate{
		sources:     map[string]*capRecordSource{},
		capStorages: map[string]*ivm.MemoryCapStorage{},
	}
	for name, spec := range tables {
		ms := ivm.NewMemorySource(name, spec.cols, spec.pk)
		ms.BulkInsert(spec.rows)
		d.sources[name] = &capRecordSource{ms: ms, sorts: new([]ivm.Ordering)}
	}
	return d
}

func (d *capRecordDelegate) GetSource(tableName string) Source {
	if s, ok := d.sources[tableName]; ok {
		return s
	}
	return nil
}

func (d *capRecordDelegate) CreateStorage(name string) ivm.TakeStorage {
	d.takeNames = append(d.takeNames, name)
	return ivm.NewMemoryTakeStorage()
}

func (d *capRecordDelegate) CreateCapStorage(name string) ivm.CapStorage {
	d.capNames = append(d.capNames, name)
	s := ivm.NewMemoryCapStorage()
	d.capStorages[name] = s
	return s
}

func (d *capRecordDelegate) sortsOf(table string) []ivm.Ordering {
	return *d.sources[table].sorts
}

// existsCond builds a non-flipped EXISTS correlatedSubquery condition.
func existsCond(childTable, alias string, parentField, childField []string, system string, sub *AST) Condition {
	subAST := AST{Table: childTable, Alias: alias}
	if sub != nil {
		subAST = *sub
		subAST.Table = childTable
		subAST.Alias = alias
	}
	return Condition{
		Type: "correlatedSubquery",
		Op:   "EXISTS",
		Related: &CorrelatedSubquery{
			Correlation: Correlation{ParentField: parentField, ChildField: childField},
			Subquery:    subAST,
			System:      system,
		},
	}
}

func issueCommentTables() map[string]struct {
	cols map[string]string
	pk   []string
	rows []ivm.Row
} {
	return map[string]struct {
		cols map[string]string
		pk   []string
		rows []ivm.Row
	}{
		"issue": {
			cols: map[string]string{"id": "string", "text": "string"},
			pk:   []string{"id"},
			rows: []ivm.Row{{"id": "i1", "text": "i1"}},
		},
		"comment": {
			cols: map[string]string{"id": "string", "issueID": "string", "text": "string", "authorID": "string"},
			pk:   []string{"id"},
			rows: []ivm.Row{
				{"id": "c1", "issueID": "i1", "text": "public", "authorID": "a1"},
				{"id": "c2", "issueID": "i1", "text": "private", "authorID": "a1"},
				{"id": "c3", "issueID": "i1", "text": "public", "authorID": "a1"},
			},
		},
		"author": {
			cols: map[string]string{"id": "string", "name": "string"},
			pk:   []string{"id"},
			rows: []ivm.Row{{"id": "a1", "name": "Alice"}},
		},
		"revision": {
			cols: map[string]string{"id": "string", "commentID": "string"},
			pk:   []string{"id"},
			rows: []ivm.Row{{"id": "r1", "commentID": "c1"}},
		},
	}
}

func hydrate(t *testing.T, p *Pipeline) []ivm.Node {
	t.Helper()
	return slices.Collect(p.Input.Fetch(ivm.FetchRequest{}))
}

// Client EXISTS auto-flips to FlippedJoin (no Cap, ordered connect).
// The pipeline still hydrates the right parents.
func TestBuilder_ExistsChildUsesCapAndUnorderedConnect(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	where := existsCond("comment", "comments", []string{"id"}, []string{"issueID"}, "", nil)
	ast := AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}, Where: &where}

	p := BuildPipeline(ast, d)
	nodes := hydrate(t, p)

	if len(nodes) != 1 || nodes[0].Row["id"] != "i1" {
		t.Fatalf("hydrate = %v, want [i1]", nodes)
	}
	// Auto-flipped: no Cap storage (FlippedJoin path).
	if len(d.capNames) != 0 {
		t.Fatalf("capNames = %v, want none (auto-flipped to FlippedJoin)", d.capNames)
	}
	// The child connects ORDERED (FlippedJoin depends on ordering).
	commentSorts := d.sortsOf("comment")
	if len(commentSorts) != 1 || commentSorts[0] == nil {
		t.Fatalf("comment connect sorts = %v, want one non-nil (ordered for FlippedJoin)", commentSorts)
	}
	// The parent connect stays ordered.
	issueSorts := d.sortsOf("issue")
	if len(issueSorts) != 1 || issueSorts[0] == nil {
		t.Fatalf("issue connect sorts = %v, want one non-nil", issueSorts)
	}
}

// TS: 'permissions-system EXISTS uses cap limit=1'
func TestBuilder_PermissionsExistsCapLimitOne(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	where := existsCond("comment", "comments", []string{"id"}, []string{"issueID"}, "permissions", nil)
	ast := AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}, Where: &where}

	p := BuildPipeline(ast, d)
	hydrate(t, p)

	st := d.capStorages["comments:cap"].States()[`["cap","i1"]`]
	if st.Size != 1 {
		t.Fatalf("cap state = %+v, want size 1 (PERMISSIONS_EXISTS_LIMIT)", st)
	}
}

// TS: 'flipped EXISTS subquery is NOT wired through Cap'
func TestBuilder_FlippedExistsChildNotCap(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	where := existsCond("comment", "comments", []string{"id"}, []string{"issueID"}, "", nil)
	where.Flip = true
	ast := AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}, Where: &where}

	BuildPipeline(ast, d)

	if len(d.capNames) != 0 {
		t.Fatalf("flipped EXISTS created cap storage: %v", d.capNames)
	}
	// The flipped child connects ORDERED (FlippedJoin depends on ordering).
	commentSorts := d.sortsOf("comment")
	if len(commentSorts) != 1 || commentSorts[0] == nil {
		t.Fatalf("flipped child connect sorts = %v, want one non-nil", commentSorts)
	}
}

// TS: 'non-flipped EXISTS child with flipped OR branch falls back to Take'.
// With auto-flip, the outer client EXISTS is also flipped, so both go
// through the FlippedJoin path (no Cap, no Take fallback needed).
func TestBuilder_ExistsChildWithFlippedOrBranchFallsBackToTake(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	flippedAuthor := existsCond("author", "author", []string{"authorID"}, []string{"id"}, "", nil)
	flippedAuthor.Flip = true
	childWhere := Condition{
		Type: "or",
		Conditions: []Condition{
			{Type: "simple", Op: "=",
				Left:  &ValuePos{Type: "column", Name: "text"},
				Right: &ValuePos{Type: "literal", Value: "public"}},
			flippedAuthor,
		},
	}
	where := existsCond("comment", "comments", []string{"id"}, []string{"issueID"}, "",
		&AST{Where: &childWhere})
	ast := AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}, Where: &where}

	p := BuildPipeline(ast, d)
	nodes := hydrate(t, p)
	if len(nodes) != 1 {
		t.Fatalf("hydrate = %v, want 1 parent", nodes)
	}

	// Auto-flipped: no cap storage.
	if len(d.capNames) != 0 {
		t.Fatalf("capNames = %v, want none (auto-flipped)", d.capNames)
	}
	// The comment child connect is ORDERED (FlippedJoin depends on ordering).
	commentSorts := d.sortsOf("comment")
	if len(commentSorts) != 1 || commentSorts[0] == nil {
		t.Fatalf("comment connect sorts = %v, want one non-nil (ordered for FlippedJoin)", commentSorts)
	}
}

// With auto-flip, both outer and inner client EXISTS auto-flip to
// FlippedJoin — no Cap storage at any level.
func TestBuilder_ExistsChildWithNonFlippedOrExistsKeepsCap(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	innerExists := existsCond("author", "author", []string{"authorID"}, []string{"id"}, "", nil)
	childWhere := Condition{
		Type: "or",
		Conditions: []Condition{
			{Type: "simple", Op: "=",
				Left:  &ValuePos{Type: "column", Name: "text"},
				Right: &ValuePos{Type: "literal", Value: "public"}},
			innerExists,
		},
	}
	where := existsCond("comment", "comments", []string{"id"}, []string{"issueID"}, "",
		&AST{Where: &childWhere})
	ast := AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}, Where: &where}

	BuildPipeline(ast, d)

	// Auto-flipped: no Cap storage.
	if len(d.capNames) != 0 {
		t.Fatalf("capNames = %v, want none (auto-flipped)", d.capNames)
	}
}

// With auto-flip, both levels of nested client EXISTS auto-flip to
// FlippedJoin — no Cap storage at any level.
func TestBuilder_NestedExistsCapAtEveryLevel(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	innerWhere := existsCond("revision", "revisions", []string{"id"}, []string{"commentID"}, "", nil)
	where := existsCond("comment", "comments", []string{"id"}, []string{"issueID"}, "",
		&AST{Where: &innerWhere})
	ast := AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}, Where: &where}

	BuildPipeline(ast, d)

	// Auto-flipped: no Cap storage.
	if len(d.capNames) != 0 {
		t.Fatalf("capNames = %v, want none (auto-flipped)", d.capNames)
	}
}

// With auto-flip, client EXISTS children are flipped (isNonFlippedExistsChild=false),
// so start/related are valid (TS allows them for flipped EXISTS). No panic.
func TestBuilder_ExistsChildWithStartPanics(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	where := existsCond("comment", "comments", []string{"id"}, []string{"issueID"}, "",
		&AST{Start: &Bound{Row: ivm.Row{"id": "c0"}, Exclusive: false}})
	ast := AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}, Where: &where}

	// Should NOT panic — auto-flipped EXISTS allows start.
	BuildPipeline(ast, d)
}

// With auto-flip, client EXISTS children are flipped, so related is valid.
func TestBuilder_ExistsChildWithRelatedPanics(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	where := existsCond("comment", "comments", []string{"id"}, []string{"issueID"}, "",
		&AST{Related: []CorrelatedSubquery{{
			Correlation: Correlation{ParentField: []string{"authorID"}, ChildField: []string{"id"}},
			Subquery:    AST{Table: "author", Alias: "author"},
		}}})
	ast := AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}, Where: &where}

	// Should NOT panic — auto-flipped EXISTS allows related.
	BuildPipeline(ast, d)
}

// Related (non-EXISTS) children keep Take: fromCondition=false, so a limit
// on a related subquery still builds the ordered Take path.
func TestBuilder_RelatedChildKeepsTake(t *testing.T) {
	d := newCapRecordDelegate(issueCommentTables())
	limit := 2
	ast := AST{
		Table:   "issue",
		OrderBy: ivm.Ordering{{"id", "asc"}},
		Related: []CorrelatedSubquery{{
			Correlation: Correlation{ParentField: []string{"id"}, ChildField: []string{"issueID"}},
			Subquery:    AST{Table: "comment", Alias: "comments", Limit: &limit},
		}},
	}

	BuildPipeline(ast, d)

	if len(d.capNames) != 0 {
		t.Fatalf("related child created cap storage: %v", d.capNames)
	}
	if want := []string{"comments:take"}; !slices.Equal(d.takeNames, want) {
		t.Fatalf("takeNames = %v, want %v", d.takeNames, want)
	}
	commentSorts := d.sortsOf("comment")
	if len(commentSorts) != 1 || commentSorts[0] == nil {
		t.Fatalf("related child connect sorts = %v, want one non-nil (ordered)", commentSorts)
	}
}
