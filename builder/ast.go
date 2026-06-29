package builder

// These types are deserialized from JSON sent by the TS zero-cache process
// over the sidecar socket. They must exactly match the TS AST shape.

import "github.com/kartikparsoya-eng/go-ivm/ivm"

// AST is the top-level query structure.
type AST struct {
	Schema  string               `json:"schema,omitempty"`
	Table   string               `json:"table"`
	Alias   string               `json:"alias,omitempty"`
	Where   *Condition           `json:"where,omitempty"`
	Related []CorrelatedSubquery `json:"related,omitempty"`
	Start   *Bound               `json:"start,omitempty"`
	Limit   *int                 `json:"limit,omitempty"`
	OrderBy ivm.Ordering         `json:"orderBy,omitempty"`
}

// Bound is a cursor position for pagination.
type Bound struct {
	Row       ivm.Row `json:"row"`
	Exclusive bool    `json:"exclusive"`
}

// Condition is a discriminated union (type field).
type Condition struct {
	Type string `json:"type"` // "simple", "and", "or", "correlatedSubquery"

	// For type == "simple"
	Op    string    `json:"op,omitempty"`
	Left  *ValuePos `json:"left,omitempty"`
	Right *ValuePos `json:"right,omitempty"`

	// For type == "and" or "or"
	Conditions []Condition `json:"conditions,omitempty"`

	// For type == "correlatedSubquery"
	Related *CorrelatedSubquery `json:"related,omitempty"`
	// Note: For correlatedSubquery, the Op field holds "EXISTS" | "NOT EXISTS"
	Flip   bool `json:"flip,omitempty"`
	Scalar bool `json:"scalar,omitempty"`
}

// ValuePos is a value position in a condition.
type ValuePos struct {
	Type string `json:"type"` // "literal", "column", "static"

	// For type == "literal"
	Value interface{} `json:"value,omitempty"`

	// For type == "column"
	Name string `json:"name,omitempty"`

	// For type == "static"
	Anchor string      `json:"anchor,omitempty"` // "authData" | "preMutationRow"
	Field  interface{} `json:"field,omitempty"`  // string | []string
}

// CorrelatedSubquery represents a join/relationship.
type CorrelatedSubquery struct {
	Correlation Correlation `json:"correlation"`
	Subquery    AST         `json:"subquery"`
	System      string      `json:"system,omitempty"` // "permissions" | "client" | "test"
	Hidden      bool        `json:"hidden,omitempty"`
}

// Correlation defines the join key mapping between parent and child.
type Correlation struct {
	ParentField []string `json:"parentField"`
	ChildField  []string `json:"childField"`
}
