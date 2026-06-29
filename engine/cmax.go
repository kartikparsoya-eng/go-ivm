package engine

// Cmax computation: the maximum concurrent-reader (cursor) demand across all
// queries in a hydrate batch. Used to size the reader pool K = P × Cmax so that
// P worker lanes can never deadlock waiting for a reader (§3d).
//
// In Phase 0/1 (all operators eager via compat shims), every operator
// materializes its upstream before opening the next cursor → only one cursor
// open at a time → Cmax = 1. The computation is gated by lazyHydrateEnabled
// and returns 1 until Phase 2 removes the shims.

import (
	"os"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// lazyHydrateEnabled gates the lazy (iter.Seq) hydrate path. When false
// (default, Phase 0/1), all operators use compat shims that materialize
// immediately → Cmax=1. When true (Phase 2+), operators stream lazily and
// Cmax is computed from the operator tree. GO_IVM_LAZY_HYDRATE=true enables.
var lazyHydrateEnabled = os.Getenv("GO_IVM_LAZY_HYDRATE") == "true"

// computeCmax returns the maximum concurrent-reader demand (Cmax) across all
// queries in the batch. Each query's demand C_q is the max number of
// simultaneously-open cursors its operator tree can hold during a lazy hydrate.
// The pool must be sized K = P × Cmax to guarantee deadlock-freedom (§3d).
//
// When lazyHydrateEnabled is false (Phase 0/1), all operators use compat shims
// that Collect immediately → only one cursor open at a time → Cmax=1.
func computeCmax(entries []*pipelineEntry) int {
	if !lazyHydrateEnabled {
		return 1
	}
	cmax := 1
	for _, entry := range entries {
		c := computeConcurrency(entry.pipeline)
		if c > cmax {
			cmax = c
		}
	}
	return cmax
}

// computeConcurrency walks a pipeline's operator tree (via Pipeline.Edges)
// and returns the max concurrent cursors C_q for that query.
//
// Operator contributions (§3b):
//   - Leaf (no inputs): 1 (one cursor)
//   - Join, FlippedJoin: C_parent + C_child (parent cursor held while child fetched)
//   - UnionFanIn: Σ C_branch (all branch cursors open for streaming k-way merge)
//   - Filter, Skip, Take, UnionFanOut, FilterStart, FilterEnd, Exists,
//     FanOut, FanIn: C_upstream (transparent — one cursor at a time)
func computeConcurrency(p *builder.Pipeline) int {
	inputsOf := make(map[ivm.InputBase][]ivm.InputBase)
	for _, edge := range p.Edges {
		from, to := edge[0], edge[1]
		inputsOf[to] = append(inputsOf[to], from)
	}
	return concurrencyOfNode(p.Input, inputsOf, make(map[ivm.InputBase]int))
}

// concurrencyOfNode recursively computes the concurrent-cursor demand for a
// node and its subtree. memo prevents redundant recomputation and guards
// against theoretical cycles.
func concurrencyOfNode(node ivm.InputBase, inputsOf map[ivm.InputBase][]ivm.InputBase, memo map[ivm.InputBase]int) int {
	if c, ok := memo[node]; ok {
		return c
	}
	children := inputsOf[node]
	if len(children) == 0 {
		memo[node] = 1
		return 1
	}
	memo[node] = 1 // cycle guard

	var c int
	switch node.(type) {
	case *ivm.Join, *ivm.FlippedJoin:
		c = 0
		for _, child := range children {
			c += concurrencyOfNode(child, inputsOf, memo)
		}
	case *ivm.UnionFanIn:
		c = 0
		for _, child := range children {
			c += concurrencyOfNode(child, inputsOf, memo)
		}
	default:
		c = 1
		for _, child := range children {
			if cc := concurrencyOfNode(child, inputsOf, memo); cc > c {
				c = cc
			}
		}
	}
	if c < 1 {
		c = 1
	}
	memo[node] = c
	return c
}
