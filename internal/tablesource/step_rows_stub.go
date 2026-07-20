//go:build !cgo

package tablesource

import "fmt"

func stepRowsShimAvailable() bool { return false }

func stepRowsShim(_ interface{}, _ string, _ []any, _ func(map[string]any) bool) error {
	return fmt.Errorf("stepRowsShim: not available (non-cgo build)")
}
