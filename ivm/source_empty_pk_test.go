package ivm

import "testing"

// V5: a MemorySource with an empty primary key is a programmer/config error —
// with no PK, every row's extracted key map is empty and all rows silently
// collide. Construction must fail fast rather than corrupt silently downstream.
func TestNewMemorySourceWithConverter_EmptyPKPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewMemorySourceWithConverter with empty PK did not panic")
		}
		// The panic message must name the table so the misconfiguration is
		// identifiable in the stack trace.
		s, ok := r.(string)
		if !ok || s == "" {
			t.Fatalf("panic value is not a non-empty string: %v", r)
		}
	}()
	_ = NewMemorySourceWithConverter("widgets", map[string]string{"id": "text"}, nil, nil)
}

// Sanity: a non-empty PK constructs normally (no panic).
func TestNewMemorySourceWithConverter_NonEmptyPKOK(t *testing.T) {
	src := NewMemorySourceWithConverter(
		"widgets", map[string]string{"id": "text"}, []string{"id"}, nil)
	if got := src.PrimaryKey(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("PrimaryKey = %v, want [id]", got)
	}
}
