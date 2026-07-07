package tablesource

// Regression tests for the invariant asserts ported from TS table-source.ts
// (faithfulness item #2):
//   - Connect PK-in-sort assert   (table-source.ts:266-268)
//   - destroy/disconnect assert   (table-source.ts:242-246)
//
// Pre-fix, neither panicked: a PK-less sort silently produced a non-total
// comparator, and a double-disconnect silently no-oped.

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// mustPanicTS runs fn and asserts it panics with a message containing want.
func mustPanicTS(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, got none", want)
		}
		msg := ""
		switch v := r.(type) {
		case string:
			msg = v
		case error:
			msg = v.Error()
		default:
			t.Fatalf("unexpected panic type %T: %v", r, r)
		}
		if !strings.Contains(msg, want) {
			t.Fatalf("panic %q does not contain %q", msg, want)
		}
	}()
	fn()
}

func TestTableSourceConnectPanicsWhenSortMissingPK(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	mustPanicTS(t, "Ordering must include all primary key fields. Missing: id.", func() {
		src.Connect(ivm.Ordering{{"score", "desc"}}, nil, nil, nil)
	})
	// nil sort falls back to the PK comparator — never asserts
	// (table-source.ts "unordered" branch).
	in := src.Connect(nil, nil, nil, nil)
	in.Destroy()
}

func TestTableSourceDisconnectTwicePanics(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	in := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil, nil)
	in.Destroy()
	mustPanicTS(t, "Connection not found", func() {
		in.Destroy()
	})
}
