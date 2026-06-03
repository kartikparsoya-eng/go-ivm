package builder

import "testing"

// TestMatchLike covers the HIGH-8 cases the old byte-by-byte matcher got
// wrong: multi-byte UTF-8 under `_`, backslash escapes, and `%`/`_` vs
// embedded newlines — plus ILIKE case folding. Ports TS like.ts semantics.
func TestMatchLike(t *testing.T) {
	cases := []struct {
		name            string
		s, pattern      string
		caseInsensitive bool
		want            bool
	}{
		// Multi-byte UTF-8: `_` matches one CODE POINT, not one byte. The old
		// matcher matched a single byte of 'é' (0xC3) and left 0xA9 dangling.
		{"underscore matches multibyte rune", "café", "caf_", false, true},
		{"underscore one rune not two", "café", "ca_", false, false},

		// Backslash escape: `\_` and `\%` match literal _ / %.
		{"escaped underscore literal", "caf_", `caf\_`, false, true},
		{"escaped underscore not wildcard", "café", `caf\_`, false, false},
		{"escaped percent literal", "10%", `10\%`, false, true},
		{"escaped percent not wildcard", "100", `10\%`, false, false},

		// Newlines: `.` (from _/%) does not cross \n (Go RE2 default, like JS).
		{"underscore does not cross newline", "foo\nbar", "foo_bar", false, false},
		{"percent matches within a line", "bar\nfoo", "%foo%", false, true},

		// ILIKE: Unicode-aware case folding via (?i).
		{"ilike folds unicode", "CAFÉ", "café", true, true},
		{"like is case sensitive", "CAFE", "cafe", false, false},

		// Trailing escape is an invalid pattern (TS throws) → no match.
		{"trailing escape no match", "x", `x\`, false, false},

		// Plain wildcards still work.
		{"percent prefix", "hello world", "%world", false, true},
		{"percent both ends", "hello world", "%lo wo%", false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matchLike(c.s, c.pattern, c.caseInsensitive); got != c.want {
				t.Fatalf("matchLike(%q, %q, ci=%v) = %v, want %v",
					c.s, c.pattern, c.caseInsensitive, got, c.want)
			}
		})
	}
}
