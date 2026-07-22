package sqlite

import "testing"

// L4: ToSQLiteType's boolean arm must apply JS truthiness to ANY non-null
// value (TS: `v === null ? null : v ? 1 : 0`), not only Go bools — kept
// symmetric with FromSQLiteType's boolean read arm. Latent today (boolean
// columns yield Go bool), but pins the alignment.
func TestToSQLiteType_BooleanTruthiness(t *testing.T) {
	cases := []struct {
		in   any
		want any
	}{
		{true, 1}, {false, 0},
		{nil, nil},
		{float64(0), 0}, {float64(1), 1}, {float64(-2), 1},
		{int64(0), 0}, {int64(5), 1},
		{"", 0}, {"x", 1}, {"0", 1}, // JS truthiness: non-empty string → true
		{[]byte{}, 0}, {[]byte{1}, 1},
	}
	for _, c := range cases {
		got := ToSQLiteType(c.in, "boolean")
		if got != c.want {
			t.Errorf("ToSQLiteType(%#v, boolean) = %#v, want %#v", c.in, got, c.want)
		}
	}
}
