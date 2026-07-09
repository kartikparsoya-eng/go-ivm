package tablesource

// sqliteRealText: byte-exact Go port of SQLite's REAL→TEXT coercion, i.e.
// what sqlite3_value_text() yields for a MEM_Real argument. Needed by the
// lower() override: TS's ICU lower() coerces non-TEXT args through
// sqlite3_value_text16, so our UDF must render numerics with the exact
// same text or ILIKE-over-numeric diverges from TS.
//
// TWO libraries, TWO algorithms (SQLite 3.53.0 changed the default from 15
// to 17 significant digits — "as was the case for all prior versions";
// 3.52.0 was withdrawn, its features shipped in 3.53.0):
//
//   17-digit (realTextDigits == 17): SQLite ≥ 3.53 = mattn's bundled
//   amalgamation (plain-tag builds; v1.14.44 bundles 3.53.0).
//   vdbeMemRenderNum → `"%!.*g"` with db->nFpDigit (default 17) →
//   sqlite3FpDecode(&s, r, 17, 20) on the Fp2Convert10 pipeline with the
//   iRound==17 round-trip shortening heuristics. Ported here as fpDecode17:
//   sqlite3Fp2Convert10, sqlite3Fp10Convert2, powerOfTen, plus the printf.c
//   etFLOAT/etEXP/etGENERIC assembly.
//
//   15-digit (realTextDigits == 15): SQLite ≤ 3.51 = the vendored wal2
//   fork (`-tags libsqlite3`, c/sqlite3/sqlite3.c, 3.51.0) — the SAME
//   fork base @rocicorp/zero-sqlite3 1.1.2 ships, i.e. WHAT PRODUCTION TS
//   RUNS. vdbeMemRenderNum → `"%!.15g"` → sqlite3FpDecode(&s, r, 15, 26)
//   on the Dekker double-double pipeline, NO shortening heuristics (they
//   are gated `iRound==17` upstream). Ported here as fpDecode15/dekkerMul2.
//
// The mode is probed from the linked library at driver registration
// (probeRealTextDigits in db.go) — behavior, not version numbers, so a
// fork backport or SQLITE_DBCONFIG_FP_DIGITS default change fails loudly
// instead of guessing wrong.
//
// The binary↔decimal conversions MUST be these exact routines, not
// strconv: SQLite's dtoa is deliberately approximate (fpDecode17's 18th
// digit is sometimes not correctly rounded — e.g. 1.307737638532754e-177
// decodes to …539 where the correctly rounded digits end …540; fpDecode15
// inherits Dekker scaling error in the 18th/19th digit), and the %!.17g
// shortening heuristics compare against Fp10Convert2's rounding, so
// substituting strconv changes output. Behavior is pinned by
// TestLowerCoercionParity's fuzz against `CAST(?1 AS TEXT)` on the linked
// library itself — run under BOTH tag sets — so an SQLite bump that
// changes rendering fails loudly in CI rather than drifting silently.

import (
	"math"
	"math/bits"
	"strconv"
)

// realTextDigits is the probed REAL→TEXT mode of the linked SQLite — 15
// (≤3.51: production wal2 fork) or 17 (≥3.53: mattn bundled). Written
// exactly once by registerGoivmDriver's sync.OnceValues before any pool
// connection (and therefore any lower() callback) can exist; read-only
// afterwards, so unsynchronized reads are race-free.
var realTextDigits int

func sqliteRealText(r float64) string {
	nDigits := realTextDigits
	if nDigits != 15 && nDigits != 17 {
		// Unreachable via the public API: Open/OpenWritable run the probe
		// before any connection exists. Loud failure beats silently
		// rendering with the wrong library's algorithm.
		panic("tablesource: sqliteRealText called before probeRealTextDigits")
	}

	// FpDecode specials (NaN unreachable via column values — SQLite stores
	// NaN as NULL — but bound params can carry it; keep the exact text).
	if math.IsNaN(r) {
		return "NaN"
	}
	if math.IsInf(r, 0) {
		if r < 0 {
			return "-Inf"
		}
		return "Inf"
	}

	neg := r < 0 // C `if( r<0.0 )`: -0.0 keeps sign '+' and renders "0.0".
	var digits []byte
	var iDP int
	switch {
	case r == 0:
		digits, iDP = []byte{'0'}, 1
	case nDigits == 15:
		digits, iDP = fpDecode15(math.Abs(r))
	default:
		digits, iDP = fpDecode17(math.Abs(r))
	}

	// ---- printf.c etGENERIC assembly; precision=nDigits, '!' only ----
	exp10 := iDP - 1
	precision := nDigits - 1 // etGENERIC does precision--
	useExp := exp10 < -4 || exp10 > precision
	var e2 int
	if useExp {
		e2 = 0
	} else {
		precision -= exp10
		e2 = iDP - 1
	}

	n := len(digits)
	b := make([]byte, 0, 32)
	if neg {
		b = append(b, '-')
	}
	// Digits prior to the decimal point.
	j := 0
	if e2 < 0 {
		b = append(b, '0')
	} else {
		j = e2 + 1
		if j > n {
			j = n
		}
		b = append(b, digits[:j]...)
		e2 -= j
		for ; e2 >= 0; e2-- { // memset('0', e2+1) then e2=-1
			b = append(b, '0')
		}
	}
	// The decimal point: flag_dp is unconditionally set under altform2 —
	// this is where SQLite's mandatory ".0" on integral REALs comes from.
	b = append(b, '.')
	// "0" digits after the point, before the first significant digit.
	if e2 < -1 && precision > 0 {
		nn := -1 - e2
		if nn > precision {
			nn = precision
		}
		for k := 0; k < nn; k++ {
			b = append(b, '0')
		}
		precision -= nn
	}
	// Significant digits after the decimal point. flag_rtz is set
	// (etGENERIC without '#'), so no zero padding follows.
	if precision > 0 {
		nn := n - j
		if nn > precision { // NEVER() in C; kept for safety
			nn = precision
		}
		if nn > 0 {
			b = append(b, digits[j:j+nn]...)
		}
	}
	// Remove trailing zeros; altform2 turns a bare trailing "." into ".0".
	for b[len(b)-1] == '0' {
		b = b[:len(b)-1]
	}
	if b[len(b)-1] == '.' {
		b = append(b, '0')
	}
	// The "e±NN" suffix (2 digits, 3 when |exp| >= 100).
	if useExp {
		x := exp10
		b = append(b, 'e')
		if x < 0 {
			b = append(b, '-')
			x = -x
		} else {
			b = append(b, '+')
		}
		if x >= 100 {
			b = append(b, byte(x/100)+'0')
			x %= 100
		}
		b = append(b, byte(x/10)+'0', byte(x%10)+'0')
	}
	return string(b)
}

// fpDecode17 mirrors sqlite3FpDecode(&s, a, iRound=17, mxRound=20) for
// finite a > 0: decode 18 (approximate — see fp2Convert10) significant
// digits, apply the two %!.17g shortening heuristics with their
// Fp10Convert2 round-trip verification, then re-round half-up at the final
// precision — including SQLite's double-rounding (1.0/3.0 renders
// "0.33333333333333332", not the correctly-rounded-at-17 "…31") — and
// strip trailing zeros.
// Returns the significant digits and iDP (value = 0.digits × 10^iDP).
func fpDecode17(a float64) (digits []byte, iDP int) {
	// IEEE754 decomposition: a = m × 2^e with the top bit of m set.
	v := math.Float64bits(a)
	e := int(v>>52) & 0x7ff
	v &= 0x000fffffffffffff
	if e == 0 { // subnormal
		nn := bits.LeadingZeros64(v)
		v <<= uint(nn)
		e = -1074 - nn
	} else {
		v = v<<11 | 1<<63
		e -= 1086
	}
	d10, exp := fp2Convert10(v, e, 18)

	d := strconv.AppendUint(make([]byte, 0, 20), d10, 10)
	n := len(d)
	iDP = n + exp

	// FpDecode's shorten+round block: `if( iRound>0 && (iRound<n ||
	// n>mxRound) )` with iRound=17, mxRound=20 — runs unless the decode
	// came back with ≤17 digits already.
	iRound := 17
	if iRound < n || n > 20 {
		// (iRound>mxRound never with 17 vs 20.) iRound==17 → the two
		// shortening heuristics; n>=18 here so d[13..15] are in range.
		if d[15] == '9' && d[14] == '9' {
			// Trailing-nines: e.g. 49.47 decodes to 4946999999999999…;
			// digits[0:jj]+1 = 4947 round-trips, so render at jj+1 digits
			// (the half-up round below reproduces the +1).
			jj := 14
			for jj > 0 && d[jj-1] == '9' {
				jj--
			}
			v2 := uint64(1)
			if jj > 0 {
				v2 = digitsVal(d[:jj]) + 1
			}
			if a == fp10Convert2(v2, exp+n-jj) {
				iRound = jj + 1
			}
		} else if iDP >= n || (d[15] == '0' && d[14] == '0' && d[13] == '0') {
			// Trailing-zeros: strip the zero tail if the short form
			// round-trips (e.g. 1e15 → "1").
			jj := 13
			for d[jj-1] == '0' { // d[0] != '0' always
				jj--
			}
			if a == fp10Convert2(digitsVal(d[:jj]), exp+n-jj) {
				iRound = jj + 1
			}
		}

		// Round half-up at iRound digits, carrying leftward; a full carry
		// prepends '1' and bumps the decimal point.
		n = iRound
		if d[n] >= '5' {
			j := n - 1
			for {
				d[j]++
				if d[j] <= '9' {
					break
				}
				d[j] = '0'
				if j == 0 {
					d = append([]byte{'1'}, d...)
					n++
					iDP++
					break
				}
				j--
			}
		}
	}
	for d[n-1] == '0' {
		n--
	}
	return d[:n], iDP
}

// fpDecode15 mirrors SQLite ≤3.51's sqlite3FpDecode(&s, a, iRound=15,
// mxRound=26) for finite a > 0 — the `%!.15g` CAST path of the production
// wal2 fork (3.51.0, c/sqlite3/sqlite3.c:37131). Unlike fpDecode17 there
// are NO round-trip shortening heuristics (upstream gates them on
// iRound==17, and 3.51 never passes 17): scale a into (9.2e17, 9.2e18]
// with Dekker double-double multiplies, take the integer's digits, round
// half-up at 15, strip trailing zeros. mxRound=26 (printf.c
// `flag_altform2 ? 26 : 16`) never binds: the scaled integer has 18-19
// digits, so iRound=15 < n and 15 < 26. Scaling-loop constants are copied
// verbatim — including the one-digit-shorter 9.22337203685477478e+17 exit
// bound (sic, upstream).
func fpDecode15(a float64) (digits []byte, iDP int) {
	rr0, rr1 := a, 0.0
	exp := 0
	if rr0 > 9.223372036854774784e+18 {
		for rr0 > 9.223372036854774784e+118 {
			exp += 100
			rr0, rr1 = dekkerMul2(rr0, rr1, 1.0e-100, -1.99918998026028836196e-117)
		}
		for rr0 > 9.223372036854774784e+28 {
			exp += 10
			rr0, rr1 = dekkerMul2(rr0, rr1, 1.0e-10, -3.6432197315497741579e-27)
		}
		for rr0 > 9.223372036854774784e+18 {
			exp++
			rr0, rr1 = dekkerMul2(rr0, rr1, 1.0e-01, -5.5511151231257827021e-18)
		}
	} else {
		for rr0 < 9.223372036854774784e-83 {
			exp -= 100
			rr0, rr1 = dekkerMul2(rr0, rr1, 1.0e+100, -1.5902891109759918046e+83)
		}
		for rr0 < 9.223372036854774784e+07 {
			exp -= 10
			rr0, rr1 = dekkerMul2(rr0, rr1, 1.0e+10, 0.0)
		}
		for rr0 < 9.22337203685477478e+17 {
			exp--
			rr0, rr1 = dekkerMul2(rr0, rr1, 1.0e+01, 0.0)
		}
	}
	// v = rr[1]<0 ? (u64)rr[0]-(u64)(-rr[1]) : (u64)rr[0]+(u64)rr[1]
	// (C double→u64 truncates toward zero; Go uint64(float64) matches for
	// these in-range values).
	var v uint64
	if rr1 < 0.0 {
		v = uint64(rr0) - uint64(-rr1)
	} else {
		v = uint64(rr0) + uint64(rr1)
	}

	d := strconv.AppendUint(make([]byte, 0, 24), v, 10)
	n := len(d)
	iDP = n + exp

	// `if( iRound>0 && (iRound<p->n || p->n>mxRound) )` — always fires
	// here (n is 18-19 > 15); kept in shape for auditability.
	const iRound = 15
	if iRound < n || n > 26 {
		n = iRound
		if d[n] >= '5' { // round half-up, carrying leftward
			j := n - 1
			for {
				d[j]++
				if d[j] <= '9' {
					break
				}
				d[j] = '0'
				if j == 0 {
					d = append([]byte{'1'}, d...)
					n++
					iDP++
					break
				}
				j--
			}
		}
	}
	for d[n-1] == '0' {
		n--
	}
	return d[:n], iDP
}

// dekkerMul2 mirrors SQLite ≤3.51's dekkerMul2 (double-double
// (x0,x1) *= (y,yy), Dekker 1971). The C original marks every
// intermediate `volatile`, forcing each store to round to binary64 and
// (with -ffp-contract=off) forbidding mul+add FMA fusion. Go's compiler
// fuses automatically on arm64/ppc64/s390x, and — measured empirically on
// darwin/arm64, bit-diffing against clang -ffp-contract=off — same-type
// float64(expr) conversions do NOT survive as rounding barriers into the
// SSA FMA rewrite. The only guaranteed materialization barrier is an
// integer bit round-trip (f64 below), so every C volatile store gets one.
// Cost is two register moves per barrier on a 3-13-call-per-render path.
func dekkerMul2(x0, x1, y, yy float64) (float64, float64) {
	hx := math.Float64frombits(math.Float64bits(x0) & 0xfffffffffc000000)
	tx := f64(x0 - hx)
	hy := math.Float64frombits(math.Float64bits(y) & 0xfffffffffc000000)
	ty := f64(y - hy)
	p := f64(hx * hy)
	q := f64(f64(hx*ty) + f64(tx*hy))
	c := f64(p + q)
	cc := f64(f64(f64(p-c)+q) + f64(tx*ty))
	cc = f64(f64(f64(x0*yy)+f64(x1*y)) + cc)
	r0 := f64(c + cc)
	r1 := f64(f64(c-r0) + cc)
	return r0, r1
}

// f64 is a floating-point materialization barrier: the value crosses into
// integer registers and back, which no floating-point rewrite (FMA
// contraction included) can cross. Equivalent to C's volatile double
// store+load in dekkerMul2. Deliberately NOT float64(v) — the compiler
// erases same-type conversions before the arm64 FMA fusion pass runs.
func f64(v float64) float64 { return math.Float64frombits(math.Float64bits(v)) }

// digitsVal parses a short ASCII digit run (≤14 digits here) as uint64.
func digitsVal(ds []byte) uint64 {
	var v uint64
	for _, c := range ds {
		v = v*10 + uint64(c-'0')
	}
	return v
}

// ---------------------------------------------------------------------------
// Bit-exact ports of SQLite's util.c binary↔decimal machinery (3.43+ dtoa).
// ---------------------------------------------------------------------------

// pwr10to2(p) = floor(log2(10^p)); pwr2to10(e) = floor(log10(2^e)).
// The ratio approximations and arithmetic right shifts match C exactly
// (Go's >> on signed ints is arithmetic, same as every C compiler SQLite
// supports).
func pwr10to2(p int) int { return (p * 108853) >> 15 }
func pwr2to10(p int) int { return (p * 78913) >> 18 }

// powerOfTen returns the most significant 64 bits of 10^p (top bit set),
// plus the next 32 bits, for p in [-348, +347]. Tables from tool/mkfptab.c
// --round, copied from the mattn v1.14.44 amalgamation.
func powerOfTen(p int) (hi uint64, lo uint32) {
	if p < 0 {
		if p == -1 {
			return pot10Scale[13], pot10ScaleLo[13]
		}
		g := p / 27 // C truncated division — Go matches.
		n := p % 27
		if n != 0 {
			g--
			n += 27
		}
		return potRefine(g, n)
	}
	if p < 27 {
		return pot10Base[p], 0
	}
	return potRefine(p/27, p%27)
}

func potRefine(g, n int) (uint64, uint32) {
	s := pot10Scale[g+13]
	if n == 0 {
		return s, pot10ScaleLo[g+13]
	}
	x, lo := multiply160(s, pot10ScaleLo[g+13], pot10Base[n])
	if x&(1<<63) == 0 {
		x = x<<1 | uint64(lo>>31&1)
		lo = lo<<1 | 1
	}
	return x, lo
}

var pot10Base = [27]uint64{
	0x8000000000000000, //  0: 1.0e+0 << 63
	0xa000000000000000, //  1: 1.0e+1 << 60
	0xc800000000000000, //  2: 1.0e+2 << 57
	0xfa00000000000000, //  3: 1.0e+3 << 54
	0x9c40000000000000, //  4: 1.0e+4 << 50
	0xc350000000000000, //  5: 1.0e+5 << 47
	0xf424000000000000, //  6: 1.0e+6 << 44
	0x9896800000000000, //  7: 1.0e+7 << 40
	0xbebc200000000000, //  8: 1.0e+8 << 37
	0xee6b280000000000, //  9: 1.0e+9 << 34
	0x9502f90000000000, // 10: 1.0e+10 << 30
	0xba43b74000000000, // 11: 1.0e+11 << 27
	0xe8d4a51000000000, // 12: 1.0e+12 << 24
	0x9184e72a00000000, // 13: 1.0e+13 << 20
	0xb5e620f480000000, // 14: 1.0e+14 << 17
	0xe35fa931a0000000, // 15: 1.0e+15 << 14
	0x8e1bc9bf04000000, // 16: 1.0e+16 << 10
	0xb1a2bc2ec5000000, // 17: 1.0e+17 << 7
	0xde0b6b3a76400000, // 18: 1.0e+18 << 4
	0x8ac7230489e80000, // 19: 1.0e+19 >> 0
	0xad78ebc5ac620000, // 20: 1.0e+20 >> 3
	0xd8d726b7177a8000, // 21: 1.0e+21 >> 6
	0x878678326eac9000, // 22: 1.0e+22 >> 10
	0xa968163f0a57b400, // 23: 1.0e+23 >> 13
	0xd3c21bcecceda100, // 24: 1.0e+24 >> 16
	0x84595161401484a0, // 25: 1.0e+25 >> 20
	0xa56fa5b99019a5c8, // 26: 1.0e+26 >> 23
}

var pot10Scale = [26]uint64{
	0x8049a4ac0c5811ae, //  0: 1.0e-351 << 1229
	0xcf42894a5dce35ea, //  1: 1.0e-324 << 1140
	0xa76c582338ed2621, //  2: 1.0e-297 << 1050
	0x873e4f75e2224e68, //  3: 1.0e-270 << 960
	0xda7f5bf590966848, //  4: 1.0e-243 << 871
	0xb080392cc4349dec, //  5: 1.0e-216 << 781
	0x8e938662882af53e, //  6: 1.0e-189 << 691
	0xe65829b3046b0afa, //  7: 1.0e-162 << 602
	0xba121a4650e4ddeb, //  8: 1.0e-135 << 512
	0x964e858c91ba2655, //  9: 1.0e-108 << 422
	0xf2d56790ab41c2a2, // 10: 1.0e-81 << 333
	0xc428d05aa4751e4c, // 11: 1.0e-54 << 243
	0x9e74d1b791e07e48, // 12: 1.0e-27 << 153
	0xcccccccccccccccc, // 13: 1.0e-1 << 67 (special case)
	0xcecb8f27f4200f3a, // 14: 1.0e+27 >> 26
	0xa70c3c40a64e6c51, // 15: 1.0e+54 >> 116
	0x86f0ac99b4e8dafd, // 16: 1.0e+81 >> 206
	0xda01ee641a708de9, // 17: 1.0e+108 >> 295
	0xb01ae745b101e9e4, // 18: 1.0e+135 >> 385
	0x8e41ade9fbebc27d, // 19: 1.0e+162 >> 475
	0xe5d3ef282a242e81, // 20: 1.0e+189 >> 564
	0xb9a74a0637ce2ee1, // 21: 1.0e+216 >> 654
	0x95f83d0a1fb69cd9, // 22: 1.0e+243 >> 744
	0xf24a01a73cf2dccf, // 23: 1.0e+270 >> 833
	0xc3b8358109e84f07, // 24: 1.0e+297 >> 923
	0x9e19db92b4e31ba9, // 25: 1.0e+324 >> 1013
}

var pot10ScaleLo = [26]uint32{
	0x205b896d, //  0: 1.0e-351 << 1229
	0x52064cad, //  1: 1.0e-324 << 1140
	0xaf2af2b8, //  2: 1.0e-297 << 1050
	0x5a7744a7, //  3: 1.0e-270 << 960
	0xaf39a475, //  4: 1.0e-243 << 871
	0xbd8d794e, //  5: 1.0e-216 << 781
	0x547eb47b, //  6: 1.0e-189 << 691
	0x0cb4a5a3, //  7: 1.0e-162 << 602
	0x92f34d62, //  8: 1.0e-135 << 512
	0x3a6a07f9, //  9: 1.0e-108 << 422
	0xfae27299, // 10: 1.0e-81 << 333
	0xaa97e14c, // 11: 1.0e-54 << 243
	0x775ea265, // 12: 1.0e-27 << 153
	0xcccccccc, // 13: 1.0e-1 << 67 (special case)
	0x00000000, // 14: 1.0e+27 >> 26
	0x999090b6, // 15: 1.0e+54 >> 116
	0x69a028bb, // 16: 1.0e+81 >> 206
	0xe80e6f48, // 17: 1.0e+108 >> 295
	0x5ec05dd0, // 18: 1.0e+135 >> 385
	0x14588f14, // 19: 1.0e+162 >> 475
	0x8f1668c9, // 20: 1.0e+189 >> 564
	0x6d953e2c, // 21: 1.0e+216 >> 654
	0x4abdaf10, // 22: 1.0e+243 >> 744
	0xbc633b39, // 23: 1.0e+270 >> 833
	0x0a862f81, // 24: 1.0e+297 >> 923
	0x6c07a2c2, // 25: 1.0e+324 >> 1013
}

// multiply160 mirrors sqlite3Multiply160: A = (a<<32)+aLo (96-bit),
// returns the upper 64 bits of A*b and, as lo, bits [32,64) of A*b.
func multiply160(a uint64, aLo uint32, b uint64) (hi uint64, lo uint32) {
	h1, l1 := bits.Mul64(a, b)
	h2, l2 := bits.Mul64(uint64(aLo), b)
	add := h2<<32 | l2>>32 // ((aLo*b) >> 32); aLo*b < 2^96 so this fits.
	sum, carry := bits.Add64(l1, add, 0)
	return h1 + carry, uint32(sum >> 32)
}

// fp2Convert10 mirrors sqlite3Fp2Convert10: given a = m × 2^e (m's top bit
// set), return d, p10 with a ≈ d × 10^p10 and d holding at least n
// significant digits. Approximate by design — do not "fix" the rounding.
func fp2Convert10(m uint64, e, n int) (d uint64, p10 int) {
	p := n - 1 - pwr2to10(e+63)
	hi, _ := powerOfTen(p)
	h, _ := bits.Mul64(m, hi)
	if n == 18 {
		h >>= uint(-(e + pwr10to2(p) + 2)) // in [0,62] per C asserts
		d = (h + (h << 1 & 2)) >> 1
	} else {
		d = h >> uint(-(e + pwr10to2(p) + 1))
	}
	return d, -p
}

// fp10Convert2 mirrors sqlite3Fp10Convert2 (rsc/fpfmt adaptation): the
// IEEE754 double closest to d × 10^p, as FpDecode's shortening round-trip
// check computes it. strconv.ParseFloat is NOT a substitute: it is
// correctly rounded, this is not always, and the equality check must agree
// with the C side bit for bit.
func fp10Convert2(d uint64, p int) float64 {
	if p < -348 {
		return 0.0
	}
	if p > 347 {
		return math.Inf(1)
	}
	b := 64 - bits.LeadingZeros64(d)
	lp := pwr10to2(p)
	e := 53 - b - lp
	if e > 1074 {
		if e >= 1130 {
			return 0.0
		}
		e = 1074
	}
	s := -(e - (64 - b) + lp + 3) // in [0,63] per C asserts
	pwr10h, pwr10l := powerOfTen(p)
	if pwr10l != 0 {
		pwr10h++
		pwr10l = ^pwr10l
	}
	x := d << uint(64-b)
	hi, lo := bits.Mul64(x, pwr10h)
	mid1 := uint32(lo >> 32)
	sticky := uint64(1)
	if hi&(uint64(1)<<uint(s)-1) == 0 {
		h2, _ := bits.Mul64(x, uint64(pwr10l)<<32)
		mid2 := uint32(h2 >> 32)
		if mid1-mid2 > 1 { // u32 wraparound compare, as in C
			sticky = 1
		} else {
			sticky = 0
		}
		if mid1 < mid2 {
			hi--
		}
	}
	u := hi>>uint(s) | sticky
	if u >= uint64(1)<<55-2 { // adj
		u = u>>1 | u&1
		e--
	}
	m := (u + 1 + (u >> 2 & 1)) >> 2
	if e <= -972 {
		return math.Inf(1)
	}
	if m&(1<<52) != 0 {
		m = m&^(1<<52) | uint64(1075-e)<<52
	}
	return math.Float64frombits(m)
}
