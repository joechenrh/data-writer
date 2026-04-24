package spec

import (
	"math/big"
	"math/rand"
	"strings"
	"testing"
)

func TestFormatDecimal(t *testing.T) {
	cases := []struct {
		unscaled int64
		scale    int
		want     string
	}{
		{12345, 2, "123.45"},
		{-12345, 2, "-123.45"},
		{5, 2, "0.05"},
		{-5, 2, "-0.05"},
		{0, 2, "0.00"},
		{42, 0, "42"},
		{-42, 0, "-42"},
		{1, 4, "0.0001"},
	}
	for _, tc := range cases {
		got := formatDecimal(tc.unscaled, tc.scale)
		if got != tc.want {
			t.Errorf("formatDecimal(%d, %d) = %q; want %q", tc.unscaled, tc.scale, got, tc.want)
		}
	}
}

func TestFormatDecimalBig(t *testing.T) {
	cases := []struct {
		s     string
		scale int
		want  string
	}{
		{"12345678901234567890", 5, "123456789012345.67890"},
		{"-12345678901234567890", 5, "-123456789012345.67890"},
		{"5", 20, "0.00000000000000000005"},
	}
	for _, tc := range cases {
		v, _ := new(big.Int).SetString(tc.s, 10)
		got := formatDecimalBig(v, tc.scale)
		if got != tc.want {
			t.Errorf("formatDecimalBig(%s, %d) = %q; want %q", tc.s, tc.scale, got, tc.want)
		}
	}
}

func TestGenerateDecimalUnscaledInt64Bounds(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	// DECIMAL(5, 2) unsigned: unscaled in [0, 99999].
	max := int64(0)
	min := int64(0)
	for range 2000 {
		v := generateDecimalUnscaledInt64(rng, 5, false)
		if v < 0 {
			t.Fatalf("unsigned got negative: %d", v)
		}
		if v >= 100000 {
			t.Fatalf("out of [0, 99999]: %d", v)
		}
		if v > max {
			max = v
		}
		if v < min {
			min = v
		}
	}
	if max < 50000 {
		t.Errorf("2000 samples should reach at least half the range; got max=%d", max)
	}
}

func TestGenerateDecimalUnscaledInt64SignedRange(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	// DECIMAL(3, 0) signed: unscaled in [-999, 999].
	sawPos := false
	sawNeg := false
	for range 2000 {
		v := generateDecimalUnscaledInt64(rng, 3, true)
		if v < -999 || v > 999 {
			t.Fatalf("out of [-999, 999]: %d", v)
		}
		if v > 0 {
			sawPos = true
		}
		if v < 0 {
			sawNeg = true
		}
	}
	if !sawPos || !sawNeg {
		t.Fatalf("expected both signs; sawPos=%v sawNeg=%v", sawPos, sawNeg)
	}
}

// End-to-end: generate() returns a correctly formatted CSV decimal.
func TestGenerateCSVDecimalFormat(t *testing.T) {
	c := &ColumnSpec{
		OrigName:  "amount",
		SQLType:   "decimal",
		Precision: 5,
		Scale:     2,
		Signed:    false,
	}
	rng := rand.New(rand.NewSource(1))
	v, def := c.generate(0, rng)
	if def != 1 {
		t.Fatalf("defLevel = %d; want 1", def)
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("expected string for decimal, got %T", v)
	}
	// Should look like "NNN.NN" or "NN.NN" etc. — contain exactly one dot and
	// exactly 2 digits after it.
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		t.Fatalf("expected one decimal point in %q", s)
	}
	if len(parts[1]) != 2 {
		t.Fatalf("expected 2 fractional digits in %q", s)
	}
}
