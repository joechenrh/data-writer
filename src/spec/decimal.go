package spec

import (
	"math/big"
	"math/rand"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
)

func deduceTypeForDecimal(precision int) (parquet.Type, int) {
	if precision <= 9 {
		return parquet.Types.Int32, 0
	}
	if precision <= 18 {
		return parquet.Types.Int64, 0
	}

	bits := decimalMaxDigitsBits(precision) + 1
	byteLen := (bits + 7) / 8
	return parquet.Types.FixedLenByteArray, byteLen
}

func decimalMaxDigitsBits(precision int) int {
	if precision <= 0 {
		return 0
	}
	pow10 := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
	pow10.Sub(pow10, big.NewInt(1))
	return pow10.BitLen()
}

// pow10Int64 returns 10^n as int64. Caller must ensure n <= 18 to avoid overflow.
func pow10Int64(n int) int64 {
	p := int64(1)
	for range n {
		p *= 10
	}
	return p
}

// generateDecimalUnscaledInt64 returns a precision-bounded unscaled integer in
// [-(10^precision - 1), 10^precision - 1] (signed) or [0, 10^precision - 1]
// (unsigned). Only valid for precision <= 18.
func generateDecimalUnscaledInt64(rng *rand.Rand, precision int, signed bool) int64 {
	if precision <= 0 {
		return 0
	}
	if precision > 18 {
		precision = 18
	}
	v := rng.Int63n(pow10Int64(precision))
	if signed && rng.Intn(2) == 1 {
		v = -v
	}
	return v
}

// generateDecimalUnscaledBig returns a precision-bounded unscaled big.Int in
// [-(10^precision - 1), 10^precision - 1] (signed) or [0, 10^precision - 1]
// (unsigned). Used for precision > 18 (FixedLenByteArray storage).
func generateDecimalUnscaledBig(rng *rand.Rand, precision int, signed bool) *big.Int {
	if precision <= 0 {
		return new(big.Int)
	}
	bound := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
	v := new(big.Int).Rand(rng, bound)
	if signed && rng.Intn(2) == 1 {
		v.Neg(v)
	}
	return v
}

// formatDecimal formats an unscaled integer as a human-readable decimal string.
// Example: (12345, 2) -> "123.45"; (-5, 2) -> "-0.05"; (42, 0) -> "42".
func formatDecimal(unscaled int64, scale int) string {
	if scale <= 0 {
		return strconv.FormatInt(unscaled, 10)
	}
	neg := unscaled < 0
	if neg {
		unscaled = -unscaled
	}
	s := strconv.FormatInt(unscaled, 10)
	if len(s) <= scale {
		s = strings.Repeat("0", scale+1-len(s)) + s
	}
	head := s[:len(s)-scale]
	tail := s[len(s)-scale:]
	out := head + "." + tail
	if neg {
		out = "-" + out
	}
	return out
}

// formatDecimalBig formats a big.Int unscaled value the same way as formatDecimal.
func formatDecimalBig(unscaled *big.Int, scale int) string {
	if scale <= 0 {
		return unscaled.String()
	}
	neg := unscaled.Sign() < 0
	abs := new(big.Int).Abs(unscaled)
	s := abs.String()
	if len(s) <= scale {
		s = strings.Repeat("0", scale+1-len(s)) + s
	}
	head := s[:len(s)-scale]
	tail := s[len(s)-scale:]
	out := head + "." + tail
	if neg {
		out = "-" + out
	}
	return out
}
