package spec

import (
	"math/big"

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
