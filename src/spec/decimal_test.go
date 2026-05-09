package spec

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
)

func TestParquetTypeForDecimal(t *testing.T) {
	// precision <= 9 -> INT32
	type9, len9 := deduceTypeForDecimal(9)
	if type9 != parquet.Types.Int32 {
		t.Fatalf("expected int32 for precision 9, got %v", type9)
	}
	if len9 != 0 {
		t.Fatalf("expected type len 0 for int32, got %d", len9)
	}

	// precision <= 18 -> INT64
	type18, len18 := deduceTypeForDecimal(18)
	if type18 != parquet.Types.Int64 {
		t.Fatalf("expected int64 for precision 18, got %v", type18)
	}
	if len18 != 0 {
		t.Fatalf("expected type len 0 for int64, got %d", len18)
	}

	// precision > 18 -> FIXED_LEN_BYTE_ARRAY
	type19, len19 := deduceTypeForDecimal(19)
	if type19 != parquet.Types.FixedLenByteArray {
		t.Fatalf("expected fixed_len_byte_array for precision 19, got %v", type19)
	}
	if len19 <= 0 {
		t.Fatalf("expected positive type len for fixed_len, got %d", len19)
	}
}

func TestDecimalMaxDigitsBits(t *testing.T) {
	bits19 := decimalMaxDigitsBits(19)
	bits20 := decimalMaxDigitsBits(20)
	if bits20 <= bits19 {
		t.Fatalf("expected bits for 20 digits > 19 digits, got %d <= %d", bits20, bits19)
	}
}
