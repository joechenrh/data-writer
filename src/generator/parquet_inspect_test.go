package generator

import (
	"bytes"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
)

// TestInspectParquetFile reads a parquet file from a path given via the
// DW_INSPECT_PARQUET env var and prints its schema + first few values from
// each column. Skipped by default; run with:
//
//	DW_INSPECT_PARQUET=/tmp/.../file.parquet go test ./src/generator/... -run TestInspect -v
func TestInspectParquetFile(t *testing.T) {
	path := os.Getenv("DW_INSPECT_PARQUET")
	if path == "" {
		t.Skip("DW_INSPECT_PARQUET not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()

	sch := rdr.MetaData().Schema
	fmt.Printf("File: %s (%d bytes)\n", path, len(data))
	fmt.Printf("NumRows: %d, NumCols: %d\n\n", rdr.NumRows(), sch.NumColumns())

	for c := 0; c < sch.NumColumns(); c++ {
		col := sch.Column(c)
		fmt.Printf("Col %d: %q\n", c, col.Name())
		fmt.Printf("  PhysicalType:  %v\n", col.PhysicalType())
		fmt.Printf("  ConvertedType: %v\n", col.ConvertedType())
		fmt.Printf("  LogicalType:   %v\n", col.LogicalType())
		fmt.Printf("  TypeLength:    %d\n", col.TypeLength())
		fmt.Println()
	}

	rg := rdr.RowGroup(0)
	for c := 0; c < sch.NumColumns(); c++ {
		cr, _ := rg.Column(c)
		fmt.Printf("Col %d (%s) first 8 values:\n", c, sch.Column(c).Name())
		switch r := cr.(type) {
		case *file.Int32ColumnChunkReader:
			vals := make([]int32, 8)
			def := make([]int16, 8)
			r.ReadBatch(8, vals, def, nil)
			fmt.Printf("  int32: %v\n", vals)
		case *file.Int64ColumnChunkReader:
			vals := make([]int64, 8)
			def := make([]int16, 8)
			r.ReadBatch(8, vals, def, nil)
			fmt.Printf("  int64: %v\n", vals)
		case *file.ByteArrayColumnChunkReader:
			vals := make([]parquet.ByteArray, 8)
			def := make([]int16, 8)
			r.ReadBatch(8, vals, def, nil)
			strs := make([]string, len(vals))
			for i, v := range vals {
				s := string(v)
				if !isPrintable(s) {
					s = fmt.Sprintf("<%x>", []byte(v))
				}
				strs[i] = s
			}
			fmt.Printf("  string: [%s]\n", strings.Join(strs, ", "))
		case *file.FixedLenByteArrayColumnChunkReader:
			vals := make([]parquet.FixedLenByteArray, 8)
			def := make([]int16, 8)
			r.ReadBatch(8, vals, def, nil)
			for i, v := range vals {
				bi := decodeTwosComplement(v)
				fmt.Printf("  flba[%d]: hex=%x twos-comp-bigint=%s\n", i, []byte(v), bi.String())
			}
		default:
			fmt.Printf("  unsupported reader type %T\n", r)
		}
		fmt.Println()
	}
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r < 0x20 || r > 0x7e {
			return false
		}
	}
	return true
}

func decodeTwosComplement(b []byte) *big.Int {
	if len(b) == 0 {
		return new(big.Int)
	}
	neg := b[0]&0x80 != 0
	if !neg {
		return new(big.Int).SetBytes(b)
	}
	inv := make([]byte, len(b))
	for i, x := range b {
		inv[i] = ^x
	}
	abs := new(big.Int).SetBytes(inv)
	abs.Add(abs, big.NewInt(1))
	return abs.Neg(abs)
}
