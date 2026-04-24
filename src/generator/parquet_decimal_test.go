package generator

import (
	"bytes"
	"context"
	"testing"

	"dataWriter/src/config"
	"dataWriter/src/spec"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

type decimalBufWriter struct{ b *bytes.Buffer }

func (w *decimalBufWriter) Write(_ context.Context, p []byte) (int, error) { return w.b.Write(p) }
func (w *decimalBufWriter) Close(_ context.Context) error                  { return nil }

// TestParquetDecimalIsInt32Unscaled verifies a DECIMAL(5,2) column is stored as
// int32 unscaled values in the precision-bounded range, NOT as formatted strings.
func TestParquetDecimalIsInt32Unscaled(t *testing.T) {
	specs := []*spec.ColumnSpec{{
		OrigName:  "amount",
		SQLType:   "decimal",
		Precision: 5,
		Scale:     2,
		Signed:    false,
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.Decimal,
	}}

	cfg := &config.Config{
		Common:  config.CommonConfig{Rows: 50, StartFileNo: 0, EndFileNo: 1},
		Parquet: config.ParquetConfig{NumRowGroups: 1, PageSizeBytes: 1 << 20, Compression: "uncompressed"},
	}

	buf := &bytes.Buffer{}
	wrapper := &writeWrapper{Writer: &decimalBufWriter{b: buf}}
	if err := generateParquetCommon(wrapper, 0, specs, cfg); err != nil {
		t.Fatalf("generate: %v", err)
	}

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	// Verify schema says Int32 (not ByteArray = string).
	col := reader.MetaData().Schema.Column(0)
	if col.PhysicalType() != parquet.Types.Int32 {
		t.Fatalf("physical type = %v; want Int32 (not a string)", col.PhysicalType())
	}

	// Read values back; assert they're all within [0, 99999] (precision=5, unsigned).
	rg := reader.RowGroup(0)
	cr, err := rg.Column(0)
	if err != nil {
		t.Fatalf("Column(0): %v", err)
	}
	r := cr.(*file.Int32ColumnChunkReader)
	vals := make([]int32, 50)
	def := make([]int16, 50)
	n, _, err := r.ReadBatch(50, vals, def, nil)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if n != 50 {
		t.Fatalf("read %d rows; want 50", n)
	}
	for i, v := range vals {
		if v < 0 || v > 99999 {
			t.Fatalf("row %d: unscaled %d out of [0, 99999]", i, v)
		}
	}
}

// TestParquetDecimalIsInt64Unscaled is the same verification for precision <= 18.
func TestParquetDecimalIsInt64Unscaled(t *testing.T) {
	specs := []*spec.ColumnSpec{{
		OrigName:  "big_amount",
		SQLType:   "decimal",
		Precision: 15,
		Scale:     4,
		Signed:    true,
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.Decimal,
	}}

	cfg := &config.Config{
		Common:  config.CommonConfig{Rows: 50, StartFileNo: 0, EndFileNo: 1},
		Parquet: config.ParquetConfig{NumRowGroups: 1, PageSizeBytes: 1 << 20, Compression: "uncompressed"},
	}

	buf := &bytes.Buffer{}
	wrapper := &writeWrapper{Writer: &decimalBufWriter{b: buf}}
	if err := generateParquetCommon(wrapper, 0, specs, cfg); err != nil {
		t.Fatalf("generate: %v", err)
	}

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	if pt := reader.MetaData().Schema.Column(0).PhysicalType(); pt != parquet.Types.Int64 {
		t.Fatalf("physical type = %v; want Int64", pt)
	}

	rg := reader.RowGroup(0)
	cr, err := rg.Column(0)
	if err != nil {
		t.Fatalf("Column(0): %v", err)
	}
	r := cr.(*file.Int64ColumnChunkReader)
	vals := make([]int64, 50)
	def := make([]int16, 50)
	n, _, err := r.ReadBatch(50, vals, def, nil)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if n != 50 {
		t.Fatalf("read %d rows; want 50", n)
	}
	const maxAbs int64 = 999_999_999_999_999 // 10^15 - 1
	for i, v := range vals {
		abs := v
		if abs < 0 {
			abs = -abs
		}
		if abs > maxAbs {
			t.Fatalf("row %d: unscaled %d out of [-10^15, 10^15)", i, v)
		}
	}
}
