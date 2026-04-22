package generator

import (
	"bytes"
	"context"
	"testing"

	"dataWriter/src/config"
	"dataWriter/src/gen"
	"dataWriter/src/spec"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// nopExternalWriter is a minimal storage.ExternalFileWriter stub for tests.
type nopExternalWriter struct{ w *bytes.Buffer }

func (n *nopExternalWriter) Write(_ context.Context, b []byte) (int, error) {
	return n.w.Write(b)
}
func (n *nopExternalWriter) Close(_ context.Context) error { return nil }

func TestParquetRowMajorWithUserCode(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	gen.Register("uid", func(c *gen.Ctx) any { return int64(c.RowID * 2) })

	specs := []*spec.ColumnSpec{
		{OrigName: "uid", SQLType: "bigint", Type: parquet.Types.Int64, Converted: schema.ConvertedTypes.None},
	}

	cfg := &config.Config{
		Common:  config.CommonConfig{Rows: 100, StartFileNo: 0, EndFileNo: 1},
		Parquet: config.ParquetConfig{NumRowGroups: 1, PageSizeBytes: 1 << 20, Compression: "uncompressed"},
	}

	out := &bytes.Buffer{}
	wrapper := &writeWrapper{Writer: &nopExternalWriter{w: out}}
	if err := generateParquetCommon(wrapper, 0, specs, cfg); err != nil {
		t.Fatalf("generateParquetCommon: %v", err)
	}

	// Read back and verify column values are 0,2,4,...,198.
	reader, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewParquetReader: %v", err)
	}
	defer reader.Close()

	rg := reader.RowGroup(0)
	col, err := rg.Column(0)
	if err != nil {
		t.Fatalf("Column(0): %v", err)
	}
	r := col.(*file.Int64ColumnChunkReader)
	vals := make([]int64, 100)
	def := make([]int16, 100)
	n, _, err := r.ReadBatch(100, vals, def, nil)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if n != 100 {
		t.Fatalf("read %d rows; want 100", n)
	}
	for i, v := range vals {
		if v != int64(i)*2 {
			t.Fatalf("row %d: got %d, want %d", i, v, i*2)
		}
	}
}
