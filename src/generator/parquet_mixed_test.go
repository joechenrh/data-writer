package generator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"dataWriter/src/config"
	"dataWriter/src/gen"
	"dataWriter/src/spec"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// writerBuffer satisfies storage.ExternalFileWriter for tests.
type writerBuffer struct{ buf *bytes.Buffer }

func (w *writerBuffer) Write(_ context.Context, b []byte) (int, error) { return w.buf.Write(b) }
func (w *writerBuffer) Close(_ context.Context) error                  { return nil }

// TestParquetRowMajorMixedBuiltinTimestamp verifies that when hasUserCode=true
// activates the row-major path, builtin TIMESTAMP columns still write correctly
// (regression for bug 1: previously panicked with "toInt64: unexpected string").
func TestParquetRowMajorMixedBuiltinTimestamp(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	gen.Register("uid", func(c *gen.Ctx) any { return int64(c.RowID) })

	specs := []*spec.ColumnSpec{
		{OrigName: "uid", SQLType: "bigint", Type: parquet.Types.Int64, Converted: schema.ConvertedTypes.None},
		{OrigName: "ts", SQLType: "timestamp", Type: parquet.Types.Int64, Converted: schema.ConvertedTypes.None},
	}

	cfg := &config.Config{
		Common:  config.CommonConfig{Rows: 50, StartFileNo: 0, EndFileNo: 1},
		Parquet: config.ParquetConfig{NumRowGroups: 1, PageSizeBytes: 1 << 20, Compression: "uncompressed"},
	}

	out := &bytes.Buffer{}
	wrapper := &writeWrapper{Writer: &writerBuffer{buf: out}}
	if err := generateParquetCommon(wrapper, 0, specs, cfg); err != nil {
		t.Fatalf("generateParquetCommon: %v", err)
	}
	// The fact that generateParquetCommon didn't panic is the regression
	// guard; sanity-check that we can read back the 50 rows.
	reader, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewParquetReader: %v", err)
	}
	defer reader.Close()
	if got := reader.NumRows(); got != 50 {
		t.Fatalf("NumRows = %d; want 50", got)
	}
}

// TestCtxTimeReadsBuiltinTimeColumn verifies a user generator on a later
// column can read a builtin TIMESTAMP sibling via ctx.Time(col) without
// panicking (regression for finding 7).
func TestCtxTimeReadsBuiltinTimeColumn(t *testing.T) {
	t.Cleanup(gen.ResetForTest)

	var capturedTime time.Time
	gen.Register("follower", func(c *gen.Ctx) any {
		capturedTime = c.Time("ts")
		return int64(capturedTime.Unix())
	})

	specs := []*spec.ColumnSpec{
		{OrigName: "ts", SQLType: "timestamp", Type: parquet.Types.Int64, Converted: schema.ConvertedTypes.None},
		{OrigName: "follower", SQLType: "bigint", Type: parquet.Types.Int64, Converted: schema.ConvertedTypes.None},
	}

	cfg := &config.Config{
		Common:  config.CommonConfig{Rows: 50, StartFileNo: 0, EndFileNo: 1},
		Parquet: config.ParquetConfig{NumRowGroups: 1, PageSizeBytes: 1 << 20, Compression: "uncompressed"},
	}

	out := &bytes.Buffer{}
	wrapper := &writeWrapper{Writer: &writerBuffer{buf: out}}
	if err := generateParquetCommon(wrapper, 0, specs, cfg); err != nil {
		t.Fatalf("generateParquetCommon panicked or errored: %v", err)
	}
	if capturedTime.IsZero() {
		t.Fatalf("capturedTime was never set — user fn didn't run or got zero time")
	}
}
