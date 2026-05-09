package generator

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// compressedWriter buffers writes through a compressor and flushes compressed
// data to the underlying storage writer on Close.
type compressedWriter struct {
	inner  storage.ExternalFileWriter
	buf    bytes.Buffer
	comp   io.WriteCloser
}

func newCompressedWriter(inner storage.ExternalFileWriter, codec string) (*compressedWriter, error) {
	cw := &compressedWriter{inner: inner}
	switch codec {
	case "zst":
		enc, err := zstd.NewWriter(&cw.buf)
		if err != nil {
			return nil, err
		}
		cw.comp = enc
	case "gz":
		cw.comp = gzip.NewWriter(&cw.buf)
	}
	return cw, nil
}

func (cw *compressedWriter) Write(ctx context.Context, p []byte) (int, error) {
	return cw.comp.Write(p)
}

func (cw *compressedWriter) Close(ctx context.Context) error {
	if err := cw.comp.Close(); err != nil {
		return err
	}
	if _, err := cw.inner.Write(ctx, cw.buf.Bytes()); err != nil {
		return err
	}
	return cw.inner.Close(ctx)
}
