package generator

import (
	"context"
	"dataWriter/src/util"

	"github.com/pingcap/tidb/br/pkg/storage"
)

// writerWithStats wraps a writer and updates progress for bytes written.
type writerWithStats struct {
	writer storage.ExternalFileWriter
	logger *util.ProgressLogger
}

func (cw *writerWithStats) Write(ctx context.Context, p []byte) (int, error) {
	n, err := cw.writer.Write(ctx, p)
	if cw.logger != nil {
		cw.logger.UpdateBytes(int64(n))
	}
	return n, err
}

func (cw *writerWithStats) Close(ctx context.Context) error {
	return cw.writer.Close(ctx)
}
