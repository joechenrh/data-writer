package util

import (
	"context"
	"dataWriter/src/config"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// writerWithStats wraps a writer and updates progress for bytes written.
type writerWithStats struct {
	writer   storage.ExternalFileWriter
	progress *ProgressLogger
}

func (cw *writerWithStats) Write(ctx context.Context, p []byte) (int, error) {
	n, err := cw.writer.Write(ctx, p)
	if cw.progress != nil {
		cw.progress.UpdateBytes(int64(n))
	}
	return n, err
}

func (cw *writerWithStats) Close(ctx context.Context) error {
	return cw.writer.Close(ctx)
}

func OpenWriter(
	ctx context.Context,
	cfg *config.Config,
	store storage.ExternalStorage,
	fileID int,
	progress *ProgressLogger,
) (*writerWithStats, error) {
	fileName := fmt.Sprintf("%s.%d.%s", cfg.Common.Prefix, fileID, cfg.FileSuffix)
	if cfg.Common.Folders > 1 {
		fileName = fmt.Sprintf("part%d/%s.%d.%s",
			fileID%cfg.Common.Folders, cfg.Common.Prefix, fileID, cfg.FileSuffix)
	}

	writer, err := store.Create(ctx, fileName, &storage.WriterOption{
		Concurrency: 8,
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	return &writerWithStats{writer: writer, progress: progress}, nil
}
