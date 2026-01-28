package generator

import (
	"context"
	"dataWriter/src/config"
	"dataWriter/src/spec"
	"dataWriter/src/util"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
)

type fileGenerator struct {
	SpecificGenerator

	*config.Config

	fileSuffix string
	specs      []*spec.ColumnSpec
	store      storage.ExternalStorage
	logger     *util.ProgressLogger
}

func newFileGenerator(cfg *config.Config, sqlPath string) (*fileGenerator, error) {
	store, err := config.GetStore(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer store.Close()

	specs, err := spec.GetSpecFromSQL(sqlPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	logger := util.InitializeProgressLogger(
		cfg.Common.EndFileNo-cfg.Common.StartFileNo,
		"writing",
		time.Second,
	)

	return &fileGenerator{
		Config: cfg,
		logger: logger,
		specs:  specs,
		store:  store,
	}, nil
}

func (fg *fileGenerator) openWriter(
	ctx context.Context,
	fileID int,
) (storage.ExternalFileWriter, error) {
	var fileName string
	if fg.Common.Folders <= 1 {
		fileName = fmt.Sprintf("%s.%d.%s",
			fg.Common.Prefix, fileID, fg.fileSuffix)
	} else {
		folderID := fileID % fg.Common.Folders
		fileName = fmt.Sprintf("part%05d/%s.%d.%s",
			folderID, fg.Common.Prefix, fileID, fg.fileSuffix)
	}

	writer, err := fg.store.Create(ctx, fileName, &storage.WriterOption{
		Concurrency: 8,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &writerWithStats{writer: writer, logger: fg.logger}, nil
}

func (fg *fileGenerator) Generate(threads int) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s (direct mode)\n", time.Since(start))
	}()

	ctx := context.Background()
	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(threads)

	startNo, endNo := fg.Common.StartFileNo, fg.Common.EndFileNo
	for fileNo := startNo; fileNo < endNo; fileNo++ {
		eg.Go(func() error {
			writer, err := fg.openWriter(ctx, fileNo)
			if err != nil {
				return errors.Trace(err)
			}

			defer writer.Close(ctx)
			if err = fg.GenerateOneFile(ctx, writer, fileNo); err != nil {
				return errors.Trace(err)
			}
			fg.logger.UpdateFiles(1)
			return nil
		})
	}

	return errors.Trace(eg.Wait())
}

func (fg *fileGenerator) GenerateStreaming(threads int) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s (streaming mode)\n", time.Since(start))
	}()

	store, err := config.GetStore(fg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	defer store.Close()

	ctx := context.Background()

	startNo, endNo := fg.Common.StartFileNo, fg.Common.EndFileNo

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var eg errgroup.Group
	eg.SetLimit(threads)

	// Create one generator-writer pair for each file
	for i := startNo; i < endNo; i++ {
		eg.Go(func() error {
			writer, err := fg.openWriter(ctx, i)
			if err != nil {
				return errors.Trace(err)
			}

			chunkChannel := make(chan *util.FileChunk, 4)

			// Start writer goroutine for this file
			var writerGroup errgroup.Group
			writerGroup.Go(func() error {
				defer writer.Close(ctx)
				for chunk := range chunkChannel {
					if len(chunk.Data) > 0 {
						if _, err := writer.Write(ctx, chunk.Data); err != nil {
							return errors.Trace(err)
						}
					}

					if chunk.IsLast {
						break
					}
				}

				return nil
			})

			// Generate file in current goroutine, sending chunks to its writer
			err = fg.GenerateOneFileStreaming(ctx, i, chunkChannel)
			close(chunkChannel)

			if err != nil {
				cancel()
			}

			// Wait for writer to finish
			if writerErr := writerGroup.Wait(); writerErr != nil {
				return writerErr
			}

			if err != nil {
				return err
			}

			fg.logger.UpdateFiles(1)
			return nil
		})
	}

	return errors.Trace(eg.Wait())
}

func NewFileGenerator(cfg *config.Config, sqlPath string) (FileGenerator, error) {
	var (
		gen FileGenerator
		err error
	)

	switch strings.ToLower(cfg.Common.FileFormat) {
	case "parquet":
		gen, err = NewParquetGenerator(cfg, sqlPath)
	case "csv":
		gen, err = NewCSVGenerator(cfg, sqlPath)
	default:
		return nil, errors.Errorf("unsupported file format: %s", cfg.Common.FileFormat)
	}
	return gen, err
}
