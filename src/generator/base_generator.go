package generator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"dataWriter/src/config"
	"dataWriter/src/spec"
	"dataWriter/src/util"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
)

// Orchestrator orchestrates file generation for a single format.
type Orchestrator struct {
	FileGenerator

	cfg    *config.Config
	store  storage.ExternalStorage
	logger *util.ProgressLogger
}

// NewOrchestrator creates a orchestrator using the config and SQL schema.
func NewOrchestrator(cfg *config.Config, sqlPath string) (*Orchestrator, error) {
	specs, err := spec.GetSpecFromSQL(sqlPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	gen, err := newGenerator(cfg, specs)
	if err != nil {
		return nil, err
	}

	store, err := config.GetStore(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	logger := util.InitializeProgressLogger(
		cfg.Common.EndFileNo-cfg.Common.StartFileNo,
		"writing",
		time.Second,
	)

	return &Orchestrator{
		FileGenerator: gen,

		cfg:    cfg,
		store:  store,
		logger: logger,
	}, nil
}

func newGenerator(cfg *config.Config, specs []*spec.ColumnSpec) (FileGenerator, error) {
	switch strings.ToLower(cfg.Common.FileFormat) {
	case "parquet":
		return newParquetGenerator(cfg, specs)
	case "csv":
		return newCSVGenerator(cfg, specs)
	default:
		return nil, errors.Errorf("unsupported file format: %s", cfg.Common.FileFormat)
	}
}

func (o *Orchestrator) openWriter(
	ctx context.Context,
	fileID int,
) (storage.ExternalFileWriter, error) {
	var fileName string
	if o.cfg.Common.Folders <= 1 {
		fileName = fmt.Sprintf("%s.%d.%s",
			o.cfg.Common.Prefix, fileID, o.FileSuffix())
	} else {
		folderID := fileID % o.cfg.Common.Folders
		fileName = fmt.Sprintf("part%05d/%s.%d.%s",
			folderID, o.cfg.Common.Prefix, fileID, o.FileSuffix())
	}

	writer, err := o.store.Create(ctx, fileName, &storage.WriterOption{
		Concurrency: 8,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &writerWithStats{writer: writer, logger: o.logger}, nil
}

func (o *Orchestrator) Close() {
	o.store.Close()
}

func (o *Orchestrator) printSummary(elapsed time.Duration) {
	files, bytes := o.logger.Snapshot()
	if files == 0 {
		files = int64(o.cfg.Common.EndFileNo - o.cfg.Common.StartFileNo)
	}
	rowsPerFile := o.cfg.Common.Rows
	totalRows := int64(rowsPerFile) * files
	throughput := 0.0
	if elapsed.Seconds() > 0 {
		throughput = float64(bytes) / elapsed.Seconds()
	}

	fmt.Println("Summary:")
	fmt.Printf("  Format: %s\n", strings.ToLower(o.cfg.Common.FileFormat))
	fmt.Printf("  Files: %d\n", files)
	fmt.Printf("  Rows/File: %d\n", rowsPerFile)
	fmt.Printf("  Total Rows: %d\n", totalRows)
	fmt.Printf("  Bytes: %s\n", units.BytesSize(float64(bytes)))
	fmt.Printf("  Throughput: %s/s\n", units.BytesSize(throughput))
	fmt.Printf("  Path: %s\n", o.cfg.Common.Path)
}

func (o *Orchestrator) generateDirect(ctx context.Context, fileNo int) error {
	writer, err := o.openWriter(ctx, fileNo)
	if err != nil {
		return errors.Trace(err)
	}

	defer writer.Close(ctx)
	if err = o.GenerateFile(ctx, writer, fileNo); err != nil {
		return errors.Trace(err)
	}
	o.logger.UpdateFiles(1)
	return nil
}

func (o *Orchestrator) generateStreaming(ctx context.Context, fileNo int) error {
	var eg errgroup.Group

	chunkChannel := make(chan *util.FileChunk, 4)
	eg.Go(func() error {
		defer close(chunkChannel)
		return o.GenerateFileStreaming(ctx, fileNo, chunkChannel)
	})

	eg.Go(func() error {
		writer, err := o.openWriter(ctx, fileNo)
		if err != nil {
			return errors.Trace(err)
		}
		defer writer.Close(ctx)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case chunk, ok := <-chunkChannel:
				if !ok {
					return nil
				}
				if _, err := writer.Write(ctx, chunk.Data); err != nil {
					return errors.Trace(err)
				}

				if chunk.IsLast {
					return nil
				}
			}
		}
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	o.logger.UpdateFiles(1)
	return nil
}

// Run creates files directly without streaming.
func (o *Orchestrator) Run(streaming bool, threads int) error {
	start := time.Now()

	ctx := context.Background()
	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(threads)

	startNo := o.cfg.Common.StartFileNo
	endNo := o.cfg.Common.EndFileNo
	for fileNo := startNo; fileNo < endNo; fileNo++ {
		fileID := fileNo
		eg.Go(func() error {
			if streaming {
				return o.generateStreaming(ctx, fileID)
			}
			return o.generateDirect(ctx, fileID)
		})
	}

	if err := eg.Wait(); err != nil {
		fmt.Printf("Generate and upload failed after %s\n", time.Since(start))
		return errors.Trace(err)
	}

	elapsed := time.Since(start)
	fmt.Printf("Generate and upload took %s\n", elapsed)
	o.printSummary(elapsed)
	return nil
}
