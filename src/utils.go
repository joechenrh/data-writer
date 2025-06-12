package main

import (
	"context"
	"log"
	"runtime"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
)

type S3Config struct {
	Region          string `toml:"region,omitempty"`
	AccessKey       string `toml:"access_key,omitempty"`
	SecretAccessKey string `toml:"secret_key,omitempty"`
	Provider        string `toml:"provider,omitempty"`
	Endpoint        string `toml:"endpoint,omitempty"`
	Force           bool   `toml:"force,omitempty"`
}

type GCSConfig struct {
	Credential string `toml:"credential,omitempty"`
}

type CommonConfig struct {
	Path        string `toml:"path"`
	Prefix      string `toml:"prefix"`
	Folders     int    `toml:"folders"`
	StartFileNo int    `toml:"start_fileno"`
	EndFileNo   int    `toml:"end_fileno"`
	Rows        int    `toml:"rows"`
	FileFormat  string `toml:"format"`
}

type ParquetConfig struct {
	PageSizeKB   int64 `toml:"page_size_kb"`
	NumRowGroups int   `toml:"row_groups"`
}

type CSVConfig struct {
	Base64 bool `toml:"base64"`
}

type Config struct {
	Common    CommonConfig  `toml:"common"`
	Parquet   ParquetConfig `toml:"parquet"`
	CSV       CSVConfig     `toml:"csv"`
	S3Config  *S3Config     `toml:"s3,omitempty"`
	GCSConfig *GCSConfig    `toml:"gcs,omitempty"`
}

func GetStore(c Config) (storage.ExternalStorage, error) {
	var op *storage.BackendOptions
	if c.S3Config != nil {
		op = &storage.BackendOptions{S3: storage.S3BackendOptions{
			Region:          c.S3Config.Region,
			AccessKey:       c.S3Config.AccessKey,
			SecretAccessKey: c.S3Config.SecretAccessKey,
			Provider:        c.S3Config.Provider,
			Endpoint:        c.S3Config.Endpoint,
		}}
	} else if c.GCSConfig != nil {
		op = &storage.BackendOptions{GCS: storage.GCSBackendOptions{
			CredentialsFile: c.GCSConfig.Credential,
		}}
	}

	s, err := storage.ParseBackend(c.Common.Path, op)
	if err != nil {
		return nil, err
	}

	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		return nil, err
	}

	return store, nil
}

type NumericOrder int

var (
	NumericNoOrder      NumericOrder = 0
	NumericTotalOrder   NumericOrder = 1
	NumericPartialOrder NumericOrder = 2
	NumericRandomOrder  NumericOrder = 3
)

type ColumnSpec struct {
	// For string type, it's the maximum length, for numeric type, it's the
	// maximum number of digits.
	length    int
	IsUnique  bool
	IsPrimary bool

	OrigName    string
	SQLType     string
	ParquetType string

	NullPercent int

	// for numeric type
	Order  NumericOrder
	Mean   int
	StdDev int
	Signed bool
}

func AssertTrue(err error) {
	if err != nil {
		panic(err)
	}
}

func DeleteAllFilesByPrefix(prefix string, cfg Config) {
	var fileNames []string
	store, err := GetStore(cfg)
	AssertTrue(err)

	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		if strings.HasPrefix(path, prefix) {
			fileNames = append(fileNames, path)
		}
		return nil
	})

	var eg errgroup.Group
	eg.SetLimit(runtime.NumCPU() / 2)
	for _, fileName := range fileNames {
		f := fileName
		eg.Go(func() error {
			return store.DeleteFile(context.Background(), f)
		})
	}
	AssertTrue(eg.Wait())
}

func ShowFiles(cfg Config) {
	store, err := GetStore(cfg)
	AssertTrue(err)

	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		log.Printf("Name: %s, Size: %d, Size (MiB): %f", path, size, float64(size)/1024/1024)
		return nil
	})
}
