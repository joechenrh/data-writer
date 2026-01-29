package config

import (
	"context"
	"fmt"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
)

const defaultPageSizeBytes = units.MiB

type S3Config struct {
	Region          string `toml:"region,omitempty"`
	AccessKey       string `toml:"access_key,omitempty"`
	SecretAccessKey string `toml:"secret_key,omitempty"`
	Provider        string `toml:"provider,omitempty"`
	Endpoint        string `toml:"endpoint,omitempty"`
	Force           bool   `toml:"force,omitempty"`
	RoleArn         string `toml:"role_arn,omitempty"`
}

type GCSConfig struct {
	Credential string `toml:"credential,omitempty"`
}

type CommonConfig struct {
	Path             string `toml:"path"`
	Prefix           string `toml:"prefix"`
	Folders          int    `toml:"folders"`
	StartFileNo      int    `toml:"start_fileno"`
	EndFileNo        int    `toml:"end_fileno"`
	Rows             int    `toml:"rows"`
	FileFormat       string `toml:"format"`
	UseStreamingMode bool   `toml:"use_streaming_mode"`
	ChunkSize        string `toml:"chunk_size"`

	// ChunkSizeBytes is derived at runtime and not read from config.
	ChunkSizeBytes int `toml:"-"`
}

type ParquetConfig struct {
	PageSize     string `toml:"page_size"`
	NumRowGroups int    `toml:"row_groups"`
	Compression  string `toml:"compression"`

	// PageSizeBytes is derived at runtime and not read from config.
	PageSizeBytes int64 `toml:"-"`
}

type CSVConfig struct {
	Base64    bool   `toml:"base64"`
	Separator string `toml:"separator,omitempty"`
	EndLine   string `toml:"endline,omitempty"`
}

type Config struct {
	Common    CommonConfig  `toml:"common"`
	Parquet   ParquetConfig `toml:"parquet"`
	CSV       CSVConfig     `toml:"csv"`
	S3Config  *S3Config     `toml:"s3,omitempty"`
	GCSConfig *GCSConfig    `toml:"gcs,omitempty"`
}

// Normalize resolves derived config values after loading.
func Normalize(cfg *Config) error {
	chunkBytes, err := cfg.Common.resolveChunkSizeBytes()
	if err != nil {
		return err
	}
	cfg.Common.ChunkSizeBytes = chunkBytes

	pageBytes, err := cfg.Parquet.resolvePageSizeBytes()
	if err != nil {
		return err
	}
	cfg.Parquet.PageSizeBytes = pageBytes
	return nil
}

func (c *CommonConfig) resolveChunkSizeBytes() (int, error) {
	if c.ChunkSize != "" {
		bytes, err := units.FromHumanSize(c.ChunkSize)
		if err != nil {
			return 0, fmt.Errorf("invalid chunk_size %q: %w", c.ChunkSize, err)
		}
		return int(bytes), nil
	}
	return 0, nil
}

func (c *ParquetConfig) resolvePageSizeBytes() (int64, error) {
	if c.PageSize != "" {
		bytes, err := units.FromHumanSize(c.PageSize)
		if err != nil {
			return 0, fmt.Errorf("invalid page_size %q: %w", c.PageSize, err)
		}
		return bytes, nil
	}
	return defaultPageSizeBytes, nil
}

// GetStore initializes and returns an ExternalStorage instance based on the provided configuration.
func GetStore(c *Config) (storage.ExternalStorage, error) {
	var op *storage.BackendOptions
	if c.S3Config != nil {
		op = &storage.BackendOptions{S3: storage.S3BackendOptions{
			Region:          c.S3Config.Region,
			AccessKey:       c.S3Config.AccessKey,
			SecretAccessKey: c.S3Config.SecretAccessKey,
			Provider:        c.S3Config.Provider,
			Endpoint:        c.S3Config.Endpoint,
			RoleARN:         c.S3Config.RoleArn,
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

	return storage.NewWithDefaultOpt(context.Background(), s)
}
