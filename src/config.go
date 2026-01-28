package main

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/storage"
)

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
	ChunkSizeKB      int    `toml:"chunk_size_kb"`
}

type ParquetConfig struct {
	PageSizeKB   int64  `toml:"page_size_kb"`
	NumRowGroups int    `toml:"row_groups"`
	Compression  string `toml:"compression"`
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

// GetStore initializes and returns an ExternalStorage instance based on the provided configuration.
func GetStore(c Config) (storage.ExternalStorage, error) {
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
