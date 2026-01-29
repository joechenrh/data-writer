package util

import (
	"dataWriter/src/config"
	"dataWriter/src/spec"

	"github.com/docker/go-units"
)

const (
	defaultCSVSeparator = ","
	defaultCSVEndLine   = "\n"
	defaultChunkSize    = 32 * units.KiB
)

func CSVSeparatorAndEndline(cfg config.CSVConfig) (string, string) {
	separator := cfg.Separator
	if separator == "" {
		separator = defaultCSVSeparator
	}
	endline := cfg.EndLine
	if endline == "" {
		endline = defaultCSVEndLine
	}
	return separator, endline
}

// Streaming data structure for chunk-based processing
type FileChunk struct {
	Data   []byte
	IsLast bool // Indicates if this is the final chunk for the file
}

// ChunkCalculator interface for determining optimal chunk sizes
type ChunkCalculator interface {
	CalculateChunkSize(specs []*spec.ColumnSpec) int
	EstimateRowSize(specs []*spec.ColumnSpec) int
}

// chunkCalculator for determining optimal chunk sizes
type chunkCalculator struct {
	cfg *config.Config
}

// NewChunkSizeCalculator creates a new chunk size calculator
func NewChunkSizeCalculator(cfg *config.Config) *chunkCalculator {
	return &chunkCalculator{cfg: cfg}
}

// EstimateRowSize calculates the approximate size of a single row in bytes
func (c *chunkCalculator) EstimateRowSize(specs []*spec.ColumnSpec) int {
	totalSize := 0

	for _, columnSpec := range specs {
		switch columnSpec.SQLType {
		case "bigint", "timestamp", "datetime":
			totalSize += 8 // 8 bytes
		case "int", "mediumint", "date":
			totalSize += 4 // 4 bytes
		case "smallint":
			totalSize += 2 // 2 bytes
		case "tinyint":
			totalSize += 1 // 1 byte
		case "float":
			totalSize += 4 // 4 bytes
		case "double":
			totalSize += 8 // 8 bytes
		case "varchar", "char", "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary", "text", "tinytext", "mediumtext", "longtext":
			// Estimate based on TypeLen, with some overhead for variable length
			if columnSpec.TypeLen > 0 {
				totalSize += columnSpec.TypeLen
			} else {
				totalSize += 32 // Default estimate for variable length strings
			}
		default:
			totalSize += 16 // Default estimate for unknown types
		}
	}

	// Add overhead for delimiters (CSV) or encoding (Parquet)
	if c.cfg.Common.FileFormat == "csv" {
		separator, endline := CSVSeparatorAndEndline(c.cfg.CSV)
		delimiterOverhead := len(endline)
		if len(specs) > 0 {
			delimiterOverhead += (len(specs) - 1) * len(separator)
		}
		totalSize += delimiterOverhead
	} else {
		totalSize = int(float64(totalSize) * 1.2) // 20% overhead for Parquet encoding
	}

	return totalSize
}

// CalculateChunkSize determines the optimal number of rows per chunk
func (c *chunkCalculator) CalculateChunkSize(specs []*spec.ColumnSpec) int {
	rowSize := c.EstimateRowSize(specs)
	if rowSize <= 0 {
		rowSize = 100 // Fallback
	}

	targetSizeBytes := c.cfg.Common.ChunkSizeBytes
	if targetSizeBytes == 0 {
		targetSizeBytes = defaultChunkSize
	}

	return max(targetSizeBytes/rowSize, 1)
}
