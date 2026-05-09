package generator

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"dataWriter/src/config"
	"dataWriter/src/gen"
	"dataWriter/src/spec"
	"dataWriter/src/util"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// generateSQLRow appends a single tuple `(v1, v2, ...)` to buf. Numeric and
// year columns are emitted unquoted; everything else (date/time/string) is
// emitted as a single-quoted SQL string. Values are escaped via sqlEscape.
func generateSQLRow(
	specs []*spec.ColumnSpec,
	rowID int,
	rng *rand.Rand,
	buf []byte,
) []byte {
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.OrigName
	}
	rowBuf := gen.NewRowBuffer(names)

	buf = append(buf, '(')
	for i, columnSpec := range specs {
		if i > 0 {
			buf = append(buf, ',')
		}
		v := spec.GenerateSingleFieldUser(rowID, columnSpec, rng, rowBuf)
		if isSQLNumericType(columnSpec.SQLType) {
			buf = append(buf, v...)
		} else {
			buf = append(buf, '\'')
			buf = sqlEscape(buf, v)
			buf = append(buf, '\'')
		}
	}
	buf = append(buf, ')')
	return buf
}

// isSQLNumericType reports whether a column's SQLType serializes as a bare
// numeric literal in INSERT statements.
func isSQLNumericType(t string) bool {
	switch t {
	case "tinyint", "smallint", "mediumint", "int", "bigint",
		"float", "double", "decimal", "year":
		return true
	}
	return false
}

// sqlEscape appends s into buf, escaping the bytes that would break a
// single-quoted SQL string literal: backslash, single-quote, and the few
// MySQL-specific escapes (\0, \n, \r, \Z). Other bytes are written verbatim.
func sqlEscape(buf []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\'':
			buf = append(buf, '\\', '\'')
		case 0:
			buf = append(buf, '\\', '0')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case 0x1A:
			buf = append(buf, '\\', 'Z')
		default:
			buf = append(buf, c)
		}
	}
	return buf
}

// SQLGenerator implements FormatGenerator for mydumper-compatible SQL files.
// Each chunk emits a self-contained `INSERT INTO db.tbl VALUES (...),(...);`
// statement so consumers (TiDB Lightning, IMPORT INTO) can parse partial
// files and so streamed chunks are individually valid.
type SQLGenerator struct {
	cfg             *config.Config
	specs           []*spec.ColumnSpec
	chunkCalculator util.ChunkCalculator
	insertHeader    []byte // "INSERT INTO `db`.`tbl` VALUES "
	rowOverhead     int    // approximate per-row syntax overhead
}

func newSQLGenerator(cfg *config.Config, specs []*spec.ColumnSpec) (*SQLGenerator, error) {
	header, err := buildSQLInsertHeader(cfg.Common.Prefix)
	if err != nil {
		return nil, err
	}

	// 2 quote chars per non-numeric column + commas + parens
	stringCols := 0
	for _, s := range specs {
		if !isSQLNumericType(s.SQLType) {
			stringCols++
		}
	}
	overhead := 2 /*parens*/ + (len(specs) - 1) /*commas between fields*/ +
		2*stringCols /*quotes*/ + 2 /*",\n" between rows*/

	return &SQLGenerator{
		cfg:             cfg,
		specs:           specs,
		chunkCalculator: util.NewChunkSizeCalculator(cfg),
		insertHeader:    header,
		rowOverhead:     overhead,
	}, nil
}

// buildSQLInsertHeader formats the `INSERT INTO ` prefix using backtick-quoted
// identifiers parsed from the `db.tbl` prefix configured for the task.
func buildSQLInsertHeader(prefix string) ([]byte, error) {
	parts := strings.SplitN(prefix, ".", 2)
	switch len(parts) {
	case 1:
		return []byte(fmt.Sprintf("INSERT INTO `%s` VALUES ", quoteIdent(parts[0]))), nil
	case 2:
		return []byte(fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ",
			quoteIdent(parts[0]), quoteIdent(parts[1]))), nil
	}
	return nil, fmt.Errorf("invalid prefix for SQL output: %q", prefix)
}

// quoteIdent escapes a backtick inside an identifier by doubling it,
// matching MySQL's quoting rules.
func quoteIdent(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}

func (g *SQLGenerator) FileSuffix() string {
	return "sql"
}

func (g *SQLGenerator) GenerateFile(
	ctx context.Context,
	writer storage.ExternalFileWriter,
	fileNo int,
) error {
	rng := rand.New(rand.NewSource(seedForFile(fileNo)))
	startRowID := fileNo * g.cfg.Common.Rows
	totalRows := g.cfg.Common.Rows

	buf := make([]byte, 0, 64*units.KiB)
	buf = append(buf, g.insertHeader...)
	for i := range totalRows {
		if i > 0 {
			buf = append(buf, ',', '\n')
		}
		buf = generateSQLRow(g.specs, startRowID+i, rng, buf)
	}
	buf = append(buf, ';', '\n')
	if _, err := writer.Write(ctx, buf); err != nil {
		return err
	}
	return nil
}

func (g *SQLGenerator) GenerateFileStreaming(
	ctx context.Context,
	fileNo int,
	chunkChannel chan<- *util.FileChunk,
) error {
	var (
		rng        = rand.New(rand.NewSource(seedForFile(fileNo)))
		startRowID = fileNo * g.cfg.Common.Rows
		totalRows  = g.cfg.Common.Rows

		// Estimate chunk size off the CSV row size + per-row SQL overhead so
		// chunk boundaries land near the configured ChunkSize (or default).
		rowSize    = g.chunkCalculator.EstimateRowSize(g.specs) + g.rowOverhead
		chunkRows  = g.chunkCalculator.CalculateChunkSize(g.specs)
		bufferSize = rowSize*chunkRows*3/2 + len(g.insertHeader) + 4
	)

	for rowOffset := 0; rowOffset < totalRows; rowOffset += chunkRows {
		buffer := make([]byte, 0, bufferSize)
		rowsInChunk := min(chunkRows, totalRows-rowOffset)
		isLast := rowOffset+chunkRows >= totalRows

		buffer = append(buffer, g.insertHeader...)
		for i := range rowsInChunk {
			if i > 0 {
				buffer = append(buffer, ',', '\n')
			}
			buffer = generateSQLRow(g.specs, startRowID+rowOffset+i, rng, buffer)
		}
		buffer = append(buffer, ';', '\n')

		select {
		case chunkChannel <- &util.FileChunk{
			Data:   buffer,
			IsLast: isLast,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
