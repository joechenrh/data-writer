package spec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/planner/core" // to setup expression.EvalSimpleAst for in core_init
	"github.com/pingcap/tidb/pkg/types"

	_ "github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
)

// validChar is a set of characters used to generate random strings.
const validChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_&*!.;<>?:-+()[]{}"

// NumericOrder defines the order of numeric data in a column.
type NumericOrder int

const (
	NumericTotalOrder NumericOrder = iota
	NumericPartialOrder
	NumericRandomOrder
)

// ColumnSpec defines the properties of a column to generate
type ColumnSpec struct {
	OrigName  string               // Original name of the column
	SQLType   string               // type in SQL, e.g., "int", "varchar"
	Type      parquet.Type         // used for parquet file
	Converted schema.ConvertedType // used for parquet file

	TypeLen   int // length of the type, e.g., 64 for bigint, 32 for int
	MinLen    int // minimum length for string types, defaults to TypeLen * 0.75
	Precision int // used for decimal type, not implemented yet
	Scale     int // used for decimal type, not implemented yet

	// Below are used for generate specified data
	NullPercent int
	ValueSet    []string
	IntSet      []int64
	IsUnique    bool
	Order       NumericOrder
	Mean        int
	StdDev      int
	Signed      bool
	Compress    int
}

func splitCommentOpts(comment string) ([]string, error) {
	var (
		opts         []string
		start        int
		bracketDepth int
		inQuotes     bool
	)

	for i := range comment {
		switch comment[i] {
		case '"':
			inQuotes = !inQuotes
		case '[':
			if !inQuotes {
				bracketDepth++
			}
		case ']':
			if !inQuotes {
				bracketDepth--
				if bracketDepth < 0 {
					return nil, fmt.Errorf("malformed comment: %q", comment)
				}
			}
		case ',':
			if !inQuotes && bracketDepth == 0 {
				opt := comment[start:i]
				if opt != "" {
					opts = append(opts, opt)
				}
				start = i + 1
			}
		}
	}

	if inQuotes || bracketDepth != 0 {
		return nil, fmt.Errorf("malformed comment: %q", comment)
	}

	if start < len(comment) {
		opt := comment[start:]
		if opt != "" {
			opts = append(opts, opt)
		}
	}

	return opts, nil
}

// parseComment parse the comment string and set the corresponding fields in ColumnSpec
func (c *ColumnSpec) parseComment(comment string) error {
	c.Order = NumericRandomOrder
	comment = strings.ReplaceAll(comment, " ", "")
	if comment == "" {
		return nil
	}

	opts, err := splitCommentOpts(comment)
	if err != nil {
		return err
	}

	for _, opt := range opts {
		s := strings.SplitN(opt, "=", 2)
		if len(s) != 2 {
			return fmt.Errorf("malformed comment option: %q", opt)
		}
		k, v := s[0], s[1]
		switch k {
		case "null_percent":
			c.NullPercent, _ = strconv.Atoi(v)
		case "max_length":
			c.TypeLen, _ = strconv.Atoi(v)
		case "min_length":
			c.MinLen, _ = strconv.Atoi(v)
		case "mean":
			c.Mean, _ = strconv.Atoi(v)
		case "stddev":
			c.StdDev, _ = strconv.Atoi(v)
		case "compress":
			compress, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid compress for column %s: %q", c.OrigName, v)
			}
			c.Compress = mathutil.Clamp(compress, 1, 100)
		case "set":
			var stringValues []string
			if err := json.Unmarshal([]byte(v), &stringValues); err == nil {
				c.ValueSet = stringValues
				continue
			}
			var intValues []int64
			if err := json.Unmarshal([]byte(v), &intValues); err == nil {
				c.IntSet = intValues
				continue
			}
			return fmt.Errorf("invalid set for column %s: %q", c.OrigName, v)
		case "order":
			switch v {
			case "total_order":
				c.Order = NumericTotalOrder
			case "partial_order":
				c.Order = NumericPartialOrder
			case "random_order":
				c.Order = NumericRandomOrder
			default:
				return fmt.Errorf("invalid order for column %s: %q", c.OrigName, v)
			}
		}
	}
	return nil
}

var DefaultSpecs = map[byte]*ColumnSpec{
	mysql.TypeNewDecimal: {
		SQLType:   "decimal",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.Decimal,
	},
	mysql.TypeDate: {
		SQLType:   "date",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.Date,
	},
	mysql.TypeTimestamp: {
		SQLType:   "timestamp",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.TimestampMicros,
	},
	mysql.TypeDatetime: {
		SQLType:   "datetime",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.TimestampMicros,
	},
	mysql.TypeDuration: {
		SQLType:   "time",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.Int64,
		TypeLen:   8,
		Signed:    true,
	},
	mysql.TypeYear: {
		SQLType:   "year",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.Int32,
		TypeLen:   8,
		Signed:    true,
	},
	mysql.TypeTiny: {
		SQLType:   "tinyint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.Int8,
		TypeLen:   8,
		Signed:    true,
	},
	mysql.TypeShort: {
		SQLType:   "smallint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.Int32,
		TypeLen:   16,
		Signed:    true,
	},
	mysql.TypeInt24: {
		SQLType:   "mediumint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.Int32,
		TypeLen:   24,
		Signed:    true,
	},
	mysql.TypeLong: {
		SQLType:   "int",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.Int32,
		TypeLen:   32,
		Signed:    true,
	},
	mysql.TypeLonglong: {
		SQLType:   "bigint",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
		Signed:    true,
	},
	mysql.TypeFloat: {
		SQLType:   "float",
		Type:      parquet.Types.Float,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   32,
	},
	mysql.TypeDouble: {
		SQLType:   "double",
		Type:      parquet.Types.Double,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   32,
	},
	mysql.TypeVarchar: {
		SQLType:   "varchar",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
	},
	mysql.TypeBlob: {
		SQLType:   "char",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
	},
	mysql.TypeTinyBlob: {
		SQLType:   "tinyblob",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
	},
	mysql.TypeLongBlob: {
		SQLType:   "char",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
	},
	mysql.TypeMediumBlob: {
		SQLType:   "char",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
	},
	mysql.TypeVarString: {
		SQLType:   "char",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
	},
	mysql.TypeString: {
		SQLType:   "char",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
	},
	// TODO(joechenrh): check if we can use nested type for JSON
	mysql.TypeJSON: {
		SQLType:   "json",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
	},
}

// String returns a string representation of the ColumnSpec
func (c *ColumnSpec) String() string {
	var builder strings.Builder

	builder.WriteString("ColumnSpec{")
	builder.WriteString("Name: " + c.OrigName)
	builder.WriteString(", SQLType: " + c.SQLType)
	builder.WriteString(", TypeLen: " + strconv.Itoa(c.TypeLen))

	if c.MinLen > 0 {
		builder.WriteString(", MinLen: " + strconv.Itoa(c.MinLen))
	}

	if c.NullPercent > 0 {
		builder.WriteString(", NullPercent: " + strconv.Itoa(c.NullPercent))
	}

	if c.IsUnique {
		builder.WriteString(", IsUnique: true")
	}

	switch c.Order {
	case NumericTotalOrder:
		builder.WriteString(", Order: total_order")
	case NumericPartialOrder:
		builder.WriteString(", Order: partial_order")
	case NumericRandomOrder:
		builder.WriteString(", Order: random_order")
	}

	if c.Mean != 0 {
		builder.WriteString(", Mean: " + strconv.Itoa(c.Mean))
	}

	if c.StdDev != 0 {
		builder.WriteString(", StdDev: " + strconv.Itoa(c.StdDev))
	}

	if c.Compress > 0 {
		builder.WriteString(", Compress: " + strconv.Itoa(c.Compress))
	}

	if c.Precision > 0 {
		builder.WriteString(", Precision: " + strconv.Itoa(c.Precision))
	}

	if c.Scale > 0 {
		builder.WriteString(", Scale: " + strconv.Itoa(c.Scale))
	}

	builder.WriteString("}")
	return builder.String()
}

func (c *ColumnSpec) Clone() *ColumnSpec {
	clone := *c
	return &clone
}

func getTableInfoBySQL(createTableSQL string) (table *model.TableInfo, err error) {
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)

	stmt, err := p.ParseOneStmt(createTableSQL, "", "")
	if err != nil {
		return nil, err
	}

	metaBuildCtx := ddl.NewMetaBuildContextWithSctx(mock.NewContext())
	s, ok := stmt.(*ast.CreateTableStmt)
	if ok {
		return ddl.BuildTableInfoWithStmt(metaBuildCtx, s, mysql.DefaultCharset, "", nil)
	}

	return nil, errors.New("not a CREATE TABLE statement")
}

// readAndCleanSQL reads SQL file and cleans up comments and extra content
func readAndCleanSQL(sqlPath string) (string, error) {
	data, err := os.ReadFile(sqlPath)
	if err != nil {
		return "", err
	}

	// Filter out lines containing /* comments at the beginning of file
	lines := strings.Split(string(data), "\n")
	var filteredLines []string
	startIndex := 0

	// Skip lines that start with /* at the beginning of the file
	for i, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmedLine, "/*") && trimmedLine != "" {
			startIndex = i
			break
		}
	}

	// Keep all lines from the first non-comment line onwards
	filteredLines = lines[startIndex:]
	query := strings.Join(filteredLines, "\n")

	// Find the last closing parenthesis and truncate everything after it except ";"
	lastParenIndex := strings.LastIndex(query, ")")
	if lastParenIndex != -1 {
		// Keep everything up to and including the last ")" and add ";"
		query = query[:lastParenIndex+1] + ";"
	}

	return query, nil
}

// GetSpecFromSQL parses a CREATE TABLE SQL file into column specs.
func GetSpecFromSQL(sqlPath string) ([]*ColumnSpec, error) {
	query, err := readAndCleanSQL(sqlPath)
	if err != nil {
		return nil, err
	}

	tbInfo, err := getTableInfoBySQL(query)
	if err != nil {
		return nil, err
	}

	specs := make([]*ColumnSpec, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		spec, ok := DefaultSpecs[col.GetType()]
		if !ok {
			return nil, errors.New("unsupported column type: " + strconv.Itoa(int(col.GetType())))
		}
		spec = spec.Clone()
		spec.OrigName = col.Name.L
		spec.Order = NumericRandomOrder
		spec.Compress = 100 // default no compression for data generation

		col.FieldType.AddFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag)
		if !types.IsTypeNumeric(col.GetType()) && col.GetFlen() > 0 {
			spec.TypeLen = min(col.GetFlen(), 64)
		}
		if col.GetType() == mysql.TypeNewDecimal {
			spec.Precision = col.FieldType.GetFlen()
			spec.Scale = col.FieldType.GetDecimal()
			if spec.Precision == 0 {
				return nil, errors.New("unsupported decimal precision=0 for column: " + spec.OrigName)
			}
			if spec.Scale < 0 || spec.Scale > spec.Precision {
				return nil, errors.New("invalid decimal scale for column: " + spec.OrigName)
			}
			spec.Type, spec.TypeLen = deduceTypeForDecimal(spec.Precision)
		}
		if col.Comment != "" {
			if err := spec.parseComment(col.Comment); err != nil {
				return nil, err
			}
		}

		if spec.MinLen == 0 {
			spec.MinLen = int(float64(spec.TypeLen) * 0.75)
		}
		spec.MinLen = min(spec.TypeLen, spec.MinLen)

		specs = append(specs, spec)
	}

	if tbInfo.PKIsHandle {
		for _, col := range tbInfo.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				specs[col.Offset].IsUnique = true
				break
			}
		}
	}

	for _, index := range tbInfo.Indices {
		if index.Primary || index.Unique {
			for _, col := range index.Columns {
				if col.Offset < len(specs) && col.Offset >= 0 {
					specs[col.Offset].IsUnique = true
				}
			}
		}
	}

	return specs, nil
}
