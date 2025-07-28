package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/planner/core" // to setup expression.EvalSimpleAst for in core_init
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
)

// validChar is a set of characters used to generate random strings.
const validChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_&*!.;<>?:-+()[]{}"

// NumericOrder defines the order of numeric data in a column.
type NumericOrder int

const (
	NumericNoOrder NumericOrder = iota
	NumericTotalOrder
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
	Precision int // used for decimal type, not implemented yet
	Scale     int // used for decimal type, not implemented yet

	// Below are used for generate specified data
	NullPercent int
	IsUnique    bool
	Order       NumericOrder
	Mean        int
	StdDev      int
	Signed      bool
}

// parseComment parse the comment string and set the corresponding fields in ColumnSpec
func (c *ColumnSpec) parseComment(comment string) {
	if comment == "" {
		return
	}

	opts := strings.Split(comment, ",")
	for _, opt := range opts {
		s := strings.Split(opt, "=")
		k, v := s[0], s[1]
		switch k {
		case "null_percent":
			c.NullPercent, _ = strconv.Atoi(v)
		case "max_length":
			c.TypeLen, _ = strconv.Atoi(v)
		case "mean":
			c.Mean, _ = strconv.Atoi(v)
		case "stddev":
			c.StdDev, _ = strconv.Atoi(v)
		case "order":
			switch v {
			case "total_order":
				c.Order = NumericTotalOrder
			case "partial_order":
				c.Order = NumericPartialOrder
			case "random_order":
				c.Order = NumericRandomOrder
			}
		}
	}
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
	mysql.TypeTiny: {
		SQLType:   "tinyint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   8,
		Signed:    true,
	},
	mysql.TypeShort: {
		SQLType:   "smallint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   16,
		Signed:    true,
	},
	mysql.TypeInt24: {
		SQLType:   "mediumint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   24,
		Signed:    true,
	},
	mysql.TypeLong: {
		SQLType:   "int",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.None,
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
	},
	mysql.TypeBlob: {
		SQLType:   "blob",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
	},
	mysql.TypeString: {
		SQLType:   "char",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
	},
}

func (c *ColumnSpec) Clone() *ColumnSpec {
	clone := *c
	return &clone
}

func getTableInfoBySQL(createTableSQL string) (table *model.TableInfo, err error) {
	p := parser.New()
	p.SetSQLMode(mysql.ModeNone)

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

func getSpecFromSQL(sqlPath string) ([]*ColumnSpec, error) {
	data, err := os.ReadFile(sqlPath)
	if err != nil {
		return nil, err
	}

	query := string(data)
	tbInfo, err := getTableInfoBySQL(query)
	if err != nil {
		return nil, err
	}

	specs := make([]*ColumnSpec, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		spec := DefaultSpecs[col.GetType()].Clone()
		spec.OrigName = col.Name.L

		col.FieldType.AddFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag)
		if !types.IsTypeNumeric(col.GetType()) && col.GetFlen() > 0 {
			spec.TypeLen = col.GetFlen()
		}
		if col.GetType() == mysql.TypeNewDecimal {
			spec.Precision = col.FieldType.GetFlen()
			spec.Scale = col.FieldType.GetDecimal()
		}
		if col.Comment != "" {
			spec.parseComment(col.Comment)
		}
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
