package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
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
	IsPrimary   bool
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

var DefaultSpecs = map[string]ColumnSpec{
	"decimal": {
		SQLType:   "decimal",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.Decimal,
	},
	"date": {
		SQLType:   "date",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.Date,
	},
	"timestamp": {
		SQLType:   "timestamp",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.TimestampMicros,
	},
	"datetime": {
		SQLType:   "datetime",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.TimestampMicros,
	},
	"bool": {
		SQLType:   "bool",
		Type:      parquet.Types.Boolean,
		Converted: schema.ConvertedTypes.None,
	},
	"tinyint": {
		SQLType:   "tinyint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   8,
		Signed:    true,
	},
	"smallint": {
		SQLType:   "smallint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   8,
		Signed:    true,
	},
	"mediumint": {
		SQLType:   "mediumint",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   8,
		Signed:    true,
	},
	"int": {
		SQLType:   "int",
		Type:      parquet.Types.Int32,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   32,
		Signed:    true,
	},
	"bigint": {
		SQLType:   "bigint",
		Type:      parquet.Types.Int64,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   64,
		Signed:    true,
	},
	"float": {
		SQLType:   "float",
		Type:      parquet.Types.Float,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   32,
	},
	"double": {
		SQLType:   "double",
		Type:      parquet.Types.Double,
		Converted: schema.ConvertedTypes.None,
		TypeLen:   32,
	},
	"varchar": {
		SQLType:   "varchar",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
	},
	"blob": {
		SQLType:   "blob",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
	},
	"char": {
		SQLType:   "char",
		Type:      parquet.Types.ByteArray,
		Converted: schema.ConvertedTypes.None,
	},
}

func getBasicSpec(sqlTypeString string) ColumnSpec {
	var (
		SQLType     = sqlTypeString
		fieldLength = -1
		precision   int
		scale       int
	)

	if strings.Contains(sqlTypeString, "(") && strings.Contains(sqlTypeString, ")") {
		start, end := strings.Index(sqlTypeString, "("), strings.Index(sqlTypeString, ")")
		SQLType = sqlTypeString[:start]
		if strings.Contains(sqlTypeString, ",") {
			// For types like decimal(10,2)
			parts := strings.Split(sqlTypeString[start+1:end], ",")
			precision, _ = strconv.Atoi(parts[0])
			scale, _ = strconv.Atoi(parts[1])
		} else {
			// For types like varchar(255)
			fieldLength, _ = strconv.Atoi(sqlTypeString[start+1 : end])
		}
	}

	SQLType = strings.ToLower(SQLType)
	spec := DefaultSpecs[SQLType]
	if fieldLength != -1 {
		spec.TypeLen = fieldLength
	}
	spec.Precision = precision
	spec.Scale = scale
	return spec
}

func getSpecFromSQL(sqlPath string) []ColumnSpec {
	file, err := os.Open(sqlPath)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("error reading file: %v", err)
	}

	specs := make([]ColumnSpec, 0)
	for _, line := range lines[1 : len(lines)-1] {
		line = strings.ReplaceAll(line, "`", "")
		splitted := strings.Split(line, "/*")

		comment := ""
		if len(splitted) > 1 {
			comment = strings.ReplaceAll(splitted[1], " ", "")
			comment = strings.TrimSuffix(comment, "*/")
		}

		parts := strings.Split(strings.TrimSpace(splitted[0]), " ")

		sqlTypeString := strings.ToLower(parts[1])
		spec := getBasicSpec(sqlTypeString)
		spec.parseComment(comment)
		spec.OrigName = parts[0]

		if strings.Contains(strings.ToLower(line), "unique") {
			spec.IsUnique = true
		}
		if strings.Contains(strings.ToLower(line), "primary") {
			spec.IsPrimary = true
		}
		specs = append(specs, spec)
	}

	return specs
}
