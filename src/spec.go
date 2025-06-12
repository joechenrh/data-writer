package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
)

type FieldType struct {
	ParquetType   string
	DefaultLength int
}

var ParquetTypeMapping = map[string]string{
	// numeric types
	"bool":    "int64",
	"int":     "int64",
	"bigint":  "int64",
	"tinyint": "int64",

	// text types
	"char":       "string",
	"varchar":    "string",
	"text":       "string",
	"timestamp":  "string",
	"varbinary":  "string",
	"mediumblob": "string",
}

var DefaultLength = map[string]int{
	// numeric types
	"bool":    1,
	"tinyint": 8,
	"int":     32,
	"bigint":  64,

	// time types
	"timestamp": 64,

	// text types
	"char":       64,
	"varchar":    64,
	"text":       64,
	"varbinary":  64,
	"mediumblob": 32768,
}

func getBasicSpec(sqlTypeString string) ColumnSpec {
	var (
		SQLType     = sqlTypeString
		fieldLength = -1
	)

	if strings.Contains(sqlTypeString, "(") && strings.Contains(sqlTypeString, ")") {
		start, end := strings.Index(sqlTypeString, "("), strings.Index(sqlTypeString, ")")
		SQLType = sqlTypeString[:start]
		fieldLength, _ = strconv.Atoi(sqlTypeString[start+1 : end])
	}

	SQLType = strings.ToLower(SQLType)
	if fieldLength == -1 {
		fieldLength = DefaultLength[SQLType]
	}

	return ColumnSpec{
		SQLType:     SQLType,
		ParquetType: ParquetTypeMapping[SQLType],
		length:      fieldLength,
		Signed:      true,
	}
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
			c.length, _ = strconv.Atoi(v)
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
		line = strings.Replace(line, "`", "", -1)
		splitted := strings.Split(line, "--")

		comment := ""
		if len(splitted) > 1 {
			comment = strings.Replace(splitted[1], " ", "", -1)
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
