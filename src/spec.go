package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/util/hack"
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

func (c *ColumnSpec) generatePartialOrderInt(rowID int) int {
	randPrefix := (rowID * 1000000007) & 0x0000000000000007
	moveBit := c.length - 5
	return (randPrefix << moveBit) + rowID
}

func (c *ColumnSpec) generateGaussianInt(rng *rand.Rand) int {
	randomFloat := (rng.Float64()-0.5)*2*float64(c.StdDev) + float64(c.Mean)
	randomInt := int(math.Round(randomFloat))

	if c.length == 64 {
		return randomInt
	}

	lower := 0
	upper := 1<<c.length - 1
	if c.Signed {
		lower -= (1 << (c.length - 1))
		upper -= (1 << (c.length - 1))
	}

	if randomInt > upper {
		randomInt = upper
	} else if randomInt < lower {
		randomInt = lower
	}
	return randomInt
}

func (c *ColumnSpec) generateRandomInt() int {
	if c.length == 64 {
		return rand.Int()
	}

	v := rand.Intn(1 << c.length)
	if c.Signed {
		v -= 1 << (c.length - 1)
	}
	return v
}

func (c *ColumnSpec) generateInt(rowID int, rng *rand.Rand) int {
	// gaussian distribution
	if c.StdDev > 0 {
		return c.generateGaussianInt(rng)
	}

	if c.IsUnique || c.IsPrimary {
		return rowID
	}

	switch c.Order {
	case NumericNoOrder:
		return c.generateRandomInt()
	case NumericTotalOrder:
		return rowID
	case NumericPartialOrder:
		if rowID%128 == 0 {
			return c.generatePartialOrderInt(rowID)
		}
		return rowID
	case NumericRandomOrder:
		return c.generatePartialOrderInt(rowID)
	default:
		log.Printf("Unsupported order: %d", c.Order)
	}
	return rowID
}

func (c *ColumnSpec) generateNULL(rng *rand.Rand) bool {
	return rng.Intn(100) < c.NullPercent
}

func (c *ColumnSpec) generateBatchNull(length int, rng *rand.Rand) []bool {
	randomIndices := make([]byte, length)
	rng.Read(randomIndices)

	null := make([]bool, length)
	for i := range length {
		null[i] = int(randomIndices[i])*100 < c.NullPercent*256
	}
	return null
}

func (c *ColumnSpec) generateString(rng *rand.Rand) string {
	if c.IsUnique || c.IsPrimary {
		return uuid.New().String()
	}

	length := rng.Intn(c.length) + 1
	b := make([]byte, length)
	rng.Read(b)
	for i := range b {
		b[i] = b[i]%70 + 50
	}

	return string(hack.String(b))
}

func (c *ColumnSpec) generateRandomTimestamp() string {
	// Get the current time
	now := time.Now()

	// Calculate the time one year ago
	oneYearAgo := now.AddDate(-1, 0, 0)

	// Generate a random duration between 0 and one year
	randomDuration := time.Duration(rand.Int63n(int64(now.Sub(oneYearAgo))))

	// Add the random duration to one year ago to get a random time within the last year
	randomTime := oneYearAgo.Add(randomDuration)

	// Format the random time into a string (e.g., RFC3339 format)
	timestamp := randomTime.Format(time.RFC3339)

	return timestamp
}

func (c *ColumnSpec) generate(rowID int, rng *rand.Rand) (any, int16) {
	if c.generateNULL(rng) {
		return "\\N", 0
	}

	switch c.SQLType {
	case "int":
		return c.generateInt(rowID, rng), 1
	case "bigint":
		return c.generateInt(rowID, rng), 1
	case "float64":
		return c.generateInt(rowID, rng), 1
	case "char", "varchar", "varbinary":
		return c.generateString(rng), 1
	case "timestamp":
		return c.generateRandomTimestamp(), 1
	}
	return nil, 0
}

func (c *ColumnSpec) generateBatchInt64(rowID int, out []int64, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = int64(c.generateInt(rowID+i, rng))
		}
	}
}

func (c *ColumnSpec) generateBatchFloat(rowID int, out []float64, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = float64(c.generateInt(rowID, rng))
		}
	}
}

func (c *ColumnSpec) generateBatchTimestamp(out []parquet.ByteArray, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = hack.Slice(c.generateRandomTimestamp())
		}
	}
}

func (c *ColumnSpec) generateBatchString(_ int, out []parquet.ByteArray, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)

	slen := rng.Intn(c.length-1) + 1
	buf := make([]byte, slen*len(out))
	rng.Read(buf)
	for i := range buf {
		buf[i] = buf[i]%120 + 55
	}

	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = buf[i*slen : (i+1)*slen]
		}
	}
}

func generateSingleField(rowID int, spec ColumnSpec, rng *rand.Rand) string {
	v, _ := spec.generate(rowID, rng)
	return fmt.Sprintf("%v", v)
}
