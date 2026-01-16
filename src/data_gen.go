package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/util/hack"
)

func (c *ColumnSpec) generatePartialOrderInt(rowID int) int {
	randPrefix := (rowID * 1000000007) & 31
	moveBit := c.TypeLen - 6
	return (randPrefix << moveBit) + rowID
}

func (c *ColumnSpec) generateGaussianInt(rng *rand.Rand) int {
	randomFloat := (rng.Float64()-0.5)*2*float64(c.StdDev) + float64(c.Mean)
	randomInt := int(math.Round(randomFloat))

	if c.TypeLen == 64 {
		return randomInt
	}

	lower := 0
	upper := 1<<c.TypeLen - 1
	if c.Signed {
		lower -= (1 << (c.TypeLen - 1))
		upper -= (1 << (c.TypeLen - 1))
	}

	if randomInt > upper {
		randomInt = upper
	} else if randomInt < lower {
		randomInt = lower
	}
	return randomInt
}

func (c *ColumnSpec) generateRandomInt(rng *rand.Rand) int {
	if c.TypeLen == 64 {
		return rng.Int()
	}

	v := rng.Intn(1 << c.TypeLen)
	if c.Signed {
		v -= 1 << (c.TypeLen - 1)
	}
	return v
}

func (c *ColumnSpec) generateInt(rowID int, rng *rand.Rand) int {
	if c.StdDev > 0 {
		return c.generateGaussianInt(rng)
	}

	if c.IsUnique && c.Order == NumericNoOrder {
		c.Order = NumericTotalOrder
	}

	switch c.Order {
	case NumericNoOrder:
		return c.generateRandomInt(rng)
	case NumericTotalOrder:
		return rowID
	case NumericPartialOrder:
		if rowID%32 == 0 {
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
	if c.IsUnique {
		return uuid.New().String()
	}

	lower := c.MinLen
	upper := c.TypeLen
	length := rng.Intn(upper-lower+1) + lower

	b := make([]byte, length)
	rng.Read(b)
	for i := range b {
		b[i] = validChar[int(b[i])%len(validChar)]
	}

	return string(hack.String(b))
}

// TODO(joechenrh): implement a real JSON generator
func (c *ColumnSpec) generateJSON(rng *rand.Rand) string {
	return "[1,2,3,4,5]"
}

func (c *ColumnSpec) generateRandomTime(format string) string {
	now := time.Now()

	oneYearAgo := now.AddDate(-1, 0, 0)

	randomDuration := time.Duration(rand.Int63n(int64(now.Sub(oneYearAgo))))

	randomTime := oneYearAgo.Add(randomDuration)

	return randomTime.Format(format)
}

func (c *ColumnSpec) generate(rowID int, rng *rand.Rand) (any, int16) {
	if c.generateNULL(rng) {
		return "\\N", 0
	}

	switch c.SQLType {
	case "int", "tinyint", "smallint", "mediumint", "decimal":
		return c.generateInt(rowID, rng), 1
	case "bigint", "double", "float":
		return c.generateInt(rowID, rng), 1
	case "char", "varchar", "varbinary", "blob", "text", "tinyblob":
		return c.generateString(rng), 1
	case "json":
		return c.generateJSON(rng), 1
	case "timestamp", "datetime":
		return c.generateRandomTime(time.DateTime), 1
	case "date":
		return c.generateRandomTime(time.DateOnly), 1
	case "time":
		return c.generateRandomTime(time.TimeOnly), 1
	case "year":
		return rng.Intn(70) + 1970, 1
	}
	return nil, 0
}

func (c *ColumnSpec) generateInt64Parquet(rowID int, out []int64, defLevel []int16, rng *rand.Rand) {
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

func (c *ColumnSpec) generateInt32Parquet(rowID int, out []int32, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = int32(c.generateInt(rowID+i, rng))
		}
	}
}

// generateFloatParquet is same as generateInt64Parquet for now
func (c *ColumnSpec) generateFloat64Parquet(rowID int, out []float64, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = float64(c.generateInt(rowID, rng)) + 0.1
		}
	}
}

func (c *ColumnSpec) generateFloat32Parquet(rowID int, out []float32, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = float32(c.generateInt(rowID, rng)) + 0.1
		}
	}
}

func (c *ColumnSpec) generateYearParquet(out []int32, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = int32(rng.Int63()%50 + 2000)
		}
	}
}

func (c *ColumnSpec) generateTimestampParquet(out []int64, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = rng.Int63() % 1576800000000000 // Random timestamp in the range of 0 to 50 years
		}
	}
}

func (c *ColumnSpec) generateDateParquet(out []int32, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = rng.Int31() & 16383
		}
	}
}

func (c *ColumnSpec) generateJSONParquet(_ int, out []parquet.ByteArray, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
		} else {
			defLevel[i] = 1
			out[i] = []byte("[1,2,3,4,5]")
		}
	}
}

func (c *ColumnSpec) generateStringParquet(_ int, out []parquet.ByteArray, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)

	lower := c.MinLen
	upper := c.TypeLen
	slen := rng.Intn(upper-lower+1) + lower

	buf := make([]byte, slen*len(out))
	rng.Read(buf)
	for i := range buf {
		buf[i] = validChar[int(buf[i])%len(validChar)]
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

func generateSingleField(rowID int, spec *ColumnSpec, rng *rand.Rand) string {
	v, _ := spec.generate(rowID, rng)
	return fmt.Sprintf("%v", v)
}
