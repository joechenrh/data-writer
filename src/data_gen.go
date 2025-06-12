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
