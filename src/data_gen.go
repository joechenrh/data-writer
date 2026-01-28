package main

import (
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/util/hack"
)

var fillA = func() []byte {
	buf := make([]byte, 1024)
	for i := range buf {
		buf[i] = 'a'
	}
	return buf
}()

func generateStringWithCompress(b []byte, length int, compress int, rng *rand.Rand) {
	nonduplicateLength := length * compress / 100
	rng.Read(b[:nonduplicateLength])
	for i := range b[:nonduplicateLength] {
		b[i] = validChar[int(b[i])%len(validChar)]
	}

	// The rest part is filled with duplicate 'a' to simulate compression
	for i := nonduplicateLength; i < length; {
		i += copy(b[i:], fillA)
	}
}

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
	if len(c.IntSet) > 0 {
		return int(c.IntSet[rng.Intn(len(c.IntSet))])
	}
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
	if len(c.ValueSet) > 0 {
		return c.ValueSet[rng.Intn(len(c.ValueSet))]
	}
	if c.IsUnique {
		return uuid.New().String()
	}

	lower := c.MinLen
	upper := c.TypeLen
	length := rng.Intn(upper-lower+1) + lower

	b := make([]byte, length)
	generateStringWithCompress(b, length, c.Compress, rng)
	return string(hack.String(b))
}

// TODO(joechenrh): implement a real JSON generator
func (c *ColumnSpec) generateJSON(_ *rand.Rand) string {
	return "[1,2,3,4,5]"
}

func (c *ColumnSpec) generateRandomTime(format string, rng *rand.Rand) string {
	now := time.Now()

	oneYearAgo := now.AddDate(-1, 0, 0)

	randomDuration := time.Duration(rng.Int63n(int64(now.Sub(oneYearAgo))))

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
		return c.generateRandomTime(time.DateTime, rng), 1
	case "date":
		return c.generateRandomTime(time.DateOnly, rng), 1
	case "time":
		return c.generateRandomTime(time.TimeOnly, rng), 1
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

func (c *ColumnSpec) generateDecimalInt32Parquet(_ int, out []int32, defLevel []int16, rng *rand.Rand) {
	unscaled := c.generateDecimalInt64Batch(len(out), rng)
	for i := range len(out) {
		if unscaled[i] < 0 {
			defLevel[i] = 0
			continue
		}
		defLevel[i] = 1
		out[i] = int32(unscaled[i])
	}
}

func (c *ColumnSpec) generateDecimalInt64Parquet(_ int, out []int64, defLevel []int16, rng *rand.Rand) {
	unscaled := c.generateDecimalInt64Batch(len(out), rng)
	for i := range len(out) {
		if unscaled[i] < 0 {
			defLevel[i] = 0
			continue
		}
		defLevel[i] = 1
		out[i] = unscaled[i]
	}
}

func (c *ColumnSpec) generateDecimalFixedLenParquet(_ int, out []parquet.FixedLenByteArray, defLevel []int16, rng *rand.Rand) {
	nullMap := c.generateBatchNull(len(out), rng)
	for i := range len(out) {
		if nullMap[i] {
			defLevel[i] = 0
			continue
		}
		defLevel[i] = 1
		if len(c.IntSet) > 0 {
			out[i] = fixedLenDecimalFromInt64(c.IntSet[rng.Intn(len(c.IntSet))], c.TypeLen)
		} else {
			out[i] = generateFixedLenDecimalBytes(c.Precision, c.TypeLen, rng)
		}
	}
}

func (c *ColumnSpec) generateDecimalInt64Batch(batch int, rng *rand.Rand) []int64 {
	nullMap := c.generateBatchNull(batch, rng)
	out := make([]int64, batch)

	if len(c.IntSet) > 0 {
		for i := range batch {
			if nullMap[i] {
				out[i] = -1
				continue
			}
			out[i] = c.IntSet[rng.Intn(len(c.IntSet))]
		}
		return out
	}

	limit := pow10Int64(c.Precision)
	for i := range batch {
		if nullMap[i] {
			out[i] = -1
			continue
		}
		out[i] = rng.Int63n(limit)
	}
	return out
}

func pow10Int64(p int) int64 {
	res := int64(1)
	for range p {
		res *= 10
	}
	return res
}

func fixedLenDecimalFromInt64(unscaled int64, byteLen int) parquet.FixedLenByteArray {
	// Parquet DECIMAL in fixed-len byte array: two's-complement big-endian.
	v := big.NewInt(unscaled)
	b := v.Bytes()
	if len(b) > byteLen {
		b = b[len(b)-byteLen:]
	}
	padded := make([]byte, byteLen)
	copy(padded[byteLen-len(b):], b)

	if unscaled < 0 {
		// Sign-extend for negative values.
		for i := 0; i < byteLen-len(b); i++ {
			padded[i] = 0xFF
		}
	}

	return padded
}

func generateFixedLenDecimalBytes(precision, byteLen int, rng *rand.Rand) parquet.FixedLenByteArray {
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
	buf := make([]byte, byteLen+1)
	rng.Read(buf)

	v := new(big.Int).SetBytes(buf)
	v.Mod(v, limit)

	b := v.Bytes()
	if len(b) > byteLen {
		b = b[len(b)-byteLen:]
	}

	padded := make([]byte, byteLen)
	copy(padded[byteLen-len(b):], b)
	return padded
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

	if len(c.ValueSet) > 0 {
		for i := range len(out) {
			if nullMap[i] {
				defLevel[i] = 0
				continue
			}
			defLevel[i] = 1
			out[i] = []byte(c.ValueSet[rng.Intn(len(c.ValueSet))])
		}
		return
	}

	lower := c.MinLen
	upper := c.TypeLen
	slen := rng.Intn(upper-lower+1) + lower

	buf := make([]byte, slen*len(out))
	for i := range out {
		generateStringWithCompress(buf[slen*i:slen*i+slen], slen, c.Compress, rng)
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
	switch val := v.(type) {
	case string:
		return val
	case int:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'g', -1, 32)
	default:
		return fmt.Sprintf("%v", v)
	}
}
