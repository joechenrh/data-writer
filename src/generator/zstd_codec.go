package generator

import (
	"sync"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/klauspost/compress/zstd"
)

// arrow-go's default zstd codec creates a fresh zstd.Encoder on every page
// write (parquet/compress/zstd.go EncodeLevel calls zstd.NewWriter inline).
// In profiles, zstd.(*Encoder).initialize alone accounts for ~30% of total CPU
// for parquet workloads. cachedZstdCodec replaces it with a per-level encoder
// cache: each *zstd.Encoder maintains an internal goroutine pool and is safe
// for concurrent EncodeAll/DecodeAll calls.

type cachedZstdCodec struct{}

var (
	zstdEncoders sync.Map // int level -> *zstd.Encoder
	zstdDecoder  *zstd.Decoder
	zstdDecOnce  sync.Once
)

func getZstdEncoder(level int) *zstd.Encoder {
	if v, ok := zstdEncoders.Load(level); ok {
		return v.(*zstd.Encoder)
	}
	var compressLevel zstd.EncoderLevel
	if level == compress.DefaultCompressionLevel {
		compressLevel = zstd.SpeedDefault
	} else {
		compressLevel = zstd.EncoderLevelFromZstd(level)
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true), zstd.WithEncoderLevel(compressLevel))
	if actual, loaded := zstdEncoders.LoadOrStore(level, enc); loaded {
		_ = enc.Close()
		return actual.(*zstd.Encoder)
	}
	return enc
}

func getZstdDecoder() *zstd.Decoder {
	zstdDecOnce.Do(func() {
		zstdDecoder, _ = zstd.NewReader(nil)
	})
	return zstdDecoder
}

func (cachedZstdCodec) Encode(dst, src []byte) []byte {
	return getZstdEncoder(compress.DefaultCompressionLevel).EncodeAll(src, dst[:0])
}

func (cachedZstdCodec) EncodeLevel(dst, src []byte, level int) []byte {
	return getZstdEncoder(level).EncodeAll(src, dst[:0])
}

func (cachedZstdCodec) CompressBound(srcLen int64) int64 {
	extra := ((128 << 10) - srcLen) >> 11
	if srcLen >= (128 << 10) {
		extra = 0
	}
	return srcLen + (srcLen >> 8) + extra
}

func (cachedZstdCodec) Decode(dst, src []byte) []byte {
	out, err := getZstdDecoder().DecodeAll(src, dst[:0])
	if err != nil {
		panic(err)
	}
	return out
}

func init() {
	compress.RegisterCodec(compress.Codecs.Zstd, cachedZstdCodec{})
}
