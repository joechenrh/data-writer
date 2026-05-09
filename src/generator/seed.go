package generator

import "time"

// seedForFile derives a unique 64-bit RNG seed from a file index. Two files
// with different fileNo always get different seeds (so their rng-driven
// columns can't collide), and the time component gives run-to-run variation
// across full re-generations of the same file set.
//
// 0x9E3779B97F4A7C15 is the 64-bit golden-ratio constant; multiplying fileNo
// by it spreads consecutive indices far apart in seed space.
func seedForFile(fileNo int) int64 {
	const golden uint64 = 0x9E3779B97F4A7C15
	return int64(uint64(fileNo)*golden) ^ time.Now().UnixNano()
}
