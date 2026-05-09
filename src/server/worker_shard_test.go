package server

import (
	"encoding/json"
	"testing"

	"dataWriter/src/config"
)

func TestShardRangeEvenSplit(t *testing.T) {
	// 100 files split into 4 shards: 25 each.
	cases := []struct {
		shard, total, wantLo, wantHi int
	}{
		{0, 4, 0, 25},
		{1, 4, 25, 50},
		{2, 4, 50, 75},
		{3, 4, 75, 100},
	}
	for _, c := range cases {
		lo, hi := shardRange(0, 100, c.shard, c.total)
		if lo != c.wantLo || hi != c.wantHi {
			t.Errorf("shardRange(0,100,%d,%d) = [%d,%d), want [%d,%d)",
				c.shard, c.total, lo, hi, c.wantLo, c.wantHi)
		}
	}
}

func TestShardRangeUnevenSplit(t *testing.T) {
	// 10 files into 3 shards: 4,3,3 (remainder distributed to first shards).
	got := [][2]int{}
	for s := range 3 {
		lo, hi := shardRange(0, 10, s, 3)
		got = append(got, [2]int{lo, hi})
	}
	want := [][2]int{{0, 4}, {4, 7}, {7, 10}}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("shard %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

func TestShardRangeOffset(t *testing.T) {
	// Non-zero start should be respected.
	lo, hi := shardRange(50, 60, 1, 2)
	if lo != 55 || hi != 60 {
		t.Errorf("shardRange(50,60,1,2) = [%d,%d), want [55,60)", lo, hi)
	}
}

func TestShardRangeFullRange(t *testing.T) {
	// All shards together must cover the original range exactly.
	const start, end = 7, 103
	for total := 1; total <= 8; total++ {
		var covered int
		prevHi := start
		for s := range total {
			lo, hi := shardRange(start, end, s, total)
			if lo != prevHi {
				t.Errorf("total=%d shard=%d gap or overlap: prevHi=%d lo=%d", total, s, prevHi, lo)
			}
			covered += hi - lo
			prevHi = hi
		}
		if covered != end-start {
			t.Errorf("total=%d covered=%d want=%d", total, covered, end-start)
		}
		if prevHi != end {
			t.Errorf("total=%d last hi=%d want=%d", total, prevHi, end)
		}
	}
}

func TestDecideShardCount(t *testing.T) {
	// 10 byte rows × 100k rows × 100 files = 100 MB → 1 shard.
	// To trigger N shards we need ~5 TiB of estimated output per shard.
	const sql = `CREATE TABLE t.t (id BIGINT NOT NULL PRIMARY KEY, pad CHAR(60))`

	build := func(files, rows int) []byte {
		cfg := &config.Config{
			Common: config.CommonConfig{
				Path:        "/tmp",
				Prefix:      "t.t",
				StartFileNo: 0,
				EndFileNo:   files,
				Rows:        rows,
				FileFormat:  "csv",
			},
			CSV: config.CSVConfig{Separator: ",", EndLine: "\n"},
		}
		b, _ := json.Marshal(cfg)
		return b
	}

	// Tiny job → 1 shard.
	if n := decideShardCount(sql, build(10, 1000)); n != 1 {
		t.Errorf("tiny job: got %d shards, want 1", n)
	}

	// 100 files × 60M rows × ~70 bytes/row ≈ 391 GiB → still under 5 TiB → 1 shard.
	if n := decideShardCount(sql, build(100, 60_000_000)); n != 1 {
		t.Errorf("medium job: got %d shards, want 1", n)
	}

	// Force a huge job: 1000 files × 100M rows × ~70 bytes ≈ 6.36 TiB → ~1 shard.
	// Push it harder: 5000 files × 200M rows ≈ 63 TiB → capped at 8 shards.
	if n := decideShardCount(sql, build(5000, 200_000_000)); n != 8 {
		t.Errorf("huge job: got %d shards, want 8 (capped)", n)
	}

	// Shard count must not exceed file count.
	// 3 files, even if each were absurdly large, can be at most 3 shards.
	if n := decideShardCount(sql, build(3, 1_000_000_000)); n > 3 {
		t.Errorf("3-file job: got %d shards, want <= 3", n)
	}
}
