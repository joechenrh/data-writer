package util

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
)

// StartProgressLogger periodically logs file and byte progress.
func StartProgressLogger(
	totalFiles int,
	action string,
	files *atomic.Int32,
	bytes *atomic.Int64,
	interval time.Duration,
) {
	if totalFiles <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		prevFiles := int64(files.Load())
		prevBytes := bytes.Load()
		prevTime := time.Now()

		for range ticker.C {
			curFiles := int64(files.Load())
			curBytes := bytes.Load()
			now := time.Now()
			elapsed := now.Sub(prevTime).Seconds()

			filesPerSec := progressRate(curFiles-prevFiles, elapsed)
			bytesPerSec := progressRate(curBytes-prevBytes, elapsed)

			log.Printf(
				"Progress: %s files %d (%.2f files/s), %s size %s (%.2f MiB/s)",
				action,
				curFiles,
				filesPerSec,
				action,
				units.BytesSize(float64(curBytes)),
				bytesPerSec/float64(units.MiB),
			)

			prevFiles = curFiles
			prevBytes = curBytes
			prevTime = now

			if int(curFiles) >= totalFiles {
				break
			}
		}
	}()
}

func progressRate(delta int64, elapsedSeconds float64) float64 {
	if elapsedSeconds <= 0 {
		return 0
	}
	return float64(delta) / elapsedSeconds
}
