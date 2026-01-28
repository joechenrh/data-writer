package util

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
)

// ProgressLogger tracks and logs progress for file and byte counts.
type ProgressLogger struct {
	totalFiles int
	action     string
	interval   time.Duration
	files      atomic.Int32
	bytes      atomic.Int64
}

// NewProgressLogger creates and starts a progress logger.
func NewProgressLogger(totalFiles int, action string, interval time.Duration) *ProgressLogger {
	logger := &ProgressLogger{
		totalFiles: totalFiles,
		action:     action,
		interval:   interval,
	}
	logger.start()
	return logger
}

// UpdateBytes increments the byte counter.
func (p *ProgressLogger) UpdateBytes(delta int64) {
	if delta == 0 {
		return
	}
	p.bytes.Add(delta)
}

// UpdateFiles increments the file counter.
func (p *ProgressLogger) UpdateFiles(delta int32) {
	if delta == 0 {
		return
	}
	p.files.Add(delta)
}

func (p *ProgressLogger) start() {
	if p.totalFiles <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		prevFiles := int64(p.files.Load())
		prevBytes := p.bytes.Load()
		prevTime := time.Now()

		for range ticker.C {
			curFiles := int64(p.files.Load())
			curBytes := p.bytes.Load()
			now := time.Now()
			elapsed := now.Sub(prevTime).Seconds()

			filesPerSec := progressRate(curFiles-prevFiles, elapsed)
			bytesPerSec := progressRate(curBytes-prevBytes, elapsed)

			log.Printf(
				"Progress: %s files %d (%.2f files/s), %s size %s (%.2f MiB/s)",
				p.action,
				curFiles,
				filesPerSec,
				p.action,
				units.BytesSize(float64(curBytes)),
				bytesPerSec/float64(units.MiB),
			)

			prevFiles = curFiles
			prevBytes = curBytes
			prevTime = now

			if int(curFiles) >= p.totalFiles {
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
