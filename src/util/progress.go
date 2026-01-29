package util

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/schollz/progressbar/v3"
)

const (
	progressPrefixWidth = 52
	progressBarWidth    = 32
)

// ProgressLogger tracks and renders progress for file and byte counts.
type ProgressLogger struct {
	totalFiles int
	action     string
	interval   time.Duration
	files      atomic.Int32
	bytes      atomic.Int64
	bar        *progressbar.ProgressBar
}

var (
	globalProgressLogger *ProgressLogger
)

// InitializeProgressLogger creates and starts a progress logger.
func InitializeProgressLogger(
	totalFiles int,
	action string,
	interval time.Duration,
) *ProgressLogger {
	if globalProgressLogger == nil {
		globalProgressLogger = &ProgressLogger{
			totalFiles: totalFiles,
			action:     action,
			interval:   interval,
		}
		globalProgressLogger.start()
	}
	return globalProgressLogger
}

func GetProgressLogger() *ProgressLogger {
	return globalProgressLogger
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

// Snapshot returns the current file and byte counts.
func (p *ProgressLogger) Snapshot() (int64, int64) {
	return int64(p.files.Load()), p.bytes.Load()
}

func (p *ProgressLogger) start() {
	if p.totalFiles <= 0 {
		return
	}

	p.bar = NewFileProgressBar(p.totalFiles, p.action)

	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		prevFiles := int64(p.files.Load())
		prevBytes := p.bytes.Load()
		prevTime := time.Now()
		lastDesc := ""

		for range ticker.C {
			curFiles := int64(p.files.Load())
			curBytes := p.bytes.Load()
			now := time.Now()
			elapsed := now.Sub(prevTime).Seconds()

			filesDelta := max(curFiles-prevFiles, 0)
			bytesPerSec := progressRate(curBytes-prevBytes, elapsed)
			filesPerSec := progressRate(curFiles-prevFiles, elapsed)
			desc := progressDescription(p.action, curBytes, bytesPerSec, filesPerSec)
			if desc != lastDesc {
				p.bar.Describe(desc)
				lastDesc = desc
			}
			if filesDelta > 0 {
				_ = p.bar.Add64(filesDelta)
			}

			prevFiles = curFiles
			prevBytes = curBytes
			prevTime = now

			if int(curFiles) >= p.totalFiles {
				_ = p.bar.Finish()
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

// NewFileProgressBar creates a themed progress bar for file-based work.
func NewFileProgressBar(totalFiles int, action string) *progressbar.ProgressBar {
	return progressbar.NewOptions(
		totalFiles,
		progressbar.OptionSetWriter(os.Stdout),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetDescription(progressDescription(action, 0, 0, 0)),
		progressbar.OptionSetPredictTime(false),
		progressbar.OptionSetElapsedTime(false),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(progressBarWidth),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprintln(os.Stdout)
		}),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[light_magenta]━",
			SaucerHead:    "[light_magenta]╸",
			SaucerPadding: "[dark_gray]━",
			BarStart:      "",
			BarEnd:        "[reset]",
		}),
	)
}

func progressDescription(action string, bytes int64, bytesPerSec float64, filesPerSec float64) string {
	prefix := fmt.Sprintf(
		"%s %s (%s/s, %.2f files/s)",
		action,
		units.BytesSize(float64(bytes)),
		units.BytesSize(bytesPerSec),
		filesPerSec,
	)
	return padOrTrim(prefix, progressPrefixWidth) + " "
}

func padOrTrim(s string, width int) string {
	if width <= 0 {
		return s
	}
	if len(s) > width {
		if width <= 3 {
			return s[:width]
		}
		return s[:width-3] + "..."
	}
	if len(s) < width {
		return s + strings.Repeat(" ", width-len(s))
	}
	return s
}
