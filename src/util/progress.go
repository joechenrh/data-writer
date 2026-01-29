package util

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/docker/go-units"
	"github.com/schollz/progressbar/v3"
)

const (
	progressBarWidth      = 34
	progressBoxInnerWidth = 92
	progressLines         = 4
	leftColumnWidth       = 52
	spaceBetweenColumns   = 1
	borderSidesWidth      = 2
)

const (
	colorReset      = "\x1b[0m"
	colorMagenta    = "\x1b[95m"
	colorDarkGray   = "\x1b[90m"
	ansiEscapeStart = '\x1b'
)

// ProgressLogger tracks and renders progress for file and byte counts.
type ProgressLogger struct {
	totalFiles int
	action     string
	interval   time.Duration
	files      atomic.Int32
	bytes      atomic.Int64
	format     string
	platform   string
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
			format:     "-",
			platform:   "-",
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

// SetContext sets the format/platform for display.
func (p *ProgressLogger) SetContext(format string, platform string) {
	if format != "" {
		p.format = format
	}
	if platform != "" {
		p.platform = platform
	}
}

// Snapshot returns the current file and byte counts.
func (p *ProgressLogger) Snapshot() (int64, int64) {
	return int64(p.files.Load()), p.bytes.Load()
}

func (p *ProgressLogger) start() {
	if p.totalFiles <= 0 {
		return
	}

	first := true
	go func() {
		// Update the same 2-line box in place using ANSI cursor moves.
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

			bytesPerSec := progressRate(curBytes-prevBytes, elapsed)
			filesPerSec := progressRate(curFiles-prevFiles, elapsed)

			box := progressBox(
				p.totalFiles,
				curFiles,
				curBytes,
				bytesPerSec,
				filesPerSec,
				p.action,
				p.format,
				p.platform,
			)

			if !first {
				fmt.Fprintf(os.Stdout, "\033[%dA", progressLines)
			}
			fmt.Fprint(os.Stdout, box)
			first = false

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

// NewFileProgressBar creates a themed progress bar for file-based work.
func NewFileProgressBar(totalFiles int, action string) *progressbar.ProgressBar {
	return progressbar.NewOptions(
		totalFiles,
		progressbar.OptionSetWriter(os.Stdout),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetDescription(action),
		progressbar.OptionSetPredictTime(false),
		progressbar.OptionSetElapsedTime(false),
		progressbar.OptionSetWidth(progressBarWidth),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprintln(os.Stdout)
		}),
	)
}

// progressBox renders a boxed 2x2 layout:
// left: progress bar + throughput, right: format + platform.
func progressBox(
	total int,
	files int64,
	bytes int64,
	bytesPerSec float64,
	filesPerSec float64,
	action string,
	format string,
	platform string,
) string {
	rightColumnWidth := progressBoxInnerWidth - leftColumnWidth - spaceBetweenColumns - borderSidesWidth

	percent := 0.0
	if total > 0 {
		percent = float64(files) / float64(total)
	}
	bar := renderBar(percent, progressBarWidth)
	leftTop := fmt.Sprintf("%3d%% %s", int(percent*100), bar)
	rightTop := "Format: " + format

	leftBottom := fmt.Sprintf(
		"%s %s (%s/s, %.2f files/s)",
		action,
		units.BytesSize(float64(bytes)),
		units.BytesSize(bytesPerSec),
		filesPerSec,
	)
	rightBottom := "Platform: " + platform

	var b strings.Builder
	b.WriteString(progressBoxTopLine())
	leftTop = padOrTrim(leftTop, leftColumnWidth)
	rightTop = padOrTrim(rightTop, rightColumnWidth)
	b.WriteString("│ ")
	b.WriteString(leftTop)
	b.WriteString(strings.Repeat(" ", leftColumnWidth-visibleLen(leftTop)))
	b.WriteString(" ")
	b.WriteString(rightTop)
	b.WriteString(strings.Repeat(" ", rightColumnWidth-visibleLen(rightTop)))
	b.WriteString(" │\n")

	leftBottom = padOrTrim(leftBottom, leftColumnWidth)
	rightBottom = padOrTrim(rightBottom, rightColumnWidth)
	b.WriteString("│ ")
	b.WriteString(leftBottom)
	b.WriteString(strings.Repeat(" ", leftColumnWidth-visibleLen(leftBottom)))
	b.WriteString(" ")
	b.WriteString(rightBottom)
	b.WriteString(strings.Repeat(" ", rightColumnWidth-visibleLen(rightBottom)))
	b.WriteString(" │\n")
	b.WriteString(progressBoxBottomLine())

	return b.String()
}

// renderBar builds a colored bar like: ━━━╸━━━━ using ANSI colors.
func renderBar(percent float64, width int) string {
	if width <= 0 {
		return ""
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 1 {
		percent = 1
	}
	filled := int(percent * float64(width))
	if filled <= 0 {
		return colorDarkGray + strings.Repeat("━", width) + colorReset
	}

	head := "╸"
	if filled >= width {
		head = "━"
	}

	filledBody := ""
	if filled > 1 {
		filledBody = strings.Repeat("━", filled-1)
	}
	padding := ""
	if filled < width {
		padding = strings.Repeat("━", width-filled)
	}

	return colorMagenta + filledBody + head + colorReset + colorDarkGray + padding + colorReset
}

// padOrTrim truncates/pads to display width, preserving ANSI escapes.
func padOrTrim(s string, width int) string {
	if width <= 0 {
		return s
	}
	visible := visibleLen(s)
	if visible > width {
		if width <= 3 {
			return trimANSI(s, width)
		}
		return trimANSI(s, width-3) + "..."
	}
	if visible < width {
		return s + strings.Repeat(" ", width-visible)
	}
	return s
}

// visibleLen counts display cells, ignoring ANSI escapes.
func visibleLen(s string) int {
	count := 0
	inEscape := false
	for i := 0; i < len(s); i++ {
		if inEscape {
			if s[i] == 'm' {
				inEscape = false
			}
			continue
		}
		if s[i] == ansiEscapeStart {
			inEscape = true
			continue
		}
		if (s[i] & 0xC0) != 0x80 {
			r, size := utf8.DecodeRuneInString(s[i:])
			count += runeDisplayWidth(r)
			i += size - 1
		}
	}
	return count
}

// trimANSI trims to display width while preserving ANSI escapes.
func trimANSI(s string, width int) string {
	if width <= 0 {
		return ""
	}
	var b strings.Builder
	count := 0
	inEscape := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inEscape {
			b.WriteByte(ch)
			if ch == 'm' {
				inEscape = false
			}
			continue
		}
		if ch == ansiEscapeStart {
			inEscape = true
			b.WriteByte(ch)
			continue
		}
		if (ch & 0xC0) != 0x80 {
			r, size := utf8.DecodeRuneInString(s[i:])
			w := runeDisplayWidth(r)
			if count+w > width {
				break
			}
			count += w
			b.WriteString(string(r))
			i += size - 1
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// runeDisplayWidth approximates terminal width for common wide/emoji runes.
func runeDisplayWidth(r rune) int {
	// Basic wide/emoji handling to keep columns aligned.
	switch {
	case r >= 0x1100 && (r <= 0x115F || r == 0x2329 || r == 0x232A ||
		(r >= 0x2E80 && r <= 0xA4CF && r != 0x303F) ||
		(r >= 0xAC00 && r <= 0xD7A3) ||
		(r >= 0xF900 && r <= 0xFAFF) ||
		(r >= 0xFE10 && r <= 0xFE19) ||
		(r >= 0xFE30 && r <= 0xFE6F) ||
		(r >= 0xFF00 && r <= 0xFF60) ||
		(r >= 0xFFE0 && r <= 0xFFE6)):
		return 2
	case r >= 0x1F300 && r <= 0x1FAFF:
		return 2
	default:
		return 1
	}
}

func progressBoxTopLine() string {
	return "╭" + strings.Repeat("─", progressBoxInnerWidth) + "╮\n"
}

func progressBoxBottomLine() string {
	return "╰" + strings.Repeat("─", progressBoxInnerWidth) + "╯\n"
}
