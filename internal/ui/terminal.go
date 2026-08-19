// Package ui owns the terminal.
//
// The governing rule is that captions are data and logs are diagnostics:
// finalized captions go to stdout, everything else to stderr. So
// `livecaption replay f.mp3 > captions.txt 2> run.log` splits cleanly and
// piping never mixes the two.
//
// Everything that writes to the terminal goes through one mutex here. The
// status line and the log handler both target stderr, and without a single
// owner they interleave into garbage.
package ui

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"livecaption/internal/audio"
	"livecaption/internal/metrics"
)

// ANSI helpers. All rendering goes through these so --no-color is one switch.
const (
	ansiReset  = "\033[0m"
	ansiDim    = "\033[2m"
	ansiBold   = "\033[1m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiBlue   = "\033[36m"
	clearLine  = "\r\033[K"
)

// Terminal renders captions, logs, the status line and the summary.
type Terminal struct {
	out   io.Writer // stdout: captions only
	err   io.Writer // stderr: everything else
	tty   bool      // stderr is a terminal, so ANSI and a status line are safe
	color bool
	quiet bool

	mu          sync.Mutex
	statusText  string
	statusShown bool
	stopped     bool
	done        chan struct{}
}

type Options struct {
	Out, Err io.Writer
	TTY      bool
	Color    bool
	Quiet    bool
}

func NewTerminal(o Options) *Terminal {
	if o.Out == nil {
		o.Out = os.Stdout
	}
	if o.Err == nil {
		o.Err = os.Stderr
	}
	return &Terminal{
		out:   o.Out,
		err:   o.Err,
		tty:   o.TTY,
		color: o.Color && o.TTY,
		quiet: o.Quiet,
		done:  make(chan struct{}),
	}
}

func (t *Terminal) c(code, s string) string {
	if !t.color {
		return s
	}
	return code + s + ansiReset
}

// --- Captions ---

// Caption prints one finalized line to stdout. Interim results are never
// printed: they would flood the scrollback, and the web page is what they are
// for.
func (t *Terminal) Caption(offset time.Duration, text string) {
	if t.quiet {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hideStatusLocked()
	stamp := fmt.Sprintf("[%s]", audio.FormatClock(offset))
	fmt.Fprintf(t.out, "%s %s\n", t.c(ansiDim, stamp), text)
	t.showStatusLocked()
}

// --- Log output ---

// writeLog is how the slog handler emits a line without fighting the status
// line for the cursor.
func (t *Terminal) writeLog(line string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hideStatusLocked()
	fmt.Fprintln(t.err, line)
	t.showStatusLocked()
}

// --- Banner ---

// BannerField is one aligned "label  value" row of the startup block.
type BannerField struct {
	Label string
	Value string
	Note  string // dimmed trailing note
}

// Banner prints the startup block, so misconfiguration is visible before any
// audio flows.
func (t *Terminal) Banner(title string, fields []BannerField) {
	if t.quiet {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	width := 0
	for _, f := range fields {
		if len(f.Label) > width {
			width = len(f.Label)
		}
	}
	fmt.Fprintln(t.err, t.c(ansiBold, title))
	for _, f := range fields {
		line := fmt.Sprintf("  %-*s  %s", width, f.Label, f.Value)
		if f.Note != "" {
			line += "  " + t.c(ansiDim, f.Note)
		}
		fmt.Fprintln(t.err, line)
	}
	fmt.Fprintln(t.err)
}

// Ready prints the "listening now" line that ends startup.
func (t *Terminal) Ready(msg string) {
	if t.quiet {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Fprintln(t.err, t.c(ansiDim, msg))
}

// --- Status line ---

// StartStatus begins redrawing the pinned status line twice a second. It is a
// no-op when stderr is not a terminal: ANSI cursor moves are garbage in a log
// file or a systemd journal.
func (t *Terminal) StartStatus(snapshot func() metrics.Snapshot) {
	if !t.tty || t.quiet {
		return
	}
	go func() {
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				t.setStatus(renderStatus(t, snapshot()))
			case <-t.done:
				return
			}
		}
	}()
}

func (t *Terminal) setStatus(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	t.statusText = s
	t.hideStatusLocked()
	t.showStatusLocked()
}

func (t *Terminal) hideStatusLocked() {
	if t.statusShown {
		fmt.Fprint(t.err, clearLine)
		t.statusShown = false
	}
}

func (t *Terminal) showStatusLocked() {
	if t.tty && !t.quiet && !t.stopped && t.statusText != "" {
		fmt.Fprint(t.err, t.statusText)
		t.statusShown = true
	}
}

// StopStatus removes the status line for good, before the summary is printed.
func (t *Terminal) StopStatus() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	t.stopped = true
	close(t.done)
	t.hideStatusLocked()
}

// renderStatus builds the pinned line:
//
//	▶ 00:04:31 / 31:51 │ stt ● connected │ lat 340ms p95 610ms │ 2 viewers │ 47 lines
func renderStatus(t *Terminal, s metrics.Snapshot) string {
	var b strings.Builder
	b.WriteString(t.c(ansiBlue, "▶"))
	b.WriteByte(' ')

	elapsed := time.Duration(s.Source.SecondsTotal * float64(time.Second))
	if s.Source.TotalSeconds > 0 {
		total := time.Duration(s.Source.TotalSeconds * float64(time.Second))
		fmt.Fprintf(&b, "%s / %s", audio.FormatClock(elapsed), audio.FormatClock(total))
	} else {
		b.WriteString(audio.FormatClock(elapsed))
	}

	b.WriteString(t.c(ansiDim, " │ "))
	b.WriteString("stt ")
	b.WriteString(t.stateGlyph(s.STT.State, s.STT.Reconnects))

	if s.STT.LatencyCount > 0 {
		b.WriteString(t.c(ansiDim, " │ "))
		fmt.Fprintf(&b, "lat %.0fms p95 %.0fms", s.STT.LatencyLast, s.STT.LatencyP95)
	}

	b.WriteString(t.c(ansiDim, " │ "))
	fmt.Fprintf(&b, "%d viewer%s", s.Web.Clients, plural(s.Web.Clients))

	b.WriteString(t.c(ansiDim, " │ "))
	fmt.Fprintf(&b, "%d line%s", s.STT.Final, plural(s.STT.Final))

	// Surface silent degradation right on the status line, not just /admin.
	if drops := s.Source.FramesDropped + s.Monitor.FramesDropped + s.Web.SlowDrops; drops > 0 {
		b.WriteString(t.c(ansiDim, " │ "))
		b.WriteString(t.c(ansiYellow, fmt.Sprintf("%d drop%s", drops, plural(drops))))
	}
	return b.String()
}

func (t *Terminal) stateGlyph(state string, reconnects int64) string {
	switch state {
	case "connected":
		return t.c(ansiGreen, "● connected")
	case "connecting":
		return t.c(ansiYellow, "◐ connecting")
	case "reconnecting":
		s := "○ reconnecting"
		if reconnects > 0 {
			s = fmt.Sprintf("○ reconnecting ×%d", reconnects)
		}
		return t.c(ansiYellow, s)
	case "closed":
		return t.c(ansiRed, "✕ closed")
	default:
		return t.c(ansiDim, "· idle")
	}
}

// --- Shutdown summary ---

// Summary prints the end-of-run report. Anything that degraded silently during
// the session shows up here in amber, reading from the same snapshot the admin
// page uses so the two can never disagree.
func (t *Terminal) Summary(s metrics.Snapshot, monitorEnabled bool) {
	t.StopStatus()
	if t.quiet {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Fprintln(t.err, t.c(ansiBold, "stopping…"))

	amber := func(n int64, format string, args ...any) string {
		s := fmt.Sprintf(format, args...)
		if n > 0 {
			return t.c(ansiYellow, s)
		}
		return s
	}

	rows := []BannerField{{
		Label: "audio",
		Value: fmt.Sprintf("%s processed, %s, %s, %s",
			audio.FormatClock(time.Duration(s.Source.SecondsTotal*float64(time.Second))),
			amber(s.Source.FramesDropped, "%d drop%s", s.Source.FramesDropped, plural(s.Source.FramesDropped)),
			amber(s.Source.FFmpegRestarts, "%d ffmpeg restart%s", s.Source.FFmpegRestarts, plural(s.Source.FFmpegRestarts)),
			amber(s.Source.Xruns, "%d xrun%s", s.Source.Xruns, plural(s.Source.Xruns))),
	}}
	if monitorEnabled {
		rows = append(rows, BannerField{
			Label: "monitor",
			Value: amber(s.Monitor.FramesDropped, "%d frame%s dropped",
				s.Monitor.FramesDropped, plural(s.Monitor.FramesDropped)),
		})
	}
	rows = append(rows, BannerField{
		Label: "captions",
		Value: fmt.Sprintf("%d line%s, %s", s.STT.Final, plural(s.STT.Final),
			amber(s.STT.Reconnects, "%d stt reconnect%s", s.STT.Reconnects, plural(s.STT.Reconnects))),
	})
	if s.STT.LatencyCount > 0 {
		rows = append(rows, BannerField{
			Label: "latency",
			Value: fmt.Sprintf("p50 %.0fms  p95 %.0fms  max %.0fms",
				s.STT.LatencyP50, s.STT.LatencyP95, s.STT.LatencyMax),
		})
	}
	if s.Transcript.Path != "" {
		rows = append(rows, BannerField{
			Label: "transcript",
			Value: fmt.Sprintf("%s (%d line%s, %s)", s.Transcript.Path,
				s.Transcript.Lines, plural(s.Transcript.Lines), humanBytes(s.Transcript.Bytes)),
		})
	}

	width := 0
	for _, r := range rows {
		if len(r.Label) > width {
			width = len(r.Label)
		}
	}
	for _, r := range rows {
		fmt.Fprintf(t.err, "  %-*s  %s\n", width, r.Label, r.Value)
	}
	fmt.Fprintln(t.err, t.c(ansiDim, "done"))
}

func plural(n int64) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
