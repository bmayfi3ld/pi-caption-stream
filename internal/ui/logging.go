package ui

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"golang.org/x/term"

	"livecaption/internal/audio"
)

// LogConfig resolves how diagnostics are rendered.
type LogConfig struct {
	Level   string // debug | info | warn | error
	Format  string // auto | pretty | json
	Verbose bool
	Quiet   bool
	NoColor bool
}

// Level maps the flag to a slog level. --verbose wins over --log-level so the
// short flag always does what you expect.
func (c LogConfig) level() slog.Level {
	if c.Verbose {
		return slog.LevelDebug
	}
	switch strings.ToLower(c.Level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Setup builds the terminal and logger together, because the pretty handler
// has to write through the terminal's mutex to coexist with the status line.
//
// Format "auto" means: pretty when stderr is a terminal, JSON otherwise. That
// makes the same binary pleasant interactively and parseable under systemd
// with no flags.
func Setup(c LogConfig) (*Terminal, *slog.Logger) {
	isTTY := term.IsTerminal(int(os.Stderr.Fd()))

	pretty := isTTY
	switch strings.ToLower(c.Format) {
	case "pretty":
		pretty = true
	case "json":
		pretty = false
	}

	color := !c.NoColor && isTTY && os.Getenv("TERM") != "dumb"

	t := NewTerminal(Options{
		TTY:   isTTY && pretty,
		Color: color,
		Quiet: c.Quiet,
	})

	// --quiet means warnings and errors only, whatever the level flag says.
	level := c.level()
	if c.Quiet && level < slog.LevelWarn {
		level = slog.LevelWarn
	}

	var h slog.Handler
	if pretty {
		h = &prettyHandler{term: t, level: level, start: time.Now()}
	} else {
		h = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	}
	log := slog.New(h)
	slog.SetDefault(log)
	return t, log
}

// prettyHandler renders logs for a human watching a live event: a glyph, a
// relative timestamp, the message, then any attributes. Warnings are the level
// that matters mid-event, so they get the colour.
type prettyHandler struct {
	term  *Terminal
	level slog.Level
	start time.Time
	attrs []slog.Attr
	group string
}

func (h *prettyHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h *prettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	n := *h
	n.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &n
}

func (h *prettyHandler) WithGroup(name string) slog.Handler {
	n := *h
	n.group = name
	return &n
}

func (h *prettyHandler) Handle(_ context.Context, r slog.Record) error {
	t := h.term
	var glyph, msgColor string
	switch {
	case r.Level >= slog.LevelError:
		glyph, msgColor = t.c(ansiRed, "✕"), ansiRed
	case r.Level >= slog.LevelWarn:
		glyph, msgColor = t.c(ansiYellow, "⚠"), ansiYellow
	case r.Level <= slog.LevelDebug:
		glyph, msgColor = t.c(ansiDim, "·"), ansiDim
	default:
		glyph, msgColor = t.c(ansiGreen, "✓"), ""
	}

	var b strings.Builder
	b.WriteString(glyph)
	b.WriteByte(' ')
	b.WriteString(t.c(ansiDim, audio.FormatClock(time.Since(h.start))))
	b.WriteString("  ")
	if msgColor != "" {
		b.WriteString(t.c(msgColor, r.Message))
	} else {
		b.WriteString(r.Message)
	}

	writeAttr := func(a slog.Attr) {
		b.WriteByte(' ')
		b.WriteString(t.c(ansiDim, a.Key+"="))
		b.WriteString(t.c(ansiDim, formatValue(a.Value)))
	}
	for _, a := range h.attrs {
		writeAttr(a)
	}
	r.Attrs(func(a slog.Attr) bool { writeAttr(a); return true })

	t.writeLog(b.String())
	return nil
}

func formatValue(v slog.Value) string {
	s := v.String()
	if strings.ContainsAny(s, " \t") {
		return fmt.Sprintf("%q", s)
	}
	return s
}
