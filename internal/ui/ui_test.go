package ui

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"livecaption/internal/metrics"
)

// TestCaptionsAndLogsNeverMix is the governing rule of this package: captions
// go to stdout, logs go to stderr. `replay f.mp3 > captions.txt 2> run.log`
// only splits cleanly if nothing ever crosses streams.
func TestCaptionsAndLogsNeverMix(t *testing.T) {
	var out, errBuf bytes.Buffer
	term := NewTerminal(Options{Out: &out, Err: &errBuf})

	term.Caption(90*time.Second, "hello there")
	term.writeLog("a diagnostic line")

	if !strings.Contains(out.String(), "hello there") {
		t.Errorf("caption missing from stdout: %q", out.String())
	}
	if strings.Contains(out.String(), "diagnostic") {
		t.Errorf("a log line leaked onto stdout: %q", out.String())
	}
	if !strings.Contains(errBuf.String(), "diagnostic") {
		t.Errorf("log missing from stderr: %q", errBuf.String())
	}
	if strings.Contains(errBuf.String(), "hello there") {
		t.Errorf("a caption leaked onto stderr: %q", errBuf.String())
	}
}

// TestQuietSuppressesCaptions covers --quiet: captions must not reach stdout
// at all, not merely be muted on the status line.
func TestQuietSuppressesCaptions(t *testing.T) {
	var out, errBuf bytes.Buffer
	term := NewTerminal(Options{Out: &out, Err: &errBuf, Quiet: true})

	term.Caption(time.Second, "should not appear")
	term.Banner("title", []BannerField{{Label: "x", Value: "y"}})
	term.Ready("ready")

	if out.Len() != 0 {
		t.Errorf("stdout should be empty under --quiet, got %q", out.String())
	}
	if errBuf.Len() != 0 {
		t.Errorf("stderr should be empty under --quiet (banner/ready suppressed too), got %q", errBuf.String())
	}
}

// TestNoTTYProducesNoANSI is what makes piping to a log file usable: with TTY
// false (or colour off), output must be plain text, not escape sequences a
// log viewer would render as garbage.
func TestNoTTYProducesNoANSI(t *testing.T) {
	var out, errBuf bytes.Buffer
	term := NewTerminal(Options{Out: &out, Err: &errBuf, TTY: false, Color: true})

	term.Caption(time.Second, "plain text")
	term.Banner("title", []BannerField{{Label: "x", Value: "y", Note: "note"}})
	term.Ready("go")

	for _, s := range []string{out.String(), errBuf.String()} {
		if strings.Contains(s, "\033[") {
			t.Errorf("ANSI escape leaked with TTY=false: %q", s)
		}
	}
}

// TestColorDisabledProducesNoANSI is the same guarantee via --no-color on an
// otherwise real terminal.
func TestColorDisabledProducesNoANSI(t *testing.T) {
	var out, errBuf bytes.Buffer
	term := NewTerminal(Options{Out: &out, Err: &errBuf, TTY: true, Color: false})

	term.Caption(time.Second, "plain text")
	term.Banner("title", []BannerField{{Label: "x", Value: "y"}})

	for _, s := range []string{out.String(), errBuf.String()} {
		if strings.Contains(s, "\033[") {
			t.Errorf("ANSI escape leaked with Color=false: %q", s)
		}
	}
}

// TestSummaryRendersDegradationCounts is what operators read at the end of a
// session: any silent degradation must be visible in the summary text, since
// that's the same snapshot /admin uses.
func TestSummaryRendersDegradationCounts(t *testing.T) {
	var out, errBuf bytes.Buffer
	term := NewTerminal(Options{Out: &out, Err: &errBuf, TTY: true})

	var snap metrics.Snapshot
	snap.Source.FramesDropped = 3
	snap.Source.FFmpegRestarts = 1
	snap.STT.Reconnects = 2
	snap.STT.Final = 47

	term.Summary(snap, false)
	got := errBuf.String()

	for _, want := range []string{"3 drop", "1 ffmpeg restart", "2 stt reconnect", "47 line"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary missing %q: %s", want, got)
		}
	}
}

// TestSummaryCleanSessionHasNoDegradationNoise is the flip side: a pristine
// snapshot's counts should still render (as zero), not be hidden — but the
// numbers should say zero, so a clean session reads as clean.
func TestSummaryCleanSessionReportsZero(t *testing.T) {
	var out, errBuf bytes.Buffer
	term := NewTerminal(Options{Out: &out, Err: &errBuf, TTY: true})

	var snap metrics.Snapshot
	snap.STT.Final = 5

	term.Summary(snap, false)
	got := errBuf.String()
	if !strings.Contains(got, "0 drops") {
		t.Errorf("summary should report 0 drops for a clean session: %s", got)
	}
}

// TestConcurrentCaptionAndLogDoNotInterleave exercises the single-mutex
// ownership claim: many goroutines writing captions and logs at once must
// never produce a line that mixes two writers' text. Run with -race.
func TestConcurrentCaptionAndLogDoNotInterleave(t *testing.T) {
	var out, errBuf bytes.Buffer
	term := NewTerminal(Options{Out: &out, Err: &errBuf})

	const n = 200
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			term.Caption(time.Duration(i)*time.Second, "CAPTION-LINE-MARKER")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			term.writeLog("LOG-LINE-MARKER")
		}
	}()
	wg.Wait()

	for _, line := range strings.Split(out.String(), "\n") {
		if line == "" {
			continue
		}
		if !strings.Contains(line, "CAPTION-LINE-MARKER") || strings.Contains(line, "LOG-LINE-MARKER") {
			t.Fatalf("garbled or cross-contaminated stdout line: %q", line)
		}
	}
	for _, line := range strings.Split(errBuf.String(), "\n") {
		if line == "" {
			continue
		}
		if !strings.Contains(line, "LOG-LINE-MARKER") || strings.Contains(line, "CAPTION-LINE-MARKER") {
			t.Fatalf("garbled or cross-contaminated stderr line: %q", line)
		}
	}
}
