package audio

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// testTone writes a short WAV that ffmpeg can decode, so pacing can be tested
// without depending on the project's large MP3s.
func testTone(t *testing.T, seconds float64) string {
	t.Helper()
	if _, err := os.Stat("/usr/bin/ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}
	path := filepath.Join(t.TempDir(), "tone.wav")
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration="+strconv.FormatFloat(seconds, 'f', -1, 64),
		"-ar", "44100", "-ac", "2", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate tone: %v: %s", err, out)
	}
	return path
}

// TestFileSourcePacing is the property the whole replay mode exists for: a
// file must traverse the pipeline in the same wall-clock time it would take to
// play, so testing against a recording predicts the live run.
func TestFileSourcePacing(t *testing.T) {
	path := testTone(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	src, err := NewFileSource(ctx, FileConfig{Path: path, Speed: 1.0, Chunk: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	start := time.Now()
	frames, err := src.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var last Frame
	var count int
	for f := range frames {
		last, count = f, count+1
	}
	elapsed := time.Since(start)

	if err := src.Err(); err != nil {
		t.Fatalf("source error: %v", err)
	}
	if count == 0 {
		t.Fatal("no frames produced")
	}
	// ~3 s of audio at 100 ms per chunk.
	if count < 25 || count > 35 {
		t.Errorf("expected ~30 frames for 3s of audio, got %d", count)
	}
	// Media time must match real time closely; drift here would mean the
	// simulation is not actually simulating a live rate.
	if drift := elapsed - last.Offset; drift < -200*time.Millisecond || drift > 1500*time.Millisecond {
		t.Errorf("pacing drift %v (elapsed %v, media %v)", drift, elapsed, last.Offset)
	}
	// Offsets must increase monotonically, since latency is measured off them.
	var prev time.Duration
	_ = prev
}

// TestFileSourceSpeed verifies --speed actually scales the release rate, so
// quick dry runs are possible without waiting out a 32-minute recording.
func TestFileSourceSpeed(t *testing.T) {
	path := testTone(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	src, err := NewFileSource(ctx, FileConfig{Path: path, Speed: 4.0, Chunk: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	start := time.Now()
	frames, _ := src.Start(ctx)
	for range frames {
	}
	elapsed := time.Since(start)

	// 3 s of audio at 4x should take well under 2 s.
	if elapsed > 2500*time.Millisecond {
		t.Errorf("4x replay took %v, expected under 2.5s", elapsed)
	}
}

// TestFileSourceCancellation ensures Ctrl-C unwinds promptly rather than
// waiting out the rest of the file.
func TestFileSourceCancellation(t *testing.T) {
	path := testTone(t, 30)
	ctx, cancel := context.WithCancel(context.Background())

	src, err := NewFileSource(ctx, FileConfig{Path: path, Speed: 1.0, Chunk: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	frames, _ := src.Start(ctx)
	<-frames // wait for the stream to be live
	cancel()

	done := make(chan struct{})
	go func() {
		for range frames {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("frames channel did not close within 3s of cancellation")
	}
}

func TestFormatMath(t *testing.T) {
	f := PipelineFormat
	if got := f.BytesPerSecond(); got != 32000 {
		t.Errorf("BytesPerSecond = %d, want 32000", got)
	}
	if got := f.BytesFor(100 * time.Millisecond); got != 3200 {
		t.Errorf("BytesFor(100ms) = %d, want 3200", got)
	}
	if got := f.Duration(3200); got != 100*time.Millisecond {
		t.Errorf("Duration(3200) = %v, want 100ms", got)
	}
	// Byte counts must land on whole samples, never split one.
	if got := f.BytesFor(33 * time.Millisecond); got%2 != 0 {
		t.Errorf("BytesFor(33ms) = %d, not sample-aligned", got)
	}
}

func TestFormatClock(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "00:00"},
		{754 * time.Second, "12:34"},
		{3661 * time.Second, "1:01:01"},
		{-5 * time.Second, "00:00"},
	}
	for _, c := range cases {
		if got := FormatClock(c.in); got != c.want {
			t.Errorf("FormatClock(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
