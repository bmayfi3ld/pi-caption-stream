package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "audio.mp3")
	if err := os.WriteFile(p, []byte("not really audio"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestMonitorRequiresRealtimeSpeed covers the one flag combination that cannot
// work: the sound card drains at a fixed rate, so replaying faster than
// wall-clock just overflows the playback buffer. Rejecting it at parse time
// beats letting the user wonder why the audio is stuttering.
func TestMonitorRequiresRealtimeSpeed(t *testing.T) {
	file := writeTempFile(t)

	_, _, err := Parse([]string{"replay", file, "--monitor", "--speed", "2"})
	if err == nil {
		t.Fatal("expected --monitor with --speed 2 to be rejected")
	}
	if !strings.Contains(err.Error(), "--speed 1.0") {
		t.Errorf("error should explain the constraint, got: %v", err)
	}

	// The same flags at wall-clock rate are fine.
	if _, _, err := Parse([]string{"replay", file, "--monitor"}); err != nil {
		t.Errorf("--monitor at default speed should be accepted: %v", err)
	}
}

func TestRejectsNonPositiveSpeed(t *testing.T) {
	file := writeTempFile(t)
	if _, _, err := Parse([]string{"replay", file, "--speed", "0"}); err == nil {
		t.Error("expected --speed 0 to be rejected")
	}
}

// TestMissingFileIsRejected relies on kong's existingfile type, so a typo is
// caught before ffmpeg is spawned.
func TestMissingFileIsRejected(t *testing.T) {
	if _, _, err := Parse([]string{"replay", "/nonexistent/nope.mp3"}); err == nil {
		t.Error("expected a missing file to be rejected")
	}
}

func TestEnumsAreEnforced(t *testing.T) {
	file := writeTempFile(t)
	for _, args := range [][]string{
		{"replay", file, "--engine", "nonsense"},
		{"live", "--device", "x", "--backend", "nonsense"},
		{"replay", file, "--log-level", "nonsense"},
	} {
		if _, _, err := Parse(args); err == nil {
			t.Errorf("expected %v to be rejected", args)
		}
	}
}

// TestAPIKeyComesFromEnvironment keeps the key out of shell history.
func TestAPIKeyComesFromEnvironment(t *testing.T) {
	file := writeTempFile(t)
	t.Setenv("DEEPGRAM_API_KEY", "secret-from-env")

	_, c, err := Parse([]string{"replay", file})
	if err != nil {
		t.Fatal(err)
	}
	if c.Replay.APIKey != "secret-from-env" {
		t.Errorf("APIKey = %q, want it bound from DEEPGRAM_API_KEY", c.Replay.APIKey)
	}
}

func TestLiveRequiresDevice(t *testing.T) {
	if _, _, err := Parse([]string{"live"}); err == nil {
		t.Error("expected 'live' without --device to be rejected")
	}
}

// TestTranscriptsDefaultOn guards the intent that recording is the default
// behaviour, not something to remember to switch on.
func TestTranscriptsDefaultOn(t *testing.T) {
	file := writeTempFile(t)
	_, c, err := Parse([]string{"replay", file})
	if err != nil {
		t.Fatal(err)
	}
	if c.Replay.NoTranscript {
		t.Error("transcripts should be enabled unless --no-transcript is given")
	}
	if c.Replay.TranscriptDir == "" {
		t.Error("transcript dir should have a default")
	}
}

func TestRequireAPIKey(t *testing.T) {
	if err := requireAPIKey("mock", ""); err != nil {
		t.Errorf("mock engine should not need a key: %v", err)
	}
	if err := requireAPIKey("deepgram", ""); err == nil {
		t.Error("deepgram without a key should fail fast")
	} else if !strings.Contains(err.Error(), "DEEPGRAM_API_KEY") {
		t.Errorf("error should name the env var, got: %v", err)
	}
	if err := requireAPIKey("deepgram", "k"); err != nil {
		t.Errorf("deepgram with a key should pass: %v", err)
	}
}

func TestChunkDurationBounds(t *testing.T) {
	if _, err := chunkDuration(100); err != nil {
		t.Errorf("100ms should be valid: %v", err)
	}
	for _, ms := range []int{0, 10, 501, -1} {
		if _, err := chunkDuration(ms); err == nil {
			t.Errorf("chunkDuration(%d) should be rejected", ms)
		}
	}
}

func TestBrowserURL(t *testing.T) {
	cases := map[string]string{
		":8080":          "http://localhost:8080",
		"0.0.0.0:8080":   "http://localhost:8080",
		"127.0.0.1:9000": "http://127.0.0.1:9000",
	}
	for in, want := range cases {
		if got := browserURL(in); got != want {
			t.Errorf("browserURL(%q) = %q, want %q", in, got, want)
		}
	}
}
