package audio

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

func requireFFmpeg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}
}

func TestDeviceDescribe(t *testing.T) {
	s := NewDeviceSource(DeviceConfig{Device: "hw:2,0", Backend: "alsa"})
	if got, want := s.Describe(), "alsa:hw:2,0 (-> 16000 Hz mono)"; got != want {
		t.Errorf("Describe() = %q, want %q", got, want)
	}
}

// TestSetCallbacksWiresThrough guards the metric hooks that make the live
// hardening story (ffmpeg restarts, xruns, stderr) actually observable —
// a hook that silently fails to register would leave those counters dark.
func TestSetCallbacksWiresThrough(t *testing.T) {
	s := NewDeviceSource(DeviceConfig{Device: "x"})
	s.SetCallbacks(DeviceCallbacks{
		OnFrame:   func(int, time.Duration) {},
		OnXrun:    func() {},
		OnRestart: func() {},
		OnStderr:  func(string) {},
	})
	if s.cfg.OnFrame == nil || s.cfg.OnXrun == nil || s.cfg.OnRestart == nil || s.cfg.OnStderr == nil {
		t.Error("SetCallbacks did not wire every hook through to cfg")
	}
}

// TestStartOnBadDeviceFailsPromptly is the fail-fast contract that makes a
// device typo a clear startup error instead of an infinite silent restart
// loop: the probe read must give up quickly rather than hang.
//
// Backend "alsa" with a nonsense card index is used rather than "pulse":
// PipeWire's pulse-compat layer silently substitutes the default source for
// an unknown device name instead of erroring, which would make this test
// depend on whether a sound server happens to be running.
func TestStartOnBadDeviceFailsPromptly(t *testing.T) {
	requireFFmpeg(t)

	ctxTimeout := 15 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	s := NewDeviceSource(DeviceConfig{Device: "hw:99,99", Backend: "alsa"})
	start := time.Now()
	_, err := s.Start(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error opening a nonexistent device")
	}
	// The property under test is "fails rather than hangs", not a specific
	// duration: the probe's own 5s internal timeout leaves little headroom
	// against a hand-tuned bound under machine load, which was observed to
	// flake with several agents running concurrently. Asserting against the
	// context deadline instead keeps the test meaningful (a hang would still
	// be caught) without being sensitive to scheduling noise.
	if elapsed >= ctxTimeout {
		t.Errorf("bad-device probe took %v, did not return before the %v context deadline", elapsed, ctxTimeout)
	}
}
