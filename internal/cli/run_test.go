package cli

import (
	"testing"
	"time"

	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

// observeLatency touches only s.met, so a session built with nothing but a
// fresh Metrics is a sufficient fixture for these tests.
func newLatencySession() *session {
	return &session{met: metrics.New("test", "session")}
}

func TestObserveLatency_UsesCapturedAt(t *testing.T) {
	s := newLatencySession()
	now := time.Now()

	s.observeLatency(stt.Transcript{
		IsFinal:    true,
		ReceivedAt: now,
		CapturedAt: now.Add(-300 * time.Millisecond),
	})

	snap := s.met.Snapshot()
	if snap.STT.LatencyCount != 1 {
		t.Fatalf("LatencyCount = %d, want 1", snap.STT.LatencyCount)
	}
	if got := snap.STT.LatencyLast; got < 295 || got > 305 {
		t.Errorf("LatencyLast = %v, want ~300ms", got)
	}
}

func TestObserveLatency_IgnoresZeroCapturedAt(t *testing.T) {
	s := newLatencySession()

	// A missing CapturedAt means the engine couldn't resolve it — record
	// nothing rather than resurrecting the old, unbounded StartedAt-relative
	// figure.
	s.observeLatency(stt.Transcript{
		IsFinal:    true,
		ReceivedAt: time.Now(),
	})

	if got := s.met.Snapshot().STT.LatencyCount; got != 0 {
		t.Errorf("LatencyCount = %d, want 0 for a zero CapturedAt", got)
	}
}

func TestObserveLatency_IgnoresInterims(t *testing.T) {
	s := newLatencySession()
	now := time.Now()

	s.observeLatency(stt.Transcript{
		IsFinal:    false,
		ReceivedAt: now,
		CapturedAt: now.Add(-300 * time.Millisecond),
	})

	if got := s.met.Snapshot().STT.LatencyCount; got != 0 {
		t.Errorf("LatencyCount = %d, want 0 for an interim", got)
	}
}

func TestObserveLatency_KeepsSmallSample(t *testing.T) {
	s := newLatencySession()
	now := time.Now()

	// The old formula clipped d <= 0 before recording; the new one must not
	// clip a small-but-real 2ms sample, since both timestamps come from
	// time.Now() in-process and Sub uses the monotonic reading.
	s.observeLatency(stt.Transcript{
		IsFinal:    true,
		ReceivedAt: now,
		CapturedAt: now.Add(-2 * time.Millisecond),
	})

	snap := s.met.Snapshot()
	if snap.STT.LatencyCount != 1 {
		t.Fatalf("LatencyCount = %d, want 1", snap.STT.LatencyCount)
	}
	if got := snap.STT.LatencyLast; got < 1 || got > 3 {
		t.Errorf("LatencyLast = %v, want ~2ms", got)
	}
}
