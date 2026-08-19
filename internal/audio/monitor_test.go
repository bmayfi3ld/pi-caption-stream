package audio

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestTapNeverBlocks is the invariant the whole monitor exists to protect: a
// stalled or absent playback consumer must never make Tap wait, or a dead
// sound card would stall the caption pipeline for a live audience.
func TestTapNeverBlocks(t *testing.T) {
	var drops atomic.Int64
	m := NewMonitor(MonitorConfig{OnDrop: func() { drops.Add(1) }})
	// Nothing ever drains m.ch: Start was never called, standing in for a
	// wedged ffmpeg sink.
	bufCap := cap(m.ch)

	const sends = 500
	done := make(chan struct{})
	go func() {
		for i := 0; i < sends; i++ {
			m.Tap([]byte{byte(i)})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Tap blocked with no consumer draining the monitor channel")
	}

	if want := int64(sends - bufCap); drops.Load() != want {
		t.Errorf("drops = %d, want %d (buffer holds %d, rest must be dropped and counted)", drops.Load(), want, bufCap)
	}
}

// TestWrapForwardsFramesUnchanged confirms the tap adds no latency and never
// mutates the main path: every frame that goes in comes out the other side
// identical, tap or no tap.
func TestWrapForwardsFramesUnchanged(t *testing.T) {
	var drops atomic.Int64
	m := NewMonitor(MonitorConfig{OnDrop: func() { drops.Add(1) }})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan Frame)
	out := m.Wrap(ctx, in)

	const n = 50 // more than the tap buffer, so some taps must drop
	go func() {
		defer close(in)
		for i := 0; i < n; i++ {
			in <- Frame{PCM: []byte{byte(i)}, Offset: time.Duration(i) * time.Millisecond}
		}
	}()

	var got []Frame
	for f := range out {
		got = append(got, f)
	}

	if len(got) != n {
		t.Fatalf("forwarded %d frames, want %d — a tap to a stalled sink must never drop the main path", len(got), n)
	}
	for i, f := range got {
		if f.PCM[0] != byte(i) || f.Offset != time.Duration(i)*time.Millisecond {
			t.Fatalf("frame %d mutated in transit: %+v", i, f)
		}
	}
	// Nothing ever drained m.ch in this test, so exactly the overflow must
	// have been dropped and counted.
	if want := int64(n - cap(m.ch)); drops.Load() != want {
		t.Errorf("monitor drops = %d, want %d", drops.Load(), want)
	}
}

// TestMonitorCloseIsIdempotent guards shutdown: Close can be called from more
// than one unwind path (Ctrl-C plus deferred cleanup), and a double close of
// the internal channel must not panic.
func TestMonitorCloseIsIdempotent(t *testing.T) {
	m := NewMonitor(MonitorConfig{})
	if err := m.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestSetCallbacksWiresOnDrop is a narrower check than TestTapNeverBlocks: it
// isolates that SetCallbacks (used when the monitor is built before its
// metrics sink exists) actually reaches Tap's drop path.
func TestSetCallbacksWiresOnDrop(t *testing.T) {
	m := NewMonitor(MonitorConfig{})
	var drops atomic.Int64
	m.SetCallbacks(func() { drops.Add(1) }, nil)

	for i := 0; i < cap(m.ch)+5; i++ {
		m.Tap([]byte{0})
	}
	if drops.Load() != 5 {
		t.Errorf("drops = %d, want 5", drops.Load())
	}
}
