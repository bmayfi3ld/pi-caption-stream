package metrics

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestPercentiles feeds a known distribution and checks the exact rank the
// nearest-rank formula picks, since /admin and the shutdown summary are read
// verbatim off this — an off-by-one here would misreport latency to an
// operator mid-event.
func TestPercentiles(t *testing.T) {
	m := New("v", "s")
	for i := 1; i <= 100; i++ {
		m.ObserveLatency(time.Duration(i) * time.Millisecond)
	}

	snap := m.Snapshot()
	if snap.STT.LatencyCount != 100 {
		t.Fatalf("count = %d, want 100", snap.STT.LatencyCount)
	}
	// nearest-rank on a sorted 1..100 slice: index = int(p*100).
	if got, want := snap.STT.LatencyP50, 51.0; got != want {
		t.Errorf("p50 = %v, want %v", got, want)
	}
	if got, want := snap.STT.LatencyP95, 96.0; got != want {
		t.Errorf("p95 = %v, want %v", got, want)
	}
	if got, want := snap.STT.LatencyMax, 100.0; got != want {
		t.Errorf("max = %v, want %v", got, want)
	}
}

// TestLatencyLastTracksIndependentlyOfMax guards a real distinction the
// status line renders side by side ("lat 340ms p95 610ms"): Last is the most
// recent sample, Max is the worst ever seen, and a smaller sample arriving
// after a spike must not erase the spike from Max.
func TestLatencyLastTracksIndependentlyOfMax(t *testing.T) {
	m := New("v", "s")
	m.ObserveLatency(400 * time.Millisecond) // the spike
	m.ObserveLatency(5 * time.Millisecond)   // a later, ordinary sample

	snap := m.Snapshot()
	if got, want := snap.STT.LatencyMax, 400.0; got != want {
		t.Errorf("max = %v, want %v (must not be pulled down by a later smaller sample)", got, want)
	}
	if got, want := snap.STT.LatencyLast, 5.0; got != want {
		t.Errorf("last = %v, want %v (must reflect the most recent observation)", got, want)
	}
}

// TestLatencyRingStaysBoundedAfterWrap is the property that keeps a
// multi-hour session from growing memory without limit: the ring holds only
// the most recent latencyRing samples, and percentiles computed after
// wrapping must still describe that recent window, not stale history.
func TestLatencyRingStaysBoundedAfterWrap(t *testing.T) {
	m := New("v", "s")
	const total = latencyRing + 88 // wrap partway through a second pass
	for i := 1; i <= total; i++ {
		m.ObserveLatency(time.Duration(i) * time.Millisecond)
	}

	snap := m.Snapshot()
	if snap.STT.LatencyCount != latencyRing {
		t.Fatalf("ring holds %d samples, want capped at %d", snap.STT.LatencyCount, latencyRing)
	}
	// Max is tracked outside the ring, so it must survive the wrap even
	// though the sample that set it may have been evicted.
	if got, want := snap.STT.LatencyMax, float64(total); got != want {
		t.Errorf("max = %v, want %v", got, want)
	}
	// The ring now holds exactly {total-latencyRing+1 .. total}.
	first := total - latencyRing + 1
	wantP50 := float64(first + latencyRing/2)
	if got := snap.STT.LatencyP50; got != wantP50 {
		t.Errorf("p50 after wrap = %v, want %v", got, wantP50)
	}
}

// TestSnapshotCleanIsFalseForEachDegradation walks every counter Clean()
// checks, one at a time, so a field silently dropped from that list is a
// build-time-invisible bug that only this test catches. This drives the
// amber highlighting operators rely on during a live event.
func TestSnapshotCleanIsFalseForEachDegradation(t *testing.T) {
	fresh := New("v", "s").Snapshot()
	if !fresh.Clean() {
		t.Fatal("a pristine session must report Clean")
	}

	cases := map[string]func(*Metrics){
		"frames dropped":   func(m *Metrics) { m.DropFrame() },
		"ffmpeg restarts":  func(m *Metrics) { m.FFmpegRestart() },
		"xruns":            func(m *Metrics) { m.Xrun() },
		"monitor drops":    func(m *Metrics) { m.MonitorDrop() },
		"stt reconnects":   func(m *Metrics) { m.STTReconnect() },
		"slow disconnects": func(m *Metrics) { m.SSESlowDrop() },
		"transcript error": func(m *Metrics) { m.SetTranscriptError(errors.New("disk full")) },
	}
	for name, apply := range cases {
		t.Run(name, func(t *testing.T) {
			m := New("v", "s")
			apply(m)
			if snap := m.Snapshot(); snap.Clean() {
				t.Errorf("%s: Clean() = true, want false", name)
			}
		})
	}
}

// TestConcurrentAccessIsRaceFree hammers every counter from many goroutines
// while repeatedly snapshotting, matching how the real pipeline hits this
// struct from capture, the recognizer, SSE handlers and the status line all
// at once. Run with -race — a torn Snapshot would misreport mid-event.
func TestConcurrentAccessIsRaceFree(t *testing.T) {
	m := New("v", "s")
	const workers = 32
	const iters = 200

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				m.AddFrame(100, time.Duration(i)*time.Millisecond)
				m.DropFrame()
				m.FFmpegRestart()
				m.Xrun()
				m.MonitorDrop()
				m.SetMonitorAlive(i%2 == 0)
				m.SetLastStderr("glitch")
				m.SetSTTState(StateConnected)
				m.STTReconnect()
				m.STTInterim()
				m.STTFinal()
				m.STTBytesSent(10)
				m.SetSTTError(errors.New("blip"))
				m.ObserveLatency(time.Duration(i) * time.Millisecond)
				m.SSEConnect()
				m.SSEDisconnect()
				m.SSEEvent()
				m.SSESlowDrop()
				m.TranscriptWrote(1, 20)
				m.SetTranscriptError(errors.New("disk"))
				_ = m.Snapshot()
			}
		}(w)
	}
	wg.Wait()

	snap := m.Snapshot()
	want := int64(workers * iters)
	if snap.Source.FramesDropped != want {
		t.Errorf("frames dropped = %d, want %d", snap.Source.FramesDropped, want)
	}
	if snap.Source.FFmpegRestarts != want {
		t.Errorf("ffmpeg restarts = %d, want %d", snap.Source.FFmpegRestarts, want)
	}
	if snap.STT.Reconnects != want {
		t.Errorf("stt reconnects = %d, want %d", snap.STT.Reconnects, want)
	}
	if snap.Web.SlowDrops != want {
		t.Errorf("slow drops = %d, want %d", snap.Web.SlowDrops, want)
	}
	if snap.Transcript.Lines != want {
		t.Errorf("transcript lines = %d, want %d", snap.Transcript.Lines, want)
	}
}
