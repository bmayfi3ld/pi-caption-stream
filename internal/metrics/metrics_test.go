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
		"stt buffer drops": func(m *Metrics) { m.STTBufferDrop() },
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

// TestHealthFreshSessionIsOK guards the default: a session with nothing
// recorded yet must not show any flavor of trouble on /admin.
func TestHealthFreshSessionIsOK(t *testing.T) {
	snap := New("v", "s").Snapshot()
	if snap.Health != "ok" {
		t.Errorf("Health = %q, want %q", snap.Health, "ok")
	}
}

// TestHealthPausedWinsOverDegradation is the case this whole phase exists
// for: a long auto-pause manufactures ring-eviction drops (STTBufferDrop
// while the gate happens to be marked active, or any other degradation
// counter left over from before the pause), but the badge must still read
// "paused", not "degraded" — the state explains itself, and stacking a
// warning on top of expected, money-saving behaviour would be exactly the
// permanent-latch bug this fixes.
func TestHealthPausedWinsOverDegradation(t *testing.T) {
	m := New("v", "s")
	m.STTReconnect() // leaves lastDegradedAt freshly stamped
	m.SetSTTState(StatePaused)

	snap := m.Snapshot()
	if snap.Health != "paused" {
		t.Errorf("Health = %q, want %q (pause must win over a stale degradation stamp)", snap.Health, "paused")
	}
}

// TestHealthClosedWinsOverDegradation is the same precedence check for the
// other terminal state: once the connection is closed, that's what the
// badge should say, not "degraded".
func TestHealthClosedWinsOverDegradation(t *testing.T) {
	m := New("v", "s")
	m.STTReconnect()
	m.SetSTTState(StateClosed)

	snap := m.Snapshot()
	if snap.Health != "closed" {
		t.Errorf("Health = %q, want %q", snap.Health, "closed")
	}
}

// TestHealthTranscriptErrorStaysDegradedPastWindow guards the distinction
// between a point event and a standing condition: lastDegradedAt ages out
// after degradedWindow, but a transcript write error is still actually
// happening until it's cleared, so it must keep the badge amber even once
// any point-event stamp is long stale. Otherwise the badge would flip to
// "ok" while the transcript diag panel on /admin is still showing a live
// error right below it.
func TestHealthTranscriptErrorStaysDegradedPastWindow(t *testing.T) {
	m := New("v", "s")
	m.SetTranscriptError(errors.New("disk full"))

	m.mu.Lock()
	m.lastDegradedAt = time.Now().Add(-degradedWindow - time.Second)
	m.mu.Unlock()

	snap := m.Snapshot()
	if snap.Health != "degraded" {
		t.Errorf("Health = %q, want %q (a standing transcript error must not age out)", snap.Health, "degraded")
	}
}

// TestHealthPausedWinsOverTranscriptError extends the pause-wins-over-
// degradation precedence to the transcript-error case specifically: a
// paused session with a stale write error should still read "paused", not
// "degraded" — the pause explains current state regardless of which kind
// of degradation is also true.
func TestHealthPausedWinsOverTranscriptError(t *testing.T) {
	m := New("v", "s")
	m.SetTranscriptError(errors.New("disk full"))
	m.SetSTTState(StatePaused)

	snap := m.Snapshot()
	if snap.Health != "paused" {
		t.Errorf("Health = %q, want %q (pause must win over a standing transcript error)", snap.Health, "paused")
	}
}

// TestHealthDegradedAfterSTTReconnect checks the ordinary case: a
// degradation event with the link otherwise up and running turns the badge
// amber right away.
func TestHealthDegradedAfterSTTReconnect(t *testing.T) {
	m := New("v", "s")
	m.STTReconnect()

	snap := m.Snapshot()
	if snap.Health != "degraded" {
		t.Errorf("Health = %q, want %q", snap.Health, "degraded")
	}
}

// TestHealthReturnsToOKAfterWindowElapses checks the badge is recency-based,
// not a permanent latch: once the degradation event falls outside
// degradedWindow, health goes back to "ok". lastDegradedAt is set directly
// (same package) instead of sleeping degradedWindow in a test.
func TestHealthReturnsToOKAfterWindowElapses(t *testing.T) {
	m := New("v", "s")
	m.STTReconnect()

	m.mu.Lock()
	m.lastDegradedAt = time.Now().Add(-degradedWindow - time.Second)
	m.mu.Unlock()

	snap := m.Snapshot()
	if snap.Health != "ok" {
		t.Errorf("Health = %q, want %q once the degradation event is outside the window", snap.Health, "ok")
	}
}

// TestSTTStateHookFiresOnChangeOnly guards the /admin status-event wiring:
// the hook drives Hub.PublishStatus, and firing it on a repeated set of the
// same state would spam subscribers with no-op transitions.
func TestSTTStateHookFiresOnChangeOnly(t *testing.T) {
	m := New("v", "s")

	var got []ConnState
	m.SetSTTStateHook(func(s ConnState) { got = append(got, s) })

	m.SetSTTState(StateConnecting)
	m.SetSTTState(StateConnecting) // repeat: must not fire again
	m.SetSTTState(StateConnected)
	m.SetSTTState(StateConnected) // repeat: must not fire again
	m.SetSTTState(StatePaused)

	want := []ConnState{StateConnecting, StateConnected, StatePaused}
	if len(got) != len(want) {
		t.Fatalf("hook fired %d times, want %d: %v", len(got), len(want), got)
	}
	for i, s := range want {
		if got[i] != s {
			t.Errorf("hook call %d = %v, want %v", i, got[i], s)
		}
	}
}

// TestSTTPauseAccounting exercises STTPauseBegin/End including a pause still
// open at snapshot time, which must count toward PausedSec live rather than
// only once it ends, and checks both calls are idempotent-safe.
func TestSTTPauseAccounting(t *testing.T) {
	m := New("v", "s")

	if snap := m.Snapshot(); snap.STT.Pauses != 0 || snap.STT.PausedSec != 0 {
		t.Fatalf("fresh session should report no pauses, got %+v", snap.STT)
	}

	m.STTPauseBegin()
	m.STTPauseBegin() // duplicate Begin must not reset the start time or double-count
	time.Sleep(20 * time.Millisecond)

	openSnap := m.Snapshot()
	if openSnap.STT.Pauses != 1 {
		t.Errorf("pauses = %d, want 1", openSnap.STT.Pauses)
	}
	if openSnap.STT.PausedSec <= 0 {
		t.Error("an open pause should already count toward PausedSec")
	}

	m.STTPauseEnd()
	m.STTPauseEnd() // duplicate End must be a no-op

	closedSnap := m.Snapshot()
	if closedSnap.STT.Pauses != 1 {
		t.Errorf("pauses after end = %d, want 1", closedSnap.STT.Pauses)
	}
	if closedSnap.STT.PausedSec < openSnap.STT.PausedSec {
		t.Errorf("PausedSec should not shrink after End: got %v, was %v", closedSnap.STT.PausedSec, openSnap.STT.PausedSec)
	}

	// A second pause/resume cycle should add on top, not reset.
	m.STTPauseBegin()
	m.STTPauseEnd()
	finalSnap := m.Snapshot()
	if finalSnap.STT.Pauses != 2 {
		t.Errorf("pauses after second cycle = %d, want 2", finalSnap.STT.Pauses)
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
