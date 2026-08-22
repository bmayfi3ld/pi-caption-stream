package mock

import (
	"context"
	"testing"
	"time"

	"livecaption/internal/audio"
	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

func TestScheduleLevel(t *testing.T) {
	// An explicit hold, not stt.DefaultPauseConfig's, so this test is immune
	// to the default changing.
	const hold = 10 * time.Second
	_, cycle := scheduleDurations(hold)
	levelFor := newScheduleLevel(hold)

	cases := []struct {
		at   time.Duration
		want float64
	}{
		{0, scheduleLoudDB},
		{scheduleLoud - time.Millisecond, scheduleLoudDB},
		{scheduleLoud, scheduleSilentDB},
		{cycle - time.Millisecond, scheduleSilentDB},
		{cycle, scheduleLoudDB}, // next cycle
	}
	for _, tc := range cases {
		if got := levelFor(tc.at); got != tc.want {
			t.Errorf("levelFor(%v) = %v, want %v", tc.at, got, tc.want)
		}
	}
}

// TestScheduleDurations_FallsBackForZeroHold covers the PauseConfig{} zero
// value (a caller that never set Hold): scheduleDurations must fall back to
// stt.DefaultPauseConfig's Hold rather than yielding a zero-length silent
// phase that would spin without ever demonstrating a pause.
func TestScheduleDurations_FallsBackForZeroHold(t *testing.T) {
	wantSilent := stt.DefaultPauseConfig().Hold + 20*time.Second
	silent, cycle := scheduleDurations(0)
	if silent != wantSilent {
		t.Errorf("silent = %v, want %v", silent, wantSilent)
	}
	if want := scheduleLoud + wantSilent; cycle != want {
		t.Errorf("cycle = %v, want %v", cycle, want)
	}
}

// TestMock2_TransitionsLandAtExpectedOffsets drives the real gate through the
// schedule directly, so the transition offsets can be asserted exactly:
// pause once the loud phase's silence has held for Hold, resume the instant
// the next loud phase begins.
func TestMock2_TransitionsLandAtExpectedOffsets(t *testing.T) {
	cfg := stt.PauseConfig{Enabled: true, ThresholdDB: -45, Hold: 10 * time.Second}
	gate := stt.NewGate(cfg)
	levelFor := newScheduleLevel(cfg.Hold)
	_, cycle := scheduleDurations(cfg.Hold)

	const step = 100 * time.Millisecond
	var pauseOffset, resumeOffset time.Duration
	sawPause, sawResume := false, false

	for now := time.Duration(0); now <= cycle+scheduleLoud; now += step {
		before := gate.Active()
		gate.ObserveLevel(levelFor(now), now)
		after := gate.Active()

		if before && !after && !sawPause {
			pauseOffset = now
			sawPause = true
		}
		if !before && after && sawPause && !sawResume {
			resumeOffset = now
			sawResume = true
		}
	}

	if !sawPause {
		t.Fatal("gate never paused")
	}
	if want := scheduleLoud + cfg.Hold; pauseOffset != want {
		t.Errorf("paused at %v, want %v (loud phase end + hold)", pauseOffset, want)
	}
	if !sawResume {
		t.Fatal("gate never resumed")
	}
	if resumeOffset != cycle {
		t.Errorf("resumed at %v, want %v (start of the next loud phase)", resumeOffset, cycle)
	}
}

// feedSchedule drives eng with one loud+silent cycle plus a bit, at 100ms
// steps of media time; the PCM payload is irrelevant since Engine2 uses the
// synthetic schedule, not frame.PCM.
func feedSchedule(t *testing.T, eng *Engine2) (transcripts []stt.Transcript, runErr error) {
	t.Helper()

	frames := make(chan audio.Frame)
	out := make(chan stt.Transcript, 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx, frames, out) }()

	_, cycle := scheduleDurations(eng.cfg.Pause.Hold)
	const step = 100 * time.Millisecond
	totalMedia := cycle + scheduleLoud
	for now := time.Duration(0); now <= totalMedia; now += step {
		frames <- audio.Frame{Offset: now}
	}
	close(frames)

	runErr = <-done
	close(out)
	for tr := range out {
		transcripts = append(transcripts, tr)
	}
	return transcripts, runErr
}

// TestEngine2_PausesAndResumes runs the actual engine end-to-end: it must
// stop emitting while the gate is paused, resume emitting once the loud
// phase returns, and account the pause on Metrics without touching the
// reconnect counter — mirroring deepgram's lifecycle so the pages look the
// same for both engines.
func TestEngine2_PausesAndResumes(t *testing.T) {
	met := metrics.New("test", "session")
	pauseCfg := stt.DefaultPauseConfig()
	eng := &Engine2{cfg: stt.Config{Metrics: met, Pause: pauseCfg}}

	if eng.Name() != "mock-2" {
		t.Fatalf("Name() = %q, want mock-2", eng.Name())
	}

	got, err := feedSchedule(t, eng)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected some transcripts")
	}

	pauseStart := scheduleLoud + pauseCfg.Hold
	_, pauseEnd := scheduleDurations(pauseCfg.Hold)
	var sawBefore, sawAfter bool
	for _, tr := range got {
		if tr.Start >= pauseStart && tr.Start < pauseEnd {
			t.Errorf("emitted a transcript starting at %v, inside the paused window [%v, %v)", tr.Start, pauseStart, pauseEnd)
		}
		if tr.Start < pauseStart {
			sawBefore = true
		}
		if tr.Start >= pauseEnd {
			sawAfter = true
		}
	}
	if !sawBefore {
		t.Error("expected transcripts before the pause")
	}
	if !sawAfter {
		t.Error("expected transcripts to resume after the pause")
	}

	snap := met.Snapshot()
	if snap.STT.Pauses != 1 {
		t.Errorf("pauses_total = %d, want 1", snap.STT.Pauses)
	}
	if snap.STT.Reconnects != 0 {
		t.Errorf("mock-2 must not touch reconnects_total, got %d", snap.STT.Reconnects)
	}
	if snap.STT.PausedSec <= 0 {
		t.Error("expected paused_sec to be positive")
	}
}

// TestEngine2_DisabledNeverPauses honors Enabled == false: the schedule still
// runs, but the gate must never trip and phrases must keep coming through
// the schedule's silent phase.
func TestEngine2_DisabledNeverPauses(t *testing.T) {
	met := metrics.New("test", "session")
	cfg := stt.DefaultPauseConfig()
	cfg.Enabled = false
	eng := &Engine2{cfg: stt.Config{Metrics: met, Pause: cfg}}

	got, err := feedSchedule(t, eng)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected transcripts even during the schedule's silent phase, since auto-pause is disabled")
	}
	if snap := met.Snapshot(); snap.STT.Pauses != 0 {
		t.Errorf("pauses_total = %d, want 0 when disabled", snap.STT.Pauses)
	}
}

// TestEngine2_BytesSentOnlyWhileActive covers the fix for the /admin "Audio
// sent" tile reading zero for the whole session on --engine mock-2: unlike
// deepgram, the mocks send nothing over a network, so bytes_sent_total has to
// be accounted from frames the engine consumed instead of bytes actually
// written to a socket. The count must land on the active side of the
// "if !active { continue }" guard in Run, so a paused stretch leaves the
// counter genuinely frozen — mirroring deepgram's writeLoop, which exits for
// the duration of a pause and writes nothing.
//
// The expected count is derived independently, by replaying the same
// (level, media-time) sequence through a bare stt.Gate rather than reading
// back anything the engine itself computed, so this doesn't just restate the
// production code's own bookkeeping.
func TestEngine2_BytesSentOnlyWhileActive(t *testing.T) {
	cfg := stt.PauseConfig{Enabled: true, ThresholdDB: -45, Hold: 10 * time.Second}
	levelFor := newScheduleLevel(cfg.Hold)
	_, cycle := scheduleDurations(cfg.Hold)
	const step = 100 * time.Millisecond
	totalMedia := cycle + scheduleLoud

	gate := stt.NewGate(cfg)
	var wantActiveFrames int64
	var totalFrames int64
	for now := time.Duration(0); now <= totalMedia; now += step {
		gate.ObserveLevel(levelFor(now), now)
		totalFrames++
		if gate.Active() {
			wantActiveFrames++
		}
	}
	if wantActiveFrames >= totalFrames {
		t.Fatalf("test setup problem: schedule never paused (active=%d, total=%d)", wantActiveFrames, totalFrames)
	}

	met := metrics.New("test", "session")
	eng := &Engine2{cfg: stt.Config{Metrics: met, Pause: cfg}}

	frames := make(chan audio.Frame)
	out := make(chan stt.Transcript, 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx, frames, out) }()

	const frameBytes = 320 // arbitrary nonzero payload; only its size matters
	payload := make([]byte, frameBytes)
	for now := time.Duration(0); now <= totalMedia; now += step {
		frames <- audio.Frame{Offset: now, PCM: payload}
	}
	close(frames)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(out)
	for range out {
	}

	wantBytes := wantActiveFrames * frameBytes
	if got := met.Snapshot().STT.BytesSent; got != wantBytes {
		t.Errorf("bytes_sent_total = %d, want %d (%d active frames x %d bytes)", got, wantBytes, wantActiveFrames, frameBytes)
	}
}
