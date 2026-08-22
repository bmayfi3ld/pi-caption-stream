package stt

import (
	"testing"
	"time"
)

func testCfg() PauseConfig {
	return PauseConfig{Enabled: true, ThresholdDB: -45, Hold: 1 * time.Second}
}

func TestGate_StartsActive(t *testing.T) {
	g := NewGate(testCfg())
	if !g.Active() {
		t.Fatal("a fresh gate must start Active: we haven't heard silence yet")
	}
}

// TestGate_HoldExpiry checks the pause trips exactly when the silence run
// (measured from frame offsets, not wall clock) reaches Hold.
func TestGate_HoldExpiry(t *testing.T) {
	g := NewGate(testCfg())

	if changed := g.ObserveLevel(-80, 0); changed {
		t.Fatal("first silent frame should not itself trip the gate")
	}
	if !g.Active() {
		t.Fatal("gate should stay active until Hold elapses")
	}

	if changed := g.ObserveLevel(-80, 500*time.Millisecond); changed {
		t.Fatal("500ms of silence with a 1s hold must not trip yet")
	}
	if !g.Active() {
		t.Fatal("gate should still be active before Hold elapses")
	}

	if changed := g.ObserveLevel(-80, 1*time.Second); !changed {
		t.Fatal("silence reaching Hold should flip the gate inactive")
	}
	if g.Active() {
		t.Fatal("gate should be inactive once Hold has elapsed")
	}
}

// TestGate_InstantResume verifies that a single loud frame reactivates the
// gate immediately, with no additional hold — a delayed resume would clip the
// first word of returning speech.
func TestGate_InstantResume(t *testing.T) {
	g := NewGate(testCfg())
	g.ObserveLevel(-80, 0)
	g.ObserveLevel(-80, 1*time.Second) // now inactive
	if g.Active() {
		t.Fatal("setup: gate should be inactive before the resume frame")
	}

	if changed := g.ObserveLevel(-10, 1*time.Second); !changed {
		t.Fatal("a loud frame after a pause must report a change")
	}
	if !g.Active() {
		t.Fatal("gate must resume instantly on the first frame above threshold")
	}
}

// TestGate_BriefSilenceDoesNotTrip covers a speaker pausing for breath: a
// silence run shorter than Hold, followed by speech resuming, must never flip
// the gate inactive.
func TestGate_BriefSilenceDoesNotTrip(t *testing.T) {
	g := NewGate(testCfg())

	g.ObserveLevel(-10, 0)
	changed := g.ObserveLevel(-80, 200*time.Millisecond)
	changed = g.ObserveLevel(-80, 400*time.Millisecond) || changed
	changed = g.ObserveLevel(-10, 600*time.Millisecond) || changed

	if changed {
		t.Fatal("a silence run shorter than Hold must never report a transition")
	}
	if !g.Active() {
		t.Fatal("gate must remain active through a brief silence")
	}
}

// TestGate_Disabled checks Enabled == false means always Active, never
// transitions, no matter how long or how quiet the audio.
func TestGate_Disabled(t *testing.T) {
	cfg := testCfg()
	cfg.Enabled = false
	g := NewGate(cfg)

	if changed := g.ObserveLevel(-100, 0); changed {
		t.Fatal("a disabled gate must never report a change")
	}
	if changed := g.ObserveLevel(-100, 10*time.Hour); changed {
		t.Fatal("a disabled gate must never trip even after a long silence")
	}
	if !g.Active() {
		t.Fatal("a disabled gate must always report Active")
	}
}

// TestGate_ChangedFiresOnBothEdges checks the channel returned by Changed()
// closes on pause, is replaced by a fresh one, and closes again on resume.
func TestGate_ChangedFiresOnBothEdges(t *testing.T) {
	g := NewGate(testCfg())

	pauseCh := g.Changed()
	g.ObserveLevel(-80, 0)
	g.ObserveLevel(-80, 1*time.Second) // trips the pause
	select {
	case <-pauseCh:
	default:
		t.Fatal("Changed() channel should be closed after the pause transition")
	}

	resumeCh := g.Changed()
	select {
	case <-resumeCh:
		t.Fatal("the re-armed channel must not already be closed")
	default:
	}

	g.ObserveLevel(-10, 1*time.Second) // resumes
	select {
	case <-resumeCh:
	default:
		t.Fatal("Changed() channel should be closed after the resume transition")
	}
}

// TestGate_OffsetDrivenNotWallClock feeds frames back-to-back with no real
// waiting in between, but whose offsets span more than Hold: the transition
// must happen immediately based on the offsets, proving the gate isn't
// secretly keyed off elapsed wall time.
func TestGate_OffsetDrivenNotWallClock(t *testing.T) {
	g := NewGate(testCfg())

	start := time.Now()
	g.ObserveLevel(-80, 0)
	changed := g.ObserveLevel(-80, 10*time.Second) // offset jump far past Hold
	elapsed := time.Since(start)

	if !changed {
		t.Fatal("an offset jump past Hold must trip the gate even with no wall-clock delay")
	}
	if g.Active() {
		t.Fatal("gate should be inactive once the offset-measured silence exceeds Hold")
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("test took %v of real time; the gate should react purely to offsets", elapsed)
	}
}

// TestGate_SourceRestartRebaselinesSilence covers replay --loop, which resets
// its media clock to zero on every pass. A silence run open across that
// boundary used to measure the hold against an offset from the previous pass,
// so the difference went negative and the gate stopped pausing for the rest of
// a soak run — exactly the long run where it matters most.
func TestGate_SourceRestartRebaselinesSilence(t *testing.T) {
	g := NewGate(testCfg())

	// Silence begins deep into the first pass, but not yet long enough to trip.
	g.ObserveLevel(-80, 30*time.Second)
	if !g.Active() {
		t.Fatal("gate should still be active before Hold elapses")
	}

	// The file loops: offsets restart at zero while the audio is still silent.
	g.ObserveLevel(-80, 0)
	if !g.Active() {
		t.Fatal("the restart itself must not trip the gate")
	}

	if changed := g.ObserveLevel(-80, 1*time.Second); !changed {
		t.Fatal("Hold measured from the restarted clock should trip the gate")
	}
	if g.Active() {
		t.Fatal("gate should be inactive one Hold after the media clock restarted")
	}
}

// TestGate_ChangedObtainedBeforeTransitionIsClosedByIt pins the invariant
// consumers rely on to wait without missing a wakeup: fetch Changed() first,
// then test Active(). A channel taken before a transition must be closed by
// that transition, so a flip landing between the two calls is still observed.
func TestGate_ChangedObtainedBeforeTransitionIsClosedByIt(t *testing.T) {
	g := NewGate(testCfg())
	g.ObserveLevel(-80, 0)
	g.ObserveLevel(-80, 1*time.Second) // now paused
	if g.Active() {
		t.Fatal("precondition: gate should be paused")
	}

	// The order a correct consumer uses: arm first, then read the state.
	armed := g.Changed()
	if g.Active() {
		t.Fatal("precondition: gate should still be paused")
	}

	g.ObserveLevel(-10, 2*time.Second) // resume lands after arming

	select {
	case <-armed:
	default:
		t.Fatal("a channel armed before the resume must be closed by it, " +
			"otherwise a waiter parks until the transition after next")
	}
}
