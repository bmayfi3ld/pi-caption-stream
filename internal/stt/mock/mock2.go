package mock

import (
	"context"
	"math/rand"
	"time"

	"livecaption/internal/audio"
	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

func init() {
	stt.Register("mock-2", func(cfg stt.Config) (stt.Engine, error) {
		return &Engine2{cfg: cfg}, nil
	})
}

const (
	// scheduleLoud is how long the loud phase of the synthetic level schedule
	// lasts. The silent phase isn't a fixed constant here: it's derived from
	// the configured pause hold (see scheduleDurations) so that whichever
	// --silence-hold is in effect, the silent phase is always long enough to
	// actually reach it and then leave the paused state visible for a while
	// afterwards — unlike a replay file's continuous speech, which would
	// never go quiet on its own.
	scheduleLoud = 20 * time.Second

	scheduleLoudDB   = -20.0
	scheduleSilentDB = -80.0
)

// scheduleDurations resolves the silent-phase length and full cycle length of
// the synthetic level schedule from the configured pause hold: the silent
// phase is hold plus a fixed 20s, so the hold is always reached regardless of
// what --silence-hold is set to, and the paused state stays visible for
// ~20s afterward before the next loud phase resumes it. A caller that never
// configured Hold (the PauseConfig{} zero value) falls back to
// stt.DefaultPauseConfig's Hold rather than producing a zero-length silent
// phase that would spin without ever demonstrating a pause.
func scheduleDurations(hold time.Duration) (silent, cycle time.Duration) {
	if hold <= 0 {
		hold = stt.DefaultPauseConfig().Hold
	}
	silent = hold + 20*time.Second
	return silent, scheduleLoud + silent
}

// newScheduleLevel builds the synthetic dBFS level function for media time,
// cycling loud then silent, with the silent phase sized to the configured
// pause hold via scheduleDurations.
func newScheduleLevel(hold time.Duration) func(now time.Duration) float64 {
	_, cycle := scheduleDurations(hold)
	return func(now time.Duration) float64 {
		phase := now % cycle
		if phase < scheduleLoud {
			return scheduleLoudDB
		}
		return scheduleSilentDB
	}
}

// Engine2 is "mock-2": it demonstrates auto-pause end-to-end with no network
// and no API key. It reuses the phrase-emitting logic from Engine, but drives
// the real stt.Gate with a synthetic level schedule instead of the frames'
// actual RMS — a replay file is continuous speech and would never go quiet,
// so a demo based on real levels would never pause. Feeding a synthetic level
// through the genuine gate means the state machine, the metrics and the
// status events all take the identical code path as deepgram; only the level
// source is faked.
type Engine2 struct {
	cfg stt.Config
}

func (e *Engine2) Name() string { return "mock-2" }

func (e *Engine2) Run(ctx context.Context, frames <-chan audio.Frame, out chan<- stt.Transcript) error {
	met := e.cfg.Metrics
	gate := stt.NewGate(e.cfg.Pause)
	levelFor := newScheduleLevel(e.cfg.Pause.Hold)
	ps := newPhraseState(rand.New(rand.NewSource(1)))

	if met != nil {
		met.SetSTTState(metrics.StateConnected) // the mock always dials "successfully"
	}

	wasActive := gate.Active()
	for frame := range frames {
		now := frame.Offset
		// Closes over the frame currently being consumed, same reasoning as
		// the mock engine: it holds the utterance's last sample by the time
		// emit fires within this iteration, and the mock has zero recognition
		// delay by construction.
		emit := func(t stt.Transcript) bool {
			t.ReceivedAt = time.Now()
			t.CapturedAt = frame.CapturedAt
			select {
			case out <- t:
				return true
			case <-ctx.Done():
				return false
			}
		}
		gate.ObserveLevel(levelFor(now), now)

		active := gate.Active()
		if active != wasActive {
			wasActive = active
			if active {
				// The interrupted utterance is a whole silent stretch in the
				// past by now; start it over so speech eases back in instead
				// of a finished phrase landing on the first frame.
				ps.restart(now)
			}
			if met != nil {
				if active {
					// Mirrors deepgram's post-pause lifecycle so the pages
					// look the same for both engines.
					met.STTPauseEnd()
					met.SetSTTState(metrics.StateConnecting)
					met.SetSTTState(metrics.StateConnected)
				} else {
					met.SetSTTState(metrics.StatePaused)
					met.STTPauseBegin()
				}
			}
		}

		if !active {
			continue // no transcripts while the gate is paused
		}

		if met != nil {
			// Like Engine, mock-2 sends nothing over a network; this counts
			// bytes the engine consumed and would have transmitted, so the
			// /admin "Audio sent" tile is exercised on the offline demo path
			// instead of sitting dead at "0 B". It sits on the active side
			// of the gate deliberately: a paused mock-2 session must show
			// this counter genuinely frozen, mirroring deepgram's writeLoop,
			// which exits for the duration of a pause and sends nothing.
			met.STTBytesSent(len(frame.PCM))
		}

		if !ps.step(now, emit) {
			return ctx.Err()
		}
	}
	return nil
}
