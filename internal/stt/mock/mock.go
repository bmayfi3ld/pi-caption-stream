// Package mock provides offline speech-to-text engines.
//
// It exists so the web layer, caption hub and terminal UI can be developed and
// tested with no network, no API key and no per-minute charge, and so tests
// have a recognizer whose output is exactly reproducible.
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
	stt.Register("mock", func(cfg stt.Config) (stt.Engine, error) {
		return &Engine{cfg: cfg, ps: newPhraseState(rand.New(rand.NewSource(1)))}, nil
	})
}

// Engine emits canned phrases paced by the audio it consumes.
//
// Everything is driven by media time from the frames, never by wall clock, so
// output is identical at --speed 1.0 and --speed 10 and reproducible in tests.
type Engine struct {
	cfg stt.Config
	ps  *phraseState
}

func (e *Engine) Name() string { return "mock" }

func (e *Engine) Run(ctx context.Context, frames <-chan audio.Frame, out chan<- stt.Transcript) error {
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.SetSTTState(metrics.StateConnected) // the mock is always "up"
	}

	for frame := range frames {
		if e.cfg.Metrics != nil {
			// The mock sends nothing over a network, but the /admin "Audio
			// sent" tile reads this counter regardless of engine; without
			// this call it sits dead at "0 B" for the whole session on
			// --engine mock, looking like the very bug this metric exists
			// to catch. This counts bytes the engine consumed and would
			// have transmitted.
			e.cfg.Metrics.STTBytesSent(len(frame.PCM))
		}

		// Closes over the frame currently being consumed, which is the one
		// holding the utterance's last sample by the time emit fires within
		// this step call — exactly what CapturedAt is meant to record. The
		// mock has zero recognition delay by construction, so this is exact,
		// not an approximation.
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
		if !e.ps.step(frame.Offset, emit) {
			return ctx.Err()
		}
	}
	return nil
}
