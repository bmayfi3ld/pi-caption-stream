// Package mock provides an offline speech-to-text engine.
//
// It exists so the web layer, caption hub and terminal UI can be developed and
// tested with no network, no API key and no per-minute charge, and so tests
// have a recognizer whose output is exactly reproducible.
package mock

import (
	"context"
	"math/rand"
	"strings"
	"time"

	"livecaption/internal/audio"
	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

func init() {
	stt.Register("mock", func(cfg stt.Config) (stt.Engine, error) {
		return &Engine{cfg: cfg, rng: rand.New(rand.NewSource(1))}, nil
	})
}

// phrases are deliberately caption-shaped: varied length, real punctuation,
// and a few proper nouns, so the viewer layout is exercised honestly.
var phrases = []string{
	"Good morning everyone, and thank you all for coming out today.",
	"We're going to start with a few announcements before the main session.",
	"If you can't hear me at the back, please raise your hand.",
	"The agenda for this morning is posted at the entrance.",
	"First, I want to thank the volunteers who set up the room.",
	"We'll take a short break at around half past ten.",
	"Please silence your phones for the duration of the talk.",
	"There's coffee and tea available in the lobby throughout the morning.",
	"Our first speaker has been working on this project for three years.",
	"I think you'll find the results genuinely surprising.",
	"Let's give a warm welcome to our guest this morning.",
	"Feel free to ask questions as we go along.",
	"That brings us to the end of the first section.",
	"We have about fifteen minutes left for discussion.",
	"If anyone needs to step out, the exits are on both sides.",
}

// Engine emits canned phrases paced by the audio it consumes.
//
// Everything is driven by media time from the frames, never by wall clock, so
// output is identical at --speed 1.0 and --speed 10 and reproducible in tests.
type Engine struct {
	cfg stt.Config
	rng *rand.Rand
}

func (e *Engine) Name() string { return "mock" }

const (
	// How much audio a whole utterance represents.
	utteranceAudio = 4500 * time.Millisecond
	// How much audio passes between interim updates within an utterance.
	interimAudio = 400 * time.Millisecond
	// Silence between utterances, so the display isn't wall-to-wall text.
	gapAudio = 900 * time.Millisecond
)

func (e *Engine) Run(ctx context.Context, frames <-chan audio.Frame, out chan<- stt.Transcript) error {
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.SetSTTState(metrics.StateConnected) // the mock is always "up"
	}

	var (
		phraseIdx    int
		uttStart     time.Duration // media time this utterance began
		nextInterim  time.Duration
		wordsEmitted int
		inGap        bool
		gapUntil     time.Duration
	)

	emit := func(t stt.Transcript) bool {
		t.ReceivedAt = time.Now()
		select {
		case out <- t:
			return true
		case <-ctx.Done():
			return false
		}
	}

	for frame := range frames {
		now := frame.Offset

		if inGap {
			if now < gapUntil {
				continue
			}
			inGap = false
			uttStart = now
			nextInterim = now + interimAudio
			wordsEmitted = 0
		}
		if uttStart == 0 && nextInterim == 0 {
			uttStart, nextInterim = now, now+interimAudio
		}

		words := strings.Fields(phrases[phraseIdx%len(phrases)])
		elapsed := now - uttStart

		// Finish the utterance: emit the full text as final.
		if elapsed >= utteranceAudio {
			if !emit(stt.Transcript{
				Text:        phrases[phraseIdx%len(phrases)],
				IsFinal:     true,
				SpeechFinal: true,
				Start:       uttStart,
				Duration:    elapsed,
				Confidence:  0.90 + e.rng.Float64()*0.09,
			}) {
				return ctx.Err()
			}
			phraseIdx++
			inGap = true
			gapUntil = now + gapAudio
			continue
		}

		// Reveal words progressively, the way a real recognizer streams.
		if now >= nextInterim {
			nextInterim = now + interimAudio
			// Spread the phrase's words across its audio duration.
			want := int(float64(len(words)) * float64(elapsed) / float64(utteranceAudio))
			if want > len(words) {
				want = len(words)
			}
			if want > wordsEmitted && want > 0 {
				wordsEmitted = want
				if !emit(stt.Transcript{
					Text:       strings.Join(words[:wordsEmitted], " "),
					Start:      uttStart,
					Duration:   elapsed,
					Confidence: 0.75 + e.rng.Float64()*0.15,
				}) {
					return ctx.Err()
				}
			}
		}
	}
	return nil
}
