package mock

import (
	"math/rand"
	"strings"
	"time"

	"livecaption/internal/stt"
)

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

const (
	// How much audio a whole utterance represents.
	utteranceAudio = 4500 * time.Millisecond
	// How much audio passes between interim updates within an utterance.
	interimAudio = 400 * time.Millisecond
	// Silence between utterances, so the display isn't wall-to-wall text.
	gapAudio = 900 * time.Millisecond
)

// phraseState drives the canned-utterance state machine shared by both mock
// engines: mock paces it off real frame offsets, mock-2 off a synthetic
// level schedule instead, but the phrase timing, word reveal and gaps are
// identical so the two engines look the same to a viewer.
type phraseState struct {
	rng *rand.Rand

	phraseIdx    int
	uttStart     time.Duration // media time this utterance began
	nextInterim  time.Duration
	wordsEmitted int
	inGap        bool
	gapUntil     time.Duration
}

func newPhraseState(rng *rand.Rand) *phraseState {
	return &phraseState{rng: rng}
}

// restart begins a fresh utterance at media time now, discarding whatever was
// in flight. mock-2 calls this on resume from a pause: the stale uttStart is
// by then a whole silent stretch in the past, so without it the first frame
// back would fire a finished phrase instantly rather than easing back in the
// way returning speech actually does. phraseIdx is left alone, so the
// interrupted sentence starts over rather than being skipped.
func (p *phraseState) restart(now time.Duration) {
	p.uttStart = now
	p.nextInterim = now + interimAudio
	p.wordsEmitted = 0
	p.inGap = false
}

// step advances the state machine to media time now, emitting an interim or
// final transcript through emit as appropriate. It reports false only when
// emit says the caller should stop (ctx cancelled); a step with nothing to
// emit yet still reports true.
func (p *phraseState) step(now time.Duration, emit func(stt.Transcript) bool) bool {
	if p.inGap {
		if now < p.gapUntil {
			return true
		}
		p.inGap = false
		p.uttStart = now
		p.nextInterim = now + interimAudio
		p.wordsEmitted = 0
	}
	if p.uttStart == 0 && p.nextInterim == 0 {
		p.uttStart, p.nextInterim = now, now+interimAudio
	}

	words := strings.Fields(phrases[p.phraseIdx%len(phrases)])
	elapsed := now - p.uttStart

	// Finish the utterance: emit the full text as final.
	if elapsed >= utteranceAudio {
		if !emit(stt.Transcript{
			Text:        phrases[p.phraseIdx%len(phrases)],
			IsFinal:     true,
			SpeechFinal: true,
			Start:       p.uttStart,
			Duration:    elapsed,
			Confidence:  0.90 + p.rng.Float64()*0.09,
		}) {
			return false
		}
		p.phraseIdx++
		p.inGap = true
		p.gapUntil = now + gapAudio
		return true
	}

	// Reveal words progressively, the way a real recognizer streams.
	if now >= p.nextInterim {
		p.nextInterim = now + interimAudio
		// Spread the phrase's words across its audio duration.
		want := int(float64(len(words)) * float64(elapsed) / float64(utteranceAudio))
		if want > len(words) {
			want = len(words)
		}
		if want > p.wordsEmitted && want > 0 {
			p.wordsEmitted = want
			if !emit(stt.Transcript{
				Text:       strings.Join(words[:p.wordsEmitted], " "),
				Start:      p.uttStart,
				Duration:   elapsed,
				Confidence: 0.75 + p.rng.Float64()*0.15,
			}) {
				return false
			}
		}
	}
	return true
}
