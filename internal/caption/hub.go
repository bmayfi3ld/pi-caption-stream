// Package caption turns a stream of recognizer results into stable display
// lines and fans them out to subscribers.
package caption

import (
	"strings"
	"sync"
	"time"

	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

// Kind distinguishes the three things a subscriber can receive.
type Kind string

const (
	KindInterim  Kind = "interim"  // in-progress line, will be replaced
	KindFinal    Kind = "final"    // closed line, will never change
	KindStatus   Kind = "status"   // connection state changed
	KindSnapshot Kind = "snapshot" // catch-up sent to a new subscriber
)

// Event is what goes over SSE. One JSON object per message.
type Event struct {
	Seq      int64  `json:"seq"`
	Kind     Kind   `json:"kind"`
	ID       string `json:"id,omitempty"`
	Text     string `json:"text,omitempty"`
	OffsetMS int64  `json:"offset_ms,omitempty"`
	// omitzero, not omitempty: omitempty has no effect on a struct, so an
	// unset time would serialize as "0001-01-01T00:00:00Z".
	At time.Time `json:"at,omitzero"`

	// Status events only.
	State  string `json:"state,omitempty"`
	Detail string `json:"detail,omitempty"`

	// Snapshot events only: the recent finalized lines plus any pending text,
	// so a browser that reconnects mid-sentence isn't left blank.
	Lines   []Line `json:"lines,omitempty"`
	Pending string `json:"pending,omitempty"`
}

// Line is one finalized caption.
type Line struct {
	ID       string    `json:"id"`
	Text     string    `json:"text"`
	OffsetMS int64     `json:"offset_ms"`
	At       time.Time `json:"at"`
}

// historyLimit is how many finalized lines are kept for late joiners. The
// viewer shows two or three; the rest is headroom for a page that wants more.
const historyLimit = 200

// subscriberBuffer is how far a subscriber may fall behind before being
// dropped. Captions are realtime — a browser that cannot keep up is better off
// reconnecting and getting a fresh snapshot than receiving stale text.
const subscriberBuffer = 16

// Hub assembles utterances and broadcasts them.
//
// A recognizer emits many interim results per utterance, then one or more
// IsFinal segments, then a SpeechFinal. Only SpeechFinal closes a display
// line, so captions don't flicker mid-sentence.
type Hub struct {
	mu      sync.RWMutex
	seq     int64
	uttSeq  int64
	history []Line
	// pending is the text of the utterance in progress: finalized segments
	// already committed, plus the latest interim tail.
	committed string
	interim   string
	uttStart  time.Duration

	subs    map[chan Event]struct{}
	metrics *metrics.Metrics

	// OnFinal is called for each closed line, on the publishing goroutine.
	// Used for the terminal and the transcript writer.
	OnFinal func(Line)
}

func NewHub(m *metrics.Metrics) *Hub {
	return &Hub{subs: make(map[chan Event]struct{}), metrics: m}
}

// Publish feeds one recognizer result into the hub.
func (h *Hub) Publish(t stt.Transcript) {
	text := strings.TrimSpace(t.Text)

	h.mu.Lock()
	if h.committed == "" && h.interim == "" {
		h.uttStart = t.Start
	}

	switch {
	case t.SpeechFinal:
		// Natural end of speech: close the line.
		full := joinText(h.committed, text)
		h.committed, h.interim = "", ""
		if full == "" {
			h.mu.Unlock()
			return
		}
		h.uttSeq++
		line := Line{
			ID:       "u" + itoa(h.uttSeq),
			Text:     full,
			OffsetMS: h.uttStart.Milliseconds(),
			At:       time.Now(),
		}
		h.history = append(h.history, line)
		if len(h.history) > historyLimit {
			h.history = h.history[len(h.history)-historyLimit:]
		}
		ev := h.newEventLocked(KindFinal)
		ev.ID, ev.Text, ev.OffsetMS, ev.At = line.ID, line.Text, line.OffsetMS, line.At
		onFinal := h.OnFinal
		h.mu.Unlock()

		if h.metrics != nil {
			h.metrics.STTFinal()
		}
		if onFinal != nil {
			onFinal(line)
		}
		h.broadcast(ev)

	case t.IsFinal:
		// A settled segment, but speech continues. Commit it and keep going;
		// the display shows the accumulated text as still-in-progress.
		h.committed = joinText(h.committed, text)
		h.interim = ""
		ev := h.newEventLocked(KindInterim)
		ev.Text = h.committed
		h.mu.Unlock()
		if h.metrics != nil {
			h.metrics.STTInterim()
		}
		h.broadcast(ev)

	default:
		// Interim: replaces only the uncommitted tail.
		if text == "" {
			h.mu.Unlock()
			return
		}
		h.interim = text
		ev := h.newEventLocked(KindInterim)
		ev.Text = joinText(h.committed, h.interim)
		h.mu.Unlock()
		if h.metrics != nil {
			h.metrics.STTInterim()
		}
		h.broadcast(ev)
	}
}

// PublishStatus broadcasts a connection state change.
func (h *Hub) PublishStatus(state, detail string) {
	h.mu.Lock()
	ev := h.newEventLocked(KindStatus)
	ev.State, ev.Detail = state, detail
	h.mu.Unlock()
	h.broadcast(ev)
}

// Flush closes any in-progress utterance. Called at shutdown so the tail of a
// session is not lost when the speaker was still talking.
func (h *Hub) Flush() {
	h.mu.RLock()
	pending := joinText(h.committed, h.interim)
	h.mu.RUnlock()
	if pending == "" {
		return
	}
	h.Publish(stt.Transcript{Text: "", SpeechFinal: true})
}

func (h *Hub) newEventLocked(k Kind) Event {
	h.seq++
	return Event{Seq: h.seq, Kind: k}
}

// Subscribe registers a new listener and returns its channel plus an
// unsubscribe function. The first event delivered is always a snapshot.
func (h *Hub) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, subscriberBuffer)

	h.mu.Lock()
	h.seq++
	snap := Event{Seq: h.seq, Kind: KindSnapshot, At: time.Now()}
	snap.Lines = append([]Line(nil), h.history...)
	snap.Pending = joinText(h.committed, h.interim)
	h.subs[ch] = struct{}{}
	h.mu.Unlock()

	ch <- snap // buffered and empty, cannot block
	if h.metrics != nil {
		h.metrics.SSEConnect()
	}

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs, ch)
			h.mu.Unlock()
			close(ch)
			if h.metrics != nil {
				h.metrics.SSEDisconnect()
			}
		})
	}
}

// broadcast sends to every subscriber without ever blocking.
//
// A subscriber that cannot keep up is dropped rather than backpressured: the
// audio pipeline must never be held up by a slow browser. EventSource
// reconnects on its own and gets a fresh snapshot, so the cost of being
// dropped is one round trip.
func (h *Hub) broadcast(ev Event) {
	h.mu.RLock()
	targets := make([]chan Event, 0, len(h.subs))
	for ch := range h.subs {
		targets = append(targets, ch)
	}
	h.mu.RUnlock()

	if h.metrics != nil {
		h.metrics.SSEEvent()
	}
	for _, ch := range targets {
		select {
		case ch <- ev:
		default:
			if h.metrics != nil {
				h.metrics.SSESlowDrop()
			}
		}
	}
}

// Snapshot returns the current history and pending text.
func (h *Hub) Snapshot() ([]Line, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return append([]Line(nil), h.history...), joinText(h.committed, h.interim)
}

func joinText(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	default:
		return a + " " + b
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
