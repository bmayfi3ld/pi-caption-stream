package caption

import (
	"testing"
	"time"

	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

func drain(ch <-chan Event) []Event {
	var out []Event
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

// TestUtteranceAssembly covers the sequence a real recognizer produces:
// several interims, one or more is_final segments, then speech_final. Only the
// last of those may close a display line, otherwise captions flicker and split
// mid-sentence.
func TestUtteranceAssembly(t *testing.T) {
	h := NewHub(metrics.New("test", "test"))
	var finals []Line
	h.OnFinal = func(l Line) { finals = append(finals, l) }

	sub, unsub := h.Subscribe()
	defer unsub()
	drain(sub) // discard the initial snapshot

	h.Publish(stt.Transcript{Text: "hello", Start: time.Second})
	h.Publish(stt.Transcript{Text: "hello there", Start: time.Second})
	h.Publish(stt.Transcript{Text: "hello there,", IsFinal: true, Start: time.Second})
	h.Publish(stt.Transcript{Text: "how are", Start: 2 * time.Second})
	h.Publish(stt.Transcript{Text: "how are you?", IsFinal: true, SpeechFinal: true, Start: 2 * time.Second})

	if len(finals) != 1 {
		t.Fatalf("expected 1 finalized line, got %d: %v", len(finals), finals)
	}
	if want := "hello there, how are you?"; finals[0].Text != want {
		t.Errorf("finalized text = %q, want %q", finals[0].Text, want)
	}
	// The line is stamped with where the utterance began, not where it ended.
	if finals[0].OffsetMS != 1000 {
		t.Errorf("offset = %dms, want 1000ms", finals[0].OffsetMS)
	}

	events := drain(sub)
	var interims, closed int
	for _, ev := range events {
		switch ev.Kind {
		case KindInterim:
			interims++
		case KindFinal:
			closed++
		}
	}
	if closed != 1 {
		t.Errorf("expected 1 final event, got %d", closed)
	}
	if interims == 0 {
		t.Error("expected interim events so the viewer can show live text")
	}
}

// TestInterimDoesNotOverwriteCommitted guards the subtle part of assembly: an
// interim replaces only the uncommitted tail, never text already settled.
func TestInterimDoesNotOverwriteCommitted(t *testing.T) {
	h := NewHub(metrics.New("test", "test"))
	sub, unsub := h.Subscribe()
	defer unsub()
	drain(sub)

	h.Publish(stt.Transcript{Text: "first part", IsFinal: true})
	h.Publish(stt.Transcript{Text: "second", Start: time.Second})

	_, pending := h.Snapshot()
	if want := "first part second"; pending != want {
		t.Errorf("pending = %q, want %q", pending, want)
	}
}

// TestFlushClosesPendingUtterance ensures the tail of a session is not lost
// when shutdown lands mid-sentence.
func TestFlushClosesPendingUtterance(t *testing.T) {
	h := NewHub(metrics.New("test", "test"))
	var finals []Line
	h.OnFinal = func(l Line) { finals = append(finals, l) }

	h.Publish(stt.Transcript{Text: "an unfinished sentence", IsFinal: true})
	h.Flush()

	if len(finals) != 1 {
		t.Fatalf("expected Flush to close the pending line, got %d finals", len(finals))
	}
	if finals[0].Text != "an unfinished sentence" {
		t.Errorf("flushed text = %q", finals[0].Text)
	}

	// Flushing again must not emit a duplicate empty line.
	h.Flush()
	if len(finals) != 1 {
		t.Errorf("second Flush produced %d finals, want 1", len(finals))
	}
}

// TestSlowSubscriberIsDropped is the rule that keeps the audio pipeline safe:
// a browser that cannot keep up loses events rather than applying backpressure
// all the way back to capture.
func TestSlowSubscriberIsDropped(t *testing.T) {
	m := metrics.New("test", "test")
	h := NewHub(m)

	sub, unsub := h.Subscribe()
	defer unsub()
	// Deliberately never read from sub.

	for i := 0; i < subscriberBuffer*3; i++ {
		h.Publish(stt.Transcript{Text: "line", IsFinal: true, SpeechFinal: true})
	}

	snap := m.Snapshot()
	if snap.Web.SlowDrops == 0 {
		t.Error("expected slow-subscriber drops to be counted")
	}
	if len(sub) > subscriberBuffer {
		t.Errorf("subscriber buffer grew to %d, want <= %d", len(sub), subscriberBuffer)
	}
	// Publishing must still have completed: the pipeline was never blocked.
	if snap.STT.Final != int64(subscriberBuffer*3) {
		t.Errorf("published %d finals, want %d", snap.STT.Final, subscriberBuffer*3)
	}
}

// TestSnapshotForLateJoiner covers the browser-refresh case: a page that
// connects mid-session must not be blank.
func TestSnapshotForLateJoiner(t *testing.T) {
	h := NewHub(metrics.New("test", "test"))
	h.Publish(stt.Transcript{Text: "earlier line", IsFinal: true, SpeechFinal: true})
	h.Publish(stt.Transcript{Text: "in progress", Start: time.Second})

	sub, unsub := h.Subscribe()
	defer unsub()

	ev := <-sub
	if ev.Kind != KindSnapshot {
		t.Fatalf("first event kind = %q, want snapshot", ev.Kind)
	}
	if len(ev.Lines) != 1 || ev.Lines[0].Text != "earlier line" {
		t.Errorf("snapshot lines = %v", ev.Lines)
	}
	if ev.Pending != "in progress" {
		t.Errorf("snapshot pending = %q, want %q", ev.Pending, "in progress")
	}
}

// TestHistoryIsBounded stops a long event from growing memory without limit.
func TestHistoryIsBounded(t *testing.T) {
	h := NewHub(metrics.New("test", "test"))
	for i := 0; i < historyLimit+50; i++ {
		h.Publish(stt.Transcript{Text: "line", IsFinal: true, SpeechFinal: true})
	}
	lines, _ := h.Snapshot()
	if len(lines) != historyLimit {
		t.Errorf("history = %d lines, want %d", len(lines), historyLimit)
	}
}
