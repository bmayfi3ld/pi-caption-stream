package deepgram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"livecaption/internal/audio"
	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

func TestDecodeTranscript(t *testing.T) {
	cases := []struct {
		name string
		json string
		want stt.Transcript
		ok   bool
	}{
		{
			name: "interim result",
			json: `{"type":"Results","is_final":false,"speech_final":false,"start":1.5,"duration":0.4,
				"channel":{"alternatives":[{"transcript":"hello there","confidence":0.82}]}}`,
			want: stt.Transcript{
				Text: "hello there", IsFinal: false, SpeechFinal: false,
				Start: 1500 * time.Millisecond, Duration: 400 * time.Millisecond, Confidence: 0.82,
			},
			ok: true,
		},
		{
			name: "final result",
			json: `{"type":"Results","is_final":true,"speech_final":true,"start":0,"duration":2.1,
				"channel":{"alternatives":[{"transcript":"good morning everyone","confidence":0.95}]}}`,
			want: stt.Transcript{
				Text: "good morning everyone", IsFinal: true, SpeechFinal: true,
				Start: 0, Duration: 2100 * time.Millisecond, Confidence: 0.95,
			},
			ok: true,
		},
		{
			name: "empty transcript is skipped",
			json: `{"type":"Results","is_final":false,"channel":{"alternatives":[{"transcript":"","confidence":0}]}}`,
			ok:   false,
		},
		{
			name: "no alternatives is skipped",
			json: `{"type":"Results","channel":{"alternatives":[]}}`,
			ok:   false,
		},
		{
			name: "utterance end closes the line",
			json: `{"type":"UtteranceEnd","channel":[0],"last_word_end":2.5}`,
			want: stt.Transcript{SpeechFinal: true},
			ok:   true,
		},
		{
			name: "metadata is ignored",
			json: `{"type":"Metadata","request_id":"abc","duration":5.0}`,
			ok:   false,
		},
		{
			name: "speech started is ignored",
			json: `{"type":"SpeechStarted","channel":[0],"timestamp":0.1}`,
			ok:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := decodeTranscript([]byte(tc.json))
			if err != nil {
				t.Fatalf("decodeTranscript: %v", err)
			}
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if got.Text != tc.want.Text || got.IsFinal != tc.want.IsFinal ||
				got.SpeechFinal != tc.want.SpeechFinal || got.Start != tc.want.Start ||
				got.Duration != tc.want.Duration || got.Confidence != tc.want.Confidence {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// --- test server plumbing ---

// serverConn is what a scripted test handler gets: an accepted connection
// plus the request context to drive reads and writes with.
type serverConn struct {
	c   *websocket.Conn
	ctx context.Context
}

func (s serverConn) send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.c.Write(s.ctx, websocket.MessageText, b)
}

// drainBinary reads and discards audio frames until the connection ends or
// the client sends CloseStream, in which case it reports that on done.
func (s serverConn) drainBinary(done chan<- struct{}) {
	for {
		typ, data, err := s.c.Read(s.ctx)
		if err != nil {
			return
		}
		if typ == websocket.MessageBinary {
			continue
		}
		var m map[string]string
		if json.Unmarshal(data, &m) == nil && m["type"] == "CloseStream" {
			close(done)
			return
		}
	}
}

func newTestServer(t *testing.T, handle func(serverConn)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer c.CloseNow()
		handle(serverConn{c: c, ctx: r.Context()})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testEngine(wsURL string) *Engine {
	return &Engine{
		cfg: stt.Config{
			Format:   audio.PipelineFormat,
			Model:    "nova-3",
			Language: "en-US",
			APIKey:   "test-key",
			Metrics:  metrics.New("test", "session"),
		},
		wsURL: wsURL,
	}
}

func resultMsg(text string, isFinal, speechFinal bool, start, dur float64) map[string]any {
	return map[string]any{
		"type": "Results", "is_final": isFinal, "speech_final": speechFinal,
		"start": start, "duration": dur,
		"channel": map[string]any{
			"alternatives": []map[string]any{{"transcript": text, "confidence": 0.9}},
		},
	}
}

// --- session tests ---

func TestEngine_FullSession(t *testing.T) {
	var bytesReceived int64
	closeSeen := make(chan struct{})

	srv := newTestServer(t, func(sc serverConn) {
		go func() {
			for {
				typ, data, err := sc.c.Read(sc.ctx)
				if err != nil {
					return
				}
				if typ == websocket.MessageBinary {
					atomic.AddInt64(&bytesReceived, int64(len(data)))
					continue
				}
				var m map[string]string
				if json.Unmarshal(data, &m) == nil && m["type"] == "CloseStream" {
					close(closeSeen)
					return
				}
			}
		}()

		sc.send(resultMsg("hello", false, false, 0, 0.4))
		sc.send(resultMsg("hello world", true, false, 0, 1.0))
		sc.send(map[string]any{"type": "UtteranceEnd", "channel": []int{0}, "last_word_end": 1.0})

		select {
		case <-closeSeen:
		case <-time.After(2 * time.Second):
		}
	})

	eng := testEngine(srv.URL)

	frames := make(chan audio.Frame)
	out := make(chan stt.Transcript, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx, frames, out) }()

	frames <- audio.Frame{PCM: make([]byte, 3200), Offset: 100 * time.Millisecond, CapturedAt: time.Now()}
	close(frames)

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(out)

	var got []stt.Transcript
	for tr := range out {
		got = append(got, tr)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 transcripts, got %d: %+v", len(got), got)
	}
	if got[0].Text != "hello" || got[0].IsFinal {
		t.Errorf("interim wrong: %+v", got[0])
	}
	if got[1].Text != "hello world" || !got[1].IsFinal || got[1].SpeechFinal {
		t.Errorf("final segment wrong: %+v", got[1])
	}
	if got[2].Text != "" || !got[2].SpeechFinal {
		t.Errorf("utterance end wrong: %+v", got[2])
	}
	if atomic.LoadInt64(&bytesReceived) == 0 {
		t.Error("server never received audio")
	}
}

func TestEngine_Reconnect(t *testing.T) {
	var connNum int32

	srv := newTestServer(t, func(sc serverConn) {
		n := atomic.AddInt32(&connNum, 1)
		if n == 1 {
			// Simulate a mid-stream drop: one result, then the connection
			// dies without a close handshake.
			sc.send(resultMsg("first connection", false, false, 0, 0.3))
			time.Sleep(50 * time.Millisecond)
			sc.c.CloseNow()
			return
		}

		done := make(chan struct{})
		go sc.drainBinary(done)
		sc.send(resultMsg("second connection", true, true, 0, 1.0))
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})

	eng := testEngine(srv.URL)

	frames := make(chan audio.Frame)
	out := make(chan stt.Transcript, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx, frames, out) }()

	// Keep a trickle of audio flowing across the reconnect.
	stopFeeding := make(chan struct{})
	go func() {
		for {
			select {
			case frames <- audio.Frame{PCM: make([]byte, 320), Offset: time.Millisecond}:
				time.Sleep(10 * time.Millisecond)
			case <-stopFeeding:
				return
			}
		}
	}()

	var got []stt.Transcript
	timeout := time.After(5 * time.Second)
loop:
	for len(got) < 2 {
		select {
		case tr := <-out:
			got = append(got, tr)
		case <-timeout:
			t.Fatal("timed out waiting for transcripts across reconnect")
		case <-done:
			break loop
		}
	}
	// Cancel first so the engine's internal frame reader stops pulling from
	// frames before the feeder goroutine is told to stop; otherwise the
	// feeder can be left trying to send with nothing left reading.
	cancel()
	close(stopFeeding)
	<-done

	if len(got) < 2 {
		t.Fatalf("expected transcripts from both connections, got %+v", got)
	}
	if got[0].Text != "first connection" {
		t.Errorf("first connection text = %q", got[0].Text)
	}
	if got[1].Text != "second connection" || !got[1].SpeechFinal {
		t.Errorf("second connection transcript wrong: %+v", got[1])
	}
	if eng.cfg.Metrics.Snapshot().STT.Reconnects == 0 {
		t.Error("expected STTReconnect to be counted")
	}
}

// TestEngine_DropOldestWhenDisconnected verifies that pushing audio while the
// connection is down (or never comes up) never blocks the caller, and that
// once the ring is full it drops the oldest frame and counts it.
func TestEngine_DropOldestWhenDisconnected(t *testing.T) {
	// Point at a server that never completes the handshake, so the engine
	// stays in its connect/backoff loop for the duration of the test.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	eng := testEngine(srv.URL)

	frames := make(chan audio.Frame)
	out := make(chan stt.Transcript, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx, frames, out) }()

	// 2s of buffer at 16kHz mono s16 is 64000 bytes; push well past that
	// without ever being drained, and never let the send block.
	chunk := make([]byte, 3200) // 100ms
	for i := 0; i < 40; i++ {
		select {
		case frames <- audio.Frame{PCM: chunk, Offset: time.Duration(i) * 100 * time.Millisecond}:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("send on frames blocked while disconnected")
		}
	}
	close(frames)
	cancel()
	<-done

	if got := eng.cfg.Metrics.Snapshot().Source.FramesDropped; got == 0 {
		t.Error("expected DropFrame to be counted while disconnected")
	}
}

func TestEngine_AuthFailureFailsFast(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)

	eng := testEngine(srv.URL)

	frames := make(chan audio.Frame)
	out := make(chan stt.Transcript, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- eng.Run(ctx, frames, out) }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected an error for a 401 on first connect")
		}
		if !strings.Contains(err.Error(), "DEEPGRAM_API_KEY") {
			t.Errorf("error should mention DEEPGRAM_API_KEY, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("auth failure did not return promptly")
	}
	close(frames)
}
