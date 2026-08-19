// Package deepgram streams PCM audio to Deepgram's real-time speech-to-text
// API over a WebSocket and turns its JSON messages into stt.Transcript.
//
// The connection is inherently unreliable (idle timeouts, network blips,
// server restarts), so most of this file is reconnect plumbing: a writer and
// a reader goroutine per connection, a bounded audio buffer that survives a
// reconnect, and exponential backoff around redials.
package deepgram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"

	"livecaption/internal/audio"
	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

func init() {
	stt.Register("deepgram", func(cfg stt.Config) (stt.Engine, error) {
		return &Engine{cfg: cfg}, nil
	})
}

const (
	endpoint = "wss://api.deepgram.com/v1/listen"

	// keepAliveInterval matches Deepgram's documented idle timeout: without
	// traffic for ~10s the server drops the connection, so 5s of silence is
	// enough margin to never trip it.
	keepAliveInterval = 5 * time.Second

	// readLimit accommodates Results messages with full word arrays, which
	// exceed the library's 32KB default read limit on longer utterances.
	readLimit = 1 << 20

	minBackoff = 250 * time.Millisecond
	maxBackoff = 8 * time.Second

	// bufferAudio is how much PCM survives a reconnect: enough to smooth a
	// brief network blip without dumping a stale chunk of audio on Deepgram
	// once the link recovers.
	bufferAudio = 2 * time.Second

	// drainTimeout bounds how long shutdown waits for trailing Results after
	// CloseStream, so ending a session can't hang on a stalled server.
	drainTimeout = 3 * time.Second
)

// Engine streams PCM to Deepgram's real-time API and turns its JSON messages
// into stt.Transcript. It owns its own reconnect logic per the stt.Engine
// contract: Run returns only when ctx is cancelled or frames run out.
type Engine struct {
	cfg stt.Config

	// wsURL overrides the Deepgram endpoint. Only ever set by tests, which
	// point it at an httptest server instead of the real API.
	wsURL string
}

func (e *Engine) Name() string { return "deepgram" }

func (e *Engine) Run(ctx context.Context, frames <-chan audio.Frame, out chan<- stt.Transcript) error {
	log := slog.Default()
	met := e.cfg.Metrics

	capBytes := e.cfg.Format.BytesFor(bufferAudio)
	if capBytes <= 0 {
		// Config.Format is zero-valued (e.g. a misconfigured caller); fall
		// back to the pipeline's own rate rather than buffering nothing.
		capBytes = audio.PipelineFormat.BytesFor(bufferAudio)
	}
	buf := newRing(capBytes, met)

	// Drains frames into buf for the whole lifetime of Run, independent of
	// connection state, so the audio source is never blocked by a dead or
	// reconnecting link.
	framesClosed := make(chan struct{})
	go func() {
		defer close(framesClosed)
		for {
			select {
			case f, ok := <-frames:
				if !ok {
					return
				}
				buf.push(f.PCM)
			case <-ctx.Done():
				return
			}
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	backoff := minBackoff
	firstAttempt := true

	for {
		if audioExhausted(framesClosed, buf) {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		if met != nil {
			met.SetSTTState(metrics.StateConnecting)
		}
		conn, err := e.connect(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if firstAttempt && isAuthError(err) {
				return fmt.Errorf("deepgram: %w (check DEEPGRAM_API_KEY)", err)
			}
			firstAttempt = false
			if met != nil {
				met.SetSTTError(err)
				met.SetSTTState(metrics.StateReconnecting)
				met.STTReconnect()
			}
			log.Warn("deepgram: connect failed, retrying", "err", err, "retry_in", backoff)
			if !sleepBackoff(ctx, backoff, rng) {
				return nil
			}
			backoff = nextBackoff(backoff)
			continue
		}

		firstAttempt = false
		backoff = minBackoff
		if met != nil {
			met.SetSTTState(metrics.StateConnected)
		}
		log.Info("deepgram: connected")

		reconnect, rerr := e.runConnection(ctx, conn, buf, framesClosed, out, log, met)
		if !reconnect {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		if met != nil {
			met.SetSTTError(rerr)
			met.SetSTTState(metrics.StateReconnecting)
			met.STTReconnect()
		}
		log.Warn("deepgram: disconnected, reconnecting", "err", rerr, "retry_in", backoff)
		if !sleepBackoff(ctx, backoff, rng) {
			return nil
		}
		backoff = nextBackoff(backoff)
	}
}

func audioExhausted(framesClosed <-chan struct{}, buf *ring) bool {
	select {
	case <-framesClosed:
		return buf.empty()
	default:
		return false
	}
}

// runConnection drives one WebSocket lifetime: a writer goroutine sending PCM
// and KeepAlives, a reader goroutine turning Results into Transcripts. It
// returns reconnect=true when the link was lost and Run should redial.
func (e *Engine) runConnection(
	ctx context.Context,
	conn *websocket.Conn,
	buf *ring,
	framesClosed <-chan struct{},
	out chan<- stt.Transcript,
	log *slog.Logger,
	met *metrics.Metrics,
) (reconnect bool, retErr error) {
	// connCtx is deliberately not derived from ctx: on shutdown we want to
	// keep reading trailing Results for a bit after CloseStream, which a
	// ctx-derived context would cut off immediately.
	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()

	readErr := make(chan error, 1)
	go func() { readErr <- e.readLoop(connCtx, conn, out, met, log) }()

	writeErr := make(chan error, 1)
	go func() { writeErr <- e.writeLoop(ctx, connCtx, conn, buf, framesClosed, met) }()

	select {
	case werr := <-writeErr:
		if werr != nil {
			// A real write failure: the connection is already dead, no point
			// sending CloseStream.
			cancelConn()
			<-readErr
			conn.CloseNow()
			return true, werr
		}
		// Audio is exhausted (frames closed and drained) or ctx was
		// cancelled: wrap up politely so the tail of the session isn't lost.
		e.finish(conn, readErr, log)
		return false, nil

	case rerr := <-readErr:
		// The server ended the session or the read failed outright.
		cancelConn()
		<-writeErr
		conn.CloseNow()
		return true, rerr
	}
}

// finish sends CloseStream and waits for the server to either close its end
// or go quiet for drainTimeout, so trailing Results aren't lost.
func (e *Engine) finish(conn *websocket.Conn, readErr <-chan error, log *slog.Logger) {
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := writeJSON(closeCtx, conn, controlMessage{Type: "CloseStream"}); err != nil {
		log.Debug("deepgram: failed to send CloseStream", "err", err)
	}
	select {
	case <-readErr:
	case <-time.After(drainTimeout):
		log.Debug("deepgram: drain timed out waiting for trailing results")
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

// writeLoop drains buf into binary WebSocket frames and sends a KeepAlive
// whenever 5s pass with no audio sent. It returns nil on a clean end (audio
// exhausted or ctx cancelled) and a non-nil error only on a real write
// failure, which the caller treats as "reconnect".
func (e *Engine) writeLoop(
	ctx context.Context,
	connCtx context.Context,
	conn *websocket.Conn,
	buf *ring,
	framesClosed <-chan struct{},
	met *metrics.Metrics,
) error {
	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()
	lastActivity := time.Now()

	for {
		if pcm, ok := buf.pop(); ok {
			if err := conn.Write(connCtx, websocket.MessageBinary, pcm); err != nil {
				return err
			}
			if met != nil {
				met.STTBytesSent(len(pcm))
			}
			lastActivity = time.Now()
			continue
		}

		select {
		case <-framesClosed:
			if buf.empty() {
				return nil
			}
			// Frames closed but buf still has data queued from just before
			// the close; loop back around to drain it.
		case <-buf.notify:
		case <-ticker.C:
			if time.Since(lastActivity) >= keepAliveInterval {
				if err := writeJSON(connCtx, conn, controlMessage{Type: "KeepAlive"}); err != nil {
					return err
				}
				lastActivity = time.Now()
			}
		case <-ctx.Done():
			return nil
		case <-connCtx.Done():
			return connCtx.Err()
		}
	}
}

// readLoop decodes server messages into Transcripts until the connection
// fails or ctx is cancelled.
func (e *Engine) readLoop(ctx context.Context, conn *websocket.Conn, out chan<- stt.Transcript, met *metrics.Metrics, log *slog.Logger) error {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		t, ok, err := decodeTranscript(data)
		if err != nil {
			log.Debug("deepgram: undecodable message", "err", err)
			continue
		}
		if !ok {
			continue
		}
		t.ReceivedAt = time.Now()
		select {
		case out <- t:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// connect dials Deepgram and returns a ready-to-use connection. Failures that
// carry an HTTP status are wrapped in dialError so the caller can recognize
// an auth failure and fail fast instead of retrying forever.
func (e *Engine) connect(ctx context.Context) (*websocket.Conn, error) {
	h := http.Header{}
	h.Set("Authorization", "Token "+e.cfg.APIKey)

	conn, resp, err := websocket.Dial(ctx, e.dialURL(), &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		if resp != nil {
			return nil, &dialError{status: resp.StatusCode, err: err}
		}
		return nil, err
	}
	conn.SetReadLimit(readLimit)
	return conn, nil
}

func (e *Engine) dialURL() string {
	base := e.wsURL
	if base == "" {
		base = endpoint
	}

	q := url.Values{}
	q.Set("encoding", encodingFor(e.cfg.Format))
	q.Set("sample_rate", strconv.Itoa(e.cfg.Format.SampleRate))
	q.Set("channels", strconv.Itoa(e.cfg.Format.Channels))
	q.Set("model", e.cfg.Model)
	q.Set("language", e.cfg.Language)
	q.Set("interim_results", "true")
	q.Set("punctuate", "true")
	q.Set("smart_format", "true")
	q.Set("endpointing", "300")
	q.Set("utterance_end_ms", "1000")
	q.Set("vad_events", "true")
	for _, k := range e.cfg.Keyterms {
		q.Add("keyterm", k)
	}
	return base + "?" + q.Encode()
}

// encodingFor names the PCM layout for Deepgram's `encoding` parameter. The
// pipeline is fixed at 16-bit signed samples, but this stays format-driven
// rather than hardcoded in case that ever changes.
func encodingFor(f audio.Format) string {
	if f.BitDepth == 8 {
		return "mulaw"
	}
	return "linear16"
}

// dialError carries the HTTP status from a failed handshake, so a 401/403 can
// be told apart from a plain network failure.
type dialError struct {
	status int
	err    error
}

func (e *dialError) Error() string { return e.err.Error() }
func (e *dialError) Unwrap() error { return e.err }

func isAuthError(err error) bool {
	var de *dialError
	if errors.As(err, &de) {
		return de.status == http.StatusUnauthorized || de.status == http.StatusForbidden
	}
	return false
}

// sleepBackoff waits a jittered duration around d, reporting whether it slept
// to completion (false means ctx was cancelled first).
func sleepBackoff(ctx context.Context, d time.Duration, rng *rand.Rand) bool {
	half := d / 2
	jittered := half + time.Duration(rng.Int63n(int64(half)+1))
	t := time.NewTimer(jittered)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

type controlMessage struct {
	Type string `json:"type"`
}

func writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, b)
}

// messageType is decoded first so the payload can be unmarshaled with a
// shape matching its type: Results carries a "channel" object, but
// UtteranceEnd and SpeechStarted carry "channel" as a plain index array, and
// unmarshaling both through one struct would fail on whichever shape didn't
// match.
type messageType struct {
	Type string `json:"type"`
}

// resultsMessage matches the subset of Deepgram's Results JSON this engine
// cares about; word timings and everything else are decoded straight through
// and ignored.
type resultsMessage struct {
	IsFinal     bool    `json:"is_final"`
	SpeechFinal bool    `json:"speech_final"`
	Start       float64 `json:"start"`
	Duration    float64 `json:"duration"`
	Channel     struct {
		Alternatives []struct {
			Transcript string  `json:"transcript"`
			Confidence float64 `json:"confidence"`
		} `json:"alternatives"`
	} `json:"channel"`
}

// decodeTranscript turns one server message into a Transcript. ok is false
// for messages that carry nothing worth publishing: empty Results (no speech
// yet), Metadata, SpeechStarted, and any other unrecognized type.
func decodeTranscript(data []byte) (t stt.Transcript, ok bool, err error) {
	var head messageType
	if err := json.Unmarshal(data, &head); err != nil {
		return stt.Transcript{}, false, err
	}

	switch head.Type {
	case "Results":
		var msg resultsMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return stt.Transcript{}, false, err
		}
		if len(msg.Channel.Alternatives) == 0 {
			return stt.Transcript{}, false, nil
		}
		alt := msg.Channel.Alternatives[0]
		if alt.Transcript == "" {
			return stt.Transcript{}, false, nil
		}
		return stt.Transcript{
			Text:        alt.Transcript,
			IsFinal:     msg.IsFinal,
			SpeechFinal: msg.SpeechFinal,
			Start:       secondsToDuration(msg.Start),
			Duration:    secondsToDuration(msg.Duration),
			Confidence:  alt.Confidence,
		}, true, nil

	case "UtteranceEnd":
		// No text of its own; it closes whatever line the hub has pending.
		return stt.Transcript{SpeechFinal: true}, true, nil

	default:
		return stt.Transcript{}, false, nil
	}
}

func secondsToDuration(s float64) time.Duration {
	return time.Duration(s * float64(time.Second))
}

// ring holds PCM frames while the connection is down or catching up,
// dropping the oldest frame once full so a blip never backpressures capture.
type ring struct {
	mu       sync.Mutex
	frames   [][]byte
	bytes    int
	capBytes int
	met      *metrics.Metrics

	// notify wakes writeLoop when data arrives; buffered 1 and non-blocking
	// to push so a slow or absent reader of it never stalls push.
	notify chan struct{}
}

func newRing(capBytes int, met *metrics.Metrics) *ring {
	return &ring{capBytes: capBytes, met: met, notify: make(chan struct{}, 1)}
}

func (r *ring) push(pcm []byte) {
	r.mu.Lock()
	r.frames = append(r.frames, pcm)
	r.bytes += len(pcm)
	for r.bytes > r.capBytes && len(r.frames) > 1 {
		dropped := r.frames[0]
		r.frames = r.frames[1:]
		r.bytes -= len(dropped)
		if r.met != nil {
			r.met.DropFrame()
		}
	}
	r.mu.Unlock()

	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *ring) pop() ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.frames) == 0 {
		return nil, false
	}
	f := r.frames[0]
	r.frames = r.frames[1:]
	r.bytes -= len(f)
	return f, true
}

func (r *ring) empty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.frames) == 0
}
