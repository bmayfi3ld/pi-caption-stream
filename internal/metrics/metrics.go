// Package metrics holds the single mutable snapshot of how the session is
// going. The admin page, the status line and the shutdown summary all read
// from here, so they can never disagree with each other.
//
// Rule of thumb: anything that can degrade silently gets a counter here.
package metrics

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ConnState is the STT connection state, shown on the status line and /admin.
type ConnState int32

const (
	StateIdle ConnState = iota
	StateConnecting
	StateConnected
	StateReconnecting
	StateClosed
	// StatePaused is appended after StateClosed so existing values don't
	// renumber; a session that never auto-pauses never sees it.
	StatePaused
)

func (s ConnState) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateClosed:
		return "closed"
	case StatePaused:
		return "paused"
	default:
		return "idle"
	}
}

// Metrics is safe for concurrent use by every stage of the pipeline.
type Metrics struct {
	// Immutable session identity, set once at startup.
	Version   string
	SessionID string
	StartedAt time.Time

	// Source.
	SourceKind   string // "replay" or "live"
	SourceSpec   string // file path or device name
	SourceFormat string // "44100 Hz stereo -> 16000 Hz mono"

	framesTotal    atomic.Int64
	bytesTotal     atomic.Int64
	framesDropped  atomic.Int64
	ffmpegRestarts atomic.Int64
	xruns          atomic.Int64

	// Monitor (replay --monitor only).
	MonitorEnabled  bool
	MonitorDevice   string
	MonitorBufferMS int
	monitorDropped  atomic.Int64
	monitorAlive    atomic.Bool

	// STT.
	Engine         string
	sttState       atomic.Int32
	sttStateHook   atomic.Pointer[func(ConnState)]
	sttReconnect   atomic.Int64
	sttInterim     atomic.Int64
	sttFinal       atomic.Int64
	sttBytesSent   atomic.Int64
	sttPauses      atomic.Int64
	sttBufferDrops atomic.Int64

	// Web.
	sseClients      atomic.Int64
	sseClientsTotal atomic.Int64
	sseEvents       atomic.Int64
	sseSlowDrops    atomic.Int64

	// Transcript.
	TranscriptPath string
	transcriptLine atomic.Int64
	transcriptByte atomic.Int64

	mu             sync.RWMutex
	lastStderr     string
	sttLastErr     string
	sttLastErrAt   time.Time
	sttPauseStart  time.Time     // zero when no pause is currently open
	sttPausedTotal time.Duration // accumulated from completed pauses only
	transcriptErr  string
	latency        []time.Duration // ring of the most recent samples
	latencyIdx     int
	latencyMax     time.Duration
	latencyLast    time.Duration
	mediaProcessed time.Duration
	mediaTotal     time.Duration // 0 for live (unknown length)
	lastDegradedAt time.Time     // zero until the first silent-degradation event
}

const latencyRing = 512

// degradedWindow is how long a silent-degradation event keeps the /admin
// health badge at "degraded" after it happens. A ring eviction or a
// reconnect is worth flagging right away, but the badge must not latch
// there forever off one blip from an hour ago — it reports current state,
// the cumulative counters (Clean, the tiles) are what remember the whole
// session.
const degradedWindow = 60 * time.Second

func New(version, sessionID string) *Metrics {
	return &Metrics{
		Version:   version,
		SessionID: sessionID,
		StartedAt: time.Now(),
		latency:   make([]time.Duration, 0, latencyRing),
	}
}

// --- Source ---

// AddFrame records one PCM frame reaching the pipeline. offset is the media
// time of the frame's last sample, which is what drives progress display.
func (m *Metrics) AddFrame(nbytes int, offset time.Duration) {
	m.framesTotal.Add(1)
	m.bytesTotal.Add(int64(nbytes))
	m.mu.Lock()
	m.mediaProcessed = offset
	m.mu.Unlock()
}

func (m *Metrics) SetMediaTotal(d time.Duration) {
	m.mu.Lock()
	m.mediaTotal = d
	m.mu.Unlock()
}

// markDegradedLocked stamps the time of the most recent silent-degradation
// event, which the health badge in Snapshot() uses to decide "degraded" vs
// "ok". Callers must already hold m.mu — it never takes the lock itself, so
// that SetTranscriptError (which holds m.mu across its own write) can call
// it inline instead of re-entering a non-reentrant sync.RWMutex.
func (m *Metrics) markDegradedLocked() {
	m.lastDegradedAt = time.Now()
}

func (m *Metrics) DropFrame() {
	m.framesDropped.Add(1)
	m.mu.Lock()
	m.markDegradedLocked()
	m.mu.Unlock()
}
func (m *Metrics) FFmpegRestart() {
	m.ffmpegRestarts.Add(1)
	m.mu.Lock()
	m.markDegradedLocked()
	m.mu.Unlock()
}
func (m *Metrics) Xrun() {
	m.xruns.Add(1)
	m.mu.Lock()
	m.markDegradedLocked()
	m.mu.Unlock()
}
func (m *Metrics) MonitorDrop() {
	m.monitorDropped.Add(1)
	m.mu.Lock()
	m.markDegradedLocked()
	m.mu.Unlock()
}
func (m *Metrics) SetMonitorAlive(v bool) { m.monitorAlive.Store(v) }

func (m *Metrics) SetLastStderr(s string) {
	m.mu.Lock()
	m.lastStderr = s
	m.mu.Unlock()
}

// --- STT ---

// SetSTTState fires the hook registered with SetSTTStateHook only when the
// value actually changes, so a stretch of identical states (e.g. repeated
// StateConnected sets) doesn't spam status subscribers with no-op events.
func (m *Metrics) SetSTTState(s ConnState) {
	old := m.sttState.Swap(int32(s))
	if old == int32(s) {
		return
	}
	if hook := m.sttStateHook.Load(); hook != nil {
		(*hook)(s)
	}
}
func (m *Metrics) STTState() ConnState { return ConnState(m.sttState.Load()) }
func (m *Metrics) STTReconnect() {
	m.sttReconnect.Add(1)
	m.mu.Lock()
	m.markDegradedLocked()
	m.mu.Unlock()
}
func (m *Metrics) STTInterim()        { m.sttInterim.Add(1) }
func (m *Metrics) STTFinal()          { m.sttFinal.Add(1) }
func (m *Metrics) STTBytesSent(n int) { m.sttBytesSent.Add(int64(n)) }

// STTBufferDrop records an eviction from the reconnect ring that happened
// while the gate was active — i.e. the link is not keeping up with live
// audio, not the pre-roll buffer discarding stale silence during a pause.
// See ring.push in internal/stt/deepgram/deepgram.go for the gating logic.
func (m *Metrics) STTBufferDrop() {
	m.sttBufferDrops.Add(1)
	m.mu.Lock()
	m.markDegradedLocked()
	m.mu.Unlock()
}

// SetSTTStateHook registers a callback invoked when the STT state changes.
// Set once before the session starts; called from engine goroutines, so the
// callback must not block.
func (m *Metrics) SetSTTStateHook(f func(ConnState)) {
	m.sttStateHook.Store(&f)
}

func (m *Metrics) SetSTTError(err error) {
	m.mu.Lock()
	if err != nil {
		m.sttLastErr = err.Error()
		m.sttLastErrAt = time.Now()
	}
	m.mu.Unlock()
}

// STTPauseBegin records the start of an automatic pause and increments the
// pause counter. Idempotent: a second Begin before the matching End leaves
// the original start time in place, so a stray duplicate call can't inflate
// PausedSec.
func (m *Metrics) STTPauseBegin() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.sttPauseStart.IsZero() {
		return
	}
	m.sttPauseStart = time.Now()
	m.sttPauses.Add(1)
}

// STTPauseEnd accumulates the elapsed time of the currently open pause.
// Idempotent: a call with no open pause (no matching Begin, or already
// ended) is a no-op.
func (m *Metrics) STTPauseEnd() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sttPauseStart.IsZero() {
		return
	}
	m.sttPausedTotal += time.Since(m.sttPauseStart)
	m.sttPauseStart = time.Time{}
}

// ObserveLatency records how far behind wall clock a caption arrived.
func (m *Metrics) ObserveLatency(d time.Duration) {
	if d < 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencyLast = d
	if d > m.latencyMax {
		m.latencyMax = d
	}
	if len(m.latency) < latencyRing {
		m.latency = append(m.latency, d)
		return
	}
	m.latency[m.latencyIdx] = d
	m.latencyIdx = (m.latencyIdx + 1) % latencyRing
}

// --- Web ---

func (m *Metrics) SSEConnect() {
	m.sseClients.Add(1)
	m.sseClientsTotal.Add(1)
}
func (m *Metrics) SSEDisconnect() { m.sseClients.Add(-1) }
func (m *Metrics) SSEEvent()      { m.sseEvents.Add(1) }
func (m *Metrics) SSESlowDrop() {
	m.sseSlowDrops.Add(1)
	m.mu.Lock()
	m.markDegradedLocked()
	m.mu.Unlock()
}
func (m *Metrics) SSEClients() int { return int(m.sseClients.Load()) }

// --- Transcript ---

func (m *Metrics) TranscriptWrote(lines, bytes int) {
	m.transcriptLine.Add(int64(lines))
	m.transcriptByte.Add(int64(bytes))
}

func (m *Metrics) SetTranscriptError(err error) {
	m.mu.Lock()
	if err != nil {
		m.transcriptErr = err.Error()
		// Call markDegradedLocked directly rather than through a
		// self-locking wrapper: we already hold m.mu here, and it's a plain
		// sync.RWMutex, not reentrant — taking it twice would deadlock.
		m.markDegradedLocked()
	}
	m.mu.Unlock()
}

// --- Snapshot ---

// Snapshot is a consistent point-in-time copy, serialized straight to JSON for
// /api/stats and used verbatim by the status line and shutdown summary.
type Snapshot struct {
	Version   string    `json:"version"`
	SessionID string    `json:"session_id"`
	StartedAt time.Time `json:"started_at"`
	UptimeSec float64   `json:"uptime_sec"`
	// Health is the server-computed "what is happening right now" summary —
	// "closed" / "paused" / "degraded" / "ok" — so the status line, /admin
	// and the shutdown summary read the exact same verdict instead of each
	// re-deriving their own from the raw counters.
	Health string `json:"health"`

	Source struct {
		Kind           string  `json:"kind"`
		Spec           string  `json:"spec"`
		Format         string  `json:"format"`
		FramesTotal    int64   `json:"frames_total"`
		BytesTotal     int64   `json:"bytes_total"`
		SecondsTotal   float64 `json:"seconds_total"`
		TotalSeconds   float64 `json:"total_seconds"` // 0 when unknown (live)
		FramesDropped  int64   `json:"frames_dropped_total"`
		FFmpegRestarts int64   `json:"ffmpeg_restarts_total"`
		Xruns          int64   `json:"xruns_total"`
		LastStderr     string  `json:"ffmpeg_last_stderr"`
	} `json:"source"`

	Monitor struct {
		Enabled       bool   `json:"enabled"`
		Device        string `json:"device"`
		BufferMS      int    `json:"buffer_ms"`
		Alive         bool   `json:"alive"`
		FramesDropped int64  `json:"frames_dropped_total"`
	} `json:"monitor"`

	STT struct {
		Engine       string  `json:"engine"`
		State        string  `json:"state"`
		Reconnects   int64   `json:"reconnects_total"`
		BufferDrops  int64   `json:"buffer_drops_total"`
		Interim      int64   `json:"interim_total"`
		Final        int64   `json:"final_total"`
		BytesSent    int64   `json:"bytes_sent_total"`
		LastError    string  `json:"last_error"`
		LastErrorAt  string  `json:"last_error_at"`
		LatencyLast  float64 `json:"latency_last_ms"`
		LatencyP50   float64 `json:"latency_p50_ms"`
		LatencyP95   float64 `json:"latency_p95_ms"`
		LatencyMax   float64 `json:"latency_max_ms"`
		LatencyCount int     `json:"latency_samples"`
		Pauses       int64   `json:"pauses_total"`
		PausedSec    float64 `json:"paused_sec"`
	} `json:"stt"`

	Web struct {
		Clients      int64 `json:"sse_clients"`
		ClientsTotal int64 `json:"sse_clients_total"`
		Events       int64 `json:"events_total"`
		SlowDrops    int64 `json:"slow_disconnects_total"`
	} `json:"web"`

	Transcript struct {
		Path      string `json:"path"`
		Lines     int64  `json:"lines_written"`
		Bytes     int64  `json:"bytes_written"`
		LastError string `json:"last_write_error"`
	} `json:"transcript"`

	Goroutines int `json:"goroutines"`
}

// Clean reports whether the session had zero silent-degradation events, front
// to back — the cumulative whole-session verdict, unlike Health above, which
// is a live, recency-windowed snapshot of what's happening right now and is
// what actually drives the /admin badge. No caller in this codebase reads
// Clean today: the shutdown summary flags each counter individually via its
// own amber() check so a specific problem is named instead of a single "not
// clean" bit. It remains the one-shot verdict for a caller that just wants a
// yes/no over the whole session.
func (s *Snapshot) Clean() bool {
	return s.Source.FramesDropped == 0 &&
		s.Source.FFmpegRestarts == 0 &&
		s.Source.Xruns == 0 &&
		s.Monitor.FramesDropped == 0 &&
		s.STT.Reconnects == 0 &&
		s.STT.BufferDrops == 0 &&
		s.Web.SlowDrops == 0 &&
		s.Transcript.LastError == ""
}

func (m *Metrics) Snapshot() Snapshot {
	m.mu.RLock()
	lat := make([]time.Duration, len(m.latency))
	copy(lat, m.latency)
	lastStderr, sttErr, sttErrAt := m.lastStderr, m.sttLastErr, m.sttLastErrAt
	transErr := m.transcriptErr
	latMax, latLast := m.latencyMax, m.latencyLast
	processed, total := m.mediaProcessed, m.mediaTotal
	pauseStart, pausedTotal := m.sttPauseStart, m.sttPausedTotal
	lastDegradedAt := m.lastDegradedAt
	m.mu.RUnlock()

	// A pause still in progress must count toward PausedSec so a long pause
	// shows live on /admin rather than only jumping once it ends.
	if !pauseStart.IsZero() {
		pausedTotal += time.Since(pauseStart)
	}

	var s Snapshot
	s.Version = m.Version
	s.SessionID = m.SessionID
	s.StartedAt = m.StartedAt
	s.UptimeSec = time.Since(m.StartedAt).Seconds()

	s.Source.Kind = m.SourceKind
	s.Source.Spec = m.SourceSpec
	s.Source.Format = m.SourceFormat
	s.Source.FramesTotal = m.framesTotal.Load()
	s.Source.BytesTotal = m.bytesTotal.Load()
	s.Source.SecondsTotal = processed.Seconds()
	s.Source.TotalSeconds = total.Seconds()
	s.Source.FramesDropped = m.framesDropped.Load()
	s.Source.FFmpegRestarts = m.ffmpegRestarts.Load()
	s.Source.Xruns = m.xruns.Load()
	s.Source.LastStderr = lastStderr

	s.Monitor.Enabled = m.MonitorEnabled
	s.Monitor.Device = m.MonitorDevice
	s.Monitor.BufferMS = m.MonitorBufferMS
	s.Monitor.Alive = m.monitorAlive.Load()
	s.Monitor.FramesDropped = m.monitorDropped.Load()

	s.STT.Engine = m.Engine
	s.STT.State = ConnState(m.sttState.Load()).String()
	s.STT.Reconnects = m.sttReconnect.Load()
	s.STT.BufferDrops = m.sttBufferDrops.Load()
	s.STT.Interim = m.sttInterim.Load()
	s.STT.Final = m.sttFinal.Load()
	s.STT.BytesSent = m.sttBytesSent.Load()
	s.STT.LastError = sttErr
	if !sttErrAt.IsZero() {
		s.STT.LastErrorAt = sttErrAt.Format(time.RFC3339)
	}
	s.STT.LatencyLast = ms(latLast)
	s.STT.LatencyMax = ms(latMax)
	s.STT.LatencyCount = len(lat)
	s.STT.Pauses = m.sttPauses.Load()
	s.STT.PausedSec = pausedTotal.Seconds()
	if len(lat) > 0 {
		sorted := make([]time.Duration, len(lat))
		copy(sorted, lat)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		s.STT.LatencyP50 = ms(percentile(sorted, 0.50))
		s.STT.LatencyP95 = ms(percentile(sorted, 0.95))
	}

	// Health is resolved after STT.State: a closed or paused connection is
	// reported as exactly that rather than "degraded", even if a drop
	// counter ticked earlier in the session — the pause/close already
	// explains the current state, and stacking "degraded" on top of it
	// would just be noise. Only once the link is actually up (or was, and
	// nothing else claims the state) does a recent degradation event win.
	//
	// The degraded check itself is two different shapes of "not ok" ORed
	// together: lastDegradedAt is a point event (a drop, a reconnect) that
	// happened once and is only worth flagging for degradedWindow after the
	// fact, so a blip from an hour ago doesn't latch the badge forever.
	// transErr is the opposite — a condition, not an event: it's set once
	// and never cleared, so it stays true for as long as transcript writes
	// are actually failing. Aging it out on the same timer as a point event
	// would let the badge go green while the transcript diag panel right
	// below it is still showing a live write error, which is exactly the
	// kind of self-contradiction this phase exists to eliminate.
	switch {
	case s.STT.State == StateClosed.String():
		s.Health = "closed"
	case s.STT.State == StatePaused.String():
		s.Health = "paused"
	case (!lastDegradedAt.IsZero() && time.Since(lastDegradedAt) <= degradedWindow) || transErr != "":
		s.Health = "degraded"
	default:
		s.Health = "ok"
	}

	s.Web.Clients = m.sseClients.Load()
	s.Web.ClientsTotal = m.sseClientsTotal.Load()
	s.Web.Events = m.sseEvents.Load()
	s.Web.SlowDrops = m.sseSlowDrops.Load()

	s.Transcript.Path = m.TranscriptPath
	s.Transcript.Lines = m.transcriptLine.Load()
	s.Transcript.Bytes = m.transcriptByte.Load()
	s.Transcript.LastError = transErr

	s.Goroutines = runtime.NumGoroutine()
	return s
}

// percentile returns the p-th percentile of an already-sorted slice using
// nearest-rank, which is stable and needs no interpolation for our purposes.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
