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
	Engine       string
	sttState     atomic.Int32
	sttReconnect atomic.Int64
	sttInterim   atomic.Int64
	sttFinal     atomic.Int64
	sttBytesSent atomic.Int64

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
	transcriptErr  string
	latency        []time.Duration // ring of the most recent samples
	latencyIdx     int
	latencyMax     time.Duration
	latencyLast    time.Duration
	mediaProcessed time.Duration
	mediaTotal     time.Duration // 0 for live (unknown length)
}

const latencyRing = 512

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

func (m *Metrics) DropFrame()             { m.framesDropped.Add(1) }
func (m *Metrics) FFmpegRestart()         { m.ffmpegRestarts.Add(1) }
func (m *Metrics) Xrun()                  { m.xruns.Add(1) }
func (m *Metrics) MonitorDrop()           { m.monitorDropped.Add(1) }
func (m *Metrics) SetMonitorAlive(v bool) { m.monitorAlive.Store(v) }

func (m *Metrics) SetLastStderr(s string) {
	m.mu.Lock()
	m.lastStderr = s
	m.mu.Unlock()
}

// --- STT ---

func (m *Metrics) SetSTTState(s ConnState) { m.sttState.Store(int32(s)) }
func (m *Metrics) STTState() ConnState     { return ConnState(m.sttState.Load()) }
func (m *Metrics) STTReconnect()           { m.sttReconnect.Add(1) }
func (m *Metrics) STTInterim()             { m.sttInterim.Add(1) }
func (m *Metrics) STTFinal()               { m.sttFinal.Add(1) }
func (m *Metrics) STTBytesSent(n int)      { m.sttBytesSent.Add(int64(n)) }

func (m *Metrics) SetSTTError(err error) {
	m.mu.Lock()
	if err != nil {
		m.sttLastErr = err.Error()
		m.sttLastErrAt = time.Now()
	}
	m.mu.Unlock()
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
func (m *Metrics) SSEDisconnect()  { m.sseClients.Add(-1) }
func (m *Metrics) SSEEvent()       { m.sseEvents.Add(1) }
func (m *Metrics) SSESlowDrop()    { m.sseSlowDrops.Add(1) }
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

// Clean reports whether the session had zero silent-degradation events. Drives
// the amber highlighting on /admin and in the shutdown summary.
func (s *Snapshot) Clean() bool {
	return s.Source.FramesDropped == 0 &&
		s.Source.FFmpegRestarts == 0 &&
		s.Source.Xruns == 0 &&
		s.Monitor.FramesDropped == 0 &&
		s.STT.Reconnects == 0 &&
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
	m.mu.RUnlock()

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
	if len(lat) > 0 {
		sorted := make([]time.Duration, len(lat))
		copy(sorted, lat)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		s.STT.LatencyP50 = ms(percentile(sorted, 0.50))
		s.STT.LatencyP95 = ms(percentile(sorted, 0.95))
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
