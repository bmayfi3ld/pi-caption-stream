package audio

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"
)

// MonitorConfig configures speaker playback of the streamed audio.
type MonitorConfig struct {
	Device   string // pulse sink or ALSA device; "default" is usually right
	Backend  string // "pulse" or "alsa"
	BufferMS int    // playback buffer; adds this much to perceived delay
	Log      *slog.Logger

	OnDrop  func()
	OnAlive func(bool)
}

// Monitor plays the exact frames the pipeline is sending to the STT service,
// so caption delay can be judged by ear.
//
// The tap point is deliberate: playing the original file with a separate
// player would drift against our scheduler and ignore --speed. Teeing the
// frames we already emit means what you hear is bit-identical to what the
// recognizer receives, released by the same clock. It also means you hear the
// 16 kHz mono downmix, which is the point — bad source audio becomes audible
// instead of being inferred from bad transcripts.
type Monitor struct {
	cfg   MonitorConfig
	ch    chan []byte
	proc  *proc
	once  sync.Once
	alive bool
	mu    sync.Mutex
}

func NewMonitor(cfg MonitorConfig) *Monitor {
	if cfg.BufferMS <= 0 {
		cfg.BufferMS = 80
	}
	if cfg.Backend == "" {
		cfg.Backend = "pulse"
	}
	if cfg.Device == "" {
		cfg.Device = "default"
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	// Roughly two seconds of frames. Deep enough to ride out a scheduling
	// hiccup, shallow enough that we notice a genuinely stalled sink.
	return &Monitor{cfg: cfg, ch: make(chan []byte, 20)}
}

// SetCallbacks registers metric hooks. Set before Start.
func (m *Monitor) SetCallbacks(onDrop func(), onAlive func(bool)) {
	m.cfg.OnDrop, m.cfg.OnAlive = onDrop, onAlive
}

func (m *Monitor) Describe() string {
	return fmt.Sprintf("%s:%s (~%dms buffer)", m.cfg.Backend, m.cfg.Device, m.cfg.BufferMS)
}

// Start launches the playback process and begins consuming tapped frames.
func (m *Monitor) Start(ctx context.Context) error {
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-f", "s16le", "-ar", "16000", "-ac", "1", "-i", "-",
	}
	if m.cfg.Backend == "pulse" {
		args = append(args,
			"-f", "pulse",
			"-buffer_duration", strconv.Itoa(m.cfg.BufferMS),
			"-name", "livecaption monitor",
			m.cfg.Device)
	} else {
		args = append(args, "-f", "alsa", m.cfg.Device)
	}

	p, err := startFFmpeg(ctx, procOpts{args: args, wantStdin: true, log: m.cfg.Log})
	if err != nil {
		return fmt.Errorf("start monitor playback: %w", err)
	}
	m.proc = p
	m.setAlive(true)

	go func() {
		defer m.setAlive(false)
		for {
			select {
			case pcm, ok := <-m.ch:
				if !ok {
					return
				}
				if _, err := p.stdin.Write(pcm); err != nil {
					// A dead speaker must never end the session; captions
					// are the product, monitoring is a convenience.
					if ctx.Err() == nil {
						m.cfg.Log.Warn("monitor playback stopped", "err", err)
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// Tap offers a frame to the monitor without ever blocking. If playback has
// stalled the frame is dropped and counted; the caption path is never held up
// by the sound card.
func (m *Monitor) Tap(pcm []byte) {
	select {
	case m.ch <- pcm:
	default:
		if m.cfg.OnDrop != nil {
			m.cfg.OnDrop()
		}
	}
}

// Wrap forwards frames unchanged while tapping each one for playback. It adds
// no latency to the main path: the tap is a non-blocking send.
func (m *Monitor) Wrap(ctx context.Context, in <-chan Frame) <-chan Frame {
	out := make(chan Frame)
	go func() {
		defer close(out)
		for f := range in {
			m.Tap(f.PCM)
			select {
			case out <- f:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// Latency is the fixed delay playback adds. Perceived caption delay overstates
// the real figure by this much, so it is reported rather than hidden.
func (m *Monitor) Latency() time.Duration {
	return time.Duration(m.cfg.BufferMS) * time.Millisecond
}

func (m *Monitor) setAlive(v bool) {
	m.mu.Lock()
	m.alive = v
	m.mu.Unlock()
	if m.cfg.OnAlive != nil {
		m.cfg.OnAlive(v)
	}
}

func (m *Monitor) Close() error {
	m.once.Do(func() { close(m.ch) })
	if m.proc != nil {
		return m.proc.Close()
	}
	return nil
}
