package audio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// DeviceConfig configures live capture from an audio input.
type DeviceConfig struct {
	Device  string        // pulse sink/source name, or ALSA "hw:2,0"
	Backend string        // "pulse" or "alsa"
	Chunk   time.Duration // PCM chunk size, default 100ms
	Log     *slog.Logger

	OnFrame   func(nbytes int, offset time.Duration)
	OnXrun    func()
	OnRestart func()
	OnStderr  func(string)
}

// DeviceSource captures live audio via ffmpeg.
//
// Unlike FileSource there is no pacing to do: the sound card releases samples
// at wall-clock rate and the reader simply keeps up. The work here is staying
// alive — if the USB interface is unplugged, ffmpeg exits, and we relaunch with
// backoff rather than ending the session.
type DeviceSource struct {
	cfg DeviceConfig

	mu   sync.Mutex
	proc *proc
	err  error

	// offset accumulates across restarts so media time stays monotonic even
	// though each ffmpeg process starts counting from zero.
	offset time.Duration
}

func NewDeviceSource(cfg DeviceConfig) *DeviceSource {
	if cfg.Chunk <= 0 {
		cfg.Chunk = 100 * time.Millisecond
	}
	if cfg.Backend == "" {
		cfg.Backend = "pulse"
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &DeviceSource{cfg: cfg}
}

func (s *DeviceSource) Format() Format { return PipelineFormat }

// DeviceCallbacks are the metric hooks for live capture. Every one of these
// corresponds to a way the capture path can degrade without the audio simply
// stopping, which is exactly what needs a counter behind it.
type DeviceCallbacks struct {
	OnFrame   func(nbytes int, offset time.Duration)
	OnXrun    func()
	OnRestart func()
	OnStderr  func(string)
}

// SetCallbacks registers metric hooks. Set before Start.
func (s *DeviceSource) SetCallbacks(c DeviceCallbacks) {
	s.cfg.OnFrame = c.OnFrame
	s.cfg.OnXrun = c.OnXrun
	s.cfg.OnRestart = c.OnRestart
	s.cfg.OnStderr = c.OnStderr
}

func (s *DeviceSource) Describe() string {
	return fmt.Sprintf("%s:%s (-> %s)", s.cfg.Backend, s.cfg.Device, trimDepth(PipelineFormat.String()))
}

func (s *DeviceSource) Start(ctx context.Context) (<-chan Frame, error) {
	// Fail fast on a bad device name: a typo should be an immediate clear
	// error, not an infinite restart loop.
	if err := s.captureOnce(ctx, nil, true); err != nil {
		return nil, err
	}

	out := make(chan Frame)
	go func() {
		defer close(out)
		backoff := 250 * time.Millisecond
		for ctx.Err() == nil {
			err := s.captureOnce(ctx, out, false)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				s.cfg.Log.Warn("capture stopped; restarting", "err", err, "retry_in", backoff)
			} else {
				s.cfg.Log.Warn("capture ended unexpectedly; restarting", "retry_in", backoff)
			}
			if s.cfg.OnRestart != nil {
				s.cfg.OnRestart()
			}
			if !sleep(ctx, backoff) {
				return
			}
			if backoff *= 2; backoff > 8*time.Second {
				backoff = 8 * time.Second
			}
		}
	}()
	return out, nil
}

// captureOnce runs one ffmpeg lifetime. With probeOnly it just verifies the
// device opens, then tears down.
func (s *DeviceSource) captureOnce(ctx context.Context, out chan<- Frame, probeOnly bool) error {
	args := []string{"-hide_banner", "-loglevel", "error", "-f", s.cfg.Backend, "-i", s.cfg.Device}
	args = append(args, "-ac", "1", "-ar", "16000", "-f", "s16le", "-")

	p, err := startFFmpeg(ctx, procOpts{
		args:     args,
		log:      s.cfg.Log,
		onXrun:   s.cfg.OnXrun,
		onStderr: s.cfg.OnStderr,
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.proc = p
	s.mu.Unlock()

	chunkBytes := PipelineFormat.BytesFor(s.cfg.Chunk)
	buf := make([]byte, chunkBytes)

	if probeOnly {
		defer p.Close()
		// One successful read proves the device is really producing audio.
		// A bad device name makes ffmpeg exit here with a useful stderr line.
		done := make(chan error, 1)
		go func() { _, err := io.ReadFull(p.stdout, buf); done <- err }()
		select {
		case err := <-done:
			if err != nil {
				if msg := p.LastStderr(); msg != "" {
					return fmt.Errorf("cannot open %s device %q: %s", s.cfg.Backend, s.cfg.Device, msg)
				}
				return fmt.Errorf("cannot open %s device %q: %w", s.cfg.Backend, s.cfg.Device, err)
			}
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timed out opening %s device %q", s.cfg.Backend, s.cfg.Device)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer p.Close()

	for {
		read, err := io.ReadFull(p.stdout, buf)
		if read > 0 {
			s.mu.Lock()
			s.offset += PipelineFormat.Duration(read)
			offset := s.offset
			s.mu.Unlock()

			if s.cfg.OnFrame != nil {
				s.cfg.OnFrame(read, offset)
			}
			select {
			case out <- Frame{
				PCM:        append([]byte(nil), buf[:read]...),
				Offset:     offset,
				CapturedAt: time.Now(),
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if msg := p.LastStderr(); msg != "" {
					return errors.New(msg)
				}
				return nil
			}
			return err
		}
	}
}

func (s *DeviceSource) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *DeviceSource) Close() error {
	s.mu.Lock()
	p := s.proc
	s.mu.Unlock()
	if p != nil {
		return p.Close()
	}
	return nil
}
