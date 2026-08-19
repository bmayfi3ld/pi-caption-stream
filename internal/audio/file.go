package audio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

// FileConfig configures replay of an audio file at wall-clock rate.
type FileConfig struct {
	Path    string
	Speed   float64       // 1.0 = true live rate
	Loop    bool          // restart on EOF, for soak tests
	Chunk   time.Duration // PCM chunk size, default 100ms
	Log     *slog.Logger
	OnFrame func(nbytes int, offset time.Duration)
}

// FileSource decodes a file with ffmpeg and releases the PCM at wall-clock
// rate, simulating a live feed.
//
// ffmpeg decodes as fast as the pipe drains, so the *reader* sets the rate.
// Pacing uses an absolute deadline per chunk rather than a ticker, so timer
// jitter cannot accumulate into drift across a 30-minute file.
type FileSource struct {
	cfg   FileConfig
	probe probeResult

	mu   sync.Mutex
	proc *proc
	err  error
}

func NewFileSource(ctx context.Context, cfg FileConfig) (*FileSource, error) {
	if cfg.Chunk <= 0 {
		cfg.Chunk = 100 * time.Millisecond
	}
	if cfg.Speed <= 0 {
		cfg.Speed = 1.0
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	// Probe is advisory: it fills in the banner and the progress denominator.
	pr, err := probeFile(ctx, cfg.Path)
	if err != nil {
		cfg.Log.Debug("ffprobe failed; continuing without duration", "err", err)
	}
	return &FileSource{cfg: cfg, probe: pr}, nil
}

func (s *FileSource) Format() Format { return PipelineFormat }

// SetOnFrame registers frame accounting. Set before Start.
func (s *FileSource) SetOnFrame(fn func(nbytes int, offset time.Duration)) {
	s.cfg.OnFrame = fn
}

// MediaDuration is the file's length, or 0 if probing failed.
func (s *FileSource) MediaDuration() time.Duration { return s.probe.Duration }

func (s *FileSource) Describe() string {
	name := filepath.Base(s.cfg.Path)
	if s.probe.Duration > 0 {
		return fmt.Sprintf("%s (%s, %s)", name,
			FormatClock(s.probe.Duration), s.probe.describeConversion(PipelineFormat))
	}
	return name
}

// ConversionDescription is the "44100 Hz stereo -> 16000 Hz mono" line.
func (s *FileSource) ConversionDescription() string {
	return s.probe.describeConversion(PipelineFormat)
}

func (s *FileSource) Start(ctx context.Context) (<-chan Frame, error) {
	out := make(chan Frame)
	go func() {
		defer close(out)
		for {
			err := s.playOnce(ctx, out)
			if err != nil || !s.cfg.Loop || ctx.Err() != nil {
				s.setErr(err)
				return
			}
			s.cfg.Log.Info("replay looping")
		}
	}()
	return out, nil
}

func (s *FileSource) playOnce(ctx context.Context, out chan<- Frame) error {
	p, err := startFFmpeg(ctx, procOpts{
		args: []string{
			"-hide_banner", "-loglevel", "error",
			"-i", s.cfg.Path,
			"-ac", "1", "-ar", "16000",
			"-f", "s16le", "-",
		},
		log: s.cfg.Log,
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.proc = p
	s.mu.Unlock()
	defer p.Close()

	chunkBytes := PipelineFormat.BytesFor(s.cfg.Chunk)
	if chunkBytes <= 0 {
		return fmt.Errorf("chunk size %v is too small for %s", s.cfg.Chunk, PipelineFormat)
	}
	// At speed 2.0 we release each chunk in half the time it represents.
	interval := time.Duration(float64(s.cfg.Chunk) / s.cfg.Speed)

	start := time.Now()
	buf := make([]byte, chunkBytes)
	var offset time.Duration

	for n := 0; ; n++ {
		read, err := io.ReadFull(p.stdout, buf)
		if read > 0 {
			// Hold each chunk until its release deadline. Deadlines are
			// computed from the fixed start time, so a slow iteration is
			// corrected on the next one instead of pushing everything later.
			due := start.Add(time.Duration(n+1) * interval)
			if !sleep(ctx, time.Until(due)) {
				return ctx.Err()
			}
			offset += PipelineFormat.Duration(read)
			frame := Frame{
				PCM:        append([]byte(nil), buf[:read]...),
				Offset:     offset,
				CapturedAt: time.Now(),
			}
			if s.cfg.OnFrame != nil {
				s.cfg.OnFrame(read, offset)
			}
			select {
			case out <- frame:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		switch {
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			return nil // clean end of file
		case err != nil:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read decoded audio: %w", err)
		}
	}
}

func (s *FileSource) setErr(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *FileSource) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *FileSource) Close() error {
	s.mu.Lock()
	p := s.proc
	s.mu.Unlock()
	if p != nil {
		return p.Close()
	}
	return nil
}
