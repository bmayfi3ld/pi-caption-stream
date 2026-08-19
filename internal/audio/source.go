// Package audio produces a uniform PCM stream from either a file replayed at
// wall-clock rate or a live capture device. Both paths shell out to ffmpeg and
// emit identical frames, so everything downstream has exactly one code path.
package audio

import (
	"context"
	"fmt"
	"time"
)

// Format describes the PCM a Source produces. The pipeline is fixed at
// 16 kHz mono signed 16-bit little-endian: what Deepgram wants, and small
// enough that a 100 ms frame is only 3200 bytes.
type Format struct {
	SampleRate int
	Channels   int
	BitDepth   int
}

// PipelineFormat is the one format every Source converts to.
var PipelineFormat = Format{SampleRate: 16000, Channels: 1, BitDepth: 16}

// BytesPerSecond is how many bytes of PCM one second of audio occupies.
func (f Format) BytesPerSecond() int {
	return f.SampleRate * f.Channels * f.BitDepth / 8
}

// Duration converts a PCM byte count into the time it represents.
func (f Format) Duration(nbytes int) time.Duration {
	bps := f.BytesPerSecond()
	if bps == 0 {
		return 0
	}
	return time.Duration(float64(nbytes) / float64(bps) * float64(time.Second))
}

// BytesFor converts a duration into the PCM byte count that represents it,
// rounded down to a whole sample frame so we never split a sample.
func (f Format) BytesFor(d time.Duration) int {
	frameSize := f.Channels * f.BitDepth / 8
	if frameSize == 0 {
		return 0
	}
	n := int(float64(d) / float64(time.Second) * float64(f.BytesPerSecond()))
	return n - n%frameSize
}

func (f Format) String() string {
	ch := "mono"
	if f.Channels == 2 {
		ch = "stereo"
	} else if f.Channels > 2 {
		ch = fmt.Sprintf("%dch", f.Channels)
	}
	return fmt.Sprintf("%d Hz %s s%d", f.SampleRate, ch, f.BitDepth)
}

// Frame is one chunk of PCM plus the timing needed for latency accounting.
type Frame struct {
	PCM []byte
	// Offset is the media time of the first sample, measured from the start
	// of the stream. Latency is computed against this, not against wall clock,
	// so a replay at --speed 1.0 and a live capture measure the same thing.
	Offset time.Duration
	// CapturedAt is when the frame was released into the pipeline.
	CapturedAt time.Time
}

// Source produces a stream of PCM frames.
//
// Start returns a channel that closes when the source is exhausted (file EOF)
// or ctx is cancelled; Err then reports why. Implementations must not send on
// the channel after it closes, and must return promptly on cancellation.
type Source interface {
	// Describe returns a short human-readable summary for the startup banner,
	// e.g. "260809_0931.mp3 (31:51, 44100 Hz stereo -> 16000 Hz mono)".
	Describe() string
	Format() Format
	Start(ctx context.Context) (<-chan Frame, error)
	Err() error
	Close() error
}

// sleep waits for d or until ctx is done, reporting which happened.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
