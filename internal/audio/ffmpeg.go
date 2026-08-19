package audio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// xrunPattern matches the ALSA/Pulse underrun and overrun complaints ffmpeg
// writes to stderr. They are the tell that the capture path is dropping audio,
// which otherwise looks like unexplained gaps in the transcript.
var xrunTokens = []string{"xrun", "overrun", "underrun", "buffer underflow", "input/output error"}

// proc wraps a running ffmpeg: stdout is the PCM stream, stderr is drained on a
// goroutine so the process can never block on a full pipe.
type proc struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stdin  io.WriteCloser

	mu         sync.Mutex
	lastStderr string
	xruns      int

	log      *slog.Logger
	onXrun   func()
	onStderr func(string)
}

type procOpts struct {
	args      []string
	wantStdin bool
	log       *slog.Logger
	onXrun    func()
	onStderr  func(string)
}

// startFFmpeg launches ffmpeg with the given args and begins draining stderr.
func startFFmpeg(ctx context.Context, o procOpts) (*proc, error) {
	cmd := exec.CommandContext(ctx, "ffmpeg", o.args...)
	// Give ffmpeg a chance to flush and exit cleanly on cancellation rather
	// than being SIGKILLed mid-write.
	cmd.Cancel = func() error { return cmd.Process.Signal(interruptSignal) }
	cmd.WaitDelay = 2 * time.Second

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg stderr: %w", err)
	}
	p := &proc{cmd: cmd, stdout: stdout, log: o.log, onXrun: o.onXrun, onStderr: o.onStderr}
	if o.wantStdin {
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, fmt.Errorf("ffmpeg stdin: %w", err)
		}
		p.stdin = stdin
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start ffmpeg (is it installed and on PATH?): %w", err)
	}
	if o.log != nil {
		o.log.Debug("ffmpeg started", "args", strings.Join(o.args, " "), "pid", cmd.Process.Pid)
	}
	go p.drainStderr(stderr)
	return p, nil
}

// drainStderr consumes ffmpeg's stderr, counting xruns and keeping the last
// line for diagnosis. ffmpeg is chatty, so this stays at debug level.
func (p *proc) drainStderr(r io.ReadCloser) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 8192), 64*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		p.mu.Lock()
		p.lastStderr = line
		p.mu.Unlock()
		if p.onStderr != nil {
			p.onStderr(line)
		}

		lower := strings.ToLower(line)
		for _, tok := range xrunTokens {
			if strings.Contains(lower, tok) {
				p.mu.Lock()
				p.xruns++
				p.mu.Unlock()
				if p.onXrun != nil {
					p.onXrun()
				}
				if p.log != nil {
					p.log.Warn("audio capture glitch", "detail", line)
				}
				break
			}
		}
		if p.log != nil {
			p.log.Debug("ffmpeg", "line", line)
		}
	}
}

func (p *proc) LastStderr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastStderr
}

// Close terminates ffmpeg and waits for it. An error caused by our own
// cancellation is not interesting, so it is swallowed.
func (p *proc) Close() error {
	if p.stdin != nil {
		_ = p.stdin.Close()
	}
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(interruptSignal)
	}
	err := p.cmd.Wait()
	if err != nil && isExpectedExit(err) {
		return nil
	}
	return err
}

func isExpectedExit(err error) bool {
	s := err.Error()
	return strings.Contains(s, "signal:") ||
		strings.Contains(s, "context canceled") ||
		strings.Contains(s, "file already closed") ||
		strings.Contains(s, "exit status 255") // ffmpeg's code for "interrupted"
}

// probe reads duration and native format from a media file. Failure is
// non-fatal: it only affects what the banner and progress display can show.
type probeResult struct {
	Duration   time.Duration
	SampleRate int
	Channels   int
	Codec      string
}

func (p probeResult) describeConversion(to Format) string {
	if p.SampleRate == 0 {
		return to.String()
	}
	from := Format{SampleRate: p.SampleRate, Channels: p.Channels, BitDepth: 16}
	return fmt.Sprintf("%s -> %s", trimDepth(from.String()), trimDepth(to.String()))
}

// trimDepth drops the "s16" suffix; bit depth is noise in the banner.
func trimDepth(s string) string {
	if i := strings.LastIndex(s, " s"); i > 0 {
		return s[:i]
	}
	return s
}

func probeFile(ctx context.Context, path string) (probeResult, error) {
	var res probeResult
	out, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-select_streams", "a:0",
		"-show_entries", "format=duration:stream=sample_rate,channels,codec_name",
		"-of", "default=noprint_wrappers=1",
		path,
	).Output()
	if err != nil {
		return res, fmt.Errorf("ffprobe %s: %w", path, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch k {
		case "duration":
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				res.Duration = time.Duration(f * float64(time.Second))
			}
		case "sample_rate":
			res.SampleRate, _ = strconv.Atoi(v)
		case "channels":
			res.Channels, _ = strconv.Atoi(v)
		case "codec_name":
			res.Codec = v
		}
	}
	return res, nil
}

// FormatClock renders a duration as mm:ss or h:mm:ss, for banners and captions.
func FormatClock(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	total := int(d.Seconds())
	h, m, s := total/3600, (total/60)%60, total%60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}
