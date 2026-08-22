package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"livecaption/internal/audio"
	"livecaption/internal/caption"
	"livecaption/internal/metrics"
	"livecaption/internal/stt"
	"livecaption/internal/ui"
	"livecaption/internal/web"
)

// session is everything one run needs, assembled once and shared by both
// replay and live. The only real difference between the two commands is which
// audio.Source gets built.
type session struct {
	src     audio.Source
	monitor *audio.Monitor
	engine  stt.Engine
	hub     *caption.Hub
	writer  *caption.Writer
	server  *web.Server
	met     *metrics.Metrics
	term    *ui.Terminal
	log     *slog.Logger

	bannerFields []ui.BannerField
}

// buildOpts is the per-command part of the wiring.
type buildOpts struct {
	kind        string // "replay" or "live"
	sourceLabel string // banner value for the source row
	extraBanner []ui.BannerField
	source      audio.Source
	monitor     *audio.Monitor
	mediaTotal  time.Duration
	conversion  string

	stt     STTFlags
	server  ServerFlags
	output  OutputFlags
	globals Globals
}

func newSession(ctx context.Context, o buildOpts, term *ui.Terminal, log *slog.Logger) (*session, error) {
	started := time.Now()
	met := metrics.New(Version, started.Format("2006-01-02T15-04-05"))
	met.SourceKind = o.kind
	met.SourceSpec = o.sourceLabel
	met.SourceFormat = o.conversion
	met.Engine = o.stt.Engine
	met.SetMediaTotal(o.mediaTotal)
	if o.monitor != nil {
		met.MonitorEnabled = true
	}

	hub := caption.NewHub(met)

	// Wired before anything starts so the very first SetSTTState call (idle ->
	// connecting) already reaches the viewer. Hub.PublishStatus takes its own
	// lock and never met's, so this can't deadlock against the metrics mutex.
	met.SetSTTStateHook(func(s metrics.ConnState) { hub.PublishStatus(s.String(), "") })

	engine, err := stt.New(o.stt.Engine, stt.Config{
		Format:         audio.PipelineFormat,
		Model:          o.stt.Model,
		Language:       o.stt.Language,
		InterimResults: true,
		Keyterms:       o.stt.Keyterm,
		APIKey:         o.stt.APIKey,
		Metrics:        met,
		Pause: stt.PauseConfig{
			Enabled:     o.stt.AutoPause,
			ThresholdDB: o.stt.SilenceDB,
			Hold:        o.stt.SilenceHold,
		},
	})
	if err != nil {
		return nil, err
	}

	s := &session{
		src: o.source, monitor: o.monitor, engine: engine,
		hub: hub, met: met, term: term, log: log,
	}

	// Transcripts are recorded for every session unless explicitly disabled.
	if !o.output.NoTranscript {
		w, err := caption.NewWriter(o.output.TranscriptDir, started, met)
		if err != nil {
			return nil, err
		}
		s.writer = w
	}

	// One place decides what happens to a finalized line: it goes to the
	// terminal and to the transcript file.
	hub.OnFinal = func(l caption.Line) {
		term.Caption(time.Duration(l.OffsetMS)*time.Millisecond, l.Text)
		if s.writer != nil {
			s.writer.Write(l)
		}
	}

	srv, err := web.NewServer(web.Config{
		Addr:      o.server.Addr,
		Lines:     o.server.Lines,
		Logo:      o.server.Logo,
		Hub:       hub,
		Metrics:   met,
		Log:       log,
		DevStatic: o.server.DevStatic,
	})
	if err != nil {
		return nil, err
	}
	s.server = srv

	// Banner assembly. Everything the run depends on is shown before any
	// audio flows, so a misconfiguration is obvious immediately.
	fields := []ui.BannerField{{Label: "source", Value: o.kind + "  " + o.sourceLabel}}
	fields = append(fields, o.extraBanner...)
	if o.monitor != nil {
		fields = append(fields, ui.BannerField{
			Label: "monitor",
			Value: o.monitor.Describe(),
			Note:  "perceived delay overstates actual by this much",
		})
	}
	sttNote := fmt.Sprintf("model=%s  language=%s", o.stt.Model, o.stt.Language)
	if len(o.stt.Keyterm) > 0 {
		sttNote += fmt.Sprintf("  keyterms=%d", len(o.stt.Keyterm))
	}
	fields = append(fields, ui.BannerField{Label: "stt", Value: o.stt.Engine, Note: sttNote})
	if s.writer != nil {
		fields = append(fields, ui.BannerField{Label: "transcript", Value: s.writer.Dir()})
	} else {
		fields = append(fields, ui.BannerField{Label: "transcript", Value: "disabled", Note: "--no-transcript"})
	}
	if o.server.Logo != "" {
		fields = append(fields, ui.BannerField{Label: "logo", Value: o.server.Logo})
	}
	base := browserURL(o.server.Addr)
	fields = append(fields,
		ui.BannerField{Label: "viewer", Value: base},
		ui.BannerField{Label: "admin", Value: base + "/admin"},
	)
	s.bannerFields = fields
	return s, nil
}

// run drives the pipeline until the source ends or ctx is cancelled, then
// shuts every stage down in order so nothing in flight is lost.
func (s *session) run(ctx context.Context, openBrowser bool, addr string) error {
	s.term.Banner("livecaption "+Version, s.bannerFields)

	ln, err := s.server.Listen()
	if err != nil {
		return err
	}
	go func() {
		if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("web server stopped", "err", err)
		}
	}()

	if s.monitor != nil {
		if err := s.monitor.Start(ctx); err != nil {
			// Monitoring is a convenience; captions are the product.
			s.log.Warn("monitor playback unavailable", "err", err)
			s.monitor = nil
		}
	}

	frames, err := s.src.Start(ctx)
	if err != nil {
		return err
	}
	if s.monitor != nil {
		frames = s.monitor.Wrap(ctx, frames)
	}

	s.term.Ready("ready — Ctrl-C to stop")
	s.term.StartStatus(s.met.Snapshot)
	if openBrowser {
		go openInBrowser(browserURL(addr), s.log)
	}

	// The engine reads frames and writes transcripts; the hub consumes them.
	transcripts := make(chan stt.Transcript, 64)
	engineDone := make(chan error, 1)
	go func() { engineDone <- s.engine.Run(ctx, frames, transcripts) }()

	hubDone := make(chan struct{})
	go func() {
		defer close(hubDone)
		for t := range transcripts {
			s.observeLatency(t)
			s.hub.Publish(t)
		}
	}()

	engineErr := <-engineDone
	close(transcripts)
	<-hubDone

	// Close any utterance still in progress so the tail is not lost.
	s.hub.Flush()

	if engineErr != nil && !errors.Is(engineErr, context.Canceled) {
		return engineErr
	}
	if err := s.src.Err(); err != nil {
		return err
	}
	return nil
}

// observeLatency records the wall-clock delay between the audio being captured
// and the caption for it arriving. Anchoring on the frame's capture instant —
// rather than on media time plus a stream origin — is what makes the figure
// survive auto-pause, reconnects, dropped frames and ffmpeg restarts, all of
// which move the recognizer's media clock relative to the wall.
func (s *session) observeLatency(t stt.Transcript) {
	if !t.IsFinal || t.ReceivedAt.IsZero() || t.CapturedAt.IsZero() {
		return
	}
	s.met.ObserveLatency(t.ReceivedAt.Sub(t.CapturedAt))
}

// shutdown tears everything down and prints the summary. Called on the way out
// regardless of how the run ended.
func (s *session) shutdown() {
	if s.monitor != nil {
		_ = s.monitor.Close()
	}
	_ = s.src.Close()

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.server.Shutdown(shutCtx)

	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			s.log.Warn("transcript close failed", "err", err)
		}
	}
	s.met.SetSTTState(metrics.StateClosed)
	s.term.Summary(s.met.Snapshot(), s.met.MonitorEnabled)
}

// browserURL turns a listen address into something clickable. ":8080" and
// "0.0.0.0:8080" both mean "this machine" to the person running the tool.
func browserURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func openInBrowser(url string, log *slog.Logger) {
	// Give the listener a moment so the first request doesn't race the server.
	time.Sleep(200 * time.Millisecond)
	if err := exec.Command("xdg-open", url).Start(); err != nil {
		log.Debug("could not open browser", "err", err)
	}
}

// chunkDuration converts the --chunk-ms flag, clamping to a range that is
// sensible for streaming recognizers.
func chunkDuration(ms int) (time.Duration, error) {
	if ms < 20 || ms > 500 {
		return 0, fmt.Errorf("--chunk-ms must be between 20 and 500 (got %d)", ms)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// requireAPIKey fails early and clearly rather than letting the recognizer
// return an opaque 401 after audio has started flowing.
func requireAPIKey(engine, key string) error {
	if strings.HasPrefix(engine, "mock") || strings.TrimSpace(key) != "" {
		return nil
	}
	return errors.New("no Deepgram API key: set DEEPGRAM_API_KEY or pass --api-key " +
		"(or use --engine mock to run offline)")
}
