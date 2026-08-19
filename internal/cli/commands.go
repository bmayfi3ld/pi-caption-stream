package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"

	"livecaption/internal/audio"
	"livecaption/internal/ui"
)

// Run executes a replay session: an audio file streamed through the real
// pipeline at wall-clock rate, so everything downstream behaves exactly as it
// will with the live feed.
func (c *ReplayCmd) Run(ctx context.Context, term *ui.Terminal, log *slog.Logger, g Globals) error {
	if err := requireAPIKey(c.Engine, c.APIKey); err != nil {
		return err
	}
	chunk, err := chunkDuration(c.ChunkMS)
	if err != nil {
		return err
	}

	src, err := audio.NewFileSource(ctx, audio.FileConfig{
		Path:  c.File,
		Speed: c.Speed,
		Loop:  c.Loop,
		Chunk: chunk,
		Log:   log,
	})
	if err != nil {
		return err
	}

	var mon *audio.Monitor
	if c.Monitor {
		mon = audio.NewMonitor(audio.MonitorConfig{
			Device:   c.MonitorDevice,
			Backend:  c.MonitorBackend,
			BufferMS: c.MonitorBufMS,
			Log:      log,
		})
	}

	speed := fmt.Sprintf("%gx", c.Speed)
	if c.Speed == 1.0 {
		speed += " (wall-clock)"
	}
	extra := []ui.BannerField{{Label: "speed", Value: speed}}
	if c.Loop {
		extra = append(extra, ui.BannerField{Label: "loop", Value: "enabled"})
	}

	o := buildOpts{
		kind:        "replay",
		sourceLabel: src.Describe(),
		extraBanner: extra,
		source:      src,
		monitor:     mon,
		mediaTotal:  src.MediaDuration(),
		conversion:  src.ConversionDescription(),
		stt:         c.STTFlags,
		server:      c.ServerFlags,
		output:      c.OutputFlags,
		globals:     g,
	}

	s, err := newSession(ctx, o, term, log)
	if err != nil {
		return err
	}
	defer s.shutdown()

	// Frame accounting has to reach the metrics the session owns, so it is
	// wired after construction.
	src.SetOnFrame(s.met.AddFrame)
	if mon != nil {
		mon.SetCallbacks(s.met.MonitorDrop, s.met.SetMonitorAlive)
		s.met.MonitorDevice = c.MonitorDevice
		s.met.MonitorBufferMS = c.MonitorBufMS
	}

	return s.run(ctx, c.Open, c.Addr)
}

// Run executes a live capture session.
func (c *LiveCmd) Run(ctx context.Context, term *ui.Terminal, log *slog.Logger, g Globals) error {
	if err := requireAPIKey(c.Engine, c.APIKey); err != nil {
		return err
	}
	chunk, err := chunkDuration(c.ChunkMS)
	if err != nil {
		return err
	}

	devices := audio.ListDevices(ctx)
	skipped, err := audio.ResolveDevice(devices, c.Backend, c.Device)
	if err != nil {
		return err
	}
	if skipped {
		log.Warn("device validation skipped: no devices enumerated for backend, proceeding without confirming --device",
			"backend", c.Backend)
	}

	src := audio.NewDeviceSource(audio.DeviceConfig{
		Device:  c.Device,
		Backend: c.Backend,
		Chunk:   chunk,
		Log:     log,
	})

	o := buildOpts{
		kind:        "live",
		sourceLabel: src.Describe(),
		source:      src,
		conversion:  "device -> " + audio.PipelineFormat.String(),
		stt:         c.STTFlags,
		server:      c.ServerFlags,
		output:      c.OutputFlags,
		globals:     g,
	}

	s, err := newSession(ctx, o, term, log)
	if err != nil {
		return err
	}
	defer s.shutdown()

	src.SetCallbacks(audio.DeviceCallbacks{
		OnFrame:   s.met.AddFrame,
		OnXrun:    s.met.Xrun,
		OnRestart: s.met.FFmpegRestart,
		OnStderr:  s.met.SetLastStderr,
	})

	return s.run(ctx, c.Open, c.Addr)
}

// Run lists capture devices, so the soundboard's device string can be found
// without leaving the tool.
func (c *DevicesCmd) Run(ctx context.Context) error {
	devices := audio.ListDevices(ctx)
	if len(devices) == 0 {
		fmt.Fprintln(os.Stderr, "No capture devices found. Is ffmpeg installed and is a sound server running?")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BACKEND\tDEVICE\tDESCRIPTION")
	for _, d := range devices {
		marker := ""
		if d.Default {
			marker = "  (default)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s%s\n", d.Backend, d.Name, d.Label, marker)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "\nUse with:  livecaption live --backend <BACKEND> --device <DEVICE>")
	return nil
}
