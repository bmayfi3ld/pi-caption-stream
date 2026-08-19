// Package cli defines the command surface.
//
// Subcommands rather than a --source flag, because replay and live take
// genuinely disjoint options: --speed and --monitor are meaningless for a
// capture device, and --device is meaningless for a file.
package cli

import (
	"fmt"

	"github.com/alecthomas/kong"
)

// Version is set at build time via -ldflags.
var Version = "0.1.0"

// CLI is the root command tree.
type CLI struct {
	Replay  ReplayCmd  `cmd:"" help:"Replay an audio file at wall-clock rate to simulate a live feed."`
	Live    LiveCmd    `cmd:"" help:"Caption live audio from a capture device."`
	Devices DevicesCmd `cmd:"" help:"List available audio capture devices."`
	Version VersionCmd `cmd:"" help:"Print version and exit."`

	Globals
}

// Globals apply to every command.
type Globals struct {
	LogLevel  string `enum:"debug,info,warn,error" default:"info" group:"Logging" help:"Diagnostic verbosity."`
	LogFormat string `enum:"auto,pretty,json" default:"auto" group:"Logging" help:"auto = pretty on a terminal, JSON when piped."`
	Verbose   bool   `short:"v" group:"Logging" help:"Shorthand for --log-level=debug."`
	Quiet     bool   `short:"q" group:"Logging" help:"Suppress captions and status line; warnings and errors only."`
	NoColor   bool   `env:"NO_COLOR" group:"Logging" help:"Disable coloured output."`
}

// STTFlags configure the speech-to-text backend.
type STTFlags struct {
	Engine   string   `default:"deepgram" enum:"deepgram,mock" group:"Speech-to-text" help:"Recognizer to use. 'mock' runs offline with no API cost."`
	APIKey   string   `env:"DEEPGRAM_API_KEY" group:"Speech-to-text" help:"Deepgram API key."`
	Model    string   `default:"nova-3" group:"Speech-to-text" help:"Deepgram model."`
	Language string   `default:"en-US" group:"Speech-to-text" help:"Recognition language."`
	Keyterm  []string `group:"Speech-to-text" help:"Proper noun to bias recognition toward. Repeatable."`
}

// ServerFlags configure the caption web server.
type ServerFlags struct {
	Addr      string `default:":8080" group:"Server" help:"Listen address for the viewer and admin pages."`
	Lines     int    `default:"3" group:"Server" help:"Caption lines visible on the viewer page."`
	Open      bool   `group:"Server" help:"Open the viewer in a browser on start."`
	DevStatic string `hidden:"" group:"Server" help:"Serve web assets from this directory instead of the embedded copy."`
}

// OutputFlags configure transcript recording, which is on by default.
type OutputFlags struct {
	TranscriptDir string `default:"./transcripts" type:"path" env:"LIVECAPTION_TRANSCRIPT_DIR" group:"Output" help:"Directory holding per-session transcript folders."`
	NoTranscript  bool   `group:"Output" help:"Disable transcript recording for this session."`
}

// AudioFlags are shared between replay and live.
type AudioFlags struct {
	ChunkMS int `name:"chunk-ms" default:"100" group:"Audio" help:"PCM chunk size sent downstream, in milliseconds."`
}

// ReplayCmd streams an audio file through the pipeline at wall-clock rate.
type ReplayCmd struct {
	File  string  `arg:"" type:"existingfile" help:"Audio file to replay."`
	Speed float64 `default:"1.0" group:"Audio" help:"Rate multiplier. 1.0 = true live rate. Must be 1.0 with --monitor."`
	Loop  bool    `group:"Audio" help:"Restart the file on EOF, for soak testing."`

	Monitor        bool   `group:"Monitor" help:"Play the streamed audio over speakers to judge caption delay by ear."`
	MonitorDevice  string `default:"default" group:"Monitor" help:"Playback device (pulse sink or ALSA device)."`
	MonitorBackend string `default:"pulse" enum:"pulse,alsa" group:"Monitor" help:"Playback backend."`
	MonitorBufMS   int    `name:"monitor-buffer-ms" default:"80" group:"Monitor" help:"Playback buffer; adds this much to perceived delay."`

	AudioFlags  `embed:""`
	STTFlags    `embed:""`
	ServerFlags `embed:""`
	OutputFlags `embed:""`
}

// Validate rejects the one flag combination that cannot work.
func (c *ReplayCmd) Validate() error {
	if c.Monitor && c.Speed != 1.0 {
		return fmt.Errorf("--monitor requires --speed 1.0 (got %g): the sound card drains at "+
			"wall-clock rate, so any other speed just overflows the playback buffer", c.Speed)
	}
	if c.Speed <= 0 {
		return fmt.Errorf("--speed must be positive (got %g)", c.Speed)
	}
	return nil
}

// LiveCmd captures from an audio device.
type LiveCmd struct {
	Device  string `required:"" group:"Audio" help:"Capture device. Run 'livecaption devices' to list."`
	Backend string `default:"pulse" enum:"pulse,alsa" group:"Audio" help:"Capture backend."`

	AudioFlags  `embed:""`
	STTFlags    `embed:""`
	ServerFlags `embed:""`
	OutputFlags `embed:""`
}

// DevicesCmd lists capture inputs.
type DevicesCmd struct{}

// VersionCmd prints the version.
type VersionCmd struct{}

func (c *VersionCmd) Run() error {
	fmt.Println("livecaption", Version)
	return nil
}

// Parse builds the kong context.
func Parse(args []string) (*kong.Context, *CLI, error) {
	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("livecaption"),
		kong.Description("Live captions: stream audio to a speech-to-text service and serve the text to a webpage."),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true, FlagsLast: true}),
		kong.Vars{"version": Version},
	)
	if err != nil {
		return nil, nil, err
	}
	ctx, err := parser.Parse(args)
	return ctx, &cli, err
}
