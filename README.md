# livecaption

Streams audio — from a soundboard's USB output or from an audio file — to a speech-to-text
service and serves the resulting captions to a webpage in near real time, for live-event
captioning. See [DESIGN.md](DESIGN.md) for the architecture and the reasoning behind it.

## Prerequisites

- Go 1.26+
- `ffmpeg` and `ffprobe` on `PATH` (used for capture, decode, resample, and playback)
- A [Deepgram](https://deepgram.com) API key, set via `DEEPGRAM_API_KEY` or `--api-key`

`--engine mock` needs neither a key nor network access — it emits canned transcripts driven by
media time, which is what makes the tool testable without hardware or API spend.

## Build

```bash
go build -o livecaption ./cmd/livecaption
./livecaption version
```

Or run directly with `go run ./cmd/livecaption <command>`.

## Commands

### `devices` — find the soundboard

```bash
livecaption devices
```

Lists capture inputs per backend (`pulse`, `alsa`) with the name to pass to `--device`.
Enumeration is best-effort (it shells out to `ffmpeg -sources`); an empty list on your machine
just means that backend didn't answer, not that you have no audio hardware.

### `replay` — the no-cost dev loop

Streams an audio file through the real pipeline at wall-clock rate, so everything downstream
behaves exactly as it will with the live feed:

```bash
livecaption replay recording.mp3 --engine mock --no-transcript
```

This is the primary development loop: no API key, no network, no Deepgram spend, and output is
reproducible at any `--speed` since the mock engine is driven by media time, not the wall clock.
`--speed 20` blows through a half-hour file in under two minutes for a quick sanity check;
`--loop` restarts on EOF for soak testing.

To judge caption delay by ear, add `--monitor`:

```bash
livecaption replay recording.mp3 --monitor
```

This tees the exact frames sent to the recognizer into a second ffmpeg writing to your speakers —
what you hear is bit-identical to what the recognizer receives. `--monitor` requires `--speed 1.0`
(the sink drains at a fixed rate) and adds a printed ~80 ms of playback buffer, so perceived delay
slightly overstates true caption latency. Use it to tune `--keyterm` and to get a feel for lag
*before* an event, not during one.

### `live` — the real thing

```bash
livecaption live --device <name-from-devices> --keyterm "Anthropic" --keyterm "Claude"
```

`--device` is validated against `livecaption devices` output before capture starts (best-effort;
if enumeration fails outright for the chosen backend, validation is skipped with a logged warning
rather than blocking the run), and then confirmed with a probe read before the session begins — so
a typo is a clear startup error, not room noise captioned as your event.

### `version`

```bash
livecaption version
```

## Transcripts

On by default — every session writes to `./transcripts/<YYYY-MM-DDTHH-MM-SS>/`, both
`transcript.txt` (human-readable, timestamped) and `transcript.jsonl` (one record per line, for
tooling). Change the location with `--transcript-dir` or `LIVECAPTION_TRANSCRIPT_DIR`; disable
with `--no-transcript`.

## Viewer and admin

The viewer page is served at the `--addr` you configured (default `http://localhost:8080/`). It's
a bottom-anchored rolling caption window sized in `vw`, so the same URL works on a phone, a
projector, or as an OBS browser source. `--logo <file>` puts an image in the top-right corner
beside the connection dot. Query parameters:

| param | effect |
|---|---|
| `?lines=N` | number of caption rows shown (overrides `--lines`) |
| `?size=N` | base font size in `vw` |
| `?theme=light` | light theme (default is dark) |
| `?debug=1` | overlays measured latency |
| `?logo=0` | hides the logo (e.g. for OBS, where branding is composited downstream) |
| `?wake=0` | disables the screen wake lock entirely, gate included (OBS browser sources, wall-mounted displays — nobody there to tap it) |

Over plain HTTP — the standard LAN deployment — the browser's Wake Lock API is unavailable outside
a secure context, so the viewer falls back to a silent looping video to keep the screen on. That
needs one tap to unlock playback, so the page shows a one-time "Tap to start" gate before captions
become visible; captions are already streaming in behind it, so tapping reveals current state
rather than an empty page. `?wake=0` skips this entirely.

`/admin` is a metrics dashboard (no auth) polling `/api/stats` once a second — restarts, xruns,
STT reconnects, SSE client counts, latency percentiles. Check it during an event to confirm
nothing is degrading silently.

## stdout vs stderr

Finalized captions go to stdout; everything else (logs, status line) goes to stderr, so they split
cleanly:

```bash
livecaption replay recording.mp3 > captions.txt 2> run.log
```

## Running an event: a short checklist

- **Feed a mono aux/matrix send of the mics, not the main mix.** `-ac 1` will happily downmix
  music and effects along with speech; this is the single biggest accuracy lever in the project.
- **Set `--keyterm` for every proper noun** in the event (names, places, in-house terms) — costs
  nothing, helps a lot.
- **Do a `--monitor` dry run beforehand** (on `replay`, with representative audio) to hear and tune
  perceived delay before you're live.
- **Check `/admin` shows a clean run** — no restarts, no reconnects, no dropped frames — before
  trusting the feed.

## Troubleshooting

- **401 on first connect** — check `DEEPGRAM_API_KEY` (or `--api-key`).
- **`unknown stt engine`** — only `deepgram` and `mock` are registered; check `--engine`.
- **No devices listed by `devices`** — confirm `ffmpeg` is on `PATH` and a sound server (PulseAudio
  / PipeWire) is running; `alsa` enumeration commonly comes back empty even when ALSA devices work
  fine, so also try known names like `hw:0,0` or `default` directly with `live --backend alsa`.
- **Captions lagging** — Deepgram's `endpointing` (300 ms) and `utterance_end_ms` (1000 ms) control
  how quickly a line closes; they're constants in `internal/stt/deepgram/deepgram.go` rather than
  flags. Lowering them closes lines sooner at the cost of more mid-sentence breaks. Tune by ear
  with `--monitor` before the event, and see DESIGN.md §10.
