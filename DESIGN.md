# livecaption — design

Live captions for a room: audio leaves a soundboard over USB, a computer streams it to a
speech-to-text service, and the resulting text appears on a webpage within a second.

The constraint that shaped everything: the signal chain isn't available during development. So the
tool has a **replay mode** that streams an audio file through the *same* pipeline at true
wall-clock rate. If replay works end to end, connecting the USB converter is a change of
subcommand, not a change of code path.

---

## 1. Pipeline

One linear flow. Each stage is a package, connected by channels, independently testable.

```
  ┌──────────────┐  chan audio.Frame  ┌────────────┐  chan stt.Transcript  ┌─────────┐
  │ audio.Source │ ─────────────────► │ stt.Engine │ ────────────────────► │   Hub   │
  └──────────────┘  16 kHz mono s16le └────────────┘   interim + final     └────┬────┘
    file │ device                      deepgram │ mock                          │
         │                                                        ┌─────────────┼─────────────┐
         └──(tap)──► monitor                                      ▼             ▼             ▼
                     (speakers)                              SSE clients   transcript     metrics
                                                             viewer page     files       /admin
```

Two invariants hold the whole design together:

**Nothing downstream may block the pipeline.** A slow browser, a stalled sound card, or a failing
disk write must never apply backpressure to audio capture. Every fan-out point drops and counts
instead of blocking.

**Every stage has an offline equivalent.** `replay` substitutes for the soundboard, `--engine mock`
substitutes for Deepgram. The full system runs with no hardware, no network, and no API spend —
which is what makes it practical to develop and test.

---

## 2. Audio sources

Both sources shell out to **ffmpeg** and read raw **16 kHz mono signed 16-bit little-endian** PCM
from its stdout.

Why ffmpeg rather than cgo bindings (PortAudio, malgo): the binary stays pure Go with no C
toolchain; ffmpeg absorbs whatever sample rate and channel count the soundboard presents; and the
same code decodes MP3s for replay. One dependency covers capture, decode, resample, downmix and
playback.

| | `replay` (`internal/audio/file.go`) | `live` (`internal/audio/device.go`) |
|---|---|---|
| command | `ffmpeg -i <file> -ac 1 -ar 16000 -f s16le -` | `ffmpeg -f pulse -i <dev> -ac 1 -ar 16000 -f s16le -` |
| paced by | our scheduler | the sound card |
| failure mode | EOF | device disappears → restart with backoff |

### Pacing

ffmpeg decodes as fast as the pipe drains, so in replay mode *the reader* sets the rate. Each chunk
is held until an **absolute deadline** computed from a fixed start time:

```go
due := start.Add(time.Duration(n+1) * interval)
```

Not a `time.Ticker`. With a ticker, every late wakeup pushes all subsequent frames later and the
error accumulates; over a 32-minute file that drift is large enough to invalidate the simulation.
With absolute deadlines a slow iteration is corrected by the next one. `TestFileSourcePacing`
asserts media time and wall time stay within tolerance.

`--speed` scales the interval for quick dry runs. `--loop` restarts for soak tests.

### Chunk size

`--chunk-ms`, default **100 ms** (3200 bytes). Inside Deepgram's recommended 20–250 ms payload
range: small enough not to add meaningful latency, large enough to avoid one WebSocket frame every
20 ms.

### Live hardening

The capture path can degrade in ways that don't stop the audio, so each has a counter behind it:

- **ffmpeg exits** (USB unplugged) → relaunch with exponential backoff, `ffmpeg_restarts_total`.
  Media offset accumulates across restarts so timestamps stay monotonic.
- **ALSA xruns** → stderr is scanned for overrun/underrun tokens, `xruns_total`.
- **Bad device name** → caught at startup by a probe read, so a typo is an immediate clear error
  rather than an infinite restart loop.

### Monitor playback (`replay --monitor`)

To judge caption delay you need to hear the audio while watching the text land. The tap point is
the design decision: playing the original file with a separate player would drift against our
scheduler and ignore `--speed`. Instead the monitor **tees the exact frames already emitted** into
a second ffmpeg writing to the speakers.

So what you hear is bit-identical to what the recognizer receives, released by the same clock. It's
the 16 kHz mono downmix, which is the point — bad source audio becomes *audible* instead of being
inferred from bad transcripts.

Two honesty constraints:

- **`--monitor` requires `--speed 1.0`.** The sink drains at a fixed 16000 samples/sec; at 2× the
  buffer would overflow continuously. Rejected at parse time with an explanatory error.
- **Playback adds a fixed ~80 ms buffer**, so perceived delay *overstates* true caption latency by
  that much. The figure is printed in the banner rather than hidden, and `/admin` reports measured
  latency for comparison.

The tap is a non-blocking send: a stalled sound card drops monitor frames
(`monitor_frames_dropped_total`) and never touches the caption path. A dead playback process is a
warning, not a session failure.

Replay-only. On `live`, monitoring the board feed through the same machine's speakers invites
acoustic feedback into the mics.

---

## 3. Speech-to-text abstraction

```go
type Engine interface {
	Name() string
	Run(ctx context.Context, frames <-chan audio.Frame, out chan<- Transcript) error
}
```

Engines self-register (`stt.Register`) and `main` blank-imports them, so adding AssemblyAI or a
local whisper.cpp touches no existing file.

`Transcript` carries `IsFinal` (text won't be revised), `SpeechFinal` (natural end of utterance),
and media-time `Start`/`Duration` — timing in *media* time, not wall clock, so replay and live
measure latency identically.

`Run` owns its own reconnect logic: it returns only when the context is cancelled or frames run
out, never on a dropped connection.

### Deepgram (`internal/stt/deepgram`)

`wss://api.deepgram.com/v1/listen`, header `Authorization: Token <key>`:

```
encoding=linear16&sample_rate=16000&channels=1&model=nova-3&language=en-US
&interim_results=true&punctuate=true&smart_format=true
&endpointing=300&utterance_end_ms=1000&vad_events=true
```

**WebSocket library: `github.com/coder/websocket`.** Worth recording why, since gorilla/websocket
is the better-known name: **gorilla is archived and no longer actively developed.** coder/websocket
is the former `nhooyr.io/websocket`, adopted by Coder in 2024 and maintained. It also fits better —
`context.Context` on every read and write maps onto our cancellation-driven shutdown, and it
serializes concurrent writes internally (we have a PCM writer and a KeepAlive ticker sharing one
connection, which gorilla would require a hand-rolled mutex for).

Used directly rather than via `deepgram-go-sdk`: it's a single endpoint, the client is small, and
the official SDK has lagged on new streaming models. Reversible if that changes.

Structure: a writer goroutine (PCM → binary frames, `{"type":"KeepAlive"}` every 5 s when idle) and
a reader goroutine (JSON → `Transcript`). On shutdown it sends `{"type":"CloseStream"}` and drains
remaining results so the tail of a session isn't lost. On disconnect it reconnects with exponential
backoff (250 ms → 8 s, jittered), holding ~2 s of audio in a bounded drop-oldest ring so a brief
blip loses nothing.

### Mock (`internal/stt/mock`)

Emits canned phrases with realistic interim → final progression, driven entirely by media time from
the frames — never wall clock — so output is identical at any `--speed` and reproducible in tests.
This is what the web layer is developed against.

---

## 4. Caption hub

A recognizer emits many interims per utterance, then one or more `is_final` segments, then
`speech_final`. The hub (`internal/caption/hub.go`) turns that into stable display lines:

| input | effect |
|---|---|
| interim | replaces the uncommitted tail only |
| `is_final` | commits a segment; line stays open |
| `speech_final` / `UtteranceEnd` | closes the line, appends to history |

Only `speech_final` closes a line — otherwise captions flicker and split mid-sentence. `Flush()` at
shutdown closes anything still open.

**Fan-out never blocks.** Subscribers get a 16-deep buffered channel; one that can't keep up is
dropped and counted (`slow_disconnects_total`) rather than backpressured. `EventSource` reconnects
on its own and receives a fresh snapshot, so the cost of a drop is one round trip.

History is capped at 200 lines for late-joiner snapshots — a browser refreshed mid-session is never
blank.

---

## 5. Web server

| Route | Purpose |
|---|---|
| `GET /` | Viewer page |
| `GET /events` | SSE stream: `snapshot` on connect, then incremental events |
| `GET /admin` | Metrics dashboard (no auth) |
| `GET /api/stats` | JSON metrics snapshot |
| `GET /healthz` | Liveness |

SSE rather than WebSocket: traffic is strictly one-way, and `EventSource` gives automatic
reconnection with no client-side logic to maintain. Headers set `no-cache` and `X-Accel-Buffering:
no`, every event is flushed immediately, and a `: ping` comment every 15 s keeps idle proxies from
closing the connection.

Event wire format:

```json
{"seq":42,"kind":"final","id":"u17","text":"...","offset_ms":91230,"at":"2026-08-19T09:31:05Z"}
{"seq":43,"kind":"interim","text":"partial words so far"}
{"seq":44,"kind":"status","state":"reconnecting","detail":"stt websocket closed"}
```

Pages are `//go:embed`-ed so the binary ships standalone; `--dev-static` serves from disk while
iterating.

**Viewer** — bottom-anchored rolling window: recent finalized lines plus the live line, dimmer.
Large high-contrast type sized in `vw` so one URL works on a phone, a projector, and as an OBS
browser source. `?lines=`, `?size=`, `?debug=1` (overlays measured latency).

**Admin** — polls `/api/stats` once a second. Simpler than SSE for a single operator client.

---

## 6. Metrics

`internal/metrics` holds one snapshot that the admin page, the status line and the shutdown summary
all read from — so they cannot disagree with each other.

The governing rule: **anything that can degrade silently gets a counter.** A session that quietly
dropped 3 % of its audio must not look identical to a clean one.

| group | fields |
|---|---|
| source | frames/bytes/seconds, `frames_dropped_total`, `ffmpeg_restarts_total`, `xruns_total`, `ffmpeg_last_stderr` |
| monitor | `enabled`, `device`, `buffer_ms`, `alive`, `frames_dropped_total` |
| stt | `state`, `reconnects_total`, `interim_total`, `final_total`, `bytes_sent_total`, `last_error`, latency last/p50/p95/max |
| web | `sse_clients`, `sse_clients_total`, `events_total`, `slow_disconnects_total` |
| transcript | `path`, `lines_written`, `bytes_written`, `last_write_error` |
| process | `version`, `session_id`, `started_at`, `uptime`, `goroutines` |

**Latency** is `ReceivedAt − (streamStart + Start + Duration)`: how far behind wall clock the
caption for a given piece of audio arrived. A 512-sample ring plus atomics; percentiles computed on
read. No metrics library needed.

`Snapshot.Clean()` reports whether every degradation counter is zero — that drives the amber
highlighting in both the summary and `/admin`.

---

## 7. Transcripts

**On by default for every session.** Recording is the expected behaviour, not something to remember
to enable. `--transcript-dir` (default `./transcripts`, or `LIVECAPTION_TRANSCRIPT_DIR`) relocates
it; `--no-transcript` is the explicit opt-out.

Per session, `<dir>/<YYYY-MM-DDTHH-MM-SS>/`:

- `transcript.txt` — `[00:12:34] text`, for humans
- `transcript.jsonl` — one record per line with offsets and timestamps, for tooling

Both `O_APPEND`, buffered, flushed every 2 s and on close, so a crash keeps what already landed.
Write failures surface as a metric rather than an error — losing the transcript must not end a live
event.

---

## 8. CLI

`github.com/alecthomas/kong`. Struct tags declare env binding, enum validation, file-existence
checks and grouped help, removing a page of hand-written validation. Subcommands rather than a
`--source` flag, because replay and live take genuinely disjoint options.

```bash
livecaption devices                              # find the soundboard
livecaption replay recording.mp3 --engine mock   # primary dev loop, no API cost
livecaption replay recording.mp3 --monitor       # hear it while watching captions land
livecaption live --device <soundboard>           # the real thing
```

Shutdown: SIGINT/SIGTERM → cancel → ffmpeg stops → frames channel closes → engine sends
`CloseStream` and drains → hub flushes the open utterance → transcript flushed → HTTP server
`Shutdown` with a 5 s grace. A second Ctrl-C exits immediately.

---

## 9. Terminal output

The governing rule: **captions are data, logs are diagnostics.** Finalized captions go to
**stdout**; everything else to **stderr**. So `livecaption replay f.mp3 > captions.txt 2> run.log`
splits cleanly and piping never mixes the two.

`internal/ui` owns the terminal behind one mutex — the status line and the log handler both target
stderr, and without a single owner they interleave into garbage.

`--log-format=auto` resolves by TTY detection:

| | captions | status line | logs |
|---|---|---|---|
| **terminal** | coloured | pinned, redrawn 2×/sec | pretty, coloured, relative timestamps |
| **piped / systemd** | plain | none | `slog` JSON |

```
livecaption 0.1.0
  source      replay  recording.mp3 (31:51, 44100 Hz stereo -> 16000 Hz mono)
  speed       1x (wall-clock)
  monitor     pulse:default (~80ms buffer)   perceived delay overstates actual by this much
  stt         deepgram  model=nova-3  language=en-US
  transcript  ./transcripts/2026-08-19T09-31-05
  viewer      http://localhost:8080
  admin       http://localhost:8080/admin

ready — Ctrl-C to stop
[00:00:12] Good morning everyone, thanks for coming out today.
▶ 00:04:31 / 31:51 │ stt ● connected │ lat 340ms p95 610ms │ 2 viewers │ 47 lines
```

**Interim results are never printed to the terminal** — they'd flood the scrollback, and the web
page is what they're for.

Log levels, each earning its place:

- `error` — the run cannot continue (ffmpeg won't start, key rejected, port in use)
- `warn` — degraded but running (reconnect, xrun, viewer dropped). *The level that matters
  mid-event*, and the one that gets a coloured glyph.
- `info` *(default)* — lifecycle only; a two-hour event should produce well under 50 lines
- `debug` — per-utterance results, backoff decisions, ffmpeg stderr, SSE connect/disconnect

No per-frame logging at any level — at 100 ms chunks that's 10 lines/sec of noise.

---

## 10. Operating notes

- **Feed a mono aux/matrix send of the mics, not the main mix.** Music beds and effects degrade
  accuracy badly and `-ac 1` will happily downmix all of it. This is the single biggest accuracy
  lever in the project — larger than any model or parameter choice.
- **`--keyterm`** with the event's proper nouns (names, places, in-house terms) is the second
  biggest lever, and costs nothing.
- **Deepgram bills by streamed audio duration**, and replay at 1.0× costs exactly what live costs.
  Use `--engine mock` for all UI work.
- If captions feel late, `endpointing` and `utterance_end_ms` are the knobs: lower closes lines
  sooner at the cost of more mid-sentence breaks. Tune by ear with `--monitor` *before* the event.
- 401 on first connect is the most likely first-run failure — check `DEEPGRAM_API_KEY`.

## 11. Layout

```
cmd/livecaption/main.go     entrypoint, signal handling
internal/cli/               kong command tree, shared wiring
internal/ui/                terminal ownership, slog handler
internal/audio/             Source interface, ffmpeg plumbing, file/device/monitor
internal/stt/               Engine interface + registry; deepgram/, mock/
internal/caption/           hub (utterance assembly, fan-out), transcript writer
internal/metrics/           counters, latency ring, snapshot
internal/web/               routes, SSE, embedded viewer + admin pages
```
