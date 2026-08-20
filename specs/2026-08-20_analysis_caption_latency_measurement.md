# Caption latency measurement — accuracy analysis

**Date:** 2026-08-20
**Question:** Does the latency shown on `/admin` accurately track audio from the moment sound
hits ffmpeg to the moment the word appears on the end user's page? Can we account for ADC delay?
**Outcome:** No. The metric is anchored to the wrong reference point, is corrupted by normal
operation, and is truncated at both ends of the chain. ADC delay *can* be accounted for, but it
is the smallest term in the budget.

**Code analyzed:** working tree at commit `40b79b7` **plus uncommitted changes** — notably the
silence auto-pause gate (`internal/stt/gate.go`, `internal/audio/level.go`, `PauseConfig`,
`StatePaused`). Line numbers below refer to that working-tree state, not to `40b79b7` alone.
`internal/cli/run.go`, `internal/metrics/metrics.go`, and `internal/stt/deepgram/deepgram.go`
all differ from the committed versions.

---

## 1. What the metric actually computes

`internal/cli/run.go:232-240`:

```go
func (s *session) observeLatency(t stt.Transcript) {
	if !t.IsFinal || t.ReceivedAt.IsZero() {
		return
	}
	elapsed := t.ReceivedAt.Sub(s.met.StartedAt)
	if d := elapsed - t.End(); d > 0 {
		s.met.ObserveLatency(d)
	}
}
```

That is:

```
latency = (ReceivedAt − metrics.StartedAt) − (Start + Duration)
```

| term | what it really is | where |
|---|---|---|
| `ReceivedAt` | wall time the WebSocket message finished decoding in Go | `deepgram/deepgram.go` readLoop |
| `Start + Duration` | media time of the segment end, **relative to the first byte of the current WebSocket stream** | Deepgram JSON |
| `StartedAt` | `time.Now()` at session construction | `run.go:56-57`, `metrics.go:114` |

`DESIGN.md:325` documents the intended formula as
`ReceivedAt − (streamStart + Start + Duration)`. The implementation substitutes **process start**
for **stream start**. Nearly every defect below follows from that one substitution.

**Measured span:** process start → transcript decoded inside the Go process.
**Claimed span:** sound at ffmpeg's input → word painted on the viewer's screen.

The measurement is truncated at both ends and mis-anchored at the front.

---

## 2. Findings by severity

### S1 — Auto-pause resets the media clock; error grows without bound

`--auto-pause` **defaults to `true`** (`internal/cli/cli.go:45`), with `--silence-hold` defaulting
to 10s and `--silence-threshold-db` to −45 dBFS.

When the gate goes inactive, `runConnection` returns `outcomePause` and `Run` closes the
WebSocket, waits for speech, then `continue`s the loop into a fresh `e.connect(ctx)`
(`deepgram.go:159-206`). **Every pause/resume cycle opens a brand-new WebSocket, and Deepgram's
`start` field restarts at 0 on a new stream.** `metrics.StartedAt` never moves.

After the first pause, the reported figure degenerates to:

```
latency ≈ (wall time since process start) − (media time since the last resume)
```

which grows by roughly the elapsed session time at each pause. For a 90-minute service with
periodic quiet stretches, the panel would read in the **tens of minutes**.

Compounding it, the code deliberately does not count a pause as a reconnect
(`deepgram.go:203-205`: *"A pause/resume cycle is not an error: no STTReconnect or SetSTTError,
since Snapshot.Clean() keys off reconnects"*). That is correct for `Clean()`, but it means the
latency baseline silently resets with **no counter, no error, and no visible signal anywhere**.

This makes what would otherwise be a reconnect edge case into the *normal operating mode*.

### S2 — ~770 ms of phantom latency on the live path (measured)

`StartedAt` is taken at `run.go:56`, before the transcript writer, web server construction,
listener, monitor start, and — critically — before the device probe in
`internal/audio/device.go:84`, which spawns an entire throwaway ffmpeg, reads 100 ms of audio,
and tears it down before the real capture process starts.

Measured on this machine, live capture against a real PulseAudio device with `--engine mock`
(mock has zero recognition delay, so anything reported is pure artifact):

```
uptime=29.89s media=29.10s | last=783ms p50=769 max=788 n=5
uptime=35.92s media=35.10s | last=758ms p50=769 max=788 n=6
uptime=41.94s media=41.10s | last=777ms p50=769 max=788 n=7
```

Every millisecond of that ~770 ms is fictitious. Startup budget, measured directly:

| stage | measured |
|---|---|
| probe ffmpeg spawn → first 100 ms chunk | 222 ms |
| probe teardown (`Close` + `Wait`) | 514 ms |
| real capture ffmpeg spawn → first sample | ~210 ms |

736 ms of probe overhead alone, which accounts for the observed offset.

**It does not drift.** Over a 73 s window the gap slope was −0.13 ms/s — within frame
quantization noise, extrapolating to under a second across 90 minutes. This is a *fixed* offset,
so a single calibration constant would cancel it.

**The dev loop hides it.** The same code reports **~1 ms** under `replay`, because `FileSource`
paces from its own start (`file.go:122`) and there is no device probe. The bug is invisible in
the primary development workflow and only manifests in production.

### S3 — Dropped audio and ffmpeg restarts shift the clock permanently

- The Deepgram ring drops the oldest frames when full. Deepgram's media clock counts only bytes
  it actually received, so every dropped frame adds its duration to **every subsequent** reading.
- `DeviceSource.offset` stays monotonic across ffmpeg restarts (`device.go:165`) — correct for
  the progress display — but the 250 ms→8 s restart backoff is wall time during which no audio
  was captured. Media time does not advance; wall time does. The difference is added permanently.

Both are silent, cumulative, and one-directional (always inflating).

### S4 — The browser half is not measured at all

Measurement stops at `ReceivedAt`, *inside* the Deepgram read loop — before the channel handoff
to the hub, hub assembly, SSE encode, network transit, and browser paint.

Measured hub→SSE-client delivery on loopback: **0.13 ms** (six consecutive `final` events).
Server-side fan-out is a non-issue.

The genuinely uncounted terms are **WiFi RTT to viewer phones** and **browser render**. On
congested event WiFi with many simultaneous viewers this is the least predictable link in the
entire chain, and it has zero instrumentation. The page also does per-word DOM work with a forced
reflow (`index.html:391-394`), which is not free on a low-end phone.

### S5 — Finals are timed; interims are what people see

`observeLatency` returns early unless `t.IsFinal` (`run.go:233`). But the viewer paints interim
text as it arrives (`index.html:673-675` → `applyUtterance`). With `endpointing=300` and
`utterance_end_ms=1000`, words are on screen several hundred ms before the final that the metric
times.

So the number is simultaneously **pessimistic** about perceived latency (S5) and **optimistic**
about the transport tail (S4). The two errors do not cancel in any principled way.

### S6 — Minor statistical issues

- `if d > 0` (`run.go:237`) silently discards negative samples, and `ObserveLatency` re-checks
  `d < 0` (`metrics.go`). Currently masked because the anchor bug makes `d` always large and
  positive; once the anchor is fixed, near-zero samples would clip and bias p50/p95 upward.
- The latency ring holds 512 **finals only**. At a realistic 10–20 finals/min that is 25–50
  minutes of history, so p50/p95 respond slowly to a degradation starting mid-event.
- `latencyMax` never decays. A single startup artifact or pause poisons it for the whole session.

---

## 3. Can we account for ADC delay?

Yes. Four approaches, ranked by reliability.

### 3.1 ffmpeg already computes it — the pipeline discards it

Verified on this machine:

```
$ ffprobe -f pulse -i <device> -show_packets
pts=1787240688026382   pts_time=1787240688.026382
pts=1787240688076380   pts_time=1787240688.076380
```

Those are **absolute epoch microseconds**, 50 ms apart. libavdevice's pulse and ALSA demuxers
stamp each packet with `av_gettime()` minus the audio server's reported stream latency — nominally
the instant the samples were captured. Using `-f s16le -` (`device.go:120`) throws all of it away.

**Caveat measured here:** this box runs PipeWire, whose pulse shim reports `Latency: 0 usec` for
both source and source-output, so the compensation term is zero and the PTS is effectively "when
ffmpeg read it." The real buffer is visible only as `node.latency = 2400/48000` (50 ms) and is not
reflected in the PTS. On the Pi — with genuine PulseAudio, or via `-f alsa` where libavdevice uses
`snd_pcm_htimestamp` — compensation should be real. **Verify on the target hardware before
relying on this.**

Recovering PTS in Go requires a container that preserves timestamps (`-f nut`) rather than
headerless PCM, which is a non-trivial change to the capture command and the frame reader.

### 3.2 One-time loopback calibration — most reliable, hardware-truthful

Inject a known impulse into the soundboard input at a recorded wall-clock instant; detect its
first sample in ffmpeg's stdout. The delta is the entire capture-side delay: ADC conversion +
USB transfer + kernel ring + audio server + ffmpeg internal buffering.

Store it as `--capture-latency-ms` and add it to every reported figure. This is the only method
that captures the physical converter delay, and for fixed hardware it is a single constant that
holds for the life of the deployment. Best run through the actual soundboard rather than a
digital loopback.

### 3.3 Bound it by construction

`device.go:119-120` passes **no buffering flags at all**, so the capture delay is whatever the
audio server happens to choose (50 ms here; potentially much more on a Pi).

```
-f alsa  -audio_buffer_size <µs>
-f pulse -fragment_size <bytes>
```

plus `-thread_queue_size`. This both shrinks the delay and converts it from an unknown into a
configured constant you can legitimately add.

### 3.4 Query the audio server at runtime

`pa_stream_get_latency` / `pactl list source-outputs` → `Buffer Latency` / `Source Latency`.
Tracks live changes, but measured returning `0 usec` under PipeWire here. Fragile and
machine-dependent — use as a cross-check, never as the source of truth.

### 3.5 Perspective: ADC delay is the smallest term

| term | typical magnitude |
|---|---|
| ADC converter group delay | sub-ms to ~2 ms |
| USB audio transfer | 1–4 ms |
| Audio server buffer | ~50 ms (measured here) |
| **Anchor bug (S2)** | **~770 ms** |
| Deepgram recognition | 300 ms – 1 s+ |
| **Auto-pause clock reset (S1)** | **unbounded, grows across the event** |

Chasing the ADC in isolation would be polishing the least significant digit. Fixing the anchor is
worth roughly 50× more, and fixing auto-pause is worth more than everything else combined.

---

## 4. The fix is mostly already built and unused

`audio.Frame` already carries the right data:

```go
// internal/audio/source.go:59-68
type Frame struct {
	PCM        []byte
	Offset     time.Duration // media time of the first sample
	CapturedAt time.Time     // when the frame was released into the pipeline
}
```

`CapturedAt` is populated by **both** sources (`device.go:176`, `file.go:140`) — and then thrown
away. `deepgram.go` pushes only bytes into the ring (`buf.push(f.PCM)`), so `Offset` and
`CapturedAt` never survive past the engine boundary.

Carrying `CapturedAt` alongside the PCM through the ring, and mapping Deepgram's
`Start`/`Duration` back onto the frame that contained those samples, would yield a byte-accurate
anchor that is immune to pauses, reconnects, drops, and ffmpeg restarts — because it stops
depending on a stream-relative clock entirely.

### Suggested order of work

1. **Anchor to the stream, not the process.** Carry `CapturedAt` through the ring; derive latency
   from the frame that produced the audio. Fixes S1, S2, S3 together.
2. **Add a capture calibration constant** (`--capture-latency-ms`) from §3.2, and set explicit
   capture buffer flags per §3.3 so the front of the chain is known rather than assumed.
3. **Instrument the browser half.** The `Event.At` timestamp already ships to the client; having
   the viewer report render time back (or simply exposing an SSE→paint figure in the page) closes
   S4 — the one link that actually varies at an event.
4. **Record interim latency separately** from final latency (S5), so the panel can show both
   "first pixels" and "settled text."
5. **Clean-up:** stop discarding negative samples, decay or window `latencyMax`, and consider a
   time-windowed ring rather than a fixed 512 finals (S6).

---

## 5. Method

All figures measured on the development machine (PipeWire, x86_64, Go 1.26.5), not on the Pi.

- **Replay baseline:** `livecaption replay <file> --engine mock`, polling `/api/stats`.
- **Live artifact isolation:** `livecaption live --backend pulse --device <monitor> --engine mock`.
  The mock engine is driven by media time with no recognition delay, so any reported latency is
  measurement error by construction.
- **Drift:** 25 samples over 73 s, least-squares slope on `uptime − seconds_total`.
- **Startup budget:** direct timing of ffmpeg spawn → first 3200-byte chunk → teardown, matching
  the probe sequence in `device.go`.
- **Capture timestamps:** `ffprobe -f pulse -show_packets`; `pactl list sources` / `source-outputs`.
- **SSE delivery:** raw `/events` client comparing `Event.At` to local receipt time.

No production code was modified during this analysis.
