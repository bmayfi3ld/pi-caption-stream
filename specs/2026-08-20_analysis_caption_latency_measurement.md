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

## Status — 2026-08-20 update

A multi-phase fix for the anchor bug (S1/S2/S3) has landed **in the working tree**, as
uncommitted changes on top of the state this analysis was written against. It is not committed
as of this update. The rest of this document is the original analysis, annotated in place; this
block is the scannable summary of what changed.

| finding | status | notes |
|---|---|---|
| S1 — auto-pause resets the media clock | **FIXED** | per-connection `anchorIndex`; a new WebSocket structurally gets a new index |
| S2 — ~770 ms phantom offset at process start | **FIXED** | process-start anchor removed entirely; capture-side delay (ADC/USB/audio-server buffering) still uncounted — see below |
| S3 — dropped audio / ffmpeg restarts shift the clock | **FIXED** | anchor no longer depends on a stream-relative clock |
| S4 — browser half unmeasured | **OPEN** | unchanged |
| S5 — only finals are timed, not interims | **OPEN** | unchanged |
| S6 — `if d > 0` clip | **FIXED** | clip removed from `observeLatency`; `metrics.ObserveLatency`'s own `d < 0` guard remains |
| S6 — fixed 512-sample ring, non-decaying `latencyMax` | **OPEN, now higher priority** | post-resume pre-roll flush makes this matter on every pause cycle — see §2 S6 and Remaining work §1 |

**Current anchor formula:** `latency = ReceivedAt − CapturedAt` (`internal/cli/run.go`,
`observeLatency`). `CapturedAt` is the wall-clock instant a frame was released into the capture
pipeline (`audio.Frame.CapturedAt`), carried through the Deepgram ring as `chunk{pcm,
capturedAt}` and resolved back from Deepgram's media-time `start`/`duration` via a
per-connection `anchorIndex` (`internal/stt/deepgram/anchor.go`) that interpolates within the
covering chunk. See §1 and §4 below for the detail, and `DESIGN.md` §6 for the canonical
one-paragraph description.

Findings by section: §2 S1/S2/S3 fixed (S2 partially — see caveat inline); §2 S4/S5 unchanged;
§2 S6 split, one item fixed, one open with new urgency; §3 (ADC delay) entirely unchanged —
none of it was implemented. See "Remaining work" at the end for what a future agent should pick
up next, in priority order.

---

## 1. What the metric actually computes

**2026-08-20: this section originally described the buggy implementation. That implementation
is gone. What follows is the current code.**

`internal/cli/run.go` (`observeLatency`):

```go
func (s *session) observeLatency(t stt.Transcript) {
	if !t.IsFinal || t.ReceivedAt.IsZero() || t.CapturedAt.IsZero() {
		return
	}
	s.met.ObserveLatency(t.ReceivedAt.Sub(t.CapturedAt))
}
```

That is:

```
latency = ReceivedAt − CapturedAt
```

| term | what it really is | where |
|---|---|---|
| `ReceivedAt` | wall time the WebSocket message finished decoding in Go | `deepgram/deepgram.go` readLoop |
| `CapturedAt` | wall-clock instant the audio frame containing the segment's last sample was released into the pipeline | `audio.Frame.CapturedAt`, resolved via `anchorIndex.At` (`internal/stt/deepgram/anchor.go`) from Deepgram's media-time `Start`/`Duration` |

`DESIGN.md` §6 documents this as the canonical formula. There is no longer a `streamStart` or a
process-start term anywhere in the calculation — the substitution that caused nearly every
defect in the original version of this document (§2 below) has been removed rather than patched
around.

**Measured span (still):** the frame's release into the pipeline → transcript decoded inside the
Go process. This is now an *honest* measurement of that span — it no longer drifts, resets, or
accumulates error from pauses, reconnects, drops, or restarts (§2 S1–S3, all fixed).

**Still not measured:** sound at the ADC → the frame's `CapturedAt` (§3, unimplemented), and
transcript decoded → word painted on the viewer's screen (§2 S4, unimplemented). The claimed
span from the original question — "sound hits ffmpeg" to "word appears on the end user's page"
— is still wider than what's measured at both ends; only the middle is now trustworthy.

---

## 2. Findings by severity

### S1 — Auto-pause resets the media clock; error grows without bound — **FIXED (2026-08-20)**

**Original finding (still accurate as a description of the bug that existed):**

`--auto-pause` **defaults to `true`** (`internal/cli/cli.go`), with `--silence-hold` defaulting
to ~~10s~~ **60s** (see below) and `--silence-threshold-db` to −45 dBFS.

When the gate goes inactive, `runConnection` returns `outcomePause` and `Run` closes the
WebSocket, waits for speech, then `continue`s the loop into a fresh `e.connect(ctx)`
(`deepgram.go`). **Every pause/resume cycle opens a brand-new WebSocket, and Deepgram's `start`
field restarts at 0 on a new stream.** The old code anchored to `metrics.StartedAt`, which never
moved, so the reported figure degenerated to roughly `(wall time since process start) − (media
time since the last resume)` — growing by roughly the elapsed session time at each pause.

**What changed:** `runConnection` now builds a fresh `anchorIndex` (`internal/stt/deepgram/anchor.go`)
before either the reader or writer goroutine launches for that connection:

```go
// runConnection, internal/stt/deepgram/deepgram.go
idx := newAnchorIndex(e.cfg.Format)
```

The index maps *this connection's* byte-counting media clock to wall-clock `CapturedAt` values,
built from the chunks `writeLoop` actually wrote to *this* WebSocket. It is scoped to the
connection's lifetime and is never shared across reconnects — "there is no shared clock left to
reset" is now true by construction, not by discipline. `readLoop` resolves each transcript's
`Start`/`Duration` against this same index (`idx.At(t.End())`), so connection 2's `start: 0` can
only ever resolve against connection 2's own freshly written bytes, never connection 1's.

Regression test: `TestEngine_S1ClockRestartAfterReconnect`
(`internal/stt/deepgram/deepgram_test.go`) forces a ~1.5s wall-clock gap between connection 1's
activity and connection 2's resume, floods the ring past its 2s capacity before resuming so no
stale pre-pause bytes survive, and asserts the reported latency stays small — i.e. that
connection 2's `start: 0` resolves against its own recent write, not an unrelated earlier origin.

The pause/resume accounting itself is unchanged and remains correct: it is still deliberately
not counted as a reconnect (`deepgram.go`: *"A pause/resume cycle is not an error: no
STTReconnect or SetSTTError, since Snapshot.Clean() keys off reconnects"*), which is correct for
`Clean()`. That was never the bug — the bug was the latency anchor, and only the anchor changed.

### S2 — ~770 ms of phantom latency on the live path (measured) — **FIXED (2026-08-20), with a caveat**

The original ~770 ms artifact came from anchoring to `metrics.StartedAt` (`time.Now()` at session
construction, before the device probe, listener, and monitor start). That anchor is gone — the
formula no longer has a `StartedAt` term at all (§1). The measured 736 ms of probe/startup
overhead described below no longer leaks into the reported figure.

**What this fix does *not* do:** it does not account for capture-side delay. The number now
starts at `CapturedAt` — the instant ffmpeg's frame was released into the Go pipeline — which is
*after* ADC conversion, USB transfer, and audio-server buffering, not at the instant sound hit
the converter. §3.2's proposed `--capture-latency-ms` loopback calibration was **not
implemented**. The original measurements below (of the old artifact, and of the fixed startup
budget) are kept for the record; they are no longer live behavior.

*(Original measurement, kept for the record — this offset is gone from the current code:)*

```
uptime=29.89s media=29.10s | last=783ms p50=769 max=788 n=5
uptime=35.92s media=35.10s | last=758ms p50=769 max=788 n=6
uptime=41.94s media=41.10s | last=777ms p50=769 max=788 n=7
```

Startup budget, measured directly at the time:

| stage | measured |
|---|---|
| probe ffmpeg spawn → first 100 ms chunk | 222 ms |
| probe teardown (`Close` + `Wait`) | 514 ms |
| real capture ffmpeg spawn → first sample | ~210 ms |

736 ms of probe overhead alone, which accounted for the observed offset at the time.

**It did not drift.** Over a 73 s window the gap slope was −0.13 ms/s — within frame
quantization noise. This was a *fixed* offset, consistent with it coming from a one-time
process-start anchor rather than a per-sample error.

**The dev loop hid it.** The same code reported **~1 ms** under `replay`, because `FileSource`
paces from its own start and there is no device probe. The bug was invisible in the primary
development workflow and only manifested in production. This asymmetry is now moot for S2, but
is worth remembering as a general lesson for the still-open items in §3.

### S3 — Dropped audio and ffmpeg restarts shift the clock permanently — **FIXED (2026-08-20)**

**Original finding:**

- The Deepgram ring drops the oldest frames when full. Deepgram's media clock counts only bytes
  it actually received, so every dropped frame added its duration to **every subsequent**
  reading, under the old stream-relative anchor.
- `DeviceSource.offset` stays monotonic across ffmpeg restarts (`device.go`) — correct for the
  progress display — but the 250 ms→8 s restart backoff was wall time during which no audio was
  captured. Media time did not advance; wall time did. The difference was added permanently
  under the old formula.

**What changed:** this class of bug is fixed as a direct consequence of the S1 fix, not by a
separate change. The new anchor never derives latency from "media time since some fixed origin"
— it always resolves a specific transcript's media-time end back to the wall-clock instant of
the frame that produced those exact samples, via `anchorIndex`. A dropped frame changes which
bytes are on the wire, but every remaining byte still maps to its own true `CapturedAt`; there is
no accumulator that a drop or restart can permanently bias. The ring in `deepgram.go`'s
`ring.push` still evicts on overflow exactly as described, but eviction no longer corrupts
anything latency-related — it only affects how much pre-roll survives a pause (see S6's new
finding below, which is a *consequence* of this pre-roll behavior, not a regression of S3).

### S4 — The browser half is not measured at all — **UNCHANGED / still open**

Measurement stops at `ReceivedAt`, *inside* the Deepgram read loop — before the channel handoff
to the hub, hub assembly, SSE encode, network transit, and browser paint.

Measured hub→SSE-client delivery on loopback: **0.13 ms** (six consecutive `final` events).
Server-side fan-out is a non-issue.

The genuinely uncounted terms are **WiFi RTT to viewer phones** and **browser render**. On
congested event WiFi with many simultaneous viewers this is the least predictable link in the
entire chain, and it has zero instrumentation. The page also does per-word DOM work with a forced
reflow (`index.html`), which is not free on a low-end phone. Nothing in the recent fix touches
this path. See Remaining work §3.

### S5 — Finals are timed; interims are what people see — **UNCHANGED / still open**

`observeLatency` still returns early unless `t.IsFinal` (now also gated on `t.CapturedAt` being
non-zero, but the `IsFinal` filter itself is untouched). The viewer still paints interim text as
it arrives. With `endpointing=300` and `utterance_end_ms=1000`, words are on screen several
hundred ms before the final that the metric times.

So the number is still simultaneously **pessimistic** about perceived latency (S5) and, per S4,
**optimistic** about the transport tail. The two errors still do not cancel in any principled
way. See Remaining work §2.

### S6 — Minor statistical issues — **one item fixed, one item open with new urgency**

- ~~`if d > 0` (`run.go`) silently discards negative samples~~ — **FIXED**: that clip is gone
  from `observeLatency` in `internal/cli/run.go`. The defensive `d < 0` guard inside
  `metrics.ObserveLatency` (`internal/metrics/metrics.go`) remains, and is now the only place
  negative samples are filtered. With the anchor fixed, `d` is a real elapsed duration rather
  than an always-large, always-positive artifact, so near-zero or (from clock skew / rounding)
  slightly negative samples are now a real possibility this guard exists to handle.
- The latency ring holds 512 **finals only**, and `latencyMax` never decays — **STILL OPEN**, and
  now matters for a reason the original analysis could not have known:

  **New finding, 2026-08-20:** after a resume, `writeLoop` flushes up to `bufferAudio` (2s) of
  ring pre-roll whose `CapturedAt` is genuinely that old — the ring is deliberately kept full of
  recent-but-possibly-silent audio across a pause so the first words of resumed speech aren't
  lost (`deepgram.go`, `ring.push` comments). That means the **first final reported after every
  resume carries a real ~1-2 s latency measurement**. This is not a bug and not an artifact of
  the anchor — it is a true reading of how long that specific audio waited. But with auto-pause
  on by default (`--auto-pause=true`), this now happens on **every pause/resume cycle** in a
  normal session, which pulls both `latencyMax` and `p95` up on a schedule tied to room silence
  rather than to any actual degradation in the pipeline. This upgrades "time-window the latency
  ring" from optional polish to the right next fix — see Remaining work §1.

  The test that currently guards the *opposite* property and will need rewriting:
  `TestLatencyRingStaysBoundedAfterWrap` (`internal/metrics/metrics_test.go`) asserts that `max`
  survives a ring wrap unchanged (it feeds the ring monotonically increasing samples and checks
  `latencyMax` equals the last, largest one) — which is a direct assertion that `max` never
  decays. That is exactly the behavior a windowed max would change. Whoever picks up the
  windowing work should expect to rewrite this test's expectations, not just its inputs.

---

## 3. Can we account for ADC delay? — **UNCHANGED / entirely unimplemented**

None of the code in this section was touched by the 2026-08-20 fix. The fix addressed the
anchor's *reference point* (§1) and its *stability under pause/reconnect/drop* (§2 S1–S3); it did
not extend the anchor earlier than "frame released into the pipeline." Everything below is still
accurate as analysis and still not implemented. See Remaining work §4–§5 for priority.

Yes, it can be accounted for. Four approaches, ranked by reliability.

### 3.1 ffmpeg already computes it — the pipeline discards it

Verified on this machine:

```
$ ffprobe -f pulse -i <device> -show_packets
pts=1787240688026382   pts_time=1787240688.026382
pts=1787240688076380   pts_time=1787240688.076380
```

Those are **absolute epoch microseconds**, 50 ms apart. libavdevice's pulse and ALSA demuxers
stamp each packet with `av_gettime()` minus the audio server's reported stream latency — nominally
the instant the samples were captured. Using `-f s16le -` (`device.go`) throws all of it away.

**Caveat measured here:** this box runs PipeWire, whose pulse shim reports `Latency: 0 usec` for
both source and source-output, so the compensation term is zero and the PTS is effectively "when
ffmpeg read it." The real buffer is visible only as `node.latency = 2400/48000` (50 ms) and is not
reflected in the PTS. On the Pi — with genuine PulseAudio, or via `-f alsa` where libavdevice uses
`snd_pcm_htimestamp` — compensation should be real. **Verify on the target hardware before
relying on this.** (Still unverified as of this update — flagged again in Remaining work §5.)

Recovering PTS in Go requires a container that preserves timestamps (`-f nut`) rather than
headerless PCM, which is a non-trivial change to the capture command and the frame reader.

### 3.2 One-time loopback calibration — most reliable, hardware-truthful

Inject a known impulse into the soundboard input at a recorded wall-clock instant; detect its
first sample in ffmpeg's stdout. The delta is the entire capture-side delay: ADC conversion +
USB transfer + kernel ring + audio server + ffmpeg internal buffering.

Store it as `--capture-latency-ms` and add it to every reported figure. This is the only method
that captures the physical converter delay, and for fixed hardware it is a single constant that
holds for the life of the deployment. Best run through the actual soundboard rather than a
digital loopback. **Not implemented** — there is no `--capture-latency-ms` flag anywhere in
`internal/cli/cli.go`.

### 3.3 Bound it by construction

`device.go` passes **no buffering flags at all**, so the capture delay is whatever the audio
server happens to choose (50 ms here; potentially much more on a Pi).

```
-f alsa  -audio_buffer_size <µs>
-f pulse -fragment_size <bytes>
```

plus `-thread_queue_size`. This both shrinks the delay and converts it from an unknown into a
configured constant you can legitimately add. **Not implemented.**

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
| ~~Anchor bug (S2)~~ | ~~~770 ms~~ **eliminated 2026-08-20** |
| Deepgram recognition | 300 ms – 1 s+ |
| ~~Auto-pause clock reset (S1)~~ | ~~unbounded, grows across the event~~ **eliminated 2026-08-20** |

At the time this table was written, chasing the ADC in isolation would have been polishing the
least significant digit while the anchor bug and auto-pause reset dominated the budget by 1-2
orders of magnitude. Now that both of those are fixed, the ADC/USB/audio-server terms (§3.2/3.3)
are relatively more significant than they were, but are still small next to Deepgram's own
recognition latency (300 ms – 1s+) and next to the still-unmeasured S4 browser/network tail. They
remain low priority — see Remaining work §4.

---

## 4. What was done: anchoring latency to capture time, not process start

**2026-08-20: this section originally proposed a fix ("The fix is mostly already built and
unused"). That proposal has since been implemented. This section is now a record of what
shipped, not a suggestion.**

`audio.Frame` already carried the right data at the time of the original analysis, and still
does:

```go
// internal/audio/source.go
type Frame struct {
	PCM        []byte
	Offset     time.Duration // media time of the first sample
	CapturedAt time.Time     // when the frame was released into the pipeline
}
```

At the time, `CapturedAt` was populated by both sources (`device.go`, `file.go`) and then thrown
away: `deepgram.go` pushed only bytes into the ring (`buf.push(f.PCM)`), so `Offset` and
`CapturedAt` never survived past the engine boundary. **That is no longer true.** The ring now
stores `chunk{pcm, capturedAt}`:

```go
// internal/stt/deepgram/deepgram.go
type chunk struct {
	pcm        []byte
	capturedAt time.Time
}

func (r *ring) push(f audio.Frame) {
	r.mu.Lock()
	r.chunks = append(r.chunks, chunk{pcm: f.PCM, capturedAt: f.CapturedAt})
	...
```

`push` takes an `audio.Frame`, not a `[]byte`. `writeLoop` records each chunk's byte range and
`capturedAt` into the connection's `anchorIndex` immediately before writing it to the WebSocket:

```go
// writeLoop, internal/stt/deepgram/deepgram.go
if c, ok := buf.pop(); ok {
	idx.Add(len(c.pcm), c.capturedAt)
	if err := conn.Write(connCtx, websocket.MessageBinary, c.pcm); err != nil {
		return err
	}
	...
```

`readLoop` then maps each transcript's `Start`/`Duration` back onto the frame that produced those
samples via `idx.At(t.End())`, and stamps the result onto `stt.Transcript.CapturedAt`. This is
exactly the "carry `CapturedAt` alongside the PCM through the ring, and map Deepgram's
`Start`/`Duration` back onto the frame that contained those samples" design this section
originally proposed. It is immune to pauses, reconnects, drops, and ffmpeg restarts, because it
never depends on a stream-relative clock (§2 S1–S3).

### What was NOT done from the original suggested order of work

The original order-of-work list proposed five steps. Only the first shipped:

1. ~~**Anchor to the stream, not the process.**~~ **Done** — this section, §1, §2 S1–S3.
2. **Add a capture calibration constant** (`--capture-latency-ms`) and explicit capture buffer
   flags. **Not done.** See §3.2/§3.3, Remaining work §4.
3. **Instrument the browser half.** **Not done.** See §2 S4, Remaining work §3.
4. **Record interim latency separately from final latency.** **Not done.** See §2 S5, Remaining
   work §2.
5. **Clean-up:** stop discarding negative samples (done, §2 S6), decay or window `latencyMax` and
   the ring (not done, §2 S6, Remaining work §1).

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

No production code was modified during this analysis. **This remains true of the analysis
itself** — nothing above was produced by editing the pipeline to make a number look better.
Production code *has* since changed, as a separate, later piece of work: the anchor fix described
in the Status block, §1, §2 (S1/S2/S3/S6-partial), and §4 landed in the working tree after this
analysis was written, as uncommitted changes, in direct response to the findings here.

---

## Remaining work

Addressed to whichever agent picks this up next. Ordered by value, per the same reasoning as the
original §3.5 priority table: fix the biggest term in the budget first. Each item below was
deliberately **not** done as part of the 2026-08-20 anchor fix — that fix was scoped to S1/S2/S3
(the anchor's stability), not to these.

1. **S6 remainder — time-window the latency samples and the max.** The ring is a fixed 512
   finals with a `latencyMax` that never decays. This was optional polish when the anchor was
   broken (every reading was an artifact anyway); it is not optional now. Motivation: after every
   auto-pause resume, `writeLoop` flushes up to 2s of genuinely-old pre-roll from the ring
   (`bufferAudio` in `internal/stt/deepgram/deepgram.go`), so the first final after each resume
   reports a real ~1-2 s latency — true, but it drags `max` and `p95` up on every pause cycle with
   auto-pause on by default. Whoever does this must rewrite
   `TestLatencyRingStaysBoundedAfterWrap` (`internal/metrics/metrics_test.go`), which currently
   asserts the opposite property (`max` surviving a wrap unchanged) — that assertion is precisely
   what a windowed max would invalidate.

2. **S5 — record interim latency separately from final latency.** `observeLatency`
   (`internal/cli/run.go`) still filters on `t.IsFinal`. The viewer paints interim text several
   hundred ms earlier (`index.html`, `applyUtterance`), so the current figure is pessimistic
   about what a viewer actually perceives. Add a second latency series (or a second field) for
   interims so `/admin` can show both "first pixels" and "settled text."

3. **S4 — instrument the browser half.** Still completely uninstrumented. `Event.At` already
   ships to the client; the cheap version is an SSE→paint figure computed client-side and
   reported back (or just surfaced locally in the page for a spot-check). The genuinely uncounted
   term at a real event is WiFi RTT to viewer phones on congested event WiFi — that's the one
   that actually varies and actually matters, and there is currently zero way to see it without
   being physically at the venue with a viewer's phone in hand.

4. **§3.2 / §3.3 — capture-side calibration.** Add `--capture-latency-ms` (one-time loopback
   calibration per §3.2) and explicit ffmpeg capture-buffer flags
   (`-audio_buffer_size` / `-fragment_size` / `-thread_queue_size`, per §3.3) to `device.go`.
   Turns the capture delay from an unknown into a configured constant that can be legitimately
   added to the reported figure. Lower priority than 1-3: per the updated §3.5 table, this term
   is still small next to Deepgram's own recognition latency and the still-unmeasured S4 tail.

5. **§3.1 — recover ffmpeg's PTS.** Needs a container that preserves timestamps (`-f nut`)
   instead of headerless PCM (`-f s16le`), which touches both the capture command and the frame
   reader. Flag as **measured-but-unverified on the Pi** — the caveat in §3.1 about PipeWire
   reporting `Latency: 0 usec` on the dev machine (meaning the PTS compensation term was
   effectively zero here) still stands and has not been checked against real PulseAudio or ALSA
   on target hardware.
