package web

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"livecaption/internal/caption"
	"livecaption/internal/metrics"
	"livecaption/internal/stt"
)

// startTestServer boots the server on an ephemeral port using the same
// Listen/Serve path main.go uses, so the tests exercise the real HTTP
// surface rather than a mux built by hand.
func startTestServer(t *testing.T, cfg Config) (baseURL string, hub *caption.Hub, m *metrics.Metrics) {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := s.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go s.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	})
	return "http://" + ln.Addr().String(), cfg.Hub, cfg.Metrics
}

func newTestConfig() Config {
	m := metrics.New("test-version", "test-session")
	return Config{
		Hub:     caption.NewHub(m),
		Metrics: m,
	}
}

func TestHealthzReturns200(t *testing.T) {
	base, _, _ := startTestServer(t, newTestConfig())
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// TestUnknownPathReturns404 guards against a typo'd URL silently rendering
// the viewer, which would be confusing rather than obviously wrong.
func TestUnknownPathReturns404(t *testing.T) {
	base, _, _ := startTestServer(t, newTestConfig())
	resp, err := http.Get(base + "/this-route-does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestAPIStatsReturnsSnapshotJSON is what the admin page polls once a second;
// it must decode straight into the Snapshot shape.
func TestAPIStatsReturnsSnapshotJSON(t *testing.T) {
	base, _, _ := startTestServer(t, newTestConfig())
	resp, err := http.Get(base + "/api/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var snap metrics.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Version != "test-version" {
		t.Errorf("version = %q, want %q", snap.Version, "test-version")
	}
}

// TestAPIStatsCarriesAutoPauseFields covers the two new /api/stats fields
// auto-pause needs: pauses_total (how many times the link auto-paused) and
// paused_sec (total time spent paused, including a pause in progress), plus
// "paused" as a valid stt.state value. All three ride the same Snapshot the
// admin page and status line read from, so if they decode here they're
// available everywhere.
func TestAPIStatsCarriesAutoPauseFields(t *testing.T) {
	cfg := newTestConfig()
	base, _, m := startTestServer(t, cfg)

	m.SetSTTState(metrics.StatePaused)
	m.STTPauseBegin()

	resp, err := http.Get(base + "/api/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var snap metrics.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if snap.STT.State != "paused" {
		t.Errorf("stt.state = %q, want %q", snap.STT.State, "paused")
	}
	if snap.STT.Pauses != 1 {
		t.Errorf("stt.pauses_total = %d, want 1", snap.STT.Pauses)
	}
	// The pause begun above is still open (no matching STTPauseEnd), so
	// paused_sec must already reflect it rather than staying at zero until
	// the pause closes.
	if snap.STT.PausedSec <= 0 {
		t.Errorf("stt.paused_sec = %v, want > 0 for a pause in progress", snap.STT.PausedSec)
	}
}

// TestEventsRelaysPausedStatus proves the SSE contract for auto-pause: a
// "paused" status published to the hub — as internal/cli/run.go's
// metrics-state hook now does for real, per Hub.PublishStatus's doc comment
// on what wires into it — must reach a connected subscriber as a "status"
// event with state "paused" and, per the contract, an empty detail: wording
// ("no audio" / "paused (no audio)") is each page's job, not the server's.
func TestEventsRelaysPausedStatus(t *testing.T) {
	cfg := newTestConfig()
	base, hub, _ := startTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	nextSSEData(t, r) // discard the initial snapshot

	hub.PublishStatus("paused", "")

	data := nextSSEData(t, r)
	var ev caption.Event
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		t.Fatalf("status event is not valid JSON: %v (%q)", err, data)
	}
	if ev.Kind != caption.KindStatus {
		t.Fatalf("event kind = %q, want %q", ev.Kind, caption.KindStatus)
	}
	if ev.State != "paused" {
		t.Errorf("event state = %q, want %q", ev.State, "paused")
	}
	if ev.Detail != "" {
		t.Errorf("event detail = %q, want empty — the page decides the wording", ev.Detail)
	}
}

// TestEventsSnapshotReplaysLastStatus is the actual regression guard for the
// hub-memory bug: publish "paused" BEFORE any client connects, then confirm
// the very first event a new subscriber receives — the snapshot — already
// carries state "paused". TestEventsRelaysPausedStatus above connects first
// and publishes second, so it cannot catch a client that missed the edge
// (e.g. a page loaded mid-pause, or an EventSource that reconnected mid-pause
// after a phone's screen slept).
func TestEventsSnapshotReplaysLastStatus(t *testing.T) {
	cfg := newTestConfig()
	base, hub, _ := startTestServer(t, cfg)

	hub.PublishStatus("paused", "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)

	data := nextSSEData(t, r)
	var ev caption.Event
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		t.Fatalf("snapshot event is not valid JSON: %v (%q)", err, data)
	}
	if ev.Kind != caption.KindSnapshot {
		t.Fatalf("event kind = %q, want %q", ev.Kind, caption.KindSnapshot)
	}
	if ev.State != "paused" {
		t.Errorf("snapshot state = %q, want %q — a late joiner must learn the current state from the snapshot, not wait for a change that may never come", ev.State, "paused")
	}
}

// TestEventsSnapshotOmitsStateWhenNonePublished confirms the replay in
// Subscribe doesn't change the wire format for the common case: a client
// connecting before any status has ever been published still gets a
// snapshot with no "state" field at all (omitempty), not state: "".
func TestEventsSnapshotOmitsStateWhenNonePublished(t *testing.T) {
	base, _, _ := startTestServer(t, newTestConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)

	data := nextSSEData(t, r)
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		t.Fatalf("snapshot event is not valid JSON: %v (%q)", err, data)
	}
	if _, present := raw["state"]; present {
		t.Errorf("snapshot JSON has a %q key when no status was ever published, want it omitted entirely: %q", "state", data)
	}
}

// TestEventsHeadersAndSnapshotFirst covers the SSE contract every viewer
// relies on: the right headers to survive proxies, and a snapshot as the
// very first event so a page load is never blank.
func TestEventsHeadersAndSnapshotFirst(t *testing.T) {
	cfg := newTestConfig()
	// Seed history before any client connects, so the snapshot has to carry
	// something for a late joiner.
	cfg.Hub.Publish(stt.Transcript{Text: "already said", IsFinal: true, SpeechFinal: true})
	base, _, _ := startTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache", cc)
	}
	if xa := resp.Header.Get("X-Accel-Buffering"); xa != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", xa)
	}

	data := nextSSEData(t, bufio.NewReader(resp.Body))
	var ev caption.Event
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		t.Fatalf("first event is not valid JSON: %v (%q)", err, data)
	}
	if ev.Kind != caption.KindSnapshot {
		t.Fatalf("first event kind = %q, want snapshot", ev.Kind)
	}
	if len(ev.Lines) != 1 || ev.Lines[0].Text != "already said" {
		t.Errorf("snapshot lines = %v, want the seeded history", ev.Lines)
	}
}

// TestEventsDeliversPublishedEvents proves the stream isn't just the initial
// snapshot: events published after connecting must reach the client too.
func TestEventsDeliversPublishedEvents(t *testing.T) {
	cfg := newTestConfig()
	base, hub, _ := startTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	nextSSEData(t, r) // discard the initial snapshot

	hub.Publish(stt.Transcript{Text: "live line", IsFinal: true, SpeechFinal: true})

	data := nextSSEData(t, r)
	var ev caption.Event
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		t.Fatalf("published event is not valid JSON: %v (%q)", err, data)
	}
	if ev.Kind != caption.KindFinal || ev.Text != "live line" {
		t.Errorf("event = %+v, want a final event with text %q", ev, "live line")
	}
}

// TestClientDisconnectCleansUpSubscription is the fan-out half of "nothing
// downstream may block the pipeline": a browser that goes away must not
// leak its subscriber slot or leave the SSE client gauge stuck non-zero.
func TestClientDisconnectCleansUpSubscription(t *testing.T) {
	cfg := newTestConfig()
	base, _, m := startTestServer(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(resp.Body)
	nextSSEData(t, r) // wait for the subscription to actually be established

	if got := m.SSEClients(); got != 1 {
		t.Fatalf("sse_clients = %d after connect, want 1", got)
	}

	resp.Body.Close()
	cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m.SSEClients() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("sse_clients = %d after disconnect, want 0", m.SSEClients())
}

// TestLogoUnsetReturns404 makes sure an unconfigured logo falls through to
// pageHandler's catch-all 404 rather than serving something unexpected.
func TestLogoUnsetReturns404(t *testing.T) {
	base, _, _ := startTestServer(t, newTestConfig())
	resp, err := http.Get(base + "/logo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestLogoServedWithETagAndConditionalRequest covers the whole contract: a
// configured logo is served with the right Content-Type and a stable ETag,
// and a follow-up conditional request gets 304 rather than the body again.
func TestLogoServedWithETagAndConditionalRequest(t *testing.T) {
	cfg := newTestConfig()
	cfg.Logo = "testdata/logo.png"
	base, _, _ := startTestServer(t, cfg)

	resp, err := http.Get(base + "/logo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/png" {
		t.Errorf("Content-Type = %q, want image/png", ct)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("ETag header is empty, want a value")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) == 0 {
		t.Error("logo body is empty, want image bytes")
	}

	req, _ := http.NewRequest(http.MethodGet, base+"/logo", nil)
	req.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Errorf("conditional status = %d, want 304", resp2.StatusCode)
	}
}

// TestAPIConfigReportsLogoState covers both sides of the logo contract the
// viewer relies on: "" unset, "/logo" set.
func TestAPIConfigReportsLogoState(t *testing.T) {
	base, _, _ := startTestServer(t, newTestConfig())
	resp, err := http.Get(base + "/api/config")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["logo"] != "" {
		t.Errorf(`logo = %v, want ""`, got["logo"])
	}

	cfg := newTestConfig()
	cfg.Logo = "testdata/logo.png"
	base2, _, _ := startTestServer(t, cfg)
	resp2, err := http.Get(base2 + "/api/config")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var got2 map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&got2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got2["logo"] != "/logo" {
		t.Errorf(`logo = %v, want "/logo"`, got2["logo"])
	}
}

// TestWakeVideoServedWithContentType covers Backend B's asset route: the
// silent looping video that keeps a phone screen on over plain HTTP must be
// reachable at a fixed URL with the right Content-Type so it can autoplay.
func TestWakeVideoServedWithContentType(t *testing.T) {
	base, _, _ := startTestServer(t, newTestConfig())
	resp, err := http.Get(base + "/wake.mp4")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "video/mp4" {
		t.Errorf("Content-Type = %q, want video/mp4", ct)
	}
}

// nextSSEData reads one "data: ..." payload from an SSE stream, skipping the
// retry directive and ping comments that aren't a message.
func nextSSEData(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	var data []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF && len(data) > 0 {
				return strings.Join(data, "\n")
			}
			t.Fatalf("reading SSE stream: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if len(data) > 0 {
				return strings.Join(data, "\n")
			}
			continue
		}
		if after, ok := strings.CutPrefix(line, "data: "); ok {
			data = append(data, after)
		}
	}
}
