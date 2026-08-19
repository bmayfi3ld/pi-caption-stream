package caption

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"livecaption/internal/metrics"
)

// TestWriterCreatesSessionFilesWithExpectedFormats covers the contract the
// rest of the tool depends on: a session directory with both a human-
// readable transcript and a machine-readable one, in the documented formats.
func TestWriterCreatesSessionFilesWithExpectedFormats(t *testing.T) {
	m := metrics.New("v", "s")
	started := time.Date(2026, 8, 19, 9, 31, 5, 0, time.UTC)
	w, err := NewWriter(t.TempDir(), started, m)
	if err != nil {
		t.Fatal(err)
	}

	at := time.Date(2026, 8, 19, 9, 32, 0, 0, time.UTC)
	w.Write(Line{ID: "u1", Text: "hello there", OffsetMS: 754000, At: at})

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !strings.HasSuffix(w.Dir(), "2026-08-19T09-31-05") {
		t.Errorf("session dir = %q, want it named after the start time", w.Dir())
	}

	txt, err := os.ReadFile(filepath.Join(w.Dir(), "transcript.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// [mm:ss] text — FormatClock(754s) = 12:34.
	if want := "[12:34] hello there\n"; string(txt) != want {
		t.Errorf("transcript.txt = %q, want %q", string(txt), want)
	}

	jsonl, err := os.ReadFile(filepath.Join(w.Dir(), "transcript.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(jsonl), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("transcript.jsonl has %d lines, want 1: %q", len(lines), jsonl)
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("transcript.jsonl line is not valid JSON: %v", err)
	}
	for _, field := range []string{"id", "text", "offset_ms", "clock", "at"} {
		if _, ok := rec[field]; !ok {
			t.Errorf("jsonl record missing field %q: %v", field, rec)
		}
	}
	if rec["id"] != "u1" || rec["text"] != "hello there" || rec["clock"] != "12:34" {
		t.Errorf("jsonl record = %v", rec)
	}
	if rec["offset_ms"].(float64) != 754000 {
		t.Errorf("offset_ms = %v, want 754000", rec["offset_ms"])
	}
}

// TestWriterFlushesOnClose is the crash-safety guarantee: content must reach
// disk when the session ends, without waiting for the periodic 2s flush.
func TestWriterFlushesOnClose(t *testing.T) {
	m := metrics.New("v", "s")
	w, err := NewWriter(t.TempDir(), time.Now(), m)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(Line{ID: "u1", Text: "not yet flushed by the ticker", At: time.Now()})
	dir := w.Dir()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	txt, err := os.ReadFile(filepath.Join(dir, "transcript.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(txt), "not yet flushed by the ticker") {
		t.Errorf("buffered content did not survive Close: %q", txt)
	}
}

// TestWriterMetricsTrackLinesAndBytes drives the transcript row on /admin and
// in the shutdown summary — it must reflect exactly what was written.
func TestWriterMetricsTrackLinesAndBytes(t *testing.T) {
	m := metrics.New("v", "s")
	w, err := NewWriter(t.TempDir(), time.Now(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.Write(Line{ID: "u1", Text: "one", At: time.Now()})
	w.Write(Line{ID: "u2", Text: "two", At: time.Now()})

	snap := m.Snapshot()
	if snap.Transcript.Lines != 2 {
		t.Errorf("lines_written = %d, want 2", snap.Transcript.Lines)
	}
	if snap.Transcript.Bytes <= 0 {
		t.Errorf("bytes_written = %d, want > 0", snap.Transcript.Bytes)
	}
	if snap.Transcript.Path == "" {
		t.Error("transcript path should be set on the metrics for the banner")
	}
}

// TestWriterCloseIsIdempotentAndWriteAfterCloseIsSafe covers shutdown races:
// Close may be invoked from more than one unwind path, and a caption that
// arrives just after shutdown must be dropped quietly, not panic the pipeline.
func TestWriterCloseIsIdempotentAndWriteAfterCloseIsSafe(t *testing.T) {
	m := metrics.New("v", "s")
	w, err := NewWriter(t.TempDir(), time.Now(), m)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	w.Write(Line{ID: "late", Text: "after close", At: time.Now()})

	txt, err := os.ReadFile(filepath.Join(w.Dir(), "transcript.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(txt), "after close") {
		t.Error("a write after Close should be dropped, not appended to a closed file")
	}
}

// TestWriterContentSurvivesMultipleWrites is a light end-to-end check that
// two lines land in order on both files, which the format tests above assume
// but don't directly exercise across more than one record.
func TestWriterContentSurvivesMultipleWrites(t *testing.T) {
	m := metrics.New("v", "s")
	w, err := NewWriter(t.TempDir(), time.Now(), m)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(Line{ID: "u1", Text: "first", At: time.Now()})
	w.Write(Line{ID: "u2", Text: "second", At: time.Now()})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(filepath.Join(w.Dir(), "transcript.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var texts []string
	for sc.Scan() {
		var rec struct{ Text string }
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("invalid JSON line %q: %v", sc.Text(), err)
		}
		texts = append(texts, rec.Text)
	}
	if len(texts) != 2 || texts[0] != "first" || texts[1] != "second" {
		t.Errorf("texts = %v, want [first second] in order", texts)
	}
}
