package caption

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"livecaption/internal/audio"
	"livecaption/internal/metrics"
)

// Writer records finalized captions to disk. Recording is on by default for
// every session — it is the expected behaviour, not something to remember to
// switch on — so the only configuration is where the files go.
//
// Two files per session, because they serve different readers:
//   - transcript.txt   human readable, "[00:12:34] text"
//   - transcript.jsonl one record per line with offsets and confidence
//
// Both are opened O_APPEND and flushed periodically, so a crash keeps
// everything already written.
type Writer struct {
	dir string

	mu      sync.Mutex
	txt     *os.File
	jsonl   *os.File
	txtBuf  *bufio.Writer
	jsonBuf *bufio.Writer
	closed  bool

	metrics *metrics.Metrics
	done    chan struct{}
}

// NewWriter creates <baseDir>/<RFC3339 session start>/ and opens both files.
func NewWriter(baseDir string, started time.Time, m *metrics.Metrics) (*Writer, error) {
	// Colons are legal on Linux but hostile in filenames, so the timestamp
	// uses dashes: 2026-08-19T09-31-05.
	name := started.Format("2006-01-02T15-04-05")
	dir := filepath.Join(baseDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create transcript dir: %w", err)
	}

	open := func(base string) (*os.File, error) {
		return os.OpenFile(filepath.Join(dir, base), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	}
	txt, err := open("transcript.txt")
	if err != nil {
		return nil, fmt.Errorf("open transcript.txt: %w", err)
	}
	jsonl, err := open("transcript.jsonl")
	if err != nil {
		txt.Close()
		return nil, fmt.Errorf("open transcript.jsonl: %w", err)
	}

	w := &Writer{
		dir:     dir,
		txt:     txt,
		jsonl:   jsonl,
		txtBuf:  bufio.NewWriter(txt),
		jsonBuf: bufio.NewWriter(jsonl),
		metrics: m,
		done:    make(chan struct{}),
	}
	if m != nil {
		m.TranscriptPath = filepath.Join(dir, "transcript.txt")
	}

	// Periodic flush bounds how much a crash can cost to a couple of seconds.
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				w.flush()
			case <-w.done:
				return
			}
		}
	}()
	return w, nil
}

// Dir is the session directory, for the banner and summary.
func (w *Writer) Dir() string { return w.dir }

type record struct {
	ID       string    `json:"id"`
	Text     string    `json:"text"`
	OffsetMS int64     `json:"offset_ms"`
	Clock    string    `json:"clock"`
	At       time.Time `json:"at"`
}

// Write records one finalized line. Write errors are surfaced as a metric
// rather than returned: losing the transcript must not end a live event.
func (w *Writer) Write(l Line) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}

	clock := audio.FormatClock(time.Duration(l.OffsetMS) * time.Millisecond)
	n, err := fmt.Fprintf(w.txtBuf, "[%s] %s\n", clock, l.Text)
	if err != nil {
		w.fail(err)
		return
	}

	buf, err := json.Marshal(record{
		ID: l.ID, Text: l.Text, OffsetMS: l.OffsetMS, Clock: clock, At: l.At,
	})
	if err != nil {
		w.fail(err)
		return
	}
	buf = append(buf, '\n')
	if _, err := w.jsonBuf.Write(buf); err != nil {
		w.fail(err)
		return
	}

	if w.metrics != nil {
		w.metrics.TranscriptWrote(1, n)
	}
}

func (w *Writer) fail(err error) {
	if w.metrics != nil {
		w.metrics.SetTranscriptError(err)
	}
}

func (w *Writer) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	if err := w.txtBuf.Flush(); err != nil {
		w.fail(err)
	}
	if err := w.jsonBuf.Flush(); err != nil {
		w.fail(err)
	}
}

func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.done)
	err1 := w.txtBuf.Flush()
	err2 := w.jsonBuf.Flush()
	err3 := w.txt.Close()
	err4 := w.jsonl.Close()
	w.mu.Unlock()

	for _, err := range []error{err1, err2, err3, err4} {
		if err != nil {
			return err
		}
	}
	return nil
}
