// Package web serves the caption viewer, the admin metrics page and the SSE
// stream that drives them.
package web

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"livecaption/internal/caption"
	"livecaption/internal/metrics"
)

//go:embed static
var embedded embed.FS

// maxLogoBytes keeps a stray high-res logo from bloating server memory and
// every subsequent response; 2 MiB is generous for a corner-of-screen image.
const maxLogoBytes = 2 << 20

// Config configures the caption server.
type Config struct {
	Addr      string
	Lines     int    // caption rows the viewer shows by default
	Logo      string // path to an image shown in the viewer's top-right corner
	Hub       *caption.Hub
	Metrics   *metrics.Metrics
	Log       *slog.Logger
	DevStatic string // serve assets from disk instead of the embedded copy
}

// Server owns the HTTP surface.
type Server struct {
	cfg     Config
	http    *http.Server
	log     *slog.Logger
	logoSet bool
}

func NewServer(cfg Config) (*Server, error) {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Lines <= 0 {
		cfg.Lines = 3
	}

	var static fs.FS
	if cfg.DevStatic != "" {
		static = os.DirFS(cfg.DevStatic)
	} else {
		sub, err := fs.Sub(embedded, "static")
		if err != nil {
			return nil, fmt.Errorf("embedded assets: %w", err)
		}
		static = sub
	}

	s := &Server{cfg: cfg, log: cfg.Log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("GET /api/config", s.handleConfig)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.Handle("GET /admin", pageHandler(static, "admin.html"))
	mux.Handle("GET /", pageHandler(static, "index.html"))

	if cfg.Logo != "" {
		handler, err := logoHandler(cfg.Logo)
		if err != nil {
			return nil, err
		}
		mux.Handle("GET /logo", handler)
		s.logoSet = true
	}

	s.http = &http.Server{
		Handler: mux,
		// No write timeout: SSE connections are meant to stay open for the
		// length of an event.
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s, nil
}

// pageHandler serves one HTML file, falling through to 404 for unknown paths
// so a typo doesn't silently render the viewer.
func pageHandler(static fs.FS, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/admin" {
			http.NotFound(w, r)
			return
		}
		body, err := fs.ReadFile(static, name)
		if err != nil {
			http.Error(w, "asset not found: "+name, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Write(body)
	})
}

// logoHandler reads the logo once at startup — not per request — so a file
// swapped mid-session has no effect; that trade-off buys a static ETag and
// zero disk I/O on the hot path.
func logoHandler(path string) (http.Handler, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read logo %s: %w", path, err)
	}
	if len(body) > maxLogoBytes {
		return nil, fmt.Errorf("logo %s is %d bytes, exceeds %d byte limit", path, len(body), maxLogoBytes)
	}

	ctype := logoContentType(path, body)
	sum := sha256.Sum256(body)
	etag := `"` + hex.EncodeToString(sum[:])[:16] + `"`

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", ctype)
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("ETag", etag)
		// A fresh Reader per request: bytes.Reader carries a read position,
		// so sharing one across concurrent requests would race.
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(body))
	}), nil
}

// logoContentType prefers the file extension over sniffing, since a
// hand-picked logo is far more likely to have a correct extension than the
// magic-byte heuristics in http.DetectContentType are to guess right for it.
func logoContentType(path string, body []byte) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".webp":
		return "image/webp"
	case ".gif":
		return "image/gif"
	case ".svg":
		return "image/svg+xml"
	default:
		return http.DetectContentType(body)
	}
}

// Listen binds the address up front, so "port already in use" is reported
// before the banner claims the server is ready.
func (s *Server) Listen() (net.Listener, error) {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", s.cfg.Addr, err)
	}
	return ln, nil
}

func (s *Server) Serve(ln net.Listener) error { return s.http.Serve(ln) }

func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

// handleEvents streams caption events over Server-Sent Events.
//
// SSE rather than WebSocket because the traffic is strictly one-way and
// EventSource reconnects on its own — a browser that drops gets a fresh
// snapshot with no client-side reconnect logic to maintain.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	// Tell nginx and friends not to buffer, which would defeat the point.
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	fmt.Fprint(w, "retry: 3000\n\n")
	flusher.Flush()

	events, unsubscribe := s.cfg.Hub.Subscribe()
	defer unsubscribe()

	s.log.Debug("viewer connected", "remote", r.RemoteAddr)
	defer s.log.Debug("viewer disconnected", "remote", r.RemoteAddr)

	// Comment-only heartbeat so idle proxies don't close the connection.
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()

	ctx := r.Context()
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			buf, err := json.Marshal(ev)
			if err != nil {
				s.log.Debug("encode event", "err", err)
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", buf); err != nil {
				return
			}
			flusher.Flush()
		case <-ping.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

// handleStats returns the metrics snapshot the admin page polls.
func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	snap := s.cfg.Metrics.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(snap)
}

// handleConfig exposes the few server-side defaults the viewer needs, so the
// --lines flag reaches the page without templating the HTML.
func (s *Server) handleConfig(w http.ResponseWriter, _ *http.Request) {
	logo := ""
	if s.logoSet {
		logo = "/logo"
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"lines":   s.cfg.Lines,
		"version": s.cfg.Metrics.Version,
		"logo":    logo,
	})
}
