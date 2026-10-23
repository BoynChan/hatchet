package diagnostics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/config/shared"
	"github.com/rs/zerolog"
)

const maxProfileBytes = 32 << 20

type memorySample struct {
	Time       time.Time `json:"time"`
	Reason     string    `json:"reason,omitempty"`
	Goroutines int       `json:"goroutines"`
	HeapAlloc  uint64    `json:"heap_alloc_bytes"`
	HeapInuse  uint64    `json:"heap_inuse_bytes"`
	StackInuse uint64    `json:"stack_inuse_bytes"`
	GoMemory   uint64    `json:"go_memory_bytes"`
	NumGC      uint32    `json:"num_gc"`
}

func sampleMemory() memorySample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return memorySample{
		Time: time.Now().UTC(), Goroutines: runtime.NumGoroutine(),
		HeapAlloc: m.HeapAlloc, HeapInuse: m.HeapInuse, StackInuse: m.StackInuse,
		GoMemory: m.Sys - m.HeapReleased, NumGC: m.NumGC,
	}
}

type monitor struct {
	config      shared.DiagnosticsConfigFile
	logger      *zerolog.Logger
	profileMu   sync.Mutex
	lastAttempt time.Time
}

// Start serves instantaneous profiles on loopback and optionally captures a
// baseline plus bounded snapshots when runtime memory or goroutines grow.
func Start(ctx context.Context, config shared.DiagnosticsConfigFile, logger *zerolog.Logger) (func() error, error) {
	if !config.Enabled {
		return func() error { return nil }, nil
	}
	host, _, err := net.SplitHostPort(config.Address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return nil, fmt.Errorf("diagnostics address must be a loopback IP and port: %q", config.Address)
	}
	if config.SnapshotDir != "" {
		if config.Interval < time.Second || config.Cooldown < config.Interval || config.MaxSnapshots < 1 || config.MaxSnapshots > 16 {
			return nil, errors.New("diagnostics requires interval >= 1s, cooldown >= interval, and 1-16 snapshots")
		}
		if config.GoroutineThreshold < 0 || (config.GoroutineThreshold == 0 && config.MemoryThreshold == 0) {
			return nil, errors.New("diagnostics requires at least one positive capture threshold")
		}
		if err := os.MkdirAll(config.SnapshotDir, 0o700); err != nil {
			return nil, fmt.Errorf("create diagnostics directory: %w", err)
		}
	}
	listener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return nil, fmt.Errorf("listen for diagnostics: %w", err)
	}
	m := &monitor{config: config, logger: logger}
	srv := &http.Server{
		Handler: m.handler(), ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second,
	}
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("diagnostics listener stopped")
		}
	}()
	monitorCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if config.SnapshotDir == "" {
			return
		}
		ticker := time.NewTicker(config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
				m.observe(sampleMemory())
			}
		}
	}()
	logger.Info().Str("address", listener.Addr().String()).Msg("memory diagnostics started")
	return func() error {
		cancel()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		err := srv.Shutdown(shutdownCtx)
		select {
		case <-done:
			return err
		case <-shutdownCtx.Done():
			return errors.Join(err, shutdownCtx.Err())
		}
	}, nil
}

func (m *monitor) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/memory", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sampleMemory())
	})
	for _, name := range []string{"heap", "allocs", "goroutine", "threadcreate"} {
		profile := httppprof.Handler(name)
		mux.HandleFunc("GET /debug/pprof/"+name, func(w http.ResponseWriter, r *http.Request) {
			// Instantaneous snapshots do not force GC or run a timed profiler.
			if r.URL.Query().Get("seconds") != "" || r.URL.Query().Get("gc") != "" {
				http.Error(w, "only instantaneous profiles without forced GC are supported", http.StatusBadRequest)
				return
			}
			if !m.profileMu.TryLock() {
				http.Error(w, "profile capture already in progress", http.StatusServiceUnavailable)
				return
			}
			defer m.profileMu.Unlock()
			profile.ServeHTTP(w, r)
		})
	}
	return mux
}

func (m *monitor) observe(sample memorySample) {
	if m.lastAttempt.IsZero() {
		sample.Reason = "baseline"
	} else if sample.Time.Sub(m.lastAttempt) < m.config.Cooldown {
		return
	} else if m.config.GoroutineThreshold > 0 && sample.Goroutines >= m.config.GoroutineThreshold {
		sample.Reason = "goroutine_threshold"
	} else if m.config.MemoryThreshold > 0 && sample.GoMemory >= m.config.MemoryThreshold {
		sample.Reason = "memory_threshold"
	} else {
		return
	}
	if !m.profileMu.TryLock() {
		return
	}
	defer m.profileMu.Unlock()
	// Failed writes also back off, so a full filesystem cannot cause a tight
	// loop of expensive profile collection.
	m.lastAttempt = sample.Time
	dir, err := m.capture(sample)
	if err != nil {
		m.logger.Error().Err(err).Str("reason", sample.Reason).Msg("memory diagnostic capture failed")
		return
	}
	level := zerolog.InfoLevel
	if sample.Reason != "baseline" {
		level = zerolog.WarnLevel
	}
	// A bounded stack summary reaches the existing log collector even if the
	// Pod is replaced before its on-disk profiles are retrieved.
	preview := &previewWriter{remaining: 8 << 10}
	_ = pprof.Lookup("goroutine").WriteTo(preview, 1)
	m.logger.WithLevel(level).Str("snapshot_dir", dir).Str("reason", sample.Reason).
		Int("goroutines", sample.Goroutines).Uint64("go_memory_bytes", sample.GoMemory).
		Uint64("heap_alloc_bytes", sample.HeapAlloc).Uint64("stack_inuse_bytes", sample.StackInuse).
		Str("goroutine_summary", preview.String()).Bool("summary_truncated", preview.truncated).
		Msg("memory diagnostic snapshot captured")
}

func (m *monitor) capture(sample memorySample) (dir string, err error) {
	entries, err := os.ReadDir(m.config.SnapshotDir)
	if err != nil {
		return "", err
	}
	var snapshots []string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "capture-") {
			snapshots = append(snapshots, entry.Name())
		}
	}
	sort.Strings(snapshots)
	for len(snapshots) >= m.config.MaxSnapshots {
		if err := os.RemoveAll(filepath.Join(m.config.SnapshotDir, snapshots[0])); err != nil {
			return "", err
		}
		snapshots = snapshots[1:]
	}
	dir, err = os.MkdirTemp(m.config.SnapshotDir, "capture-"+sample.Time.UTC().Format("20060102T150405.000000000Z")+"-")
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dir)
		}
	}()
	for _, name := range []string{"goroutine", "heap"} {
		if err = writeProfile(filepath.Join(dir, name+".pprof"), name); err != nil {
			return dir, err
		}
	}
	metadata, err := json.MarshalIndent(sample, "", "  ")
	if err != nil {
		return dir, err
	}
	err = os.WriteFile(filepath.Join(dir, "metadata.json"), metadata, 0o600)
	return dir, err
}

func writeProfile(path, name string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	err = pprof.Lookup(name).WriteTo(&limitedWriter{writer: f, remaining: maxProfileBytes}, 0)
	return errors.Join(err, f.Close())
}

type limitedWriter struct {
	writer    io.Writer
	remaining int
}

func (w *limitedWriter) Write(b []byte) (int, error) {
	if len(b) > w.remaining {
		return 0, errors.New("diagnostic profile exceeds size limit")
	}
	n, err := w.writer.Write(b)
	w.remaining -= n
	return n, err
}

type previewWriter struct {
	bytes.Buffer
	remaining int
	truncated bool
}

func (w *previewWriter) Write(b []byte) (int, error) {
	n := min(len(b), w.remaining)
	_, _ = w.Buffer.Write(b[:n])
	w.remaining -= n
	w.truncated = w.truncated || n < len(b)
	return len(b), nil
}
