package diagnostics

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/config/shared"
	"github.com/rs/zerolog"
)

func testConfig(t *testing.T) shared.DiagnosticsConfigFile {
	t.Helper()
	return shared.DiagnosticsConfigFile{
		Enabled: true, Address: "127.0.0.1:0", SnapshotDir: t.TempDir(),
		Interval: time.Second, Cooldown: time.Minute, MaxSnapshots: 2,
		GoroutineThreshold: 5000, MemoryThreshold: 1 << 30,
	}
}

func TestDisabledDiagnosticsHasNoSideEffects(t *testing.T) {
	logger := zerolog.Nop()
	config := shared.DiagnosticsConfigFile{SnapshotDir: filepath.Join(t.TempDir(), "unused"), Address: "invalid"}
	stop, err := Start(context.Background(), config, &logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(config.SnapshotDir); !os.IsNotExist(err) {
		t.Fatalf("disabled diagnostics created a directory: %v", err)
	}
}

func TestDiagnosticsRejectsNonLoopback(t *testing.T) {
	logger := zerolog.Nop()
	for _, address := range []string{":6060", "0.0.0.0:6060", "192.0.2.1:6060", "[::]:6060", "localhost:6060"} {
		t.Run(address, func(t *testing.T) {
			config := testConfig(t)
			config.Address = address
			if _, err := Start(context.Background(), config, &logger); err == nil {
				t.Fatal("accepted non-loopback IP address")
			}
		})
	}
}

func TestDiagnosticsListenerAndMonitorStop(t *testing.T) {
	logger := zerolog.Nop()
	config := testConfig(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config.Address = listener.Addr().String()
	_ = listener.Close()
	stop, err := Start(context.Background(), config, &logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop() })
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + config.Address + "/debug/memory")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var sample memorySample
	if err := json.NewDecoder(response.Body).Decode(&sample); err != nil || sample.Goroutines == 0 || sample.GoMemory == 0 {
		t.Fatalf("invalid live memory sample: %+v, %v", sample, err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if conn, err := net.DialTimeout("tcp", config.Address, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("diagnostics listener remained open")
	}
}

func TestProfileRoutesExposeStacksAndEnforceCaptureBounds(t *testing.T) {
	m := &monitor{}
	handler := m.handler()
	blocked := make(chan struct{})
	var started sync.WaitGroup
	started.Add(12)
	for range 12 {
		go func() { started.Done(); <-blocked }()
	}
	started.Wait()
	defer close(blocked)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/debug/pprof/goroutine?debug=1", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "diagnostics.TestProfileRoutesExposeStacksAndEnforceCaptureBounds.func") {
		t.Fatalf("blocked goroutine stack is missing: %d %s", response.Code, response.Body.String())
	}
	for _, route := range []string{"heap", "goroutine", "allocs", "threadcreate"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/debug/pprof/"+route, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("profile %s returned %d", route, response.Code)
		}
		assertGzipProfile(t, response.Body.Bytes())
	}
	for _, test := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/debug/pprof/heap?gc=1", http.StatusBadRequest},
		{http.MethodGet, "/debug/pprof/heap?seconds=60", http.StatusBadRequest},
		{http.MethodPost, "/debug/pprof/heap", http.StatusMethodNotAllowed},
		{http.MethodGet, "/debug/pprof/profile", http.StatusNotFound},
		{http.MethodGet, "/debug/pprof/cmdline", http.StatusNotFound},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		if response.Code != test.status {
			t.Errorf("%s %s: got %d, want %d", test.method, test.path, response.Code, test.status)
		}
	}
	m.profileMu.Lock()
	defer m.profileMu.Unlock()
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/debug/pprof/heap", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("parallel capture was not rejected: %d", response.Code)
	}
}

func TestAutomaticSnapshotsThresholdCooldownAndRetention(t *testing.T) {
	var logs bytes.Buffer
	logger := zerolog.New(&logs)
	config := testConfig(t)
	m := &monitor{config: config, logger: &logger}
	start := time.Now().UTC()
	sample := memorySample{Time: start, Goroutines: 100, GoMemory: 20 << 20}
	m.observe(sample)
	assertSnapshots(t, config.SnapshotDir, 1, "baseline")
	sample.Time = start.Add(30 * time.Second)
	sample.Goroutines = 6000
	m.observe(sample)
	assertSnapshots(t, config.SnapshotDir, 1, "baseline")
	sample.Time = start.Add(time.Minute)
	m.observe(sample)
	assertSnapshots(t, config.SnapshotDir, 2, "goroutine_threshold")
	sample.Time = start.Add(2 * time.Minute)
	sample.Goroutines = 100
	sample.GoMemory = 2 << 30
	m.observe(sample)
	assertSnapshots(t, config.SnapshotDir, 2, "memory_threshold")
	if !strings.Contains(logs.String(), "goroutine_summary") || !strings.Contains(logs.String(), "memory_threshold") {
		t.Fatal("capture did not emit stack evidence to the logger")
	}
	// A restarted process must count profiles already present on the volume.
	m = &monitor{config: config, logger: &logger}
	sample.Time = start.Add(3 * time.Minute)
	m.observe(sample)
	assertSnapshots(t, config.SnapshotDir, 2, "baseline")
}

func TestCaptureFailureBacksOffAndWritersAreBounded(t *testing.T) {
	logger := zerolog.Nop()
	config := testConfig(t)
	config.SnapshotDir = filepath.Join(config.SnapshotDir, "missing")
	m := &monitor{config: config, logger: &logger}
	sample := memorySample{Time: time.Now().UTC(), Goroutines: 6000}
	m.observe(sample)
	if !m.lastAttempt.Equal(sample.Time) {
		t.Fatal("failed capture did not advance cooldown")
	}
	var output bytes.Buffer
	w := &limitedWriter{writer: &output, remaining: 4}
	if _, err := w.Write([]byte("12345")); err == nil || output.Len() != 0 {
		t.Fatal("oversized profile write was not rejected")
	}
	preview := &previewWriter{remaining: 4}
	if n, err := preview.Write([]byte("12345")); n != 5 || err != nil || preview.String() != "1234" || !preview.truncated {
		t.Fatal("log preview is not bounded")
	}
}

func assertGzipProfile(t *testing.T, data []byte) {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	decoded, err := io.ReadAll(r)
	if err != nil || len(decoded) == 0 {
		t.Fatalf("empty or unreadable profile: %v", err)
	}
}

func assertSnapshots(t *testing.T, dir string, count int, latestReason string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != count {
		t.Fatalf("snapshot count: got %d, want %d, err %v", len(entries), count, err)
	}
	for _, entry := range entries {
		for _, name := range []string{"goroutine", "heap"} {
			data, err := os.ReadFile(filepath.Join(dir, entry.Name(), name+".pprof"))
			if err != nil {
				t.Fatal(err)
			}
			assertGzipProfile(t, data)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, entries[len(entries)-1].Name(), "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sample memorySample
	if err := json.Unmarshal(data, &sample); err != nil || sample.Reason != latestReason {
		t.Fatalf("unexpected snapshot metadata: %+v, %v", sample, err)
	}
}
