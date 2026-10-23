package loader

import (
	"testing"
	"time"
)

func TestDiagnosticsConfigDefaults(t *testing.T) {
	cf, err := LoadServerConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	c := cf.Diagnostics
	if c.Enabled || c.Address != "127.0.0.1:6060" || c.SnapshotDir != "" || c.Interval != 15*time.Second || c.Cooldown != 5*time.Minute || c.GoroutineThreshold != 5000 || c.MemoryThreshold != 1<<30 || c.MaxSnapshots != 4 {
		t.Fatalf("unexpected diagnostics defaults: %+v", c)
	}
}

func TestDiagnosticsConfigEnvironment(t *testing.T) {
	dir := t.TempDir()
	for k, v := range map[string]string{
		"SERVER_DIAGNOSTICS_ENABLED": "true", "SERVER_DIAGNOSTICS_ADDRESS": "127.0.0.1:6061",
		"SERVER_DIAGNOSTICS_SNAPSHOT_DIR": dir, "SERVER_DIAGNOSTICS_INTERVAL": "20s",
		"SERVER_DIAGNOSTICS_COOLDOWN": "10m", "SERVER_DIAGNOSTICS_GOROUTINE_THRESHOLD": "7000",
		"SERVER_DIAGNOSTICS_MEMORY_THRESHOLD": "2147483648", "SERVER_DIAGNOSTICS_MAX_SNAPSHOTS": "3",
	} {
		t.Setenv(k, v)
	}
	cf, err := LoadServerConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	c := cf.Diagnostics
	if !c.Enabled || c.Address != "127.0.0.1:6061" || c.SnapshotDir != dir || c.Interval != 20*time.Second || c.Cooldown != 10*time.Minute || c.GoroutineThreshold != 7000 || c.MemoryThreshold != 2<<30 || c.MaxSnapshots != 3 {
		t.Fatalf("environment did not configure diagnostics: %+v", c)
	}
}
