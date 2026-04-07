package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sneh-joshi/pulsemq/internal/config"
)

func TestDefault_HasSensibleValues(t *testing.T) {
	cfg := config.Default()

	if cfg.Node.Port != 8080 {
		t.Errorf("expected default port 8080, got %d", cfg.Node.Port)
	}
	if cfg.Node.Host != "0.0.0.0" {
		t.Errorf("expected default host 0.0.0.0, got %s", cfg.Node.Host)
	}
	if cfg.Node.DataDir != "./data" {
		t.Errorf("expected default data_dir ./data, got %s", cfg.Node.DataDir)
	}
	if cfg.Queue.MaxBatchSize != 100 {
		t.Errorf("expected default max_batch_size 100, got %d", cfg.Queue.MaxBatchSize)
	}
	if cfg.Queue.MaxMessages != 100_000 {
		t.Errorf("expected default max_messages 100000, got %d", cfg.Queue.MaxMessages)
	}
	if cfg.Storage.Fsync != config.FsyncInterval {
		t.Errorf("expected default fsync interval, got %s", cfg.Storage.Fsync)
	}
	if cfg.Cluster.Enabled {
		t.Error("cluster must be disabled by default")
	}
	if len(cfg.Webhook.RetryDelaysMs) != 3 {
		t.Errorf("expected 3 webhook retry delays, got %d", len(cfg.Webhook.RetryDelaysMs))
	}
}

func TestLoad_MissingFile_ReturnsDefaults(t *testing.T) {
	cfg, err := config.Load("/tmp/pulsemq_nonexistent_config_12345.yaml")
	if err != nil {
		t.Fatalf("expected no error for missing file, got: %v", err)
	}
	if cfg.Node.Port != 8080 {
		t.Errorf("expected default port for missing file, got %d", cfg.Node.Port)
	}
}

func TestLoad_OverridesDefaults(t *testing.T) {
	yaml := `
node:
  port: 9999
  host: "127.0.0.1"
  data_dir: "/tmp/pulsemq_test"
queue:
  max_messages: 500
  max_retries: 5
storage:
  fsync: "always"
`
	path := writeTempYAML(t, yaml)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Node.Port != 9999 {
		t.Errorf("expected port 9999, got %d", cfg.Node.Port)
	}
	if cfg.Node.Host != "127.0.0.1" {
		t.Errorf("expected host 127.0.0.1, got %s", cfg.Node.Host)
	}
	if cfg.Queue.MaxMessages != 500 {
		t.Errorf("expected max_messages 500, got %d", cfg.Queue.MaxMessages)
	}
	if cfg.Queue.MaxRetries != 5 {
		t.Errorf("expected max_retries 5, got %d", cfg.Queue.MaxRetries)
	}
	if cfg.Storage.Fsync != config.FsyncAlways {
		t.Errorf("expected fsync always, got %s", cfg.Storage.Fsync)
	}
	// Unset fields keep their defaults.
	if cfg.Queue.MaxBatchSize != 100 {
		t.Errorf("expected default max_batch_size 100 (unchanged), got %d", cfg.Queue.MaxBatchSize)
	}
}

func TestLoad_InvalidYAML_ReturnsError(t *testing.T) {
	path := writeTempYAML(t, "node: [invalid: yaml: {{{}}")
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		t.Errorf("Default config should be valid, got: %v", err)
	}
}

func TestValidate_InvalidPort(t *testing.T) {
	cfg := config.Default()
	cfg.Node.Port = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for port 0")
	}

	cfg.Node.Port = 99999
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for port 99999")
	}
}

func TestValidate_EmptyDataDir(t *testing.T) {
	cfg := config.Default()
	cfg.Node.DataDir = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for empty data_dir")
	}
}

func TestValidate_BatchSizeConsistency(t *testing.T) {
	cfg := config.Default()
	cfg.Queue.DefaultBatchSize = 200 // exceeds MaxBatchSize (100)
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error when default_batch_size > max_batch_size")
	}
}

func TestValidate_InvalidFsync(t *testing.T) {
	cfg := config.Default()
	cfg.Storage.Fsync = "magic"
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for unknown fsync policy")
	}
}

func TestValidate_NegativeMaxRetries(t *testing.T) {
	cfg := config.Default()
	cfg.Queue.MaxRetries = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for negative max_retries")
	}
}

// writeTempYAML writes content to a temp file and returns its path.
func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writeTempYAML: %v", err)
	}
	return path
}
