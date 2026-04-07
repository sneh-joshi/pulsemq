// Package config holds all configuration types and loading logic for PulseMQ.
// Config structure never shrinks — fields are only added, never renamed or removed.
package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration for a PulseMQ server instance.
type Config struct {
	Node      NodeConfig     `yaml:"node"`
	Cluster   ClusterConfig  `yaml:"cluster"`
	Storage   StorageConfig  `yaml:"storage"`
	Queue     QueueConfig    `yaml:"queue"`
	Producers ProducerConfig `yaml:"producers"`
	Auth      AuthConfig     `yaml:"auth"`
	Metrics   MetricsConfig  `yaml:"metrics"`
	Webhook   WebhookConfig  `yaml:"webhook"`
}

// NodeConfig holds identity and network settings for this server node.
type NodeConfig struct {
	// ID is a ULID string. Use "auto" to generate and persist one on first start.
	ID      string `yaml:"id"`
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
	DataDir string `yaml:"data_dir"`
}

// ClusterConfig controls multi-node behaviour.
// Phase 1: Enabled is always false. Fields are present so Phase 2 requires
// only a config change, not a code change.
type ClusterConfig struct {
	Enabled bool     `yaml:"enabled"`
	Peers   []string `yaml:"peers"`
}

// FsyncPolicy controls when data is flushed to physical disk.
type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"   // safest, slowest
	FsyncInterval FsyncPolicy = "interval" // flush every FsyncIntervalMs — default
	FsyncBatch    FsyncPolicy = "batch"    // flush every FsyncBatchSize writes
	FsyncNever    FsyncPolicy = "never"    // fastest, unsafe (dev/test only)
)

// StorageConfig controls how messages are persisted on disk.
type StorageConfig struct {
	SegmentSizeMB      int         `yaml:"segment_size_mb"`
	CompactionInterval string      `yaml:"compaction_interval"`
	Fsync              FsyncPolicy `yaml:"fsync"`
	FsyncIntervalMs    int         `yaml:"fsync_interval_ms"`
	FsyncBatchSize     int         `yaml:"fsync_batch_size"`
}

// QueueConfig sets defaults and limits that apply to every queue.
type QueueConfig struct {
	DefaultVisibilityTimeoutMs int    `yaml:"default_visibility_timeout_ms"`
	MaxMessageSizeKB           int    `yaml:"max_message_size_kb"`
	MaxBatchSize               int    `yaml:"max_batch_size"`
	DefaultBatchSize           int    `yaml:"default_batch_size"`
	MaxMessages                int    `yaml:"max_messages"`
	MaxRetries                 int    `yaml:"max_retries"`
	RetentionPeriod            string `yaml:"retention_period"`
	// MaxScheduleAhead caps how far in the future a deliverAt can be set.
	MaxScheduleAhead string `yaml:"max_schedule_ahead"`
}

// ProducerConfig sets rate limiting applied per-producer.
type ProducerConfig struct {
	// MaxRate is messages per second per producer.
	MaxRate int `yaml:"max_rate"`
	// Burst allows temporary spikes above MaxRate.
	Burst int `yaml:"burst"`
}

// AuthConfig controls API key authentication.
type AuthConfig struct {
	Enabled bool   `yaml:"enabled"`
	APIKey  string `yaml:"api_key"`
}

// MetricsConfig controls the Prometheus metrics endpoint.
type MetricsConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

// WebhookConfig controls behaviour when pushing messages to webhook subscribers.
type WebhookConfig struct {
	// RetryDelaysMs is the list of delays between successive retry attempts.
	RetryDelaysMs []int `yaml:"retry_delays_ms"`
	TimeoutMs     int   `yaml:"timeout_ms"`
}

// Default returns a Config populated with safe, sensible defaults.
// It is the canonical source of truth for default values.
func Default() *Config {
	return &Config{
		Node: NodeConfig{
			ID:      "auto",
			Host:    "0.0.0.0",
			Port:    8080,
			DataDir: "./data",
		},
		Cluster: ClusterConfig{
			Enabled: false,
			Peers:   []string{},
		},
		Storage: StorageConfig{
			SegmentSizeMB:      256,
			CompactionInterval: "1h",
			Fsync:              FsyncInterval,
			FsyncIntervalMs:    1000,
			FsyncBatchSize:     1000,
		},
		Queue: QueueConfig{
			DefaultVisibilityTimeoutMs: 30_000,
			MaxMessageSizeKB:           256,
			MaxBatchSize:               100,
			DefaultBatchSize:           1,
			MaxMessages:                100_000,
			MaxRetries:                 5,
			RetentionPeriod:            "7d",
			MaxScheduleAhead:           "90d",
		},
		Producers: ProducerConfig{
			MaxRate: 10_000,
			Burst:   50_000,
		},
		Auth: AuthConfig{
			Enabled: false,
			APIKey:  "",
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
		},
		Webhook: WebhookConfig{
			RetryDelaysMs: []int{1_000, 5_000, 30_000},
			TimeoutMs:     5_000,
		},
	}
}

// Load reads a YAML config file at path and overlays it on top of Default().
// If the file does not exist the default config is returned without error,
// making it easy to run PulseMQ with no config file at all.
//
// After loading the file, environment variables are applied as overrides:
//
//	PULSEMQ_AUTH_API_KEY   — sets auth.api_key and enables auth (auth.enabled = true)
//	PULSEMQ_DATA_DIR       — sets node.data_dir
//	PULSEMQ_PORT           — sets node.port
func Load(path string) (*Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			applyEnv(cfg)
			return cfg, nil
		}
		return nil, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	applyEnv(cfg)
	return cfg, nil
}

// applyEnv overlays environment variable overrides onto cfg.
func applyEnv(cfg *Config) {
	if v := os.Getenv("PULSEMQ_AUTH_API_KEY"); v != "" {
		cfg.Auth.APIKey = v
		cfg.Auth.Enabled = true
	}
	if v := os.Getenv("PULSEMQ_DATA_DIR"); v != "" {
		cfg.Node.DataDir = v
	}
	if v := os.Getenv("PULSEMQ_PORT"); v != "" {
		var p int
		if _, err := fmt.Sscanf(v, "%d", &p); err == nil && p > 0 {
			cfg.Node.Port = p
		}
	}
}

// Validate checks that the config values are consistent and within acceptable
// ranges. It returns the first error found.
func (c *Config) Validate() error {
	if c.Node.Port < 1 || c.Node.Port > 65535 {
		return errors.New("node.port must be between 1 and 65535")
	}
	if c.Node.DataDir == "" {
		return errors.New("node.data_dir must not be empty")
	}
	if c.Queue.MaxBatchSize < 1 {
		return errors.New("queue.max_batch_size must be at least 1")
	}
	if c.Queue.DefaultBatchSize < 1 {
		return errors.New("queue.default_batch_size must be at least 1")
	}
	if c.Queue.DefaultBatchSize > c.Queue.MaxBatchSize {
		return errors.New("queue.default_batch_size must not exceed queue.max_batch_size")
	}
	if c.Queue.MaxMessages < 1 {
		return errors.New("queue.max_messages must be at least 1")
	}
	if c.Queue.MaxRetries < 0 {
		return errors.New("queue.max_retries must be >= 0")
	}
	if c.Metrics.Port < 1 || c.Metrics.Port > 65535 {
		return errors.New("metrics.port must be between 1 and 65535")
	}
	switch c.Storage.Fsync {
	case FsyncAlways, FsyncInterval, FsyncBatch, FsyncNever:
		// valid
	default:
		return errors.New(`storage.fsync must be one of "always", "interval", "batch", "never"`)
	}
	return nil
}
