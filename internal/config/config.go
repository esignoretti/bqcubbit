package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source      SourceConfig      `yaml:"source"`
	Destination DestinationConfig `yaml:"destination"`
	Sync        SyncConfig        `yaml:"sync"`
	Scheduler   SchedulerConfig   `yaml:"scheduler"`
	WorkerPool  WorkerPoolConfig  `yaml:"worker_pool"`
	RateLimit   RateLimitConfig   `yaml:"rate_limit"`
	GCS         GCSConfig         `yaml:"gcs"`
}

type SourceConfig struct {
	ProjectID string   `yaml:"project_id"`
	Location  string   `yaml:"location"`
	Datasets  []string `yaml:"datasets"`
}

type DestinationConfig struct {
	Endpoint         string `yaml:"endpoint"`
	Bucket           string `yaml:"bucket"`
	Prefix           string `yaml:"prefix"`
	AccessKey        string `yaml:"access_key"`
	SecretKey        string `yaml:"secret_key"`
	Compression      string `yaml:"compression"`
	CompressionLevel int    `yaml:"compression_level"`
}

type TableSyncConfig struct {
	Match               string `yaml:"match"`
	IncrementalStrategy string `yaml:"incremental_strategy"`
}

type SchedulerConfig struct {
	Cron            string `yaml:"cron"`
	OverlapPolicy   string `yaml:"overlap_policy"`
	InitialSyncMode string `yaml:"initial_sync_mode"`
}

type WorkerPoolConfig struct {
	MinWorkers int `yaml:"min_workers"`
	MaxWorkers int `yaml:"max_workers"`
	QueueDepth int `yaml:"queue_depth"`
}

type RateLimitConfig struct {
	BQReadSessionsPerHour int `yaml:"bq_read_sessions_per_hour"`
	BQExportJobsPerHour   int `yaml:"bq_export_jobs_per_hour"`
	CubbitUploadsPerMin   int `yaml:"cubbit_uploads_per_minute"`
}

type GCSConfig struct {
	StagingBucket string `yaml:"staging_bucket"`
	StagingPrefix string `yaml:"staging_prefix"`
	LifecycleDays int    `yaml:"lifecycle_days"`
}

type SyncConfig struct {
	Table               string            `yaml:"table"`
	Datasets            []string          `yaml:"datasets"`
	IncrementalStrategy string            `yaml:"incremental_strategy"`
	Tables              []TableSyncConfig `yaml:"tables"`
	MaxConcurrent       int               `yaml:"max_concurrent"`
	ExtractionMethod    string            `yaml:"extraction_method"`
	MaxPartitionSizeGB  int               `yaml:"max_partition_size_gb"`
}

func (c *Config) Validate() error {
	if c.Source.ProjectID == "" {
		return fmt.Errorf("source.project_id is required")
	}
	if c.Source.Location == "" {
		return fmt.Errorf("source.location is required")
	}
	if c.Destination.Endpoint == "" {
		return fmt.Errorf("destination.endpoint is required")
	}
	if c.Destination.Bucket == "" {
		return fmt.Errorf("destination.bucket is required")
	}
	if c.Sync.IncrementalStrategy != "" &&
		c.Sync.IncrementalStrategy != "full_refresh" &&
		c.Sync.IncrementalStrategy != "partition" {
		return fmt.Errorf("sync.incremental_strategy must be 'full_refresh' or 'partition', got %q", c.Sync.IncrementalStrategy)
	}
	if c.Scheduler.OverlapPolicy != "" &&
		c.Scheduler.OverlapPolicy != "skip" &&
		c.Scheduler.OverlapPolicy != "queue" &&
		c.Scheduler.OverlapPolicy != "cancel_and_restart" {
		return fmt.Errorf("scheduler.overlap_policy invalid: %q", c.Scheduler.OverlapPolicy)
	}
	if c.WorkerPool.MinWorkers < 1 {
		return fmt.Errorf("worker_pool.min_workers must be >= 1")
	}
	if c.WorkerPool.MaxWorkers < c.WorkerPool.MinWorkers {
		return fmt.Errorf("worker_pool.max_workers (%d) must be >= min_workers (%d)", c.WorkerPool.MaxWorkers, c.WorkerPool.MinWorkers)
	}
	return nil
}

func Default() *Config {
	return &Config{
		Destination: DestinationConfig{
			Prefix:           "bq-export/",
			Compression:      "zstd",
			CompressionLevel: 9,
		},
		Sync: SyncConfig{
			IncrementalStrategy: "full_refresh",
			MaxConcurrent:       1,
			ExtractionMethod:    "auto",
			MaxPartitionSizeGB:  5,
		},
		Scheduler: SchedulerConfig{
			Cron:            "0 2 * * *",
			OverlapPolicy:   "skip",
			InitialSyncMode: "full_refresh",
		},
		WorkerPool: WorkerPoolConfig{
			MinWorkers: 2,
			MaxWorkers: 8,
			QueueDepth: 10,
		},
		RateLimit: RateLimitConfig{
			BQReadSessionsPerHour: 100,
			BQExportJobsPerHour:   50,
			CubbitUploadsPerMin:   60,
		},
		GCS: GCSConfig{
			StagingPrefix: "_bqcubbit_staging/",
			LifecycleDays: 1,
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		path = os.Getenv("BQCUBBIT_CONFIG")
	}
	if path == "" {
		return nil, fmt.Errorf("config path required (set BQCUBBIT_CONFIG or pass --config)")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
