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

type SyncConfig struct {
	Table               string            `yaml:"table"`
	Datasets            []string          `yaml:"datasets"`
	IncrementalStrategy string            `yaml:"incremental_strategy"`
	Tables              []TableSyncConfig `yaml:"tables"`
	MaxConcurrent       int               `yaml:"max_concurrent"`
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
