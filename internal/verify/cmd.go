package verify

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
)

type CLIConfig struct {
	ProjectID  string
	Location   string
	SampleRate float64
}

func RunCLI(ctx context.Context, cfg CLIConfig) error {
	client, err := bigquery.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("create bq client: %w", err)
	}
	defer client.Close()

	log.Printf("[verify] sampling %.1f%% of partitions", cfg.SampleRate*100)
	// Phase 4: iterate tables from state store, sample partitions, verify row counts
	return nil
}
