package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/esignoretti/bqcubbit/internal/bigquery"
	"github.com/esignoretti/bqcubbit/internal/config"
	pq "github.com/esignoretti/bqcubbit/internal/parquet"
	"github.com/esignoretti/bqcubbit/internal/state"
	"github.com/esignoretti/bqcubbit/internal/storage"
	"github.com/esignoretti/bqcubbit/internal/sync"
)

func main() {
	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: bqcubbit [flags] <command>\n\nCommands:\n  sync   Export a table from BigQuery to Cubbit DS3\n\nFlags:\n")
		flag.PrintDefaults()
	}

	configPath := flag.String("config", "", "Path to config file (env: BQCUBBIT_CONFIG)")
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	switch flag.Arg(0) {
	case "sync":
		if err := runSync(cfg); err != nil {
			log.Fatalf("sync: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", flag.Arg(0))
		flag.Usage()
		os.Exit(1)
	}
}

func runSync(cfg *config.Config) error {
	statePath := os.Getenv("BQCUBBIT_STATE")
	if statePath == "" {
		statePath = "bqcubbit_state.db"
	}

	store, err := state.NewSQLiteStore(statePath)
	if err != nil {
		return fmt.Errorf("state store: %w", err)
	}
	defer store.Close()

	if err := store.Init(context.Background()); err != nil {
		return fmt.Errorf("init state: %w", err)
	}

	bqReader, err := bigquery.NewStorageReadReader(context.Background(), cfg.Source.ProjectID, cfg.Source.Location)
	if err != nil {
		return fmt.Errorf("bigquery reader: %w", err)
	}
	defer bqReader.Close()

	storageClient, err := storage.NewClient(
		context.Background(),
		cfg.Destination.Endpoint,
		cfg.Destination.AccessKey,
		cfg.Destination.SecretKey,
		cfg.Destination.Bucket,
		cfg.Destination.Prefix,
	)
	if err != nil {
		return fmt.Errorf("storage client: %w", err)
	}

	pqWriterCfg := pq.DefaultWriterConfig()
	if cfg.Destination.Compression != "" {
		pqWriterCfg.Compression = cfg.Destination.Compression
	}
	if cfg.Destination.CompressionLevel != 0 {
		pqWriterCfg.CompressionLevel = cfg.Destination.CompressionLevel
	}
	pqWriter := pq.NewWriter(pqWriterCfg)

	go func() {
		if err := storageClient.AbortStaleUploads(context.Background(), 24*time.Hour); err != nil {
			log.Printf("[main] warning: cleanup stale uploads: %v", err)
		}
	}()

	orch := sync.NewOrchestrator(cfg, bqReader, storageClient, store, pqWriter)

	if cfg.Sync.Table != "" {
		parts := strings.SplitN(cfg.Sync.Table, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid table format %q: expected dataset.table", cfg.Sync.Table)
		}
		dataset, table := parts[0], parts[1]
		return orch.SyncTable(context.Background(), dataset, table)
	}

	return orch.SyncAll(context.Background())
}
