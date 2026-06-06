package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"net/http"

	"github.com/esignoretti/bqcubbit/internal/bigquery"
	"github.com/esignoretti/bqcubbit/internal/config"
	"github.com/esignoretti/bqcubbit/internal/coordinator"
	"github.com/esignoretti/bqcubbit/internal/metrics"
	pq "github.com/esignoretti/bqcubbit/internal/parquet"
	"github.com/esignoretti/bqcubbit/internal/rate"
	"github.com/esignoretti/bqcubbit/internal/scheduler"
	"github.com/esignoretti/bqcubbit/internal/state"
	"github.com/esignoretti/bqcubbit/internal/storage"
	"github.com/esignoretti/bqcubbit/internal/sync"
	"github.com/esignoretti/bqcubbit/internal/webui"
	"github.com/esignoretti/bqcubbit/internal/verify"
)

func main() {
	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: bqcubbit [flags] <command>\n\nCommands:\n  sync             Export a table from BigQuery to Cubbit DS3\n  serve            Run as daemon with scheduler\n  verify           Verify exported data against BigQuery\n  ack-schema-change Acknowledge a breaking schema change\n\nFlags:\n")
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
	case "serve":
		if err := runServe(cfg); err != nil {
			log.Fatalf("serve: %v", err)
		}
	case "verify":
		if err := runVerify(cfg); err != nil {
			log.Fatalf("verify: %v", err)
		}
	case "ack-schema-change":
		table := flag.Arg(1)
		if table == "" {
			log.Fatal("usage: bqcubbit ack-schema-change <dataset.table>")
		}
		if err := runAckSchemaChange(cfg, table); err != nil {
			log.Fatalf("ack: %v", err)
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

	// Recover stale runs before starting a new one
	if staleIDs, err := store.AbortStaleRuns(context.Background()); err != nil {
		log.Printf("[main] warning: abort stale runs: %v", err)
	} else if len(staleIDs) > 0 {
		log.Printf("[main] aborted %d stale runs", len(staleIDs))
		if n, err := store.CleanupStaleTasks(context.Background(), staleIDs); err == nil {
			log.Printf("[main] cleaned %d stale tasks", n)
		}
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
		if n, err := storageClient.DeleteObjects(context.Background(), "_staging/"); err != nil {
			log.Printf("[main] warning: cleanup staging files: %v", err)
		} else if n > 0 {
			log.Printf("[main] removed %d stale staging files", n)
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

func runServe(cfg *config.Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		log.Println("[serve] received shutdown signal")
		cancel()
	}()

	bqReader, err := bigquery.NewStorageReadReader(ctx, cfg.Source.ProjectID, cfg.Source.Location)
	if err != nil {
		return fmt.Errorf("create bq reader: %w", err)
	}
	defer bqReader.Close()

	storageClient, err := storage.NewClient(ctx, cfg.Destination.Endpoint, cfg.Destination.AccessKey, cfg.Destination.SecretKey, cfg.Destination.Bucket, cfg.Destination.Prefix)
	if err != nil {
		return fmt.Errorf("create storage: %w", err)
	}

	statePath := os.Getenv("BQCUBBIT_STATE")
	if statePath == "" {
		statePath = "bqcubbit_state.db"
	}
	stateStore, err := state.NewSQLiteStore(statePath)
	if err != nil {
		return fmt.Errorf("create state: %w", err)
	}
	defer stateStore.Close()
	stateStore.Init(ctx)

	pqWriter := pq.NewWriter(pq.DefaultWriterConfig())

	limiters := rate.NewLimiters(
		cfg.RateLimit.BQReadSessionsPerHour,
		cfg.RateLimit.BQExportJobsPerHour,
		cfg.RateLimit.CubbitUploadsPerMin,
	)

	var bqExport *bigquery.ExportDataBackend
	if cfg.GCS.StagingBucket != "" {
		bqExport, err = bigquery.NewExportDataBackend(ctx, cfg.Source.ProjectID, cfg.Source.Location, cfg.GCS.StagingBucket, cfg.GCS.StagingPrefix, storageClient)
		if err != nil {
			return fmt.Errorf("create export backend: %w", err)
		}
		defer bqExport.Close()
	}

	executor := sync.NewTaskExecutor(cfg, bqReader, bqExport, storageClient, pqWriter, limiters, stateStore)
	coord := coordinator.NewCoordinator(cfg, stateStore, limiters, executor)
	sched := scheduler.NewScheduler(cfg, coord, stateStore)

	go func() {
		if err := storageClient.AbortStaleUploads(ctx, 24*time.Hour); err != nil {
			log.Printf("[serve] warning: cleanup stale uploads: %v", err)
		}
		if n, err := storageClient.DeleteObjects(ctx, "_staging/"); err != nil {
			log.Printf("[serve] warning: cleanup staging files: %v", err)
		} else if n > 0 {
			log.Printf("[serve] removed %d stale staging files", n)
		}
	}()

	log.Printf("[serve] bqcubbit daemon starting (cron: %s)", cfg.Scheduler.Cron)

	if cfg.Scheduler.InitialSyncMode == "full_refresh" {
		log.Println("[serve] running initial sync")
		if _, err := coord.RunOnce(ctx); err != nil {
			log.Printf("[serve] initial sync failed: %v", err)
		}
	}

	webHandler, err := webui.NewHandler(stateStore)
	if err != nil {
		return fmt.Errorf("create webui: %w", err)
	}

	mux := http.NewServeMux()
	webHandler.RegisterRoutes(mux)
	mux.Handle("/metrics", metrics.MetricsHandler())

	webServer := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		log.Printf("[webui] listening on :8080")
		if err := webServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[webui] error: %v", err)
		}
	}()
	defer webServer.Shutdown(context.Background())

	return sched.Start(ctx)
}

func runVerify(cfg *config.Config) error {
	return verify.RunCLI(context.Background(), verify.CLIConfig{
		ProjectID:  cfg.Source.ProjectID,
		Location:   cfg.Source.Location,
		SampleRate: 0.01,
	})
}

func runAckSchemaChange(cfg *config.Config, table string) error {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid table format: %s (expected dataset.table)", table)
	}

	statePath := os.Getenv("BQCUBBIT_STATE")
	if statePath == "" {
		statePath = "bqcubbit_state.db"
	}
	store, err := state.NewSQLiteStore(statePath)
	if err != nil {
		return fmt.Errorf("state store: %w", err)
	}
	defer store.Close()
	store.Init(context.Background())

	ts, err := store.GetOrCreateTable(context.Background(), cfg.Source.ProjectID, parts[0], parts[1])
	if err != nil {
		return fmt.Errorf("get table: %w", err)
	}

	if err := store.AcknowledgeSchemaChange(context.Background(), ts.ID, ts.SchemaVersion); err != nil {
		return fmt.Errorf("acknowledge: %w", err)
	}

	log.Printf("[ack] acknowledged schema change for %s", table)
	return nil
}
