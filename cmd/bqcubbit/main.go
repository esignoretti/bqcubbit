package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/esignoretti/bqcubbit/internal/config"
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
	fmt.Printf("syncing table %s from %s to %s/%s\n", cfg.Sync.Table, cfg.Source.ProjectID, cfg.Destination.Endpoint, cfg.Destination.Bucket)
	return nil
}
