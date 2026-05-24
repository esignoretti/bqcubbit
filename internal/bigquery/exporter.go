package bigquery

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	gcs "cloud.google.com/go/storage"

	"github.com/esignoretti/bqcubbit/internal/storage"
)

type ExportDataBackend struct {
	bqClient      *bigquery.Client
	gcsClient     *gcs.Client
	cubbitClient  *storage.Client
	stagingBucket string
	stagingPrefix string
	projectID     string
	location      string
}

func NewExportDataBackend(ctx context.Context, projectID, location, stagingBucket, stagingPrefix string, cubbitClient *storage.Client) (*ExportDataBackend, error) {
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("create bq client: %w", err)
	}
	gcsClient, err := gcs.NewClient(ctx)
	if err != nil {
		bqClient.Close()
		return nil, fmt.Errorf("create gcs client: %w", err)
	}
	return &ExportDataBackend{
		bqClient: bqClient, gcsClient: gcsClient, cubbitClient: cubbitClient,
		stagingBucket: stagingBucket, stagingPrefix: stagingPrefix,
		projectID: projectID, location: location,
	}, nil
}

func (e *ExportDataBackend) Close() error {
	e.bqClient.Close()
	return e.gcsClient.Close()
}

// ExportPartition runs EXPORT DATA for a partition and transfers results to Cubbit.
// This is a placeholder/canary — the full implementation needs partition column detection.
// For Phase 3, implement the structure and a simple export that works for ingestion-time partitioned tables.
func (e *ExportDataBackend) ExportPartition(ctx context.Context, dataset, table, partitionID string, schemaVersion int) ([]string, error) {
	// Build EXPORT DATA SQL
	// EXPORT DATA doesn't support WHERE clause filtering directly.
	// Two approaches:
	// 1. Export whole table (no filter) — simple but wasteful
	// 2. Use a query-based export with SELECT + WHERE
	// For Phase 3, implement approach 1 with a note that approach 2 needs partition column detection.

	// The EXPORT DATA approach needs GCS bucket configured in the GCP project, not here.
	// For Phase 3 MVP, export the whole table to GCS then transfer files.

	return nil, fmt.Errorf("ExportDataBackend: not fully implemented — needs GCS bucket and partition column detection")
}

func (e *ExportDataBackend) transferFile(ctx context.Context, gcsPath, cubbitKey string) error {
	bkt := e.gcsClient.Bucket(e.stagingBucket)
	obj := bkt.Object(gcsPath)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("open gcs reader: %w", err)
	}
	defer reader.Close()

	if err := e.cubbitClient.UploadStream(ctx, cubbitKey, reader); err != nil {
		return fmt.Errorf("upload to cubbit: %w", err)
	}

	log.Printf("[exporter] transferred %s to %s", gcsPath, cubbitKey)
	return nil
}
