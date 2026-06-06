package bigquery

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

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
// Uses EXPORT DATA with SELECT + WHERE _PARTITIONTIME for ingestion-time partitioned tables.
func (e *ExportDataBackend) ExportPartition(ctx context.Context, dataset, table, partitionID string, schemaVersion int) ([]string, error) {
	date, err := partitionIDToDate(partitionID)
	if err != nil {
		return nil, fmt.Errorf("parse partition ID: %w", err)
	}

	gcsPrefix := fmt.Sprintf("%s/%s/%s/schema_version=v%d/%s", e.stagingPrefix, dataset, table, schemaVersion, partitionID)
	uri := fmt.Sprintf("gs://%s/%s/*.parquet", e.stagingBucket, gcsPrefix)

	sql := fmt.Sprintf(`EXPORT DATA OPTIONS(
		uri='%s',
		format='PARQUET',
		overwrite=true,
		compression='ZSTD'
	) AS
	SELECT * FROM %s.%s
	WHERE _PARTITIONTIME = TIMESTAMP('%s')`, uri, dataset, table, date)

	job, err := e.bqClient.Query(sql).Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("start export job: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("export job wait: %w", err)
	}
	if status.Err() != nil {
		return nil, fmt.Errorf("export job failed: %w", status.Err())
	}

	log.Printf("[exporter] EXPORT DATA job completed for %s.%s/%s", dataset, table, partitionID)

	bkt := e.gcsClient.Bucket(e.stagingBucket)
	it := bkt.Objects(ctx, &gcs.Query{Prefix: gcsPrefix})

	var cubbitKeys []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list gcs objects: %w", err)
		}

		fname := attrs.Name[len(gcsPrefix)+1:]
		cubbitKey := fmt.Sprintf("%s/%s/schema_version=v%d/%s/%s", dataset, table, schemaVersion, partitionID, fname)
		if err := e.transferFile(ctx, attrs.Name, cubbitKey); err != nil {
			log.Printf("[exporter] warning: transfer %s: %v", attrs.Name, err)
			continue
		}
		cubbitKeys = append(cubbitKeys, cubbitKey)

		if err := bkt.Object(attrs.Name).Delete(ctx); err != nil {
			log.Printf("[exporter] warning: delete gcs temp %s: %v", attrs.Name, err)
		}
	}

	return cubbitKeys, nil
}

// partitionIDToDate converts a BigQuery ingestion-time partition ID (e.g. "20240101") to a date string.
func partitionIDToDate(partitionID string) (string, error) {
	if len(partitionID) == 8 {
		return fmt.Sprintf("%s-%s-%s", partitionID[:4], partitionID[4:6], partitionID[6:8]), nil
	}
	return "", fmt.Errorf("unexpected partition ID format: %s", partitionID)
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
