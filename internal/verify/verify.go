package verify

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
)

type Verifier struct {
	bqClient *bigquery.Client
}

func NewVerifier(ctx context.Context, projectID string) (*Verifier, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("create bq client: %w", err)
	}
	return &Verifier{bqClient: client}, nil
}

func (v *Verifier) Close() error { return v.bqClient.Close() }

type Result struct {
	TableDataset   string
	TableName      string
	PartitionID    string
	BQRowCount     int64
	CubbitRowCount int64
	RowCountMatch  bool
	BQBytes        int64
	CubbitBytes    int64
}

// VerifyPartition compares BQ row count against expected count from export.
// Uses _PARTITIONTIME filter for ingestion-time partitions, or _PARTITIONDATE for date-range.
func (v *Verifier) VerifyPartition(ctx context.Context, projectID, dataset, table, partitionID string, expectedRows int64) (*Result, error) {
	q := v.bqClient.Query(fmt.Sprintf(
		"SELECT COUNT(*) as cnt FROM `%s.%s.%s` WHERE _PARTITIONTIME = TIMESTAMP(@partition_id)",
		projectID, dataset, table))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "partition_id", Value: partitionID},
	}

	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	it, err := q.Read(ctx2)
	if err != nil {
		return nil, fmt.Errorf("query row count: %w", err)
	}

	var row struct{ Cnt int64 }
	if err := it.Next(&row); err != nil {
		return nil, fmt.Errorf("read row count: %w", err)
	}

	result := &Result{
		TableDataset:   dataset, TableName: table, PartitionID: partitionID,
		BQRowCount: row.Cnt, CubbitRowCount: expectedRows,
		RowCountMatch: row.Cnt == expectedRows,
	}

	if !result.RowCountMatch {
		log.Printf("[verify] MISMATCH %s.%s/%s: BQ=%d Cubbit=%d", dataset, table, partitionID, row.Cnt, expectedRows)
	}
	return result, nil
}
