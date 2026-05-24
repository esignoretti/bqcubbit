package sync

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type PartitionInfo struct {
	TableProject      string
	TableDataset      string
	TableName         string
	PartitionID       string
	TotalRows         int64
	TotalLogicalBytes int64
	LastModifiedTime  time.Time
}

func DiscoverPartitions(ctx context.Context, projectID, location string, datasets []string, watermark *time.Time) ([]PartitionInfo, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("create bigquery client: %w", err)
	}
	defer client.Close()

	placeholders := make([]string, len(datasets))
	args := make([]bigquery.QueryParameter, len(datasets))
	for i, ds := range datasets {
		placeholders[i] = fmt.Sprintf("@dataset%d", i)
		args[i] = bigquery.QueryParameter{Name: fmt.Sprintf("dataset%d", i), Value: ds}
	}

	q := fmt.Sprintf(`
		SELECT
			table_catalog,
			table_schema,
			table_name,
			partition_id,
			total_rows,
			total_logical_bytes,
			last_modified_time
		FROM %s.INFORMATION_SCHEMA.PARTITIONS
		WHERE table_schema IN (%s)
	`, regionQualified(projectID, location), strings.Join(placeholders, ", "))

	if watermark != nil {
		q += fmt.Sprintf(" AND last_modified_time > @watermark")
		args = append(args, bigquery.QueryParameter{Name: "watermark", Value: *watermark})
	}

	q += " ORDER BY table_catalog, table_schema, table_name, partition_id"

	query := client.Query(q)
	query.Location = location
	query.Parameters = args

	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("query INFORMATION_SCHEMA.PARTITIONS: %w", err)
	}

	var results []PartitionInfo
	for {
		var row struct {
			TableCatalog      string    `bigquery:"table_catalog"`
			TableSchema       string    `bigquery:"table_schema"`
			TableName         string    `bigquery:"table_name"`
			PartitionID       string    `bigquery:"partition_id"`
			TotalRows         int64     `bigquery:"total_rows"`
			TotalLogicalBytes int64     `bigquery:"total_logical_bytes"`
			LastModifiedTime  time.Time `bigquery:"last_modified_time"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		results = append(results, PartitionInfo{
			TableProject:      row.TableCatalog,
			TableDataset:      row.TableSchema,
			TableName:         row.TableName,
			PartitionID:       row.PartitionID,
			TotalRows:         row.TotalRows,
			TotalLogicalBytes: row.TotalLogicalBytes,
			LastModifiedTime:  row.LastModifiedTime,
		})
	}

	return results, nil
}

// regionQualified returns a region-qualified table reference for INFORMATION_SCHEMA queries.
// For the "US" or "EU" locations, no region prefix is needed.
// For regional locations (e.g. "us-west1"), the regional dataset prefix is used.
func regionQualified(projectID, location string) string {
	loc := strings.ToLower(location)
	switch loc {
	case "us", "eu", "":
		return fmt.Sprintf("`%s.INFORMATION_SCHEMA`", projectID)
	default:
		return fmt.Sprintf("`%s.region-%s.INFORMATION_SCHEMA`", projectID, loc)
	}
}
