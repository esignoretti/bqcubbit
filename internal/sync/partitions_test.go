package sync

import (
	"context"
	"testing"
	"time"
)

func TestDiscoverPartitions(t *testing.T) {
	t.Skip("requires real GCP project")

	ctx := context.Background()
	projectID := "my-project"
	location := "US"
	datasets := []string{"my_dataset"}
	watermark := time.Now().Add(-24 * time.Hour)

	results, err := DiscoverPartitions(ctx, projectID, location, datasets, &watermark)
	if err != nil {
		t.Fatalf("DiscoverPartitions failed: %v", err)
	}
	t.Logf("found %d partitions", len(results))
	for _, p := range results {
		t.Logf("  %s.%s.%s/%s rows=%d bytes=%d modified=%v",
			p.TableProject, p.TableDataset, p.TableName, p.PartitionID,
			p.TotalRows, p.TotalLogicalBytes, p.LastModifiedTime)
	}
}
