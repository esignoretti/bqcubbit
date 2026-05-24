package bigquery

import (
	"context"
	"testing"
)

func TestNewExportDataBackend(t *testing.T) {
	t.Skip("requires GCP credentials, GCS bucket, and Cubbit credentials")
	_, err := NewExportDataBackend(context.Background(), "test-project", "EU", "bucket", "prefix", nil)
	if err != nil {
		t.Logf("expected credential error (no GCP env): %v", err)
	}
}
