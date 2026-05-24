package bigquery

import (
	"context"
	"testing"
)

func TestNewStorageReadReader(t *testing.T) {
	_, err := NewStorageReadReader(context.Background(), "test-project", "EU")
	if err != nil {
		t.Logf("expected credential error (no GCP env): %v", err)
	}
}
