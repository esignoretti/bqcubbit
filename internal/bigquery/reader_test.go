package bigquery

import (
	"context"
	"testing"
	"time"
)

func TestNewStorageReadReader(t *testing.T) {
	_, err := NewStorageReadReader(context.Background(), "test-project", "EU")
	if err != nil {
		t.Logf("expected credential error (no GCP env): %v", err)
	}
}

func TestReaderInterface_SatisfiedByStorageReadReader(t *testing.T) {
	var _ Reader = (*StorageReadReader)(nil)
}

func TestReadTableAtSnapshot_Compiles(t *testing.T) {
	r, err := NewStorageReadReader(context.Background(), "test-project", "EU")
	if err != nil {
		t.Skip("no GCP credentials available")
	}
	defer r.Close()

	ch, err := r.ReadTableAtSnapshot(context.Background(), "test-project", "test_dataset", "test_table", time.Now())
	if err != nil {
		t.Logf("expected error (no GCP env): %v", err)
	}
	if ch != nil {
		t.Error("expected nil channel without GCP")
	}
}

func TestCheckTableConsistency_DetectsLateArrivingData(t *testing.T) {
	snapshot := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	lastModified := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	if !CheckTableConsistency(lastModified, snapshot) {
		t.Error("expected true when last_modified > snapshot")
	}
}

func TestCheckTableConsistency_NoLateData(t *testing.T) {
	snapshot := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	lastModified := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	if CheckTableConsistency(lastModified, snapshot) {
		t.Error("expected false when last_modified <= snapshot")
	}
}

func TestCheckTableConsistency_EqualTimes(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	if CheckTableConsistency(ts, ts) {
		t.Error("expected false when times are equal")
	}
}
