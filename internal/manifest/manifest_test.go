package manifest

import (
	"testing"
	"time"
)

func TestManifestRoundTrip(t *testing.T) {
	m := New(time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC))
	m.AddFile("part-00001.parquet", 1024, 100, "abc123")
	m.AddFile("part-00002.parquet", 2048, 200, "def456")

	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	m2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}

	if m2.RowCount != 300 {
		t.Fatalf("expected 300 rows, got %d", m2.RowCount)
	}
	if len(m2.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(m2.Files))
	}
}
