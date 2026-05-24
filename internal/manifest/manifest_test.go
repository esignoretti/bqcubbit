package manifest

import (
	"testing"
	"time"
)

func TestMerge(t *testing.T) {
	m1 := New(time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC))
	m1.AddFile("part-00001.parquet", 1000, 100, "abc")
	m1.AddFile("part-00002.parquet", 2000, 200, "def")

	m2 := New(time.Date(2026, 5, 24, 11, 0, 0, 0, time.UTC))
	m2.AddFile("part-00002.parquet", 100, 50, "dup")   // same path, different content
	m2.AddFile("part-00003.parquet", 3000, 300, "ghi")

	m1.Merge(m2)

	if len(m1.Files) != 3 {
		t.Fatalf("expected 3 files after merge, got %d", len(m1.Files))
	}
	if m1.Files[0].Path != "part-00001.parquet" {
		t.Fatalf("expected first file part-00001.parquet, got %s", m1.Files[0].Path)
	}
	if m1.Files[1].Path != "part-00002.parquet" {
		t.Fatalf("expected second file part-00002.parquet (original), got %s", m1.Files[1].Path)
	}
	if m1.Files[1].SHA256 != "def" {
		t.Fatalf("expected original file's sha256 kept, got %s", m1.Files[1].SHA256)
	}
	if m1.Files[2].Path != "part-00003.parquet" {
		t.Fatalf("expected third file part-00003.parquet, got %s", m1.Files[2].Path)
	}
	if m1.RowCount != 600 {
		t.Fatalf("expected 600 rows (100+200+300), got %d", m1.RowCount)
	}
	if m1.BytesInCubbit != 6000 {
		t.Fatalf("expected 6000 bytes (1000+2000+3000), got %d", m1.BytesInCubbit)
	}
}

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
