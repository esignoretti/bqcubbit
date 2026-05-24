package manifest

import (
	"encoding/json"
	"fmt"
	"time"
)

type TableManifest struct {
	SchemaVersion string         `json:"schema_version"`
	ExportedAt    string         `json:"exported_at"`
	PartitionRefs []PartitionRef `json:"partition_refs"`
	RowCount      int64          `json:"row_count"`
	BytesInCubbit int64          `json:"bytes_in_cubbit"`
	Files         []FileInfo     `json:"files"`
}

type PartitionRef struct {
	PartitionID string `json:"partition_id"`
	RowCount    int64  `json:"row_count"`
}

type FileInfo struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	RowCount int64  `json:"row_count"`
	SHA256   string `json:"sha256"`
}

func New(exportedAt time.Time) *TableManifest {
	return &TableManifest{
		SchemaVersion: "v1",
		ExportedAt:    exportedAt.UTC().Format(time.RFC3339),
	}
}

func (m *TableManifest) AddFile(path string, size, rowCount int64, sha256 string) {
	m.Files = append(m.Files, FileInfo{
		Path:     path,
		Size:     size,
		RowCount: rowCount,
		SHA256:   sha256,
	})
	m.RowCount += rowCount
	m.BytesInCubbit += size
}

func (m *TableManifest) Serialize() ([]byte, error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	return data, nil
}

func Deserialize(data []byte) (*TableManifest, error) {
	m := &TableManifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return m, nil
}
