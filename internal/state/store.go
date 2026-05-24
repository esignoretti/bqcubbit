package state

import (
	"context"
	"time"
)

type SyncRun struct {
	ID             int64
	StartedAt      time.Time
	CompletedAt    *time.Time
	State          string
	TotalTasks     int
	CompletedTasks int
	FailedTasks    int
}

type TableState struct {
	ID                 int64
	Project            string
	Dataset            string
	TableName          string
	SchemaVersion      int
	LastSyncWatermark  *time.Time
	LastModifiedTime   *time.Time
}

type Task struct {
	ID              string
	SyncRunID       int64
	TableID         int64
	SchemaVersion   int
	PartitionID     string
	ShardIdx        int
	State           string
	WorkerID        *string
	LeaseExpiresAt  *time.Time
	LeaseGeneration int
	BytesRead       int64
	BytesWritten    int64
	RetryCount      int
	LastError       *string
	CreatedAt       time.Time
	CompletedAt     *time.Time
}

type SchemaVersion struct {
	ID          int64
	TableID     int64
	Version     int
	SchemaHash  string
	SchemaJSON  string
	ChangeType  string
	ChangesJSON string
	ValidFrom   time.Time
}

type PartitionState struct {
	ID                 int64
	TableID            int64
	PartitionID        string
	SchemaVersion      int
	BQLastModified     time.Time
	LastSuccessfulSync *time.Time
	RowCount           int64
	BytesInCubbit      int64
}

type StateStore interface {
	Init(ctx context.Context) error
	BeginRun(ctx context.Context) (*SyncRun, error)
	CompleteRun(ctx context.Context, runID int64, state string) error
	GetOrCreateTable(ctx context.Context, project, dataset, tableName string) (*TableState, error)
	UpdateTableWatermark(ctx context.Context, tableID int64, watermark time.Time) error
	CreateTasks(ctx context.Context, tasks []Task) error
	ClaimTask(ctx context.Context, workerID string) (*Task, error)
	UpdateTaskState(ctx context.Context, taskID, state string, generation int) error
	RecordSchemaVersion(ctx context.Context, sv *SchemaVersion) error
	GetCurrentSchemaVersion(ctx context.Context, tableID int64) (*SchemaVersion, error)
	GetOrCreatePartition(ctx context.Context, tableID int64, partitionID string) (*PartitionState, error)
	Close() error
}
