package state

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, fmt.Errorf("enable WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Init(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS sync_runs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		started_at TIMESTAMP NOT NULL,
		completed_at TIMESTAMP,
		state TEXT NOT NULL DEFAULT 'running',
		total_tasks INTEGER NOT NULL DEFAULT 0,
		completed_tasks INTEGER NOT NULL DEFAULT 0,
		failed_tasks INTEGER NOT NULL DEFAULT 0
	);
	CREATE TABLE IF NOT EXISTS tables (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		project TEXT NOT NULL,
		dataset TEXT NOT NULL,
		table_name TEXT NOT NULL,
		current_schema_version INTEGER NOT NULL DEFAULT 1,
		last_sync_watermark TIMESTAMP,
		last_modified_time TIMESTAMP,
		UNIQUE(project, dataset, table_name)
	);
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		sync_run_id INTEGER REFERENCES sync_runs(id),
		table_id INTEGER REFERENCES tables(id),
		schema_version INTEGER NOT NULL DEFAULT 1,
		partition_id TEXT,
		shard_idx INTEGER DEFAULT 0,
		state TEXT NOT NULL DEFAULT 'pending',
		worker_id TEXT,
		lease_expires_at TIMESTAMP,
		lease_generation INTEGER DEFAULT 0,
		bytes_read INTEGER DEFAULT 0,
		bytes_written INTEGER DEFAULT 0,
		retry_count INTEGER DEFAULT 0,
		last_error TEXT,
		created_at TIMESTAMP NOT NULL,
		started_at TIMESTAMP,
		completed_at TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state, lease_expires_at);
	CREATE TABLE IF NOT EXISTS schema_versions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		table_id INTEGER NOT NULL REFERENCES tables(id),
		version INTEGER NOT NULL,
		schema_hash TEXT NOT NULL,
		schema_json TEXT NOT NULL,
		change_type TEXT NOT NULL,
		changes_json TEXT,
		valid_from TIMESTAMP NOT NULL,
		UNIQUE(table_id, version)
	);
	CREATE TABLE IF NOT EXISTS partitions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		table_id INTEGER NOT NULL REFERENCES tables(id),
		partition_id TEXT NOT NULL,
		schema_version INTEGER NOT NULL DEFAULT 1,
		bq_last_modified TIMESTAMP NOT NULL,
		last_successful_sync TIMESTAMP,
		row_count INTEGER DEFAULT 0,
		bytes_in_cubbit INTEGER DEFAULT 0,
		UNIQUE(table_id, partition_id)
	);
	`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *SQLiteStore) BeginRun(ctx context.Context) (*SyncRun, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, "INSERT INTO sync_runs (started_at, state) VALUES (?, 'running')", now)
	if err != nil {
		return nil, fmt.Errorf("begin run: %w", err)
	}
	id, _ := res.LastInsertId()
	return &SyncRun{ID: id, StartedAt: now, State: "running"}, nil
}

func (s *SQLiteStore) CompleteRun(ctx context.Context, runID int64, state string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, "UPDATE sync_runs SET completed_at=?, state=? WHERE id=?", now, state, runID)
	return err
}

func (s *SQLiteStore) GetOrCreateTable(ctx context.Context, project, dataset, tableName string) (*TableState, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, project, dataset, table_name, current_schema_version, last_sync_watermark, last_modified_time FROM tables WHERE project=? AND dataset=? AND table_name=?",
		project, dataset, tableName)
	ts := &TableState{}
	var watermark, modified *time.Time
	err := row.Scan(&ts.ID, &ts.Project, &ts.Dataset, &ts.TableName, &ts.SchemaVersion, &watermark, &modified)
	if err == sql.ErrNoRows {
		res, err := s.db.ExecContext(ctx, "INSERT INTO tables (project, dataset, table_name) VALUES (?, ?, ?)", project, dataset, tableName)
		if err != nil {
			return nil, fmt.Errorf("create table: %w", err)
		}
		id, _ := res.LastInsertId()
		return &TableState{ID: id, Project: project, Dataset: dataset, TableName: tableName, SchemaVersion: 1}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get table: %w", err)
	}
	ts.LastSyncWatermark = watermark
	ts.LastModifiedTime = modified
	return ts, nil
}

func (s *SQLiteStore) UpdateTableWatermark(ctx context.Context, tableID int64, watermark time.Time) error {
	_, err := s.db.ExecContext(ctx, "UPDATE tables SET last_sync_watermark=? WHERE id=?", watermark, tableID)
	return err
}

func (s *SQLiteStore) CreateTasks(ctx context.Context, tasks []Task) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		"INSERT INTO tasks (id, sync_run_id, table_id, schema_version, partition_id, shard_idx, state, created_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().UTC()
	for _, t := range tasks {
		if _, err := stmt.ExecContext(ctx, t.ID, t.SyncRunID, t.TableID, t.SchemaVersion, t.PartitionID, t.ShardIdx, now); err != nil {
			return fmt.Errorf("insert task: %w", err)
		}
	}
	return tx.Commit()
}

func (s *SQLiteStore) ClaimTask(ctx context.Context, workerID string) (*Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx,
		"SELECT id, sync_run_id, table_id, schema_version, partition_id, shard_idx, state, lease_generation FROM tasks WHERE state='pending' ORDER BY created_at ASC LIMIT 1")
	t := &Task{}
	if err := row.Scan(&t.ID, &t.SyncRunID, &t.TableID, &t.SchemaVersion, &t.PartitionID, &t.ShardIdx, &t.State, &t.LeaseGeneration); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	leaseExp := now.Add(30 * time.Minute)
	_, err = tx.ExecContext(ctx,
		"UPDATE tasks SET state='assigned', worker_id=?, lease_expires_at=?, lease_generation=lease_generation+1 WHERE id=? AND state='pending'",
		workerID, leaseExp, t.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	t.State = "assigned"
	t.WorkerID = &workerID
	t.LeaseExpiresAt = &leaseExp
	t.LeaseGeneration++
	return t, nil
}

func (s *SQLiteStore) UpdateTaskState(ctx context.Context, taskID, state string, generation int) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE tasks SET state=?, lease_generation=? WHERE id=? AND lease_generation=?",
		state, generation+1, taskID, generation)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("task %s: optimistic lock failed (generation mismatch)", taskID)
	}
	return nil
}

func (s *SQLiteStore) RecordSchemaVersion(ctx context.Context, sv *SchemaVersion) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO schema_versions (table_id, version, schema_hash, schema_json, change_type, changes_json, valid_from)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		sv.TableID, sv.Version, sv.SchemaHash, sv.SchemaJSON, sv.ChangeType, sv.ChangesJSON, sv.ValidFrom)
	if err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetCurrentSchemaVersion(ctx context.Context, tableID int64) (*SchemaVersion, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, table_id, version, schema_hash, schema_json, change_type, COALESCE(changes_json,''), valid_from
		 FROM schema_versions WHERE table_id=? ORDER BY version DESC LIMIT 1`, tableID)
	sv := &SchemaVersion{}
	if err := row.Scan(&sv.ID, &sv.TableID, &sv.Version, &sv.SchemaHash, &sv.SchemaJSON, &sv.ChangeType, &sv.ChangesJSON, &sv.ValidFrom); err != nil {
		return nil, fmt.Errorf("get current schema version: %w", err)
	}
	return sv, nil
}

func (s *SQLiteStore) GetOrCreatePartition(ctx context.Context, tableID int64, partitionID string) (*PartitionState, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, table_id, partition_id, schema_version, bq_last_modified, last_successful_sync, row_count, bytes_in_cubbit
		 FROM partitions WHERE table_id=? AND partition_id=?`, tableID, partitionID)
	ps := &PartitionState{}
	var lastSync *time.Time
	err := row.Scan(&ps.ID, &ps.TableID, &ps.PartitionID, &ps.SchemaVersion, &ps.BQLastModified, &lastSync, &ps.RowCount, &ps.BytesInCubbit)
	if err == sql.ErrNoRows {
		now := time.Now().UTC()
		res, err := s.db.ExecContext(ctx,
			`INSERT INTO partitions (table_id, partition_id, bq_last_modified) VALUES (?, ?, ?)`,
			tableID, partitionID, now)
		if err != nil {
			return nil, fmt.Errorf("create partition: %w", err)
		}
		id, _ := res.LastInsertId()
		return &PartitionState{ID: id, TableID: tableID, PartitionID: partitionID, SchemaVersion: 1, BQLastModified: now}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get partition: %w", err)
	}
	ps.LastSuccessfulSync = lastSync
	return ps, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}
