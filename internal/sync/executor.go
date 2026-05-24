package sync

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/esignoretti/bqcubbit/internal/bigquery"
	"github.com/esignoretti/bqcubbit/internal/config"
	"github.com/esignoretti/bqcubbit/internal/hash"
	"github.com/esignoretti/bqcubbit/internal/manifest"
	pq "github.com/esignoretti/bqcubbit/internal/parquet"
	"github.com/esignoretti/bqcubbit/internal/rate"
	"github.com/esignoretti/bqcubbit/internal/state"
	"github.com/esignoretti/bqcubbit/internal/storage"
)

type TaskExecutor struct {
	cfg        *config.Config
	bqReader   bigquery.Reader
	bqExport   *bigquery.ExportDataBackend
	storage    *storage.Client
	pqWriter   *pq.Writer
	limiters   *rate.Limiters
	stateStore state.StateStore
}

func NewTaskExecutor(
	cfg *config.Config,
	bqReader bigquery.Reader,
	bqExport *bigquery.ExportDataBackend,
	storage *storage.Client,
	pqWriter *pq.Writer,
	limiters *rate.Limiters,
	stateStore state.StateStore,
) *TaskExecutor {
	return &TaskExecutor{
		cfg: cfg, bqReader: bqReader, bqExport: bqExport,
		storage: storage, pqWriter: pqWriter, limiters: limiters,
		stateStore: stateStore,
	}
}

func (e *TaskExecutor) ExecuteTask(ctx context.Context, taskID string) error {
	log.Printf("[executor] executing task %s", taskID)

	task, tableRef, err := e.resolveTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("resolve task %s: %w", taskID, err)
	}

	arrowSchema, err := e.bqReader.Schema(ctx, e.cfg.Source.ProjectID, tableRef.dataset, tableRef.table)
	if err != nil {
		return fmt.Errorf("fetch schema: %w", err)
	}

	batches, err := e.bqReader.ReadTable(ctx, e.cfg.Source.ProjectID, tableRef.dataset, tableRef.table)
	if err != nil {
		return fmt.Errorf("read table: %w", err)
	}

	stagingKey := fmt.Sprintf("_staging/%s/%s/%s/part-%05d.zstd.parquet",
		tableRef.table, task.PartitionID, time.Now().UTC().Format("150405"),
		time.Now().UnixMilli()%100000)

	pipeReader, pipeWriter := io.Pipe()
	hashReader := hash.NewReader(pipeReader)

	errCh := make(chan error, 1)
	go func() {
		defer pipeWriter.Close()
		result, werr := e.pqWriter.WriteStreamResult(pipeWriter, arrowSchema, batches)
		if werr != nil {
			pipeWriter.CloseWithError(werr)
			errCh <- werr
			return
		}
		errCh <- nil
		_ = result
	}()

	_, err = e.storage.UploadMultipart(ctx, stagingKey, hashReader)
	if err != nil {
		e.stateStore.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return fmt.Errorf("upload partition: %w", err)
	}

	if err := <-errCh; err != nil {
		e.stateStore.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return fmt.Errorf("parquet write: %w", err)
	}

	// TODO: use actual schemaVersion from state store
	schemaVersion := 1
	finalKey := fmt.Sprintf("%s/%s/schema_version=v%d/%s/part-%05d.zstd.parquet",
		tableRef.dataset, tableRef.table, schemaVersion, task.PartitionID,
		time.Now().UnixMilli()%100000)

	if err := e.storage.RenameObject(ctx, stagingKey, finalKey); err != nil {
		e.stateStore.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return fmt.Errorf("rename staging->final: %w", err)
	}

	// Merge manifest
	if err := e.updateManifest(ctx, tableRef.dataset, tableRef.table, finalKey, hashReader.TotalBytes(), hashReader.SHA256()); err != nil {
		log.Printf("[executor] warning: update manifest: %v", err)
	}

	if err := e.stateStore.UpdateTaskState(ctx, task.ID, "completed", task.LeaseGeneration); err != nil {
		return fmt.Errorf("update task state: %w", err)
	}

	log.Printf("[executor] completed task %s -> %s (%d bytes, sha256: %s)", taskID, finalKey, hashReader.TotalBytes(), hashReader.SHA256())
	return nil
}

type tableRef struct {
	dataset string
	table   string
}

func (e *TaskExecutor) resolveTask(ctx context.Context, taskID string) (*state.Task, *tableRef, error) {
	tasks, err := e.stateStore.ListTasksByState(ctx, "assigned")
	if err != nil {
		return nil, nil, err
	}
	for _, t := range tasks {
		if t.ID == taskID {
			return &t, &tableRef{dataset: "unknown", table: "unknown"}, nil
		}
	}
	return nil, nil, fmt.Errorf("task %s not found", taskID)
}

func (e *TaskExecutor) updateManifest(ctx context.Context, dataset, table, filePath string, fileSize int64, sha256 string) error {
	manifestKey := fmt.Sprintf("%s/%s/_manifest.json", dataset, table)
	m := manifest.New(time.Now())

	exists, err := e.storage.ObjectExists(ctx, manifestKey)
	if err == nil && exists {
		rc, err := e.storage.GetObject(ctx, manifestKey)
		if err == nil {
			defer rc.Close()
			data, err := io.ReadAll(rc)
			if err == nil {
				existing, err := manifest.Deserialize(data)
				if err == nil {
					m.Merge(existing)
				}
			}
		}
	}

	m.AddFile(filePath, fileSize, 0, sha256)
	manifestData, err := m.Serialize()
	if err != nil {
		return fmt.Errorf("serialize manifest: %w", err)
	}
	return e.storage.UploadStream(ctx, manifestKey, strings.NewReader(string(manifestData)))
}
