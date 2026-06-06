package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/esignoretti/bqcubbit/internal/bigquery"
	"github.com/esignoretti/bqcubbit/internal/config"
	"github.com/esignoretti/bqcubbit/internal/hash"
	"github.com/esignoretti/bqcubbit/internal/manifest"
	"github.com/esignoretti/bqcubbit/internal/metrics"
	pq "github.com/esignoretti/bqcubbit/internal/parquet"
	"github.com/esignoretti/bqcubbit/internal/rate"
	"github.com/esignoretti/bqcubbit/internal/schema"
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

	task, err := e.resolveTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("resolve task %s: %w", taskID, err)
	}

	tableState, err := e.stateStore.GetTableByID(ctx, task.TableID)
	if err != nil {
		return fmt.Errorf("get table %d: %w", task.TableID, err)
	}

	dataset, tableName := tableState.Dataset, tableState.TableName

	arrowSchema, err := e.bqReader.Schema(ctx, e.cfg.Source.ProjectID, dataset, tableName)
	if err != nil {
		return fmt.Errorf("fetch schema: %w", err)
	}

	// Skip if partition was already exported and file still exists
	if task.PartitionID != "" && task.PartitionID != "full" {
		ps, psErr := e.stateStore.GetOrCreatePartition(ctx, tableState.ID, task.PartitionID)
		if psErr == nil && ps.LastExportedPath != "" {
			exists, _ := e.storage.ObjectExists(ctx, ps.LastExportedPath)
			if exists {
				log.Printf("[executor] partition %s already exported at %s, skipping", task.PartitionID, ps.LastExportedPath)
				e.stateStore.UpdateTaskState(ctx, task.ID, "completed", task.LeaseGeneration)
				return nil
			}
		}
	}

	batches, err := e.bqReader.ReadTable(ctx, e.cfg.Source.ProjectID, dataset, tableName)
	if err != nil {
		return fmt.Errorf("read table: %w", err)
	}

	schemaVersion, err := e.resolveSchemaVersion(ctx, tableState, dataset, tableName)
	if err != nil {
		return fmt.Errorf("resolve schema version: %w", err)
	}

	stagingKey := fmt.Sprintf("_staging/%s/%s/%s/part-%05d.zstd.parquet",
		tableName, task.PartitionID, time.Now().UTC().Format("150405"),
		time.Now().UnixMilli()%100000)

	pipeReader, pipeWriter := io.Pipe()
	hashReader := hash.NewReader(pipeReader)

	startTime := time.Now()

	type writeOutcome struct {
		result *pq.WriteResult
		err    error
	}
	outcomeCh := make(chan writeOutcome, 1)
	go func() {
		defer pipeWriter.Close()
		result, werr := e.pqWriter.WriteStreamResult(pipeWriter, arrowSchema, batches)
		if werr != nil {
			pipeWriter.CloseWithError(werr)
			outcomeCh <- writeOutcome{err: werr}
			return
		}
		outcomeCh <- writeOutcome{result: result}
	}()

	if err := e.limiters.WaitUpload(ctx); err != nil {
		return err
	}

	_, err = e.storage.UploadMultipart(ctx, stagingKey, hashReader)
	if err != nil {
		e.stateStore.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return fmt.Errorf("upload partition: %w", err)
	}

	outcome := <-outcomeCh
	if outcome.err != nil {
		e.stateStore.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return fmt.Errorf("parquet write: %w", outcome.err)
	}

	elapsed := time.Since(startTime).Seconds()
	bytesUploaded := hashReader.TotalBytes()
	rowCount := int64(0)
	if outcome.result != nil {
		rowCount = outcome.result.RowCount
	}
	metrics.BytesExtracted.WithLabelValues(dataset, tableName).Add(float64(rowCount))
	metrics.BytesUploaded.WithLabelValues(dataset, tableName).Add(float64(bytesUploaded))
	metrics.TasksTotal.WithLabelValues("completed").Inc()
	metrics.TaskDuration.WithLabelValues(dataset, tableName, "completed").Observe(elapsed)

	finalKey := fmt.Sprintf("%s/%s/schema_version=v%d/%s/part-%05d.zstd.parquet",
		dataset, tableName, schemaVersion, task.PartitionID,
		time.Now().UnixMilli()%100000)

	if err := e.storage.RenameObject(ctx, stagingKey, finalKey); err != nil {
		e.stateStore.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return fmt.Errorf("rename staging->final: %w", err)
	}

	if err := e.updateManifest(ctx, dataset, tableName, finalKey, hashReader.TotalBytes(), hashReader.SHA256()); err != nil {
		log.Printf("[executor] warning: update manifest: %v", err)
	}

	ps, err := e.stateStore.GetOrCreatePartition(ctx, tableState.ID, task.PartitionID)
	if err == nil {
		now := time.Now().UTC()
		ps.SchemaVersion = schemaVersion
		ps.LastSuccessfulSync = &now
		ps.RowCount = rowCount
		ps.BytesInCubbit = int64(bytesUploaded)
		ps.LastExportedPath = finalKey
		if err := e.stateStore.UpdatePartitionSync(ctx, ps); err != nil {
			log.Printf("[executor] warning: update partition state: %v", err)
		}
	} else {
		log.Printf("[executor] warning: get/create partition state: %v", err)
	}

	if err := e.stateStore.UpdateTaskState(ctx, task.ID, "completed", task.LeaseGeneration); err != nil {
		return fmt.Errorf("update task state: %w", err)
	}

	log.Printf("[executor] completed task %s -> %s (%d bytes, sha256: %s)", taskID, finalKey, hashReader.TotalBytes(), hashReader.SHA256())
	return nil
}

func (e *TaskExecutor) resolveSchemaVersion(ctx context.Context, tableState *state.TableState, dataset, tableName string) (int, error) {
	currentVersion, err := e.stateStore.GetCurrentSchemaVersion(ctx, tableState.ID)
	if err != nil || currentVersion == nil {
		sv := &state.SchemaVersion{
			TableID:    tableState.ID,
			Version:    1,
			SchemaHash: "initial",
			SchemaJSON: "{}",
			ChangeType: "INITIAL",
			ValidFrom:  time.Now().UTC(),
		}
		if err := e.stateStore.RecordSchemaVersion(ctx, sv); err != nil {
			return 0, fmt.Errorf("record initial schema: %w", err)
		}
		return 1, nil
	}

	arrowSchema, err := e.bqReader.Schema(context.Background(), e.cfg.Source.ProjectID, dataset, tableName)
	if err != nil {
		return currentVersion.Version, nil
	}

	var bqFields []schema.BQField
	for i := 0; i < arrowSchema.NumFields(); i++ {
		f := arrowSchema.Field(i)
		bqFields = append(bqFields, schema.BQField{
			Name: f.Name,
			Type: f.Type.String(),
			Mode: "NULLABLE",
		})
	}

	newHash := schema.CanonicalHash(bqFields)
	if currentVersion.SchemaHash == newHash {
		return currentVersion.Version, nil
	}

	var oldFields []schema.BQField
	if err := json.Unmarshal([]byte(currentVersion.SchemaJSON), &oldFields); err == nil {
		diff := schema.Diff(oldFields, bqFields)
		newVersion := currentVersion.Version + 1
		changeType := diff.ChangeType.String()
		changesJSON, _ := json.Marshal(diff.Changes)

		sv := &state.SchemaVersion{
			TableID:    tableState.ID,
			Version:    newVersion,
			SchemaHash: newHash,
			SchemaJSON: string(mustMarshal(bqFields)),
			ChangeType: changeType,
			ChangesJSON: string(changesJSON),
			ValidFrom:  time.Now().UTC(),
		}
		if err := e.stateStore.RecordSchemaVersion(ctx, sv); err != nil {
			return currentVersion.Version, fmt.Errorf("record schema version: %w", err)
		}
		log.Printf("[schema] table %s.%s: v%d -> v%d (%s)", dataset, tableName, currentVersion.Version, newVersion, changeType)
		return newVersion, nil
	}

	return currentVersion.Version, nil
}

func (e *TaskExecutor) resolveTask(ctx context.Context, taskID string) (*state.Task, error) {
	tasks, err := e.stateStore.ListTasksByState(ctx, "assigned")
	if err != nil {
		return nil, err
	}
	for _, t := range tasks {
		if t.ID == taskID {
			return &t, nil
		}
	}
	return nil, fmt.Errorf("task %s not found in assigned tasks", taskID)
}

func mustMarshal(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(data)
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
