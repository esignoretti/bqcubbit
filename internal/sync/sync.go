package sync

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/esignoretti/bqcubbit/internal/bigquery"
	"github.com/esignoretti/bqcubbit/internal/config"
	"github.com/esignoretti/bqcubbit/internal/hash"
	"github.com/esignoretti/bqcubbit/internal/manifest"
	pq "github.com/esignoretti/bqcubbit/internal/parquet"
	"github.com/esignoretti/bqcubbit/internal/state"
	"github.com/esignoretti/bqcubbit/internal/storage"
)

type Orchestrator struct {
	cfg        *config.Config
	bqReader   bigquery.Reader
	storage    *storage.Client
	stateStore state.StateStore
	pqWriter   *pq.Writer
}

func NewOrchestrator(cfg *config.Config, bqReader bigquery.Reader, storage *storage.Client, stateStore state.StateStore, pqWriter *pq.Writer) *Orchestrator {
	return &Orchestrator{
		cfg:        cfg,
		bqReader:   bqReader,
		storage:    storage,
		stateStore: stateStore,
		pqWriter:   pqWriter,
	}
}

// SyncAll syncs all configured tables. This is the Phase 2 entry point.
func (o *Orchestrator) SyncAll(ctx context.Context) error {
	log.Printf("[sync] starting sync run (strategy: %s)", o.cfg.Sync.IncrementalStrategy)

	run, err := o.stateStore.BeginRun(ctx)
	if err != nil {
		return fmt.Errorf("begin run: %w", err)
	}
	defer func() {
		finalState := "completed"
		if err != nil {
			finalState = "failed"
		}
		if cerr := o.stateStore.CompleteRun(ctx, run.ID, finalState); cerr != nil {
			log.Printf("[sync] warning: complete run: %v", cerr)
		}
	}()

	partitions, err := DiscoverPartitions(ctx, o.cfg.Source.ProjectID, o.cfg.Source.Location, o.cfg.Sync.Datasets, nil)
	if err != nil {
		return fmt.Errorf("discover partitions: %w", err)
	}
	log.Printf("[sync] discovered %d partitions", len(partitions))

	groups := groupByTable(partitions)
	for tableKey, parts := range groups {
		parts2 := strings.SplitN(tableKey, ".", 2)
		if len(parts2) != 2 {
			log.Printf("[sync] warning: invalid table key %q, skipping", tableKey)
			continue
		}
		dataset, table := parts2[0], parts2[1]
		if err := o.syncTable(ctx, run.ID, dataset, table, parts); err != nil {
			log.Printf("[sync] error syncing %s.%s: %v", dataset, table, err)
		}
	}
	return nil
}

// SyncTable is Phase 1 backward compat — single table full export.
func (o *Orchestrator) SyncTable(ctx context.Context, dataset, table string) (err error) {
	log.Printf("[sync] starting full export of %s.%s", dataset, table)

	run, err := o.stateStore.BeginRun(ctx)
	if err != nil {
		return fmt.Errorf("begin run: %w", err)
	}
	defer func() {
		finalState := "completed"
		if err != nil {
			finalState = "failed"
		}
		if cerr := o.stateStore.CompleteRun(ctx, run.ID, finalState); cerr != nil {
			log.Printf("[sync] warning: complete run: %v", cerr)
		}
	}()

	tableState, err := o.stateStore.GetOrCreateTable(ctx, o.cfg.Source.ProjectID, dataset, table)
	if err != nil {
		return fmt.Errorf("get table: %w", err)
	}

	schema, err := o.bqReader.Schema(ctx, o.cfg.Source.ProjectID, dataset, table)
	if err != nil {
		return fmt.Errorf("fetch schema: %w", err)
	}

	batches, err := o.bqReader.ReadTable(ctx, o.cfg.Source.ProjectID, dataset, table)
	if err != nil {
		return fmt.Errorf("read table: %w", err)
	}

	taskID := fmt.Sprintf("%s-%s-%s-%d", o.cfg.Source.ProjectID, dataset, table, time.Now().Unix())
	tasks := []state.Task{
		{ID: taskID, SyncRunID: run.ID, TableID: tableState.ID, PartitionID: "full", ShardIdx: 0},
	}
	err = o.stateStore.CreateTasks(ctx, tasks)
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}

	task, err := o.stateStore.ClaimTask(ctx, "worker-0")
	if err != nil {
		return fmt.Errorf("claim task: %w", err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05")
	outputKey := fmt.Sprintf("%s/%s/%s/%s/part-00000.zstd.parquet",
		strings.TrimRight(o.cfg.Destination.Prefix, "/"),
		fmt.Sprintf("project=%s", o.cfg.Source.ProjectID),
		fmt.Sprintf("dataset=%s", dataset),
		fmt.Sprintf("table=%s/%s", table, timestamp),
	)

	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		if err := o.pqWriter.WriteStream(pipeWriter, schema, batches); err != nil {
			log.Printf("[sync] parquet write error: %v", err)
			pipeWriter.CloseWithError(err)
		}
	}()

	err = o.storage.UploadStream(ctx, outputKey, pipeReader)
	if err != nil {
		_ = o.stateStore.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return fmt.Errorf("upload to cubbit: %w", err)
	}

	err = o.stateStore.UpdateTaskState(ctx, task.ID, "completed", task.LeaseGeneration)
	if err != nil {
		return fmt.Errorf("update task state: %w", err)
	}

	m := manifest.New(time.Now())
	m.AddFile(outputKey, 0, 0, "")
	manifestData, err := m.Serialize()
	if err != nil {
		return fmt.Errorf("serialize manifest: %w", err)
	}

	manifestKey := fmt.Sprintf("%s/%s/%s/%s/_manifest.json",
		strings.TrimRight(o.cfg.Destination.Prefix, "/"),
		fmt.Sprintf("project=%s", o.cfg.Source.ProjectID),
		fmt.Sprintf("dataset=%s", dataset),
		fmt.Sprintf("table=%s", table),
	)
	if err := o.storage.UploadStream(ctx, manifestKey, strings.NewReader(string(manifestData))); err != nil {
		log.Printf("[sync] warning: upload manifest: %v", err)
	}

	log.Printf("[sync] completed export of %s.%s", dataset, table)
	return nil
}

func (o *Orchestrator) syncTable(ctx context.Context, runID int64, dataset, table string, partitions []PartitionInfo) error {
	log.Printf("[sync] processing table %s.%s (%d partitions)", dataset, table, len(partitions))

	tableState, err := o.stateStore.GetOrCreateTable(ctx, o.cfg.Source.ProjectID, dataset, table)
	if err != nil {
		return fmt.Errorf("get table state: %w", err)
	}

	arrowSchema, err := o.bqReader.Schema(ctx, o.cfg.Source.ProjectID, dataset, table)
	if err != nil {
		return fmt.Errorf("fetch schema: %w", err)
	}

	schemaVersion := 1
	currentVersion, err := o.stateStore.GetCurrentSchemaVersion(ctx, tableState.ID)
	if err != nil || currentVersion == nil {
		sv := &state.SchemaVersion{
			TableID:    tableState.ID,
			Version:    1,
			SchemaHash: "initial",
			SchemaJSON: "{}",
			ChangeType: "INITIAL",
			ValidFrom:  time.Now().UTC(),
		}
		if err := o.stateStore.RecordSchemaVersion(ctx, sv); err != nil {
			return fmt.Errorf("record initial schema: %w", err)
		}
	} else {
		schemaVersion = currentVersion.Version
		log.Printf("[sync] table %s.%s at schema version v%d", dataset, table, schemaVersion)
	}

	// Filter to partitions modified since last sync (incremental recovery on gap)
	var filtered []PartitionInfo
	for _, p := range partitions {
		if tableState.LastSyncWatermark != nil && !p.LastModifiedTime.After(*tableState.LastSyncWatermark) {
			continue
		}
		filtered = append(filtered, p)
	}
	if len(filtered) < len(partitions) {
		log.Printf("[sync] %s.%s: filtered %d already-synced partitions", dataset, table, len(partitions)-len(filtered))
	}
	partitions = filtered

	if len(partitions) == 0 {
		return nil
	}

	batches := groupIntoBatches(partitions, o.cfg.Sync.BatchSizeDays)
	log.Printf("[sync] %s.%s: grouped %d partitions into %d batch(es)", dataset, table, len(partitions), len(batches))

	// Single full-table read, shared across all batches
	batchesCh, err := o.bqReader.ReadTable(ctx, o.cfg.Source.ProjectID, dataset, table)
	if err != nil {
		return fmt.Errorf("read table: %w", err)
	}

	for _, batch := range batches {
		if err := o.exportBatch(ctx, runID, tableState, arrowSchema, schemaVersion, batch, batchesCh); err != nil {
			log.Printf("[sync] error exporting batch %s..%s: %v", batch[0].PartitionID, batch[len(batch)-1].PartitionID, err)
		}
		// Note: batchesCh is shared across batches; each exportBatch reads from it.
		// For a single-file-per-table approach (the common case), the entire stream is consumed
		// in the first and only batch. Multiple batches would need filtered reads (not yet supported).
		break
	}

	latestMod := partitions[len(partitions)-1].LastModifiedTime
	if err := o.stateStore.UpdateTableWatermark(ctx, tableState.ID, latestMod); err != nil {
		log.Printf("[sync] warning: update watermark: %v", err)
	}
	return nil
}

// groupIntoBatches groups sorted partition IDs into batches covering batchSizeDays each.
func groupIntoBatches(partitions []PartitionInfo, batchSizeDays int) [][]PartitionInfo {
	if batchSizeDays <= 0 {
		batchSizeDays = 7
	}
	if len(partitions) == 0 {
		return nil
	}
	minDate, maxDate := partitions[0].PartitionID, partitions[0].PartitionID
	for _, p := range partitions {
		if p.PartitionID < minDate {
			minDate = p.PartitionID
		}
		if p.PartitionID > maxDate {
			maxDate = p.PartitionID
		}
	}

	minTime, errMin := time.Parse("20060102", minDate)
	maxTime, errMax := time.Parse("20060102", maxDate)
	if errMin != nil || errMax != nil || minTime.IsZero() || maxTime.IsZero() {
		return [][]PartitionInfo{partitions}
	}

	var result [][]PartitionInfo
	batchStart := minTime
	for batchStart.Before(maxTime) || batchStart.Equal(maxTime) {
		batchEnd := batchStart.AddDate(0, 0, batchSizeDays-1)
		if batchEnd.After(maxTime) {
			batchEnd = maxTime
		}
		var batch []PartitionInfo
		for _, p := range partitions {
			t, err := time.Parse("20060102", p.PartitionID)
			if err != nil {
				continue
			}
			if (t.After(batchStart) || t.Equal(batchStart)) && (t.Before(batchEnd) || t.Equal(batchEnd)) {
				batch = append(batch, p)
			}
		}
		if len(batch) > 0 {
			result = append(result, batch)
		}
		batchStart = batchEnd.AddDate(0, 0, 1)
	}
	return result
}

func (o *Orchestrator) exportBatch(ctx context.Context, runID int64, tableState *state.TableState, arrowSchema *arrow.Schema, schemaVersion int, batch []PartitionInfo, batchesCh <-chan arrow.Record) error {
	if len(batch) == 0 {
		return nil
	}
	dataset, table := tableState.Dataset, tableState.TableName
	startPID, endPID := batch[0].PartitionID, batch[len(batch)-1].PartitionID
	log.Printf("[sync] exporting batch %s..%s (%d partitions)", startPID, endPID, len(batch))

	// Check if all partitions already exported
	allExported := true
	for _, p := range batch {
		ps, psErr := o.stateStore.GetOrCreatePartition(ctx, tableState.ID, p.PartitionID)
		if psErr != nil || ps.LastExportedPath == "" {
			allExported = false
			continue
		}
		exists, _ := o.storage.ObjectExists(ctx, ps.LastExportedPath)
		if !exists {
			allExported = false
		}
	}
	if allExported {
		log.Printf("[sync] batch %s..%s already fully exported, skipping", startPID, endPID)
		return nil
	}

	stagingKey := fmt.Sprintf("_staging/%s/%s/%s.parquet", table, startPID, time.Now().UTC().Format("150405"))

	pipeReader, pipeWriter := io.Pipe()
	hashReader := hash.NewReader(pipeReader)

	errCh := make(chan error, 1)
	go func() {
		defer pipeWriter.Close()
		_, werr := o.pqWriter.WriteStreamResult(pipeWriter, arrowSchema, batchesCh)
		if werr != nil {
			pipeWriter.CloseWithError(werr)
			errCh <- werr
			return
		}
		errCh <- nil
	}()

	if _, err := o.storage.UploadMultipart(ctx, stagingKey, hashReader); err != nil {
		return fmt.Errorf("upload batch: %w", err)
	}

	if err := <-errCh; err != nil {
		return fmt.Errorf("parquet write: %w", err)
	}

	finalKey := fmt.Sprintf("%s/%s/schema_version=v%d/%s_%s.parquet",
		dataset, table, schemaVersion, startPID, endPID)
	if err := o.storage.RenameObject(ctx, stagingKey, finalKey); err != nil {
		return fmt.Errorf("rename staging->final: %w", err)
	}

	// Merge manifest
	if err := o.mergeManifest(ctx, dataset, table, finalKey, hashReader.TotalBytes(), hashReader.SHA256()); err != nil {
		log.Printf("[sync] warning: update manifest: %v", err)
	}

	// Update partition states
	now := time.Now().UTC()
	for _, p := range batch {
		ps, psErr := o.stateStore.GetOrCreatePartition(ctx, tableState.ID, p.PartitionID)
		if psErr != nil {
			log.Printf("[sync] warning: get/create partition %s: %v", p.PartitionID, psErr)
			continue
		}
		ps.SchemaVersion = schemaVersion
		ps.LastSuccessfulSync = &now
		ps.BytesInCubbit = int64(hashReader.TotalBytes())
		ps.LastExportedPath = finalKey
		if err := o.stateStore.UpdatePartitionSync(ctx, ps); err != nil {
			log.Printf("[sync] warning: update partition state: %v", err)
		}
	}

	log.Printf("[sync] completed batch %s..%s (%d partitions, sha256: %s, %d bytes)", startPID, endPID, len(batch), hashReader.SHA256(), hashReader.TotalBytes())
	return nil
}

// mergeManifest reads existing manifest, merges the new file, and writes back.
func (o *Orchestrator) mergeManifest(ctx context.Context, dataset, table, filePath string, fileSize int64, sha256 string) error {
	manifestKey := fmt.Sprintf("%s/%s/_manifest.json", dataset, table)
	m := manifest.New(time.Now())
	exists, err := o.storage.ObjectExists(ctx, manifestKey)
	if err == nil && exists {
		rc, err := o.storage.GetObject(ctx, manifestKey)
		if err == nil {
			data, _ := io.ReadAll(rc)
			rc.Close()
			existing, err := manifest.Deserialize(data)
			if err == nil {
				m.Merge(existing)
			}
		}
	}
	m.AddFile(filePath, fileSize, 0, sha256)
	manifestData, _ := m.Serialize()
	return o.storage.UploadStream(ctx, manifestKey, strings.NewReader(string(manifestData)))
}

func groupByTable(partitions []PartitionInfo) map[string][]PartitionInfo {
	groups := make(map[string][]PartitionInfo)
	for _, p := range partitions {
		key := p.TableDataset + "." + p.TableName
		groups[key] = append(groups[key], p)
	}
	return groups
}
