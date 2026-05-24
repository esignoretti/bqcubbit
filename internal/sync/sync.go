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
	"github.com/esignoretti/bqcubbit/internal/manifest"
	pq "github.com/esignoretti/bqcubbit/internal/parquet"
	"github.com/esignoretti/bqcubbit/internal/state"
	"github.com/esignoretti/bqcubbit/internal/storage"
)

type Orchestrator struct {
	cfg      *config.Config
	bqReader bigquery.Reader
	storage  *storage.Client
	state    state.StateStore
	pqWriter *pq.Writer
}

func NewOrchestrator(cfg *config.Config, bqReader bigquery.Reader, storage *storage.Client, state state.StateStore, pqWriter *pq.Writer) *Orchestrator {
	return &Orchestrator{
		cfg:      cfg,
		bqReader: bqReader,
		storage:  storage,
		state:    state,
		pqWriter: pqWriter,
	}
}

func (o *Orchestrator) SyncTable(ctx context.Context, dataset, table string) (err error) {
	log.Printf("[sync] starting full export of %s.%s", dataset, table)

	run, err := o.state.BeginRun(ctx)
	if err != nil {
		return fmt.Errorf("begin run: %w", err)
	}
	defer func() {
		finalState := "completed"
		if err != nil {
			finalState = "failed"
		}
		if cerr := o.state.CompleteRun(ctx, run.ID, finalState); cerr != nil {
			log.Printf("[sync] warning: complete run: %v", cerr)
		}
	}()

	tableState, err := o.state.GetOrCreateTable(ctx, o.cfg.Source.ProjectID, dataset, table)
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
	err = o.state.CreateTasks(ctx, tasks)
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}

	task, err := o.state.ClaimTask(ctx, "worker-0")
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
		_ = o.state.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return fmt.Errorf("upload to cubbit: %w", err)
	}

	err = o.state.UpdateTaskState(ctx, task.ID, "completed", task.LeaseGeneration)
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
