package state

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestSQLiteStore(t *testing.T) {
	path := os.TempDir() + "/bqcubbit_test.db"
	defer os.Remove(path)

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	run, err := store.BeginRun(ctx)
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	if run.State != "running" {
		t.Fatalf("expected running, got %s", run.State)
	}

	ts, err := store.GetOrCreateTable(ctx, "proj", "ds", "tbl")
	if err != nil {
		t.Fatalf("GetOrCreateTable: %v", err)
	}
	if ts.TableName != "tbl" {
		t.Fatalf("expected tbl, got %s", ts.TableName)
	}

	ts2, err := store.GetOrCreateTable(ctx, "proj", "ds", "tbl")
	if err != nil {
		t.Fatalf("GetOrCreateTable second: %v", err)
	}
	if ts2.ID != ts.ID {
		t.Fatalf("expected same id %d, got %d", ts.ID, ts2.ID)
	}

	tasks := []Task{
		{ID: "t1", SyncRunID: run.ID, TableID: ts.ID, PartitionID: "p1", ShardIdx: 0},
	}
	if err := store.CreateTasks(ctx, tasks); err != nil {
		t.Fatalf("CreateTasks: %v", err)
	}

	claimed, err := store.ClaimTask(ctx, "worker-1")
	if err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}
	if claimed.ID != "t1" {
		t.Fatalf("expected t1, got %s", claimed.ID)
	}

	_, err = store.ClaimTask(ctx, "worker-2")
	if err == nil {
		t.Fatal("expected no tasks left")
	}

	if err := store.CompleteRun(ctx, run.ID, "completed"); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}

	sv := &SchemaVersion{
		TableID:    ts.ID,
		Version:    1,
		SchemaHash: "abc123",
		SchemaJSON: `{"type":"record"}`,
		ChangeType: "initial",
		ValidFrom:  time.Now().UTC(),
	}
	if err := store.RecordSchemaVersion(ctx, sv); err != nil {
		t.Fatalf("RecordSchemaVersion: %v", err)
	}

	gotSV, err := store.GetCurrentSchemaVersion(ctx, ts.ID)
	if err != nil {
		t.Fatalf("GetCurrentSchemaVersion: %v", err)
	}
	if gotSV.SchemaHash != "abc123" {
		t.Fatalf("expected schema hash abc123, got %s", gotSV.SchemaHash)
	}

	ps, err := store.GetOrCreatePartition(ctx, ts.ID, "p20250101")
	if err != nil {
		t.Fatalf("GetOrCreatePartition: %v", err)
	}
	if ps.PartitionID != "p20250101" {
		t.Fatalf("expected partition p20250101, got %s", ps.PartitionID)
	}

	ps2, err := store.GetOrCreatePartition(ctx, ts.ID, "p20250101")
	if err != nil {
		t.Fatalf("GetOrCreatePartition second: %v", err)
	}
	if ps2.ID != ps.ID {
		t.Fatalf("expected same partition id %d, got %d", ps.ID, ps2.ID)
	}
}
