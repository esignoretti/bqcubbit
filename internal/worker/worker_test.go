package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockStateStore struct {
	mu           sync.Mutex
	claimCalled  int32
	updateCalled int32
	renewReturns bool
	renewErr     error
}

func (m *mockStateStore) ClaimTask(_ context.Context, workerID string) (*Task, error) {
	atomic.AddInt32(&m.claimCalled, 1)
	return &Task{
		ID:              "test-task-1",
		SyncRunID:       1,
		TableID:         1,
		SchemaVersion:   1,
		PartitionID:     "p0",
		ShardIdx:        0,
		State:           "pending",
		LeaseGeneration: 1,
	}, nil
}

func (m *mockStateStore) RenewLease(_ context.Context, taskID string, generation int) (bool, error) {
	return m.renewReturns, m.renewErr
}

func (m *mockStateStore) UpdateTaskState(_ context.Context, taskID, state string, generation int) error {
	atomic.AddInt32(&m.updateCalled, 1)
	return nil
}

type mockExecutor struct {
	mu          sync.Mutex
	executedIDs []string
}

func (m *mockExecutor) ExecuteTask(_ context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executedIDs = append(m.executedIDs, taskID)
	return nil
}

func TestPoolExecutesTask(t *testing.T) {
	store := &mockStateStore{renewReturns: true}
	exec := &mockExecutor{}
	pool := NewPool(1, 3, exec, store)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool.Start(ctx)

	<-ctx.Done()

	pool.Stop()

	if n := atomic.LoadInt32(&store.claimCalled); n == 0 {
		t.Errorf("expected at least 1 ClaimTask call, got %d", n)
	}

	exec.mu.Lock()
	ids := make([]string, len(exec.executedIDs))
	copy(ids, exec.executedIDs)
	exec.mu.Unlock()

	if len(ids) == 0 {
		t.Fatal("expected at least 1 task to be executed, got 0")
	}
	if ids[0] != "test-task-1" {
		t.Errorf("expected task ID test-task-1, got %s", ids[0])
	}

	if n := atomic.LoadInt32(&store.updateCalled); n == 0 {
		t.Errorf("expected at least 1 UpdateTaskState call, got %d", n)
	}
}
