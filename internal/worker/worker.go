package worker

import (
	"context"
	"log"
	"sync"
	"time"
)

type TaskExecutor interface {
	ExecuteTask(ctx context.Context, taskID string) error
}

type StateStore interface {
	ClaimTask(ctx context.Context, workerID string) (*Task, error)
	RenewLease(ctx context.Context, taskID string, generation int) (bool, error)
	UpdateTaskState(ctx context.Context, taskID, state string, generation int) error
}

type Task struct {
	ID              string
	SyncRunID       int64
	TableID         int64
	SchemaVersion   int
	PartitionID     string
	ShardIdx        int
	State           string
	LeaseGeneration int
}

type Worker struct {
	id       string
	executor TaskExecutor
	state    StateStore
	running  bool
	mu       sync.Mutex
	stopCh   chan struct{}
}

func New(id string, executor TaskExecutor, state StateStore) *Worker {
	return &Worker{id: id, executor: executor, state: state, stopCh: make(chan struct{})}
}

func (w *Worker) ID() string { return w.id }

func (w *Worker) Start(ctx context.Context) {
	w.mu.Lock()
	w.running = true
	w.mu.Unlock()
	log.Printf("[worker %s] started", w.id)
	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		default:
			w.runOnce(ctx)
		}
	}
}

func (w *Worker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		close(w.stopCh)
		w.running = false
	}
}

func (w *Worker) runOnce(ctx context.Context) {
	task, err := w.state.ClaimTask(ctx, w.id)
	if err != nil {
		select {
		case <-w.stopCh:
			return
		case <-time.After(5 * time.Second):
			return
		}
	}

	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go w.heartbeat(taskCtx, cancel, task)

	if err := w.executor.ExecuteTask(taskCtx, task.ID); err != nil {
		log.Printf("[worker %s] task %s failed: %v", w.id, task.ID, err)
		_ = w.state.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
		return
	}
	_ = w.state.UpdateTaskState(ctx, task.ID, "completed", task.LeaseGeneration)
}

func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, task *Task) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := w.state.RenewLease(ctx, task.ID, task.LeaseGeneration)
			if err != nil || !ok {
				log.Printf("[worker %s] lease renewal failed for task %s: %v", w.id, task.ID, err)
				cancel()
				return
			}
		}
	}
}


