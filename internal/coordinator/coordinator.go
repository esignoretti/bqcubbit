package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/esignoretti/bqcubbit/internal/config"
	"github.com/esignoretti/bqcubbit/internal/rate"
	"github.com/esignoretti/bqcubbit/internal/state"
	"github.com/esignoretti/bqcubbit/internal/worker"
)

// Coordinator manages the sync lifecycle.
type Coordinator struct {
	cfg        *config.Config
	stateStore state.StateStore
	limiters   *rate.Limiters
	executor   worker.TaskExecutor

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
}

func NewCoordinator(cfg *config.Config, stateStore state.StateStore, limiters *rate.Limiters, executor worker.TaskExecutor) *Coordinator {
	return &Coordinator{
		cfg: cfg, stateStore: stateStore, limiters: limiters, executor: executor,
	}
}

func (c *Coordinator) RunOnce(ctx context.Context) (int64, error) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return 0, fmt.Errorf("sync run already in progress")
	}
	c.running = true
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.cancel = nil
		c.mu.Unlock()
	}()

	run, err := c.stateStore.BeginRun(runCtx)
	if err != nil {
		return 0, fmt.Errorf("begin run: %w", err)
	}
	log.Printf("[coordinator] started sync run %d", run.ID)

	if n, err := c.stateStore.ResetExpiredLeases(runCtx); err == nil && n > 0 {
		log.Printf("[coordinator] reset %d expired leases", n)
	}

	pool := worker.NewPool(
		c.cfg.WorkerPool.MinWorkers,
		c.cfg.WorkerPool.MaxWorkers,
		c.executor,
		&workerStateAdapter{store: c.stateStore},
	)
	pool.Start(runCtx)

	c.waitForCompletion(runCtx)

	pool.Stop()

	finalState := "completed"
	if ctx.Err() != nil {
		finalState = "cancelled"
	}
	if err := c.stateStore.CompleteRun(runCtx, run.ID, finalState); err != nil {
		return run.ID, fmt.Errorf("complete run: %w", err)
	}

	log.Printf("[coordinator] sync run %d completed (%s)", run.ID, finalState)
	return run.ID, nil
}

func (c *Coordinator) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
}

type workerStateAdapter struct {
	store state.StateStore
}

func (a *workerStateAdapter) ClaimTask(ctx context.Context, workerID string) (*worker.Task, error) {
	t, err := a.store.ClaimTask(ctx, workerID)
	if err != nil {
		return nil, err
	}
	return &worker.Task{
		ID:              t.ID,
		SyncRunID:       t.SyncRunID,
		TableID:         t.TableID,
		SchemaVersion:   t.SchemaVersion,
		PartitionID:     t.PartitionID,
		ShardIdx:        t.ShardIdx,
		State:           t.State,
		LeaseGeneration: t.LeaseGeneration,
	}, nil
}

func (a *workerStateAdapter) RenewLease(ctx context.Context, taskID string, generation int) (bool, error) {
	return a.store.RenewLease(ctx, taskID, generation)
}

func (a *workerStateAdapter) UpdateTaskState(ctx context.Context, taskID, state string, generation int) error {
	return a.store.UpdateTaskState(ctx, taskID, state, generation)
}

func (c *Coordinator) waitForCompletion(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	idleLoops := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending, _ := c.stateStore.ListTasksByState(ctx, "pending")
			assigned, _ := c.stateStore.ListTasksByState(ctx, "assigned")
			if len(pending) == 0 && len(assigned) == 0 {
				idleLoops++
				if idleLoops >= 2 {
					return
				}
			} else {
				idleLoops = 0
				log.Printf("[coordinator] %d pending, %d assigned tasks remaining", len(pending), len(assigned))
			}
		}
	}
}
