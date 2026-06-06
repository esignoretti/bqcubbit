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

// stateToWorkerAdapter adapts state.StateStore to worker.StateStore.
type stateToWorkerAdapter struct {
	state.StateStore
}

func (a *stateToWorkerAdapter) ClaimTask(ctx context.Context, workerID string) (*worker.Task, error) {
	t, err := a.StateStore.ClaimTask(ctx, workerID)
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

type Coordinator struct {
	cfg        *config.Config
	stateStore state.StateStore
	workerStore worker.StateStore
	limiters   *rate.Limiters
	taskPool   *worker.Pool
	executor   worker.TaskExecutor

	mu         sync.Mutex
	running    bool
	currentRun *state.SyncRun
	cancel     context.CancelFunc
}

func NewCoordinator(cfg *config.Config, stateStore state.StateStore, limiters *rate.Limiters, executor worker.TaskExecutor) *Coordinator {
	return &Coordinator{
		cfg:         cfg,
		stateStore:  stateStore,
		workerStore: &stateToWorkerAdapter{StateStore: stateStore},
		limiters:    limiters,
		executor:    executor,
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

	staleIDs, err := c.stateStore.AbortStaleRuns(runCtx)
	if err != nil {
		log.Printf("[coordinator] warning: abort stale runs: %v", err)
	} else if len(staleIDs) > 0 {
		log.Printf("[coordinator] aborted %d stale runs", len(staleIDs))
		if n, err := c.stateStore.CleanupStaleTasks(runCtx, staleIDs); err == nil {
			log.Printf("[coordinator] cleaned %d stale tasks", n)
		}
	}

	run, err := c.stateStore.BeginRun(runCtx)
	if err != nil {
		return 0, fmt.Errorf("begin run: %w", err)
	}
	c.currentRun = run
	log.Printf("[coordinator] started sync run %d", run.ID)

	if n, err := c.stateStore.ResetExpiredLeases(runCtx); err == nil && n > 0 {
		log.Printf("[coordinator] reset %d expired leases", n)
	}

	pool := worker.NewPool(
		c.cfg.WorkerPool.MinWorkers,
		c.cfg.WorkerPool.MaxWorkers,
		c.executor,
		c.workerStore,
	)
	c.taskPool = pool

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

func (c *Coordinator) waitForCompletion(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	idleLoops := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, _ := c.stateStore.ListTasksByState(ctx, "pending")
			assigned, _ := c.stateStore.ListTasksByState(ctx, "assigned")
			if len(tasks) == 0 && len(assigned) == 0 {
				idleLoops++
				if idleLoops >= 2 {
					return
				}
			} else {
				idleLoops = 0
				log.Printf("[coordinator] %d pending, %d assigned tasks remaining", len(tasks), len(assigned))
			}
		}
	}
}
