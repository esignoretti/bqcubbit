package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/esignoretti/bqcubbit/internal/config"
	"github.com/esignoretti/bqcubbit/internal/state"
)

type mockCoordinator struct {
	runCount int32
}

func (m *mockCoordinator) RunOnce(ctx context.Context) (int64, error) {
	atomic.AddInt32(&m.runCount, 1)
	return int64(m.runCount), nil
}

func (m *mockCoordinator) Cancel() {}

type mockState struct {
	state.StateStore
}

func (m *mockState) AcquireJobLock(ctx context.Context, lockName string, ttl time.Duration) (bool, error) {
	return true, nil
}

func (m *mockState) ReleaseJobLock(ctx context.Context, lockName string) error {
	return nil
}

func TestScheduler(t *testing.T) {
	cfg := config.Default()
	cfg.Scheduler.Cron = "@every 1s"

	coord := &mockCoordinator{}
	ms := &mockState{}
	s := NewScheduler(cfg, coord, ms)

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	s.Start(ctx)

	if atomic.LoadInt32(&coord.runCount) < 2 {
		t.Fatalf("expected at least 2 runs in 2.5s, got %d", coord.runCount)
	}
}
