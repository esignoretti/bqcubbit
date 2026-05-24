package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/esignoretti/bqcubbit/internal/config"
	"github.com/esignoretti/bqcubbit/internal/state"
)

type Coordinator interface {
	RunOnce(ctx context.Context) (int64, error)
	Cancel()
}

type Scheduler struct {
	cfg         *config.Config
	coordinator Coordinator
	stateStore  state.StateStore
	cron        *cron.Cron
	jobLockName string
}

func NewScheduler(cfg *config.Config, coordinator Coordinator, stateStore state.StateStore) *Scheduler {
	return &Scheduler{
		cfg:         cfg,
		coordinator: coordinator,
		stateStore:  stateStore,
		jobLockName: "bqcubbit-sync-run",
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.cron = cron.New(cron.WithParser(cron.NewParser(
		cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)))

	_, err := s.cron.AddFunc(s.cfg.Scheduler.Cron, func() {
		if err := s.runSync(ctx); err != nil {
			log.Printf("[scheduler] sync run failed: %v", err)
		}
	})
	if err != nil {
		return fmt.Errorf("parse cron expression %q: %w", s.cfg.Scheduler.Cron, err)
	}

	s.cron.Start()
	log.Printf("[scheduler] started with cron: %s", s.cfg.Scheduler.Cron)

	<-ctx.Done()
	s.cron.Stop()
	return nil
}

func (s *Scheduler) runSync(ctx context.Context) error {
	lockTTL := 2 * time.Hour
	acquired, err := s.stateStore.AcquireJobLock(ctx, s.jobLockName, lockTTL)
	if err != nil {
		return fmt.Errorf("acquire job lock: %w", err)
	}
	if !acquired {
		switch s.cfg.Scheduler.OverlapPolicy {
		case "skip":
			log.Println("[scheduler] previous run still in progress — skipping")
			return nil
		case "cancel_and_restart":
			log.Println("[scheduler] previous run still in progress — cancelling")
			s.coordinator.Cancel()
			time.Sleep(5 * time.Second)
		default:
			log.Println("[scheduler] previous run still in progress — skipping")
			return nil
		}
	}
	defer s.stateStore.ReleaseJobLock(ctx, s.jobLockName)

	_, err = s.coordinator.RunOnce(ctx)
	return err
}
