package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type Pool struct {
	minWorkers int
	maxWorkers int
	workers    []*Worker
	executor   TaskExecutor
	state      StateStore
	wg         sync.WaitGroup
}

func NewPool(minWorkers, maxWorkers int, executor TaskExecutor, state StateStore) *Pool {
	return &Pool{minWorkers: minWorkers, maxWorkers: maxWorkers, executor: executor, state: state}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.minWorkers; i++ {
		w := New(fmt.Sprintf("worker-%d", i), p.executor, p.state)
		p.workers = append(p.workers, w)
		p.wg.Add(1)
		go func(w *Worker) {
			defer p.wg.Done()
			w.Start(ctx)
		}(w)
	}
	log.Printf("[pool] started %d workers", p.minWorkers)
}

func (p *Pool) Stop() {
	for _, w := range p.workers {
		w.Stop()
	}
	p.wg.Wait()
	log.Printf("[pool] all workers stopped")
}

func (p *Pool) ScaleUp(ctx context.Context, n int) {
	for i := len(p.workers); i < p.maxWorkers && n > 0; i++ {
		w := New(fmt.Sprintf("worker-%d", i), p.executor, p.state)
		p.workers = append(p.workers, w)
		p.wg.Add(1)
		go func(w *Worker) {
			defer p.wg.Done()
			w.Start(ctx)
		}(w)
		n--
	}
}
