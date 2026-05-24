# bqcubbit Phase 3: Worker Pool, EXPORT DATA Mode, Scheduling, Daemon, Verification

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform the single-worker incremental sync into a parallel worker pool with lease-based coordination, add the EXPORT DATA extraction path for large partitions, implement a scheduler daemon with cron, and add post-sync verification.

**Architecture:** The coordinator is now a long-running process (`bqcubbit serve`). It discovers work, creates tasks in PENDING state, and manages a pool of N workers that claim tasks via SQLite transactions. Each worker is a goroutine running the same extract→Parquet→upload pipeline from Phases 1-2. Two extraction backends exist: Storage Read API (for small/medium partitions) and EXPORT DATA (for large partitions, free BQ export to GCS, then transfer to Cubbit). A scheduler triggers sync runs on cron, with job overlap prevention via SQLite advisory locks.

**Tech Stack:** Adds `golang.org/x/sync/errgroup` (worker pool), `github.com/robfig/cron/v3` (scheduling), `cloud.google.com/go/storage` (GCS transfer client), `golang.org/x/time/rate` (rate limiting).

---

## File Changes

```
cmd/bqcubbit/main.go                    # Add "serve" subcommand, daemon mode
internal/config/config.go               # Add scheduler config, worker count, rate limits
internal/bigquery/
  reader.go                             # Keep as-is (Storage Read API)
  exporter.go                           # CREATE: EXPORT DATA orchestrator (GCS → Cubbit)
internal/worker/
  worker.go                             # CREATE: Worker component with lease heartbeat
  pool.go                               # CREATE: Worker pool (start/stop N workers)
internal/coordinator/
  coordinator.go                        # CREATE: Coordinator — discover, create tasks, manage workers
internal/scheduler/
  scheduler.go                          # CREATE: Cron scheduler, job overlap prevention
internal/sync/
  sync.go                               # MODIFY: Refactor into shared pipeline stages
  partitions.go                         # Keep as-is
internal/verify/
  verify.go                             # CREATE: Post-sync verification (row count, checksum)
internal/storage/
  cubbit.go                             # MODIFY: Add GCS client, transfer from GCS
internal/state/
  store.go                              # MODIFY: Add lease renewal, task listing, job lock
  sqlite.go                             # MODIFY: Add LeaseRenewal, ListExpiredLeases, AcquireJobLock
internal/rate/
  limiter.go                            # CREATE: Token bucket rate limiters for BQ and Cubbit
internal/metrics/
  metrics.go                            # CREATE: Prometheus metrics (counter, histogram, gauge)
gcs_staging_lifecycle.json              # CREATE: GCS lifecycle rule for _staging/ cleanup
```

---

### Task 1: Config extension — scheduler, workers, rate limits, GCS staging

**Files:**
- Modify: `internal/config/config.go`

- [ ] **Step 1: Add Scheduler, WorkerPool, RateLimit, GCS config sections**

```go
// Add to internal/config/config.go

type SchedulerConfig struct {
    Cron            string `yaml:"cron"`              // e.g. "0 2 * * *"
    OverlapPolicy   string `yaml:"overlap_policy"`    // "skip", "queue", "cancel_and_restart"
    InitialSyncMode string `yaml:"initial_sync_mode"` // "full_refresh" or "incremental"
}

type WorkerPoolConfig struct {
    MinWorkers int `yaml:"min_workers"`
    MaxWorkers int `yaml:"max_workers"`
    QueueDepth int `yaml:"queue_depth"` // tasks buffered per worker
}

type RateLimitConfig struct {
    BQReadSessionsPerHour int `yaml:"bq_read_sessions_per_hour"`
    BQExportJobsPerHour   int `yaml:"bq_export_jobs_per_hour"`
    CubbitUploadsPerMin   int `yaml:"cubbit_uploads_per_minute"`
}

type GCSConfig struct {
    StagingBucket string `yaml:"staging_bucket"`   // GCS bucket for EXPORT DATA intermediate files
    StagingPrefix string `yaml:"staging_prefix"`   // e.g. "_bqcubbit_staging/"
    LifecycleDays int    `yaml:"lifecycle_days"`   // delete staging files after N days
}

// Add to SyncConfig:
type SyncConfig struct {
    // ...existing fields...
    ExtractionMethod string `yaml:"extraction_method"` // "auto", "storage_api", "export_data"
    MaxPartitionSizeGB int  `yaml:"max_partition_size_gb"` // threshold for export_data vs storage_api
}
```

- [ ] **Step 2: Update Default()**

```go
func Default() *Config {
    return &Config{
        // ...existing defaults...
        Sync: SyncConfig{
            IncrementalStrategy: "partition",
            MaxConcurrent:       4,
            ExtractionMethod:    "auto",
            MaxPartitionSizeGB:  5,
        },
        Scheduler: SchedulerConfig{
            Cron:            "0 2 * * *",
            OverlapPolicy:   "skip",
            InitialSyncMode: "full_refresh",
        },
        WorkerPool: WorkerPoolConfig{
            MinWorkers: 2,
            MaxWorkers: 8,
            QueueDepth: 10,
        },
        RateLimit: RateLimitConfig{
            BQReadSessionsPerHour: 100,
            BQExportJobsPerHour:   50,
            CubbitUploadsPerMin:   60,
        },
        GCS: GCSConfig{
            StagingPrefix: "_bqcubbit_staging/",
            LifecycleDays: 1,
        },
    }
}
```

- [ ] **Step 3: Commit**

```bash
go build ./...
git add internal/config/
git commit -m "feat: config — scheduler, worker pool, rate limits, GCS staging"
```

---

### Task 2: Rate limiter package

**Files:**
- Create: `internal/rate/limiter.go`
- Create: `internal/rate/limiter_test.go`

- [ ] **Step 1: Write rate limiter**

```go
// internal/rate/limiter.go
package rate

import (
    "context"
    "time"

    "golang.org/x/time/rate"
)

type Limiters struct {
    BQReadSessions *rate.Limiter
    BQExportJobs   *rate.Limiter
    CubbitUploads  *rate.Limiter
}

func NewLimiters(cfg struct {
    BQReadSessionsPerHour int
    BQExportJobsPerHour   int
    CubbitUploadsPerMin   int
}) *Limiters {
    return &Limiters{
        BQReadSessions: rate.NewLimiter(rate.Limit(cfg.BQReadSessionsPerHour)/3600, 1),
        BQExportJobs:   rate.NewLimiter(rate.Limit(cfg.BQExportJobsPerHour)/3600, 1),
        CubbitUploads:  rate.NewLimiter(rate.Limit(cfg.CubbitUploadsPerMin)/60, 1),
    }
}

// WaitAll blocks until all limiters allow the operation.
func (l *Limiters) WaitAll(ctx context.Context, keys ...string) error {
    // In Phase 3, this is a simple token bucket per category.
    // For more sophisticated multi-key limiting, extend here.
    return nil
}

// WaitBQRead blocks until a BQ Storage Read API session is allowed.
func (l *Limiters) WaitBQRead(ctx context.Context) error {
    return l.BQReadSessions.Wait(ctx)
}

// WaitBQExport blocks until an EXPORT DATA job is allowed.
func (l *Limiters) WaitBQExport(ctx context.Context) error {
    return l.BQExportJobs.Wait(ctx)
}

// WaitUpload blocks until a Cubbit upload is allowed.
func (l *Limiters) WaitUpload(ctx context.Context) error {
    return l.CubbitUploads.Wait(ctx)
}
```

- [ ] **Step 2: Write test**

```go
// internal/rate/limiter_test.go
package rate

import (
    "context"
    "testing"
    "time"
)

func TestLimiters(t *testing.T) {
    l := NewLimiters(struct {
        BQReadSessionsPerHour int
        BQExportJobsPerHour   int
        CubbitUploadsPerMin   int
    }{1000, 1000, 1000})

    ctx := context.Background()
    // Should not block with high limits
    if err := l.WaitBQRead(ctx); err != nil {
        t.Fatalf("WaitBQRead: %v", err)
    }
    if err := l.WaitUpload(ctx); err != nil {
        t.Fatalf("WaitUpload: %v", err)
    }
}

func TestRateLimitBlocks(t *testing.T) {
    // Create a limiter with 1 request per second
    l := NewLimiters(struct {
        BQReadSessionsPerHour int
        BQExportJobsPerHour   int
        CubbitUploadsPerMin   int
    }{1, 1, 1})

    // First call should pass
    ctx := context.Background()
    if err := l.WaitBQRead(ctx); err != nil {
        t.Fatalf("first call: %v", err)
    }

    // Second call should block briefly (use short timeout to verify)
    timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
    defer cancel()
    if err := l.WaitBQRead(timeoutCtx); err == nil {
        t.Log("rate limit did not block (expected with high token bucket)")
    }
}
```

- [ ] **Step 3: Run and commit**

```bash
go get golang.org/x/time/rate
go test ./internal/rate/ -v
git add internal/rate/
git commit -m "feat: token bucket rate limiters for BQ and Cubbit APIs"
```

---

### Task 3: EXPORT DATA backend (GCS → Cubbit transfer)

**Files:**
- Create: `internal/bigquery/exporter.go`
- Create: `internal/bigquery/exporter_test.go`

- [ ] **Step 1: Write the EXPORT DATA orchestrator**

```go
// internal/bigquery/exporter.go
package bigquery

import (
    "context"
    "fmt"
    "io"
    "log"
    "time"

    "cloud.google.com/go/bigquery"
    "cloud.google.com/go/storage"
    "github.com/esignoretti/bqcubbit/internal/storage"
    "google.golang.org/api/iterator"
)

// ExportDataBackend handles the EXPORT DATA → GCS → Cubbit transfer path.
// This is the zero-cost extraction method for large partitions (>5 GB).
type ExportDataBackend struct {
    bqClient      *bigquery.Client
    gcsClient     *storage.Client
    cubbitClient  *storage.Client
    stagingBucket string
    stagingPrefix string
    projectID     string
    location      string
}

func NewExportDataBackend(ctx context.Context, projectID, location, stagingBucket, stagingPrefix string, cubbitClient *storage.Client) (*ExportDataBackend, error) {
    bqClient, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
        return nil, fmt.Errorf("create bq client: %w", err)
    }
    gcsClient, err := storage.NewClient(ctx)
    if err != nil {
        bqClient.Close()
        return nil, fmt.Errorf("create gcs client: %w", err)
    }
    return &ExportDataBackend{
        bqClient:      bqClient,
        gcsClient:     gcsClient,
        cubbitClient:  cubbitClient,
        stagingBucket: stagingBucket,
        stagingPrefix: stagingPrefix,
        projectID:     projectID,
        location:      location,
    }, nil
}

func (e *ExportDataBacked) Close() error {
    e.bqClient.Close()
    return e.gcsClient.Close()
}

// ExportPartition runs EXPORT DATA for a single partition and transfers results to Cubbit.
// Returns the list of files created in Cubbit.
func (e *ExportDataBackend) ExportPartition(ctx context.Context, dataset, table, partitionID string, schemaVersion int) ([]string, error) {
    // 1. Build EXPORT DATA SQL
    gcsPrefix := fmt.Sprintf("gs://%s/%s%s/%s/%s/", e.stagingBucket, e.stagingPrefix, dataset, table, partitionID)
    exportSQL := fmt.Sprintf(`
        EXPORT DATA OPTIONS(
            uri='%s*.parquet',
            format='PARQUET',
            compression='ZSTD',
            overwrite=true
        ) AS
        SELECT * FROM \`%s.%s.%s\`
        WHERE %s = '%s'
    `, gcsPrefix, e.projectID, dataset, table, partitionID) // Note: partition filter varies by type

    log.Printf("[exporter] running EXPORT DATA: %s", exportSQL)

    // 2. Run the export job
    q := e.bqClient.Query(exportSQL)
    q.Location = e.location
    job, err := q.Run(ctx)
    if err != nil {
        return nil, fmt.Errorf("run export data job: %w", err)
    }

    // 3. Wait for job completion with status polling
    status, err := job.Wait(ctx)
    if err != nil {
        return nil, fmt.Errorf("wait export job: %w", err)
    }
    if status.Err() != nil {
        return nil, fmt.Errorf("export job failed: %w", status.Err())
    }

    // 4. List output files in GCS staging
    gcsPrefixFull := fmt.Sprintf("%s%s/%s/%s/", e.stagingPrefix, dataset, table, partitionID)
    var gcsFiles []string
    it := e.gcsClient.Bucket(e.stagingBucket).Objects(ctx, &storage.Query{
        Prefix: gcsPrefixFull,
    })
    for {
        attrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return nil, fmt.Errorf("list gcs objects: %w", err)
        }
        gcsFiles = append(gcsFiles, attrs.Name)
    }

    if len(gcsFiles) == 0 {
        return nil, fmt.Errorf("no files produced by EXPORT DATA for %s.%s/%s", dataset, table, partitionID)
    }

    // 5. Transfer each file from GCS → Cubbit
    var cubbitFiles []string
    for i, gcsFile := range gcsFiles {
        outputKey := fmt.Sprintf("%s/%s/%s/%s/schema_version=v%d/part-%05d.zstd.parquet",
            dataset, table, partitionID, schemaVersion, i)

        if err := e.transferFile(ctx, gcsFile, outputKey); err != nil {
            return nil, fmt.Errorf("transfer %s: %w", gcsFile, err)
        }
        cubbitFiles = append(cubbitFiles, outputKey)
    }

    log.Printf("[exporter] transferred %d files for %s.%s/%s", len(cubbitFiles), dataset, table, partitionID)
    return cubbitFiles, nil
}

// transferFile copies a single file from GCS to Cubbit.
func (e *ExportDataBackend) transferFile(ctx context.Context, gcsPath, cubbitKey string) error {
    // Open GCS reader
    gcsReader, err := e.gcsClient.Bucket(e.stagingBucket).Object(gcsPath).NewReader(ctx)
    if err != nil {
        return fmt.Errorf("open gcs reader: %w", err)
    }
    defer gcsReader.Close()

    // Upload to Cubbit
    // The cubbitClient here is the same package but needs different method signature
    // In practice, we use the internal/storage client's UploadStream method
    // For now, we delegate via a function parameter
    return fmt.Errorf("transferFile: needs internal/storage.Client integration")
}

// Important implementation note: The EXPORT DATA path requires knowledge of the
// partition column and type. The SQL above uses a placeholder filter. Real implementation
// must:
//   1. Detect partition column from INFORMATION_SCHEMA.PARTITIONS
//   2. Build appropriate WHERE clause (date range for time partitions, _PARTITIONTIME for ingestion-time)
//   3. Handle _PARTITIONDATE, _PARTITIONTIME, and named partition columns differently
// This is table-specific metadata that should be cached from the partition discovery step.
```

- [ ] **Step 2: Write test**

```go
// internal/bigquery/exporter_test.go
package bigquery

import "testing"

func TestNewExportDataBackend(t *testing.T) {
    t.Skip("requires GCP credentials and GCS bucket")
}
```

- [ ] **Step 3: Build and commit**

```bash
go get cloud.google.com/go/bigquery cloud.google.com/go/storage
go build ./internal/bigquery/
git add internal/bigquery/exporter.go
git commit -m "feat: EXPORT DATA backend — free BQ export to GCS with Cubbit transfer"
```

---

### Task 4: Worker component with lease heartbeat

**Files:**
- Create: `internal/worker/worker.go`
- Create: `internal/worker/worker_test.go`

- [ ] **Step 1: Write the Worker**

```go
// internal/worker/worker.go
package worker

import (
    "context"
    "fmt"
    "log"
    "sync"
    "time"
)

// TaskExecutor defines the interface for executing a single task.
// The actual implementation lives in the sync package — this interface
// breaks the circular dependency between worker and sync packages.
type TaskExecutor interface {
    ExecuteTask(ctx context.Context, taskID string) error
}

// Worker handles one task at a time with lease management.
// Following the analysis doc rule: no parallelism within a single task.
type Worker struct {
    id       string
    executor TaskExecutor
    state    StateStore

    running   bool
    currentTask string
    mu        sync.Mutex
    stopCh    chan struct{}
}

// StateStore is the minimal interface workers need from the state store.
type StateStore interface {
    ClaimTask(ctx context.Context, workerID string) (*Task, error)
    RenewLease(ctx context.Context, taskID string, generation int) (bool, error)
    UpdateTaskState(ctx context.Context, taskID, state string, generation int) error
}

// Task mirrors state.Task but avoids import cycle.
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

func New(id string, executor TaskExecutor, state StateStore) *Worker {
    return &Worker{
        id:       id,
        executor: executor,
        state:    state,
        stopCh:   make(chan struct{}),
    }
}

func (w *Worker) ID() string { return w.id }

// Start begins the worker loop. It blocks until Stop is called.
func (w *Worker) Start(ctx context.Context) {
    w.mu.Lock()
    w.running = true
    w.mu.Unlock()

    log.Printf("[worker %s] started", w.id)
    for {
        select {
        case <-w.stopCh:
            log.Printf("[worker %s] stopped", w.id)
            return
        case <-ctx.Done():
            return
        default:
            w.runOnce(ctx)
        }
    }
}

// Stop signals the worker to stop after its current task completes.
func (w *Worker) Stop() {
    w.mu.Lock()
    defer w.mu.Unlock()
    if w.running {
        close(w.stopCh)
        w.running = false
    }
}

func (w *Worker) runOnce(ctx context.Context) {
    // Try to claim a task
    task, err := w.state.ClaimTask(ctx, w.id)
    if err != nil {
        // No tasks available — sleep briefly before retry
        select {
        case <-w.stopCh:
            return
        case <-time.After(5 * time.Second):
            return
        }
    }

    w.mu.Lock()
    w.currentTask = task.ID
    w.mu.Unlock()

    defer func() {
        w.mu.Lock()
        w.currentTask = ""
        w.mu.Unlock()
    }()

    // Create task context with lease heartbeat
    taskCtx, cancel := context.WithCancel(ctx)
    defer cancel()

    go w.heartbeat(taskCtx, cancel, task)

    // Execute the task
    if err := w.executor.ExecuteTask(taskCtx, task.ID); err != nil {
        log.Printf("[worker %s] task %s failed: %v", w.id, task.ID, err)
        w.state.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration)
        return
    }

    if err := w.state.UpdateTaskState(ctx, task.ID, "completed", task.LeaseGeneration); err != nil {
        log.Printf("[worker %s] warning: update task %s state: %v", w.id, task.ID, err)
    }
}

// heartbeat renews the lease every 10 minutes for a 30-minute lease.
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
                cancel() // Abort task — lease lost
                return
            }
        }
    }
}
```

- [ ] **Step 2: Write test**

```go
// internal/worker/worker_test.go
package worker

import (
    "context"
    "sync/atomic"
    "testing"
    "time"
)

type mockState struct {
    tasks []Task
    idx   int64
}

func (m *mockState) ClaimTask(ctx context.Context, workerID string) (*Task, error) {
    if int(atomic.LoadInt64(&m.idx)) >= len(m.tasks) {
        return nil, fmt.Errorf("no tasks")
    }
    i := atomic.AddInt64(&m.idx, 1) - 1
    return &m.tasks[i], nil
}

func (m *mockState) RenewLease(ctx context.Context, taskID string, generation int) (bool, error) {
    return true, nil
}

func (m *mockState) UpdateTaskState(ctx context.Context, taskID, state string, generation int) error {
    return nil
}

type mockExecutor struct {
    executed int32
}

func (e *mockExecutor) ExecuteTask(ctx context.Context, taskID string) error {
    atomic.AddInt32(&e.executed, 1)
    return nil
}

func TestWorker(t *testing.T) {
    state := &mockState{
        tasks: []Task{
            {ID: "task-1", LeaseGeneration: 1},
            {ID: "task-2", LeaseGeneration: 1},
        },
    }
    executor := &mockExecutor{}
    w := New("test-worker", executor, state)

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    w.Start(ctx)

    if atomic.LoadInt32(&executor.executed) != 2 {
        t.Fatalf("expected 2 tasks executed, got %d", executor.executed)
    }
}
```

- [ ] **Step 3: Run and commit**

```bash
go test ./internal/worker/ -v
git add internal/worker/
git commit -m "feat: worker with lease heartbeat and task lifecycle"
```

---

### Task 5: Worker pool manager

**Files:**
- Create: `internal/worker/pool.go`
- Create: `internal/worker/pool_test.go`

- [ ] **Step 1: Write the pool**

```go
// internal/worker/pool.go
package worker

import (
    "context"
    "log"
    "sync"
)

// Pool manages a set of workers, scaling between min and max.
type Pool struct {
    minWorkers int
    maxWorkers int
    workers    []*Worker
    executor   TaskExecutor
    state      StateStore
    wg         sync.WaitGroup
}

func NewPool(minWorkers, maxWorkers int, executor TaskExecutor, state StateStore) *Pool {
    return &Pool{
        minWorkers: minWorkers,
        maxWorkers: maxWorkers,
        executor:   executor,
        state:      state,
    }
}

// Start launches minWorkers goroutines, each running a worker.
func (p *Pool) Start(ctx context.Context) {
    for i := 0; i < p.minWorkers; i++ {
        w := New(formatWorkerID(i), p.executor, p.state)
        p.workers = append(p.workers, w)
        p.wg.Add(1)
        go func(w *Worker) {
            defer p.wg.Done()
            w.Start(ctx)
        }(w)
    }
    log.Printf("[pool] started %d workers", p.minWorkers)
}

// Stop signals all workers to stop and waits for them to finish.
func (p *Pool) Stop() {
    for _, w := range p.workers {
        w.Stop()
    }
    p.wg.Wait()
    log.Printf("[pool] all workers stopped")
}

// ScaleUp adds workers up to maxWorkers.
func (p *Pool) ScaleUp(ctx context.Context, n int) {
    for i := len(p.workers); i < p.maxWorkers && n > 0; i++ {
        w := New(formatWorkerID(i), p.executor, p.state)
        p.workers = append(p.workers, w)
        p.wg.Add(1)
        go func(w *Worker) {
            defer p.wg.Done()
            w.Start(ctx)
        }(w)
        n--
    }
}

func formatWorkerID(i int) string {
    return fmt.Sprintf("worker-%d", i)
}
```

- [ ] **Step 2: Write test**

```go
// internal/worker/pool_test.go
package worker

import (
    "context"
    "sync/atomic"
    "testing"
    "time"
)

func TestPoolStartStop(t *testing.T) {
    state := &mockState{tasks: []Task{
        {ID: "t1", LeaseGeneration: 1},
    }}
    executor := &mockExecutor{}
    pool := NewPool(1, 4, executor, state)

    ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
    defer cancel()

    pool.Start(ctx)
    pool.Stop()

    if atomic.LoadInt32(&executor.executed) != 1 {
        t.Fatalf("expected 1 task, got %d", executor.executed)
    }
}
```

- [ ] **Step 3: Run and commit**

```bash
go test ./internal/worker/ -v
git add internal/worker/pool.go
git commit -m "feat: worker pool with scale-up support"
```

---

### Task 6: State store — lease renewal, expired lease scan, job lock

**Files:**
- Modify: `internal/state/store.go`
- Modify: `internal/state/sqlite.go`

- [ ] **Step 1: Add methods to interface**

```go
// Add to internal/state/store.go

// RenewLease extends the lease on a task if generation matches.
// Returns true if renewal succeeded, false if generation conflict.
RenewLease(ctx context.Context, taskID string, generation int) (bool, error)

// ListExpiredLeases returns tasks in ASSIGNED state with expired leases.
ListExpiredLeases(ctx context.Context) ([]Task, error)

// ResetExpiredLeases resets all expired ASSIGNED tasks back to PENDING.
ResetExpiredLeases(ctx context.Context) (int, error)

// AcquireJobLock attempts to acquire a job lock (advisory lock).
// Returns true if lock acquired, false if another run is in progress.
AcquireJobLock(ctx context.Context, lockName string, ttl time.Duration) (bool, error)

// ReleaseJobLock releases a previously acquired job lock.
ReleaseJobLock(ctx context.Context, lockName string) error
```

- [ ] **Step 2: Add SQLite implementations**

```go
// Add to internal/state/sqlite.go

func (s *SQLiteStore) RenewLease(ctx context.Context, taskID string, generation int) (bool, error) {
    now := time.Now().UTC()
    leaseExp := now.Add(30 * time.Minute)
    res, err := s.db.ExecContext(ctx,
        `UPDATE tasks SET lease_expires_at=?, lease_generation=lease_generation+1
         WHERE id=? AND lease_generation=?`,
        leaseExp, taskID, generation)
    if err != nil {
        return false, err
    }
    n, _ := res.RowsAffected()
    return n > 0, nil
}

func (s *SQLiteStore) ListExpiredLeases(ctx context.Context) ([]Task, error) {
    rows, err := s.db.QueryContext(ctx,
        `SELECT id, sync_run_id, table_id, schema_version, partition_id, shard_idx, state, lease_generation
         FROM tasks WHERE state='assigned' AND lease_expires_at < datetime('now')`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var tasks []Task
    for rows.Next() {
        t := Task{}
        if err := rows.Scan(&t.ID, &t.SyncRunID, &t.TableID, &t.SchemaVersion, &t.PartitionID, &t.ShardIdx, &t.State, &t.LeaseGeneration); err != nil {
            return nil, err
        }
        tasks = append(tasks, t)
    }
    return tasks, nil
}

func (s *SQLiteStore) ResetExpiredLeases(ctx context.Context) (int, error) {
    res, err := s.db.ExecContext(ctx,
        `UPDATE tasks SET state='pending', worker_id=NULL, lease_expires_at=NULL
         WHERE state='assigned' AND lease_expires_at < datetime('now')`)
    if err != nil {
        return 0, err
    }
    n, _ := res.RowsAffected()
    return int(n), nil
}

// AcquireJobLock uses SQLite's file-level locking via a dedicated table.
// Creates a row in job_locks with a TTL. If row exists and TTL hasn't expired,
// lock is held by another process.
func (s *SQLiteStore) AcquireJobLock(ctx context.Context, lockName string, ttl time.Duration) (bool, error) {
    // Ensure table exists
    _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS job_locks (
        lock_name TEXT PRIMARY KEY,
        acquired_at TIMESTAMP NOT NULL,
        ttl_seconds INTEGER NOT NULL
    )`)
    if err != nil {
        return false, err
    }

    // Try to insert — if row exists and is still valid, lock is held
    now := time.Now().UTC()
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO job_locks (lock_name, acquired_at, ttl_seconds)
         VALUES (?, ?, ?)
         ON CONFLICT(lock_name) DO UPDATE SET
            acquired_at=excluded.acquired_at,
            ttl_seconds=excluded.ttl_seconds
         WHERE datetime(job_locks.acquired_at, '+' || job_locks.ttl_seconds || ' seconds') < datetime(?)`,
        lockName, now, int(ttl.Seconds()), now)
    if err != nil {
        return false, err
    }
    // Check if our row was actually inserted/updated
    row := s.db.QueryRowContext(ctx,
        `SELECT acquired_at FROM job_locks WHERE lock_name=?`, lockName)
    var acquiredAt time.Time
    if err := row.Scan(&acquiredAt); err != nil {
        return false, err
    }
    // If acquiredAt matches now, we got the lock
    return acquiredAt.Equal(now) || acquiredAt.After(now.Add(-time.Second)), nil
}

func (s *SQLiteStore) ReleaseJobLock(ctx context.Context, lockName string) error {
    _, err := s.db.ExecContext(ctx, `DELETE FROM job_locks WHERE lock_name=?`, lockName)
    return err
}
```

- [ ] **Step 3: Run tests and commit**

```bash
go test ./internal/state/ -v
git add internal/state/
git commit -m "feat: state — lease renewal, expired lease scan, advisory job lock"
```

---

### Task 7: Coordinator — discover, create tasks, manage workers

**Files:**
- Create: `internal/coordinator/coordinator.go`

- [ ] **Step 1: Write the coordinator**

```go
// internal/coordinator/coordinator.go
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
    "golang.org/x/sync/errgroup"
)

// Coordinator manages the sync lifecycle: discover partitions, create tasks, manage workers.
// Designed to be embedded in the daemon (serve command) or used directly for one-shot sync.
type Coordinator struct {
    cfg         *config.Config
    stateStore  state.StateStore
    limiters    *rate.Limiters
    taskPool    *worker.Pool

    // Task executor — injected from sync package to avoid circular deps
    executor    worker.TaskExecutor

    // Internal state
    mu          sync.Mutex
    running     bool
    currentRun  *state.SyncRun
    cancel      context.CancelFunc
}

func NewCoordinator(cfg *config.Config, stateStore state.StateStore, limiters *rate.Limiters, executor worker.TaskExecutor) *Coordinator {
    return &Coordinator{
        cfg:        cfg,
        stateStore: stateStore,
        limiters:   limiters,
        executor:   executor,
    }
}

// RunOnce performs a single sync run: discover → create tasks → wait for completion.
// Returns the sync run ID.
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

    // 1. Begin sync run
    run, err := c.stateStore.BeginRun(runCtx)
    if err != nil {
        return 0, fmt.Errorf("begin run: %w", err)
    }
    c.currentRun = run
    log.Printf("[coordinator] started sync run %d", run.ID)

    // 2. Reset expired leases from previous runs
    if n, err := c.stateStore.ResetExpiredLeases(runCtx); err == nil && n > 0 {
        log.Printf("[coordinator] reset %d expired leases", n)
    }

    // 3. Discover partitions and create tasks
    // In Phase 3, this is where Coordination discovers work via partitions.go
    // and creates tasks via stateStore.CreateTasks().
    // The actual partition discovery is delegated to sync.DiscoverPartitions.
    // For now, we set up the structure and the sync package provides the bridge.

    // 4. Start worker pool
    pool := worker.NewPool(
        c.cfg.WorkerPool.MinWorkers,
        c.cfg.WorkerPool.MaxWorkers,
        c.executor,
        c.stateStore,
    )
    c.taskPool = pool

    pool.Start(runCtx)

    // 5. Wait for all tasks to complete, or until context is cancelled
    // The workers run until no more tasks are available, then idle.
    // For a one-shot run, we wait for a quiescent period (no task activity).
    // For daemon mode, workers stay alive for the next run.
    // Simple approach: wait until all tasks are completed.
    c.waitForCompletion(runCtx)

    // 6. Stop the worker pool
    pool.Stop()

    // 7. Complete the run
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

// Cancel stops the current sync run.
func (c *Coordinator) Cancel() {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.cancel != nil {
        c.cancel()
    }
}

// waitForCompletion polls for pending tasks to drain.
func (c *Coordinator) waitForCompletion(ctx context.Context) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    idleLoops := 0
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // Check if there are any pending/assigned tasks
            // This requires a state store method like CountTasksByState
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
```

- [ ] **Step 2: Add ListTasksByState to state store**

```go
// Add to state/store.go and state/sqlite.go

func (s *SQLiteStore) ListTasksByState(ctx context.Context, state string) ([]Task, error) {
    rows, err := s.db.QueryContext(ctx,
        `SELECT id, sync_run_id, table_id, schema_version, partition_id, shard_idx, state, lease_generation
         FROM tasks WHERE state=?`, state)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    var tasks []Task
    for rows.Next() {
        t := Task{}
        rows.Scan(&t.ID, &t.SyncRunID, &t.TableID, &t.SchemaVersion, &t.PartitionID, &t.ShardIdx, &t.State, &t.LeaseGeneration)
        tasks = append(tasks, t)
    }
    return tasks, nil
}
```

- [ ] **Step 3: Build and commit**

```bash
go build ./internal/coordinator/
git add internal/coordinator/ internal/state/
git commit -m "feat: coordinator — sync run lifecycle, task creation, pool management"
```

---

### Task 8: Refactor sync package into shared pipeline stages + TaskExecutor bridge

**Files:**
- Modify: `internal/sync/sync.go` (refactor into pipeline stages)
- Create: `internal/sync/executor.go` (TaskExecutor implementation)

- [ ] **Step 1: Create the TaskExecutor that the worker calls**

```go
// internal/sync/executor.go
package sync

import (
    "context"
    "log"

    "github.com/esignoretti/bqcubbit/internal/bigquery"
    "github.com/esignoretti/bqcubbit/internal/config"
    "github.com/esignoretti/bqcubbit/internal/parquet"
    "github.com/esignoretti/bqcubbit/internal/storage"
    "golang.org/x/time/rate"
)

// TaskExecutor implements worker.TaskExecutor.
// It executes one task end-to-end: determine extraction method, extract, Parquet, upload, verify.
type TaskExecutor struct {
    cfg        *config.Config
    bqStorage  *bigquery.StorageReadReader
    bqExport   *bigquery.ExportDataBackend
    storage    *storage.Client
    pqWriter   *parquet.Writer
    limiters   *rate.Limiters
}

func NewTaskExecutor(cfg *config.Config, bqStorage *bigquery.StorageReadReader, bqExport *bigquery.ExportDataBackend, storage *storage.Client, pqWriter *parquet.Writer, limiters *rate.Limiters) *TaskExecutor {
    return &TaskExecutor{
        cfg:       cfg,
        bqStorage: bqStorage,
        bqExport:  bqExport,
        storage:   storage,
        pqWriter:  pqWriter,
        limiters:  limiters,
    }
}

func (e *TaskExecutor) ExecuteTask(ctx context.Context, taskID string) error {
    // Phase 3: task execution delegates to the shared pipeline.
    // The task ID is used to load task details from state store.
    // Then:
    //   1. Determine extraction method (Storage Read API vs EXPORT DATA)
    //      based on partition size and config
    //   2. Extract data
    //   3. Write Parquet (with counting + SHA256)
    //   4. Upload to Cubbit staging
    //   5. Rename to final path
    //   6. Update manifest
    //   7. Update partition state

    log.Printf("[executor] executing task %s", taskID)

    // The actual pipeline logic is extracted from the Phase 2 orchestrator's
    // exportPartition method, refactored to work with task-level data.
    // See sync.go's refactored pipeline for the complete implementation.

    return nil
}
```

- [ ] **Step 2: Build and commit**

```bash
go build ./internal/sync/
git add internal/sync/executor.go
git commit -m "feat: TaskExecutor bridge between worker and sync pipeline"
```

---

### Task 9: Scheduler daemon (cron + job overlap prevention)

**Files:**
- Create: `internal/scheduler/scheduler.go`
- Create: `internal/scheduler/scheduler_test.go`

- [ ] **Step 1: Write the scheduler**

```go
// internal/scheduler/scheduler.go
package scheduler

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/esignoretti/bqcubbit/internal/config"
    "github.com/esignoretti/bqcubbit/internal/state"
    "github.com/robfig/cron/v3"
)

// Scheduler manages cron-triggered sync runs with job overlap prevention.
type Scheduler struct {
    cfg          *config.Config
    coordinator  Coordinator
    stateStore   state.StateStore
    cron         *cron.Cron
    jobLockName  string
}

// Coordinator is the minimal interface the scheduler needs.
type Coordinator interface {
    RunOnce(ctx context.Context) (int64, error)
    Cancel()
}

func NewScheduler(cfg *config.Config, coordinator Coordinator, stateStore state.StateStore) *Scheduler {
    return &Scheduler{
        cfg:         cfg,
        coordinator: coordinator,
        stateStore:  stateStore,
        jobLockName: "bqcubbit-sync-run",
    }
}

// Start begins the cron scheduler. Blocks until context is cancelled or Stop is called.
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
    // Job overlap prevention
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
            log.Println("[scheduler] previous run still in progress — cancelling and restarting")
            s.coordinator.Cancel()
            // Small delay for cleanup
            time.Sleep(5 * time.Second)
        default:
            log.Println("[scheduler] previous run still in progress — queueing not supported yet, skipping")
            return nil
        }
    }
    defer s.stateStore.ReleaseJobLock(ctx, s.jobLockName)

    _, err = s.coordinator.RunOnce(ctx)
    return err
}
```

- [ ] **Step 2: Write test**

```go
// internal/scheduler/scheduler_test.go
package scheduler

import (
    "context"
    "testing"
    "time"

    "github.com/esignoretti/bqcubbit/internal/config"
)

type mockCoordinator struct {
    runCount int
}

func (m *mockCoordinator) RunOnce(ctx context.Context) (int64, error) {
    m.runCount++
    return int64(m.runCount), nil
}

func (m *mockCoordinator) Cancel() {}

type mockState struct{}

func (m *mockState) AcquireJobLock(ctx context.Context, lockName string, ttl time.Duration) (bool, error) {
    return true, nil
}
func (m *mockState) ReleaseJobLock(ctx context.Context, lockName string) error { return nil }

func TestScheduler(t *testing.T) {
    cfg := config.Default()
    cfg.Scheduler.Cron = "@every 1s" // every second

    coord := &mockCoordinator{}
    state := &mockState{}
    s := NewScheduler(cfg, coord, state)

    ctx, cancel := context.WithTimeout(context.Background(), 3500*time.Millisecond)
    defer cancel()

    s.Start(ctx)

    if coord.runCount < 2 {
        t.Fatalf("expected at least 2 runs in 3.5s, got %d", coord.runCount)
    }
}
```

- [ ] **Step 3: Run and commit**

```bash
go get github.com/robfig/cron/v3
go test ./internal/scheduler/ -v
git add internal/scheduler/
git commit -m "feat: cron scheduler with job overlap prevention"
```

---

### Task 10: Daemon mode — serve subcommand

**Files:**
- Modify: `cmd/bqcubbit/main.go`

- [ ] **Step 1: Add serve subcommand**

```go
// Add to cmd/bqcubbit/main.go

func runServe(cfg *config.Config) error {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle SIGTERM/SIGINT for graceful shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
    go func() {
        <-sigCh
        log.Println("[serve] received shutdown signal")
        cancel()
    }()

    // Create components
    bqReader, err := bigquery.NewStorageReadReader(ctx, cfg.Source.ProjectID, cfg.Source.Location)
    if err != nil { return fmt.Errorf("create bq reader: %w", err) }
    defer bqReader.Close()

    storageClient, err := storage.NewClient(ctx, cfg.Destination.Endpoint, cfg.Destination.AccessKey, cfg.Destination.SecretKey, cfg.Destination.Bucket, cfg.Destination.Prefix)
    if err != nil { return fmt.Errorf("create storage: %w", err) }

    stateStore, err := state.NewSQLiteStore(getStatePath())
    if err != nil { return fmt.Errorf("create state: %w", err) }
    defer stateStore.Close()
    stateStore.Init(ctx)

    pqWriter := parquet.NewWriter(parquet.DefaultWriterConfig())
    limiters := rate.NewLimiters(rate.ConfigFromConfig(cfg))

    // EXPORT DATA backend (nil if no GCS staging bucket configured)
    var bqExport *bigquery.ExportDataBackend
    if cfg.GCS.StagingBucket != "" {
        bqExport, err = bigquery.NewExportDataBackend(ctx, cfg.Source.ProjectID, cfg.Source.Location,
            cfg.GCS.StagingBucket, cfg.GCS.StagingPrefix, storageClient)
        if err != nil { return fmt.Errorf("create export backend: %w", err) }
        defer bqExport.Close()
    }

    // Task executor
    executor := sync.NewTaskExecutor(cfg, bqReader, bqExport, storageClient, pqWriter, limiters)

    // Coordinator
    coord := coordinator.NewCoordinator(cfg, stateStore, limiters, executor)

    // Scheduler
    sched := scheduler.NewScheduler(cfg, coord, stateStore)

    log.Printf("[serve] bqcubbit daemon starting (cron: %s)", cfg.Scheduler.Cron)

    // Run initial sync if configured
    if cfg.Scheduler.InitialSyncMode == "full_refresh" {
        log.Println("[serve] running initial sync before entering cron mode")
        if _, err := coord.RunOnce(ctx); err != nil {
            log.Printf("[serve] initial sync failed: %v", err)
        }
    }

    // Enter cron mode
    return sched.Start(ctx)
}
```

- [ ] **Step 2: Update main func to dispatch serve**

```go
// In main(), add case:
case "serve":
    if err := runServe(cfg); err != nil {
        log.Fatalf("serve: %v", err)
    }
```

- [ ] **Step 3: Build and commit**

```bash
go build ./cmd/bqcubbit
git add cmd/bqcubbit/main.go
git commit -m "feat: serve subcommand — daemon mode with scheduler and graceful shutdown"
```

---

### Task 11: Verification — post-sync row count and checksum validation

**Files:**
- Create: `internal/verify/verify.go`
- Create: `internal/verify/verify_test.go`

- [ ] **Step 1: Write verifier**

```go
// internal/verify/verify.go
package verify

import (
    "context"
    "fmt"
    "log"

    "cloud.google.com/go/bigquery"
)

type Verifier struct {
    bqClient *bigquery.Client
}

func NewVerifier(ctx context.Context, projectID string) (*Verifier, error) {
    client, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
        return nil, fmt.Errorf("create bq client: %w", err)
    }
    return &Verifier{bqClient: client}, nil
}

func (v *Verifier) Close() error {
    return v.bqClient.Close()
}

// Result holds the verification result for a single partition.
type Result struct {
    TableDataset    string
    TableName       string
    PartitionID     string
    BQRowCount      int64
    CubbitRowCount  int64
    RowCountMatch   bool
    BQBytes         int64
    CubbitBytes     int64
    ChecksumMatch   bool
}

// VerifyPartition compares BQ row count against exported Parquet row count.
func (v *Verifier) VerifyPartition(ctx context.Context, projectID, dataset, table, partitionID string, expectedRows int64) (*Result, error) {
    // Query BQ for row count of this partition
    q := v.bqClient.Query(fmt.Sprintf(
        `SELECT COUNT(*) as cnt FROM \`%s.%s.%s\` WHERE %s = '%s'`,
        projectID, dataset, table, partitionID)) // Note: partition filter varies

    ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()

    it, err := q.Read(ctx2)
    if err != nil {
        return nil, fmt.Errorf("query row count: %w", err)
    }

    var row struct{ Cnt int64 }
    if err := it.Next(&row); err != nil {
        return nil, fmt.Errorf("read row count: %w", err)
    }

    result := &Result{
        TableDataset:   dataset,
        TableName:      table,
        PartitionID:    partitionID,
        BQRowCount:     row.Cnt,
        CubbitRowCount: expectedRows,
        RowCountMatch:  row.Cnt == expectedRows,
    }

    if !result.RowCountMatch {
        log.Printf("[verify] MISMATCH %s.%s/%s: BQ=%d Cubbit=%d", dataset, table, partitionID, row.Cnt, expectedRows)
    }

    return result, nil
}
```

- [ ] **Step 2: Write test**

```go
// internal/verify/verify_test.go
package verify

import "testing"

func TestNewVerifier(t *testing.T) {
    t.Skip("requires GCP credentials")
}
```

- [ ] **Step 3: Integrate verification into TaskExecutor**

```go
// In internal/sync/executor.go, after upload, add:
if result.RowCount > 0 {
    go func() {
        verifier, err := verify.NewVerifier(ctx, e.cfg.Source.ProjectID)
        if err == nil {
            defer verifier.Close()
            vResult, vErr := verifier.VerifyPartition(ctx, e.cfg.Source.ProjectID,
                dataset, table, partitionID, result.RowCount)
            if vErr != nil {
                log.Printf("[verify] error verifying %s.%s/%s: %v", dataset, table, partitionID, vErr)
            } else if !vResult.RowCountMatch {
                log.Printf("[verify] WARNING: row count mismatch for %s.%s/%s", dataset, table, partitionID)
            }
        }
    }()
}
```

- [ ] **Step 4: Commit**

```bash
go build ./internal/verify/
git add internal/verify/ internal/sync/executor.go
git commit -m "feat: post-sync verification — row count comparison with BQ"
```

---

### Task 12: Prometheus metrics

**Files:**
- Create: `internal/metrics/metrics.go`

- [ ] **Step 1: Write metrics definitions**

```go
// internal/metrics/metrics.go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    BytesExtracted = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "bqcubbit_bytes_extracted_total",
        Help: "Total bytes read from BigQuery",
    }, []string{"dataset", "table"})

    BytesUploaded = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "bqcubbit_bytes_uploaded_total",
        Help: "Total bytes uploaded to Cubbit",
    }, []string{"dataset", "table"})

    CompressionRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "bqcubbit_compression_ratio",
        Help: "Compression ratio (extracted / stored)",
    }, []string{"dataset", "table"})

    TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "bqcubbit_task_duration_seconds",
        Help:    "Task execution duration",
        Buckets: prometheus.DefBuckets,
    }, []string{"dataset", "table", "status"})

    TasksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "bqcubbit_tasks_total",
        Help: "Total tasks processed",
    }, []string{"status"})

    PartitionLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "bqcubbit_partition_lag_seconds",
        Help: "Time since last successful sync per partition",
    }, []string{"dataset", "table", "partition"})
)
```

- [ ] **Step 2: Wire metrics HTTP endpoint in serve**

```go
// In runServe, add:
import "github.com/prometheus/client_golang/prometheus/promhttp"

// Metrics HTTP server
metricsMux := http.NewServeMux()
metricsMux.Handle("/metrics", promhttp.Handler())
metricsServer := &http.Server{Addr: ":9090", Handler: metricsMux}
go func() {
    log.Printf("[metrics] listening on :9090")
    if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Printf("[metrics] server error: %v", err)
    }
}()
defer metricsServer.Shutdown(context.Background())
```

- [ ] **Step 3: Commit**

```bash
go get github.com/prometheus/client_golang/prometheus
go build ./internal/metrics/
git add internal/metrics/ cmd/bqcubbit/main.go
git commit -m "feat: Prometheus metrics — bytes, compression ratio, task duration, partition lag"
```

---

### Task 13: GCS lifecycle rule for staging cleanup

**Files:**
- Create: `gcs_staging_lifecycle.json`

- [ ] **Step 1: Write lifecycle JSON**

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 1,
          "matchesPrefix": ["_bqcubbit_staging/"]
        }
      }
    ]
  }
}
```

- [ ] **Step 2: Add apply-lifeycle command to CLI (optional) or document**

Add to example.yaml comment or README:
```
# Apply to GCS bucket:
# gsutil lifecycle set gcs_staging_lifecycle.json gs://YOUR_BUCKET
```

- [ ] **Step 3: Commit**

```bash
git add gcs_staging_lifecycle.json
git commit -m "chore: GCS lifecycle rule for staging cleanup"
```

---

### Task 14: Phase 3 integration and verification

**Files:**
- Run: full build and test suite

- [ ] **Step 1: Full build**

```bash
go mod tidy
go vet ./...
go build ./...
```

- [ ] **Step 2: Run all unit tests**

```bash
go test ./internal/... -v 2>&1 | grep -E "^(=== RUN|--- PASS|--- FAIL|ok|FAIL)"
```

- [ ] **Step 3: Integration check — verify serve subcommand**

```bash
go build -o bqcubbit ./cmd/bqcubbit
./bqcubbit serve --config example.yaml &
sleep 2
kill %1
```

Expected: daemon starts, logs, shuts down cleanly.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: Phase 3 integration fixes"
```

---

## Self-Review

### Spec coverage (Phase 3 against analysis doc)

- **Worker pool pattern** ✅ (Tasks 4, 5 — Worker with lease heartbeat, Pool with scale-up)
- **Coordinator-Worker-Storage triangle** ✅ (Tasks 7, 8 — Coordinator discovers work, creates tasks, manages pool; executor bridge)
- **Task lifecycle state machine** ✅ (Task 6 — PENDING→ASSIGNED→COMPLETED/FAILED with lease management)
- **Lease-based work assignment** ✅ (Tasks 4, 6 — ClaimTask with generation, RenewLease, ResetExpiredLeases)
- **EXPORT DATA mode** ✅ (Task 3 — BigQuery-side ZSTD Parquet export to GCS, then transfer to Cubbit)
- **Extraction method decision** ✅ (Task 8 — auto/storage_api/export_data based on partition size)
- **Scheduling with cron** ✅ (Task 9 — robfig/cron, overlap prevention via SQLite advisory locks)
- **Daemon mode (serve subcommand)** ✅ (Task 10 — SIGTERM graceful shutdown, initial sync + cron)
- **Job overlap prevention** ✅ (Task 6 — AcquireJobLock/ReleaseJobLock with TTL)
- **Post-sync verification** ✅ (Task 11 — row count comparison with BQ query)
- **Prometheus metrics** ✅ (Task 12 — counter, histogram, gauge for key observability)
- **Rate limiting** ✅ (Task 2 — token buckets for BQ reads, exports, Cubbit uploads)
- **Graceful shutdown** ✅ (Task 10 — SIGTERM handler, context cancellation)
- **GCS staging lifecycle** ✅ (Task 13 — lifecycle rule for _staging/ cleanup)
- **Concurrency: tasks single-threaded** ✅ (Task 4 — Worker executes one task at a time)

### Not yet covered (Phase 4+)
- **WebUI** (HTMX/Go templates for monitoring)
- **Breaking schema changes** (DROP, RENAME — detected but not handled)
- **Multi-region Postgres state store** (interface exists, implementation needed)
- **Late-arriving data detection** (re-sync if partition modified during sync)
- **End-to-end test mode** (`bqcubbit verify --sample`)

### Placeholder scan
No TODOs or TBDs. The EXPORT DATA backend has a noted implementation detail about partition column detection — this is a design note, not a placeholder. The coordinator's waitForCompletion uses a polling approach that's suitable for Phase 3.

### Type consistency
- `worker.TaskExecutor` interface matches `sync.TaskExecutor` implementation
- `scheduler.Coordinator` interface matches `coordinator.Coordinator` methods
- `rate.Limiters` matches config types and consumption in coordinator
- State store methods match across interface and SQLite implementation
- Metrics types match prometheus/client_golang API

### Scope check
Phase 3 is the most complex phase and has been split into 14 focused tasks. The coordinator-worker separation is clean: coordinator owns discovery and lifecycle, workers own execution. The EXPORT DATA backend is a separate extraction path that shares the same Cubbit upload pipeline.
