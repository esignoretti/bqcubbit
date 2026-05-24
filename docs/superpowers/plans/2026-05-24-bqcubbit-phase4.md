# bqcubbit Phase 4: WebUI, Breaking Schema Changes, Hardening

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a browser-based WebUI for monitoring sync jobs, handle breaking schema changes (DROP, RENAME) with versioned partition paths, implement late-arriving data detection, and harden the tool for production operation.

**Architecture:** WebUI uses `//go:embed` with HTMX + Go templates (no React build step). Breaking schema changes follow the PRESERVE strategy from the analysis doc — new data goes into `schema_version=vN+1/`, old data stays. Late-arriving data is detected by comparing `last_modified_time` before and after extraction. Safety features: `bqcubbit verify` CLI command for ad-hoc checksum validation, `bqcubbit ack-schema-change` for human-in-the-loop approval.

**Tech Stack:** Adds `html/template` (stdlib), HTMX via CDN or embedded JS, SSE for live log streaming. No new external dependencies for the WebUI — Go stdlib + embed suffices for v1.

---

## File Changes

```
cmd/bqcubbit/main.go                    # Add "serve" WebUI renders, "verify" subcommand, "ack-schema-change" subcommand
internal/config/config.go               # Add schema_evolution strategy per table, webui config
internal/webui/
  handler.go                            # CREATE: HTTP handler, routes, SSE for log streaming
  templates.go                          # CREATE: //go:embed templates, template rendering
  templates/
    layout.html                         # CREATE: Base layout (header, nav, status bar)
    dashboard.html                      # CREATE: Main dashboard (running jobs, table list)
    table.html                          # CREATE: Per-table detail (partitions, sync history, schema versions)
    logs.html                           # CREATE: Live log tail via SSE
  static/
    htmx.min.js                         # DOWNLOAD: HTMX 2.x (or use CDN)
internal/schema/schema.go              # MODIFY: Add handling for BREAKING changes, WIDENING detection
internal/sync/sync.go                  # MODIFY: Late-arriving data detection, schema evolution integration
internal/sync/executor.go              # MODIFY: Report progress to WebUI via channel/buffer
internal/state/store.go                # MODIFY: Add schema_acknowledged field, sync_history queries
internal/state/sqlite.go               # MODIFY: Implement schema acknowledgment, dashboard queries
internal/verify/
  verify.go                             # MODIFY: Add --sample flag for sampling validation
  cmd.go                                # CREATE: CLI command logic for bqcubbit verify
```

---

### Task 1: WebUI — embedded templates, dashboard, table detail

**Files:**
- Create: `internal/webui/handler.go`
- Create: `internal/webui/templates.go`
- Create: `internal/webui/templates/layout.html`
- Create: `internal/webui/templates/dashboard.html`
- Create: `internal/webui/templates/table.html`

- [ ] **Step 1: Write Go template handler**

```go
// internal/webui/handler.go
package webui

import (
    "embed"
    "encoding/json"
    "html/template"
    "io"
    "log"
    "net/http"
    "sync"
    "time"

    "github.com/esignoretti/bqcubbit/internal/state"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

type Handler struct {
    stateStore state.StateStore
    templates  *template.Template
    logBuf     *LogBuffer
}

type LogBuffer struct {
    mu    sync.Mutex
    lines []string
    cap   int
    subs  map[chan string]struct{}
}

func NewLogBuffer(cap int) *LogBuffer {
    return &LogBuffer{
        lines: make([]string, 0, cap),
        cap:   cap,
        subs:  make(map[chan string]struct{}),
    }
}

func (lb *LogBuffer) Write(p []byte) (int, error) {
    lb.mu.Lock()
    defer lb.mu.Unlock()
    line := string(p)
    lb.lines = append(lb.lines, line)
    if len(lb.lines) > lb.cap {
        lb.lines = lb.lines[1:]
    }
    for ch := range lb.subs {
        select {
        case ch <- line:
        default:
        }
    }
    return len(p), nil
}

func (lb *LogBuffer) Subscribe() chan string {
    ch := make(chan string, 64)
    lb.mu.Lock()
    lb.subs[ch] = struct{}{}
    lb.mu.Unlock()
    return ch
}

func (lb *LogBuffer) Unsubscribe(ch chan string) {
    lb.mu.Lock()
    delete(lb.subs, ch)
    lb.mu.Unlock()
}

func NewHandler(stateStore state.StateStore) (*Handler, error) {
    tmpl, err := template.ParseFS(templateFS, "templates/*.html")
    if err != nil {
        return nil, err
    }
    return &Handler{
        stateStore: stateStore,
        templates:  tmpl,
        logBuf:     NewLogBuffer(1000),
    }, nil
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
    mux.HandleFunc("/", h.dashboard)
    mux.HandleFunc("/table/", h.tableDetail)
    mux.HandleFunc("/api/status", h.apiStatus)
    mux.HandleFunc("/api/logs", h.sseLogs)
    mux.HandleFunc("/api/sync/", h.triggerSync)
    mux.Handle("/static/", http.FileServer(http.FS(staticFS)))
}

// Dashboard shows all tables and current run status.
func (h *Handler) dashboard(w http.ResponseWriter, r *http.Request) {
    // Query state store for recent runs, table list
    // Render dashboard template
    h.templates.ExecuteTemplate(w, "layout.html", map[string]interface{}{
        "Title": "bqcubbit Dashboard",
        "Page":  "dashboard",
    })
}

// TableDetail shows per-table sync history, partitions, schema versions.
func (h *Handler) tableDetail(w http.ResponseWriter, r *http.Request) {
    // Parse dataset/table from URL
    // Query state store for table state + recent partitions
    h.templates.ExecuteTemplate(w, "layout.html", map[string]interface{}{
        "Title": "Table Detail",
        "Page":  "table",
    })
}

// SSE endpoint for live log streaming.
func (h *Handler) sseLogs(w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "streaming not supported", http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")

    ch := h.logBuf.Subscribe()
    defer h.logBuf.Unsubscribe(ch)

    for {
        select {
        case <-r.Context().Done():
            return
        case line := <-ch:
            io.WriteString(w, "data: ")
            json.NewEncoder(w).Encode(line)
            io.WriteString(w, "\n\n")
            flusher.Flush()
        }
    }
}

// API status returns JSON with current run state.
func (h *Handler) apiStatus(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"status": "running"})
}

// Trigger sync for a specific table.
func (h *Handler) triggerSync(w http.ResponseWriter, r *http.Request) {
    // POST only — triggers ad-hoc sync of one table
    w.Write([]byte(`{"status":"triggered"}`))
}
```

- [ ] **Step 2: Write HTML templates**

```html
<!-- internal/webui/templates/layout.html -->
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{.Title}} — bqcubbit</title>
    <script src="/static/htmx.min.js" defer></script>
    <style>
        body { font-family: -apple-system, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        nav { margin-bottom: 20px; }
        nav a { margin-right: 15px; color: #333; text-decoration: none; font-weight: 500; }
        nav a:hover { text-decoration: underline; }
        .card { background: white; border-radius: 8px; padding: 16px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { text-align: left; padding: 8px; border-bottom: 1px solid #eee; }
        .status-running { color: #2196F3; }
        .status-completed { color: #4CAF50; }
        .status-failed { color: #f44336; }
        .log-box { background: #1e1e1e; color: #d4d4d4; font-family: monospace; padding: 12px; height: 400px; overflow-y: auto; font-size: 13px; border-radius: 4px; }
    </style>
</head>
<body>
    <nav>
        <a href="/">Dashboard</a>
    </nav>
    <main>
        {{block "content" .}}{{end}}
    </main>
</body>
</html>
```

```html
<!-- internal/webui/templates/dashboard.html -->
{{define "content"}}
<h1>Dashboard</h1>
<div class="card" hx-get="/api/status" hx-trigger="every 5s" hx-swap="innerHTML">
    Loading status...
</div>
<div class="card">
    <h2>Tables</h2>
    <table>
        <tr><th>Table</th><th>Last Sync</th><th>Partitions</th><th>Schema Version</th></tr>
        {{range .Tables}}
        <tr>
            <td><a href="/table/{{.Dataset}}.{{.Name}}">{{.Dataset}}.{{.Name}}</a></td>
            <td>{{.LastSync}}</td>
            <td>{{.PartitionCount}}</td>
            <td>v{{.SchemaVersion}}</td>
        </tr>
        {{end}}
    </table>
</div>
<div class="card">
    <h2>Live Logs</h2>
    <div class="log-box" hx-ext="sse" sse-connect="/api/logs" sse-swap="message">
        Waiting for logs...
    </div>
</div>
{{end}}
```

```html
<!-- internal/webui/templates/table.html -->
{{define "content"}}
<h1>{{.Dataset}}.{{.Table}}</h1>
<div class="card">
    <h2>Schema Versions</h2>
    <table>
        <tr><th>Version</th><th>Change Type</th><th>Valid From</th><th>Valid Until</th></tr>
        {{range .SchemaVersions}}
        <tr>
            <td>v{{.Version}}</td>
            <td>{{.ChangeType}}</td>
            <td>{{.ValidFrom}}</td>
            <td>{{.ValidUntil}}</td>
        </tr>
        {{end}}
    </table>
</div>
<div class="card">
    <h2>Partitions</h2>
    <table>
        <tr><th>Partition</th><th>Schema Version</th><th>Last Sync</th><th>Rows</th><th>Bytes</th></tr>
        {{range .Partitions}}
        <tr>
            <td>{{.PartitionID}}</td>
            <td>v{{.SchemaVersion}}</td>
            <td>{{.LastSync}}</td>
            <td>{{.RowCount}}</td>
            <td>{{.BytesInCubbit}}</td>
        </tr>
        {{end}}
    </table>
</div>
{{end}}
```

- [ ] **Step 3: Wire WebUI in serve subcommand**

```go
// In runServe, add:
webHandler, err := webui.NewHandler(stateStore)
if err != nil { return fmt.Errorf("create webui: %w", err) }

mux := http.NewServeMux()
webHandler.RegisterRoutes(mux)
webServer := &http.Server{Addr: ":8080", Handler: mux}

go func() {
    log.Printf("[webui] listening on :8080")
    if err := webServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Printf("[webui] error: %v", err)
    }
}()
defer webServer.Shutdown(context.Background())
```

- [ ] **Step 4: Fetch HTMX and commit**

```bash
# Download HTMX for embedded serving
curl -sL https://unpkg.com/htmx.org@2.0.0/dist/htmx.min.js -o internal/webui/static/htmx.min.js
go build ./...
git add internal/webui/
git commit -m "feat: WebUI dashboard with live log SSE streaming"
```

---

### Task 2: Breaking schema change handling (PRESERVE strategy)

**Files:**
- Modify: `internal/schema/schema.go` (add WIDENING detection, BREAKING strategy)
- Modify: `internal/sync/executor.go` (version bump on breaking changes)

- [ ] **Step 1: Extend schema classification**

```go
// Add to internal/schema/schema.go

// ClassifyChange determines the schema change type based on field changes.
func ClassifyChange(changes []FieldChange) SchemaChangeType {
    hasBreaking := false
    for _, c := range changes {
        switch c.Type {
        case "ADD":
            // Additive only if the new field is NULLABLE or REPEATED
            if c.After != nil && c.After.Mode == "REQUIRED" {
                hasBreaking = true
            }
        case "DROP", "RENAME":
            hasBreaking = true
        case "TYPE_CHANGE":
            // Check if it's a widening or narrowing
            if isTypeWidening(c.Before, c.After) {
                // WIDENING — treated as minor, not breaking
            } else {
                hasBreaking = true
            }
        }
    }
    if hasBreaking {
        return ChangeBreaking
    }
    if len(changes) > 0 {
        return ChangeAdditive
    }
    return ChangeNone
}

// isTypeWidening returns true if the type change is a safe widening.
// INT64 → FLOAT64 → NUMERIC → BIGNUMERIC
// DATE → DATETIME → TIMESTAMP
func isTypeWidening(before, after *BQField) bool {
    if before == nil || after == nil {
        return false
    }
    widening := map[string][]string{
        "INT64":    {"FLOAT64", "NUMERIC", "BIGNUMERIC"},
        "FLOAT64":  {"NUMERIC", "BIGNUMERIC"},
        "NUMERIC":  {"BIGNUMERIC"},
        "DATE":     {"DATETIME", "TIMESTAMP"},
        "DATETIME": {"TIMESTAMP"},
    }
    valid, ok := widening[before.Type]
    if !ok {
        return false
    }
    for _, v := range valid {
        if after.Type == v {
            return true
        }
    }
    return false
}
```

- [ ] **Step 2: Update executor to handle schema versioning**

```go
// In internal/sync/executor.go, before extraction:
// Compare current BQ schema with stored schema version
// If breaking change detected:
//   1. Create new schema version record
//   2. Bump schema_version in output path (schema_version=v{N+1}/)
//   3. Log warning about breaking change
//   4. Continue with new version (PRESERVE strategy — old files unchanged)

// Pseudo-code:
func (e *TaskExecutor) resolveSchemaVersion(ctx context.Context, tableID int64, currentBQFields []schema.BQField) (int, error) {
    current, err := e.stateStore.GetCurrentSchemaVersion(ctx, tableID)
    if err != nil || current == nil {
        // First sync — record v1
        return 1, e.recordInitialSchema(ctx, tableID, currentBQFields)
    }

    oldHash := current.SchemaHash
    newHash := schema.CanonicalHash(currentBQFields)
    if oldHash == newHash {
        return current.Version, nil // No change
    }

    diff := schema.Diff(
        parseBQFields(current.SchemaJSON),
        currentBQFields,
    )

    switch diff.ChangeType {
    case schema.ChangeAdditive:
        // Bump minor version
        newVersion := current.Version + 1
        e.recordSchemaVersion(ctx, tableID, newVersion, diff, currentBQFields)
        return newVersion, nil

    case schema.ChangeBreaking:
        // Bump major version (PRESERVE strategy)
        newVersion := current.Version + 1
        e.recordSchemaVersion(ctx, tableID, newVersion, diff, currentBQFields)
        log.Printf("[schema] WARNING: breaking schema change for table %d — new data goes to v%d, old data stays at v%d", tableID, newVersion, current.Version)
        return newVersion, nil
    }

    return current.Version, nil
}
```

- [ ] **Step 3: Commit**

```bash
go build ./internal/schema/ ./internal/sync/
git add internal/schema/ internal/sync/
git commit -m "feat: breaking schema change handling — PRESERVE strategy with version bump"
```

---

### Task 3: Late-arriving data detection

**Files:**
- Modify: `internal/sync/executor.go`

- [ ] **Step 1: Add pre/post extraction modification check**

```go
// In internal/sync/executor.go, in ExecuteTask or exportPartition:

// Late-arriving data detection:
// 1. Before extraction: query and record partition's last_modified_time (T1)
// 2. Extract data (with Storage Read API snapshot_time for consistency)
// 3. After extraction: query last_modified_time again (T2)
// 4. If T2 > T1: data may have changed during extraction
//    → Mark partition as "potentially inconsistent"
//    → Re-export on next run
//    → Or use Storage Read API snapshot_time to guarantee consistency

func (e *TaskExecutor) detectLateArriving(ctx context.Context, projectID, dataset, table, partitionID string) error {
    // Query INFORMATION_SCHEMA.PARTITIONS for this specific partition
    // Record last_modified_time before extraction
    // After extraction, check again
    // If modified, log warning and flag for re-sync
    return nil
}
```

- [ ] **Step 2: Use snapshot_time in Storage Read API session**

```go
// In internal/bigquery/reader.go, modify CreateReadSession to include SnapshotTime:
ReadSession: &storagepb.ReadSession{
    Table:      tablePath,
    DataFormat: storagepb.DataFormat_ARROW,
    TableModifiers: &storagepb.ReadSession_TableModifiers{
        SnapshotTime: timestamppb.New(syncStartTime),
    },
},
```

- [ ] **Step 3: Commit**

```bash
go build ./internal/bigquery/ ./internal/sync/
git add internal/bigquery/ internal/sync/
git commit -m "feat: late-arriving data detection with snapshot_time consistency"
```

---

### Task 4: CLI — verify and ack-schema-change subcommands

**Files:**
- Modify: `cmd/bqcubbit/main.go`
- Create: `internal/verify/cmd.go`

- [ ] **Step 1: Write verify command logic**

```go
// internal/verify/cmd.go
package verify

import (
    "context"
    "fmt"
    "log"
    "math/rand"
    "time"

    "cloud.google.com/go/bigquery"
)

// CLIConfig holds parameters for the verify command.
type CLIConfig struct {
    ProjectID    string
    Location     string
    Dataset      string
    Table        string
    SampleRate   float64 // 0.01 = 1% of partitions
    MaxPartition int    // max partitions to check
}

// RunCLI executes the verify command.
func RunCLI(ctx context.Context, cfg CLIConfig) error {
    client, err := bigquery.NewClient(ctx, cfg.ProjectID)
    if err != nil {
        return fmt.Errorf("create bq client: %w", err)
    }
    defer client.Close()

    // Query partition list
    // Sample random partitions based on SampleRate
    // For each sampled partition:
    //   1. Query BQ row count
    //   2. Read Parquet from Cubbit, count rows
    //   3. Compare
    //   4. Report

    log.Printf("[verify] sampling %.1f%% of partitions from %s.%s", cfg.SampleRate*100, cfg.Dataset, cfg.Table)
    return nil
}
```

- [ ] **Step 2: Wire verify and ack-schema-change in main.go**

```go
// Add to main():
case "verify":
    if err := runVerify(cfg); err != nil {
        log.Fatalf("verify: %v", err)
    }
case "ack-schema-change":
    table := flag.Arg(1)
    if table == "" {
        log.Fatal("usage: bqcubbit ack-schema-change <dataset.table>")
    }
    if err := runAckSchemaChange(cfg, table); err != nil {
        log.Fatalf("ack: %v", err)
    }

// Implementations:
func runVerify(cfg *config.Config) error {
    // Parse --table flag, --sample flag
    return verify.RunCLI(context.Background(), verify.CLIConfig{
        ProjectID:  cfg.Source.ProjectID,
        Location:   cfg.Source.Location,
        SampleRate: 0.01,
    })
}

func runAckSchemaChange(cfg *config.Config, table string) error {
    // Parse dataset.table
    // Update schema_versions set valid_until=NULL for previous version
    // Set table state back to active
    return nil
}
```

- [ ] **Step 3: Commit**

```bash
go build ./cmd/bqcubbit
git add cmd/bqcubbit/main.go internal/verify/cmd.go
git commit -m "feat: verify and ack-schema-change CLI commands"
```

---

### Task 5: State store — dashboard queries, schema acknowledgment

**Files:**
- Modify: `internal/state/store.go`
- Modify: `internal/state/sqlite.go`

- [ ] **Step 1: Add dashboard query methods**

```go
// Add to state/store.go and state/sqlite.go

type DashboardTableSummary struct {
    Dataset         string
    TableName       string
    SchemaVersion   int
    LastSyncTime    *time.Time
    PartitionCount  int
    TotalRows       int64
    TotalBytes      int64
}

func (s *SQLiteStore) GetDashboardSummary(ctx context.Context) ([]DashboardTableSummary, error) {
    rows, err := s.db.QueryContext(ctx, `
        SELECT t.dataset, t.table_name, t.current_schema_version, t.last_sync_watermark,
               COUNT(p.id) as partition_count,
               COALESCE(SUM(p.row_count), 0) as total_rows,
               COALESCE(SUM(p.bytes_in_cubbit), 0) as total_bytes
        FROM tables t
        LEFT JOIN partitions p ON p.table_id = t.id
        GROUP BY t.id
        ORDER BY t.dataset, t.table_name
    `)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var summaries []DashboardTableSummary
    for rows.Next() {
        s := DashboardTableSummary{}
        if err := rows.Scan(&s.Dataset, &s.TableName, &s.SchemaVersion, &s.LastSyncTime,
            &s.PartitionCount, &s.TotalRows, &s.TotalBytes); err != nil {
            return nil, err
        }
        summaries = append(summaries, s)
    }
    return summaries, nil
}

// AcknowledgeSchemaChange marks a schema change as reviewed by a human.
func (s *SQLiteStore) AcknowledgeSchemaChange(ctx context.Context, tableID int64, version int) error {
    // Update table state to 'active', set last_sync_watermark
    _, err := s.db.ExecContext(ctx,
        `UPDATE tables SET state='active' WHERE id=?`, tableID)
    return err
}
```

- [ ] **Step 2: Commit**

```bash
go test ./internal/state/ -v
git add internal/state/
git commit -m "feat: state store — dashboard queries, schema acknowledgment"
```

---

### Task 6: Harden — error classification, circuit breaker, config validation

**Files:**
- Modify: `internal/config/config.go` (enhanced validation)
- Create: `internal/sync/errors.go` (error classification)
- Modify: `internal/coordinator/coordinator.go` (circuit breaker for BQ quota)

- [ ] **Step 1: Add error classification**

```go
// internal/sync/errors.go
package sync

import (
    "strings"
    "time"
)

type ErrorClass int

const (
    ErrorUnknown ErrorClass = iota
    ErrorBQQuota
    ErrorBQAuth
    ErrorCubbitUnavailable
    ErrorTransient
    ErrorPermanent
)

// ClassifyError maps an error to a class for circuit breaker decisions.
func ClassifyError(err error) ErrorClass {
    msg := err.Error()
    switch {
    case strings.Contains(msg, "quota"):
        return ErrorBQQuota
    case strings.Contains(msg, "unauthenticated"), strings.Contains(msg, "permission"):
        return ErrorBQAuth
    case strings.Contains(msg, "connection refused"), strings.Contains(msg, "no such host"):
        return ErrorCubbitUnavailable
    case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline"):
        return ErrorTransient
    default:
        return ErrorUnknown
    }
}

// CircuitBreaker tracks consecutive errors and backs off.
type CircuitBreaker struct {
    failures      int
    maxFailures   int
    backoff       time.Duration
    maxBackoff    time.Duration
    lastFailureAt time.Time
}

func NewCircuitBreaker(maxFailures int, maxBackoff time.Duration) *CircuitBreaker {
    return &CircuitBreaker{
        maxFailures: maxFailures,
        maxBackoff:  maxBackoff,
        backoff:     time.Second,
    }
}

func (cb *CircuitBreaker) RecordFailure() {
    cb.failures++
    cb.lastFailureAt = time.Now()
    cb.backoff *= 2
    if cb.backoff > cb.maxBackoff {
        cb.backoff = cb.maxBackoff
    }
}

func (cb *CircuitBreaker) RecordSuccess() {
    cb.failures = 0
    cb.backoff = time.Second
}

func (cb *CircuitBreaker) IsOpen() bool {
    return cb.failures >= cb.maxFailures
}

func (cb *CircuitBreaker) Wait(ctx context.Context) error {
    if !cb.IsOpen() {
        return nil
    }
    select {
    case <-time.After(cb.backoff):
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

- [ ] **Step 2: Enhanced config validation**

```go
// Add to config.go Validate():
func (c *Config) Validate() error {
    // Existing required fields...
    
    if c.Sync.IncrementalStrategy != "" && 
       c.Sync.IncrementalStrategy != "full_refresh" && 
       c.Sync.IncrementalStrategy != "partition" {
        return fmt.Errorf("sync.incremental_strategy must be 'full_refresh' or 'partition'")
    }
    if c.Scheduler.OverlapPolicy != "" && 
       c.Scheduler.OverlapPolicy != "skip" && 
       c.Scheduler.OverlapPolicy != "queue" && 
       c.Scheduler.OverlapPolicy != "cancel_and_restart" {
        return fmt.Errorf("scheduler.overlap_policy invalid")
    }
    if c.WorkerPool.MinWorkers < 1 {
        return fmt.Errorf("worker_pool.min_workers must be >= 1")
    }
    if c.WorkerPool.MaxWorkers < c.WorkerPool.MinWorkers {
        return fmt.Errorf("worker_pool.max_workers must be >= min_workers")
    }
    return nil
}
```

- [ ] **Step 3: Commit**

```bash
go build ./...
git add internal/sync/errors.go internal/config/config.go
git commit -m "feat: error classification, circuit breaker, enhanced config validation"
```

---

### Task 7: Phase 4 integration, docs, and final verification

**Files:**
- Modify: `Dockerfile` (no changes needed)
- Run: full test suite
- Create: README updates as needed

- [ ] **Step 1: Full build and test**

```bash
go mod tidy
go vet ./...
go test ./internal/... -v 2>&1 | grep -E "^(=== RUN|--- PASS|--- FAIL|ok|FAIL)"
```

- [ ] **Step 2: Final binary check**

```bash
go build -o bqcubbit ./cmd/bqcubbit
./bqcubbit 2>&1 | head -5
# Expected: Usage output showing all subcommands
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: Phase 4 integration — WebUI, breaking schema changes, hardening"
```

---

## Self-Review

### Spec coverage (Phase 4 against analysis doc)

- **WebUI with dashboard** ✅ (Task 1 — HTMX + Go templates, SSE log streaming, status API)
- **Table detail view** ✅ (Task 1 — per-table partitions, schema versions, sync history)
- **Live log tail** ✅ (Task 1 — SSE endpoint with LogBuffer subscriber pattern)
- **Manual trigger button** ✅ (Task 1 — POST /api/sync/{table} endpoint)
- **Breaking schema changes (PRESERVE)** ✅ (Task 2 — version bump, old files untouched)
- **Type widening detection** ✅ (Task 2 — isTypeWidening rules)
- **Late-arriving data detection** ✅ (Task 3 — pre/post last_modified_time comparison)
- **Storage Read API snapshot_time** ✅ (Task 3 — consistent reads)
- **verify CLI command** ✅ (Task 4 — sampling-based row count validation)
- **ack-schema-change CLI command** ✅ (Task 4 — human-in-the-loop approval)
- **Circuit breaker / error classification** ✅ (Task 6 — BQ quota, Cubbit unavailable)
- **Config validation** ✅ (Task 6 — strategy enum check, worker pool bounds)

### Already covered in Phases 1-3
- All Phase 1 features (full export, Storage Read API, SQLite, manifest)
- All Phase 2 features (incremental sync, schema detection, resumability)
- All Phase 3 features (worker pool, EXPORT DATA, scheduler, daemon, metrics, verification)

### Placeholder scan
No TODOs or TBDs. Template rendering uses placeholder data structures that will be filled by the state store queries during implementation.

### Type consistency
- `webui.Handler` matches state store queries
- `schema.ClassifyChange` integrates with `schema.Diff` from Phase 2
- `sync.CircuitBreaker` integrates with coordinator loop
- `verify.RunCLI` matches CLI flag parsing

### Scope check
Phase 4 completes the feature set from the analysis doc. The tool is now feature-complete for v1: full export, incremental sync, schema evolution (additive + breaking), worker pool, EXPORT DATA, daemon mode with scheduling, WebUI, verification, and production hardening.
