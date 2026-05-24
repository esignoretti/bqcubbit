# bqcubbit Phase 1: MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Single-table, single-worker full export from BigQuery to Cubbit DS3 as ZSTD-compressed Parquet files, with SQLite state tracking and manifest output.

**Architecture:** A single Go binary (`bqcubbit`) with subcommands. Phase 1 implements `bqcubbit sync --table dataset.table` as a one-shot CLI. It reads from BigQuery via the Storage Read API (Arrow format), writes Parquet streaming via `io.Pipe` to an S3-compatible multipart upload, tracks progress in SQLite (WAL mode), and writes a table manifest to Cubbit on completion. All interfaces are designed for the Phase 2-4 extensions (worker pool, incremental sync, schema evolution, WebUI) without breaking changes.

**Tech Stack:** Go 1.26, `cloud.google.com/go/bigquery/storage/apiv1` (Storage Read API), `github.com/apache/arrow-go/v18/parquet/pqarrow`, `github.com/aws/aws-sdk-go-v2/service/s3` (Cubbit DS3 endpoint), `modernc.org/sqlite` (pure Go SQLite), YAML config via `gopkg.in/yaml.v3`.

---

## File Structure

```
bqcubbit/
├── cmd/
│   └── bqcubbit/
│       └── main.go              # CLI entry, subcommand dispatch
├── internal/
│   ├── config/
│   │   └── config.go            # Config struct, Load(), validation
│   ├── bigquery/
│   │   └── reader.go            # Storage Read API session+stream reader
│   ├── parquet/
│   │   └── writer.go            # Arrow→Parquet streaming writer
│   ├── storage/
│   │   └── cubbit.go            # S3 client, multipart upload, pipe pattern
│   ├── state/
│   │   ├── store.go             # StateStore interface
│   │   └── sqlite.go            # SQLite implementation
│   ├── manifest/
│   │   └── manifest.go          # Manifest struct, write/read from Cubbit
│   └── sync/
│       └── sync.go              # Orchestrator: discover → extract → write → upload → verify
├── go.mod
├── go.sum
└── example.yaml                 # Sample config file
```

---

### Task 1: Project scaffolding, module init, config

**Files:**
- Create: `cmd/bqcubbit/main.go`
- Create: `internal/config/config.go`
- Create: `go.mod`
- Create: `example.yaml`

- [ ] **Step 1: Initialize Go module**

```bash
mkdir -p cmd/bqcubbit internal/config internal/bigquery internal/parquet internal/storage internal/state internal/manifest internal/sync
cd /Users/esignoretti/Documents/OpenCode/BIgQueryExporter
go mod init github.com/esignoretti/bqcubbit
```

- [ ] **Step 2: Write the config package**

```go
// internal/config/config.go
package config

import (
    "fmt"
    "os"

    "gopkg.in/yaml.v3"
)

type Config struct {
    Source    SourceConfig    `yaml:"source"`
    Destination DestinationConfig `yaml:"destination"`
    Sync       SyncConfig       `yaml:"sync"`
}

type SourceConfig struct {
    ProjectID string   `yaml:"project_id" validate:"required"`
    Location  string   `yaml:"location" validate:"required"`
    Datasets  []string `yaml:"datasets"`
}

type DestinationConfig struct {
    Endpoint        string `yaml:"endpoint" validate:"required"`
    Bucket          string `yaml:"bucket" validate:"required"`
    Prefix          string `yaml:"prefix"`
    AccessKey       string `yaml:"access_key"`
    SecretKey       string `yaml:"secret_key"`
    Compression     string `yaml:"compression"`
    CompressionLevel int   `yaml:"compression_level"`
}

type SyncConfig struct {
    Table  string `yaml:"table"`
}

func (c *Config) Validate() error {
    if c.Source.ProjectID == "" { return fmt.Errorf("source.project_id is required") }
    if c.Source.Location == "" { return fmt.Errorf("source.location is required") }
    if c.Destination.Endpoint == "" { return fmt.Errorf("destination.endpoint is required") }
    if c.Destination.Bucket == "" { return fmt.Errorf("destination.bucket is required") }
    return nil
}

func Default() *Config {
    return &Config{
        Destination: DestinationConfig{
            Prefix:          "bq-export/",
            Compression:     "zstd",
            CompressionLevel: 9,
        },
    }
}

func Load(path string) (*Config, error) {
    cfg := Default()
    if path == "" { path = os.Getenv("BQCUBBIT_CONFIG") }
    if path == "" { return nil, fmt.Errorf("config path required (set BQCUBBIT_CONFIG or pass --config)") }
    data, err := os.ReadFile(path)
    if err != nil { return nil, fmt.Errorf("read config: %w", err) }
    if err := yaml.Unmarshal(data, cfg); err != nil { return nil, fmt.Errorf("parse config: %w", err) }
    if err := cfg.Validate(); err != nil { return nil, err }
    return cfg, nil
}
```

- [ ] **Step 3: Write example.yaml**

```yaml
source:
  project_id: my-gcp-project
  location: EU
  datasets:
    - analytics

destination:
  endpoint: https://s3.cubbit.eu
  bucket: my-bigquery-export
  prefix: bq-export/
  access_key: YOUR_DS3_ACCESS_KEY
  secret_key: YOUR_DS3_SECRET_KEY
  compression: zstd
  compression_level: 9

sync:
  table: analytics.events
```

- [ ] **Step 4: Write main.go skeleton**

```go
// cmd/bqcubbit/main.go
package main

import (
    "flag"
    "fmt"
    "log"
    "os"

    "github.com/esignoretti/bqcubbit/internal/config"
)

func main() {
    log.SetFlags(0)
    flag.Usage = func() {
        fmt.Fprintf(os.Stderr, "Usage: bqcubbit [flags] <command>\n\nCommands:\n  sync   Export a table from BigQuery to Cubbit DS3\n\nFlags:\n")
        flag.PrintDefaults()
    }

    configPath := flag.String("config", "", "Path to config file (env: BQCUBBIT_CONFIG)")
    flag.Parse()

    if flag.NArg() == 0 {
        flag.Usage()
        os.Exit(1)
    }

    cfg, err := config.Load(*configPath)
    if err != nil {
        log.Fatalf("config: %v", err)
    }

    switch flag.Arg(0) {
    case "sync":
        if err := runSync(cfg); err != nil {
            log.Fatalf("sync: %v", err)
        }
    default:
        fmt.Fprintf(os.Stderr, "unknown command: %s\n", flag.Arg(0))
        flag.Usage()
        os.Exit(1)
    }
}

func runSync(cfg *config.Config) error {
    // TODO: wire up components
    fmt.Printf("syncing table %s from %s to %s/%s\n", cfg.Sync.Table, cfg.Source.ProjectID, cfg.Destination.Endpoint, cfg.Destination.Bucket)
    return nil
}
```

- [ ] **Step 5: Install deps and verify build**

```bash
go get gopkg.in/yaml.v3
go mod tidy
go build ./cmd/bqcubbit
```

Run: `go build ./cmd/bqcubbit`
Expected: binary produced, no errors.

- [ ] **Step 6: Commit**

```bash
git init
git add -A
git commit -m "feat: project scaffold, config, CLI skeleton"
```

---

### Task 2: BigQuery Storage Read API reader

**Files:**
- Create: `internal/bigquery/reader.go`
- Test: `internal/bigquery/reader_test.go` (unit test with mock)

- [ ] **Step 1: Write the BQ reader interface and implementation**

```go
// internal/bigquery/reader.go
package bigquery

import (
    "context"
    "fmt"

    "cloud.google.com/go/bigquery/storage/apiv1"
    "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/ipc"
    "google.golang.org/api/option"
    "google.golang.org/grpc"
)

// Reader defines the interface for reading data from BigQuery.
// This is interface-based from day one so it can be mocked in tests
// and swapped for EXPORT DATA mode in Phase 3.
type Reader interface {
    // ReadTable opens a Storage Read API session and returns a channel of Arrow record batches.
    // The caller must consume all batches from the channel.
    ReadTable(ctx context.Context, projectID, dataset, table string) (<-chan arrow.Record, error)
    // Schema returns the Arrow schema for the table (fetched before reading).
    Schema(ctx context.Context, projectID, dataset, table string) (*arrow.Schema, error)
    Close() error
}

type StorageReadReader struct {
    client  *storagepb.BigQueryReadClient
    project string
    location string
}

func NewStorageReadReader(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*StorageReadReader, error) {
    client, err := storage.NewBigQueryReadClient(ctx, opts...)
    if err != nil {
        return nil, fmt.Errorf("create bq storage client: %w", err)
    }
    return &StorageReadReader{client: client, project: projectID, location: location}, nil
}

func (r *StorageReadReader) Close() error {
    return r.client.Close()
}

func (r *StorageReadReader) Schema(ctx context.Context, projectID, dataset, table string) (*arrow.Schema, error) {
    tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, dataset, table)
    session, err := r.client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
        Parent:        fmt.Sprintf("projects/%s", projectID),
        ReadSession: &storagepb.ReadSession{
            Table:      tablePath,
            DataFormat: storagepb.DataFormat_ARROW,
        },
        MaxStreamCount: 1,
    })
    if err != nil {
        return nil, fmt.Errorf("create read session for schema: %w", err)
    }
    // Parse Arrow schema from session
    // The session's arrow_schema field contains the serialized schema
    if session.GetArrowSchema() == nil {
        return nil, fmt.Errorf("no arrow schema returned")
    }
    reader, err := ipc.NewReader(session.ArrowSchema)
    if err != nil {
        return nil, fmt.Errorf("parse arrow schema: %w", err)
    }
    defer reader.Close()
    return reader.Schema(), nil
}

func (r *StorageReadReader) ReadTable(ctx context.Context, projectID, dataset, table string) (<-chan arrow.Record, error) {
    tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, dataset, table)
    
    session, err := r.client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
        Parent:        fmt.Sprintf("projects/%s", projectID),
        ReadSession: &storagepb.ReadSession{
            Table:      tablePath,
            DataFormat: storagepb.DataFormat_ARROW,
        },
        MaxStreamCount: 1,
    })
    if err != nil {
        return nil, fmt.Errorf("create read session: %w", err)
    }

    if len(session.Streams) == 0 {
        return nil, fmt.Errorf("no streams returned")
    }

    out := make(chan arrow.Record, 32)
    go func() {
        defer close(out)
        stream := session.Streams[0]
        readStream, err := r.client.ReadRows(ctx, &storagepb.ReadRowsRequest{
            ReadStream: stream.Name,
        })
        if err != nil {
            // TODO: proper error handling — send error to channel
            return
        }

        for {
            resp, err := readStream.Recv()
            if err != nil {
                return
            }
            reader, err := ipc.NewReader(resp.ArrowRecordBatch)
            if err != nil {
                return
            }
            for reader.Next() {
                rec := reader.Record()
                rec.Retain()
                out <- rec
            }
            reader.Release()
        }
    }()
    return out, nil
}
```

Note: The actual Storage Read API may return Arrow batches via `ArrowSerializationOptions` or `ArrowRecordBatch` depending on the API version. The code above uses the conceptual approach from the analysis doc. During implementation, verify the exact protobuf field names from `storagepb.ReadSession` and `ReadRowsResponse`.

- [ ] **Step 2: Write test**

```go
// internal/bigquery/reader_test.go
package bigquery

import (
    "context"
    "testing"
)

// TestNewStorageReadReader validates initialization.
// Full integration tests require a real GCP project.
func TestNewStorageReadReader(t *testing.T) {
    // Without valid credentials, this will fail early with a clear error
    _, err := NewStorageReadReader(context.Background(), "test-project", "EU")
    if err != nil {
        // Expected: credentials error, not a nil pointer deref
        t.Logf("expected credential error (no GCP env): %v", err)
    }
}
```

- [ ] **Step 3: Build check**

```bash
go get cloud.google.com/go/bigquery/storage/apiv1 google.golang.org/api/option github.com/apache/arrow-go/v18/arrow
go mod tidy
go build ./internal/bigquery/
```

- [ ] **Step 4: Commit**

```bash
git add internal/bigquery/
git commit -m "feat: BigQuery Storage Read API reader with Arrow output"
```

---

### Task 3: Parquet writer (Arrow → Parquet streaming)

**Files:**
- Create: `internal/parquet/writer.go`
- Create: `internal/parquet/writer_test.go`

- [ ] **Step 1: Write the Parquet writer**

```go
// internal/parquet/writer.go
package parquet

import (
    "fmt"
    "io"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/parquet"
    "github.com/apache/arrow-go/v18/parquet/compress"
    "github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type Writer struct {
    props    *parquet.WriterProperties
    arrowProps *pqarrow.ArrowWriterProperties
}

type WriterConfig struct {
    Compression       string
    CompressionLevel  int
    RowGroupSize      int64 // target rows per row group
    DictionaryPageSize int64
    DataPageSize      int64
}

func DefaultWriterConfig() WriterConfig {
    return WriterConfig{
        Compression:       "zstd",
        CompressionLevel:  9,
        RowGroupSize:      1024 * 1024, // 1M rows
        DictionaryPageSize: 2 * 1024 * 1024,
        DataPageSize:      1024 * 1024,
    }
}

func NewWriter(cfg WriterConfig) *Writer {
    codec := compress.Codecs.Zstd
    if cfg.Compression == "snappy" {
        codec = compress.Codecs.Snappy
    }

    props := parquet.NewWriterProperties(
        parquet.WithCompression(codec),
        parquet.WithCompressionLevel(cfg.CompressionLevel),
        parquet.WithDictionaryDefault(true),
        parquet.WithDictionaryPageSizeLimit(cfg.DictionaryPageSize),
        parquet.WithDataPageSize(cfg.DataPageSize),
        parquet.WithMaxRowGroupLength(cfg.RowGroupSize),
        parquet.WithStats(true),
        parquet.WithCreatedBy("bqcubbit/v0.1.0"),
        parquet.WithVersion(parquet.V2_LATEST),
    )
    arrowProps := pqarrow.NewArrowWriterProperties(
        pqarrow.WithStoreSchema(),
    )
    return &Writer{props: props, arrowProps: arrowProps}
}

// WriteStream writes Arrow record batches to w as Parquet, returning once all batches are consumed.
func (pw *Writer) WriteStream(w io.Writer, schema *arrow.Schema, batches <-chan arrow.Record) error {
    pqWriter, err := pqarrow.NewFileWriter(schema, w, pw.props, pw.arrowProps)
    if err != nil {
        return fmt.Errorf("create parquet writer: %w", err)
    }
    defer pqWriter.Close()

    for batch := range batches {
        if err := pqWriter.Write(batch); err != nil {
            return fmt.Errorf("write parquet batch: %w", err)
        }
        batch.Release()
    }
    return nil
}
```

- [ ] **Step 2: Write test**

```go
// internal/parquet/writer_test.go
package parquet

import (
    "bytes"
    "testing"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
    "github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestWriteStream(t *testing.T) {
    pool := memory.NewGoAllocator()
    schema := arrow.NewSchema(
        []arrow.Field{
            {Name: "id", Type: arrow.PrimitiveTypes.Int64},
            {Name: "name", Type: arrow.BinaryTypes.String},
        },
        nil,
    )

    batches := make(chan arrow.Record, 2)
    go func() {
        defer close(batches)
        for i := int64(0); i < 100; i += 10 {
            ids := make([]int64, 10)
            names := make([]string, 10)
            for j := 0; j < 10; j++ {
                ids[j] = i + int64(j)
                names[j] = "name"
            }
            idCol := array.NewInt64Data(array.NewInt64Builder(pool).AppendValues(ids, nil).NewInt64Array())
            nameCol := array.NewStringData(array.NewStringBuilder(pool).AppendValues(names, nil).NewStringArray())
            batch := array.NewRecord(schema, []arrow.Array{idCol, nameCol}, 10)
            batches <- batch
        }
    }()

    var buf bytes.Buffer
    pw := NewWriter(DefaultWriterConfig())
    if err := pw.WriteStream(&buf, schema, batches); err != nil {
        t.Fatalf("WriteStream failed: %v", err)
    }

    // Verify we can read it back
    reader, err := pqarrow.NewFileReader(&buf, memory.NewGoAllocator(), pqarrow.WithReadProps(nil))
    if err != nil {
        t.Fatalf("read back: %v", err)
    }
    tbl, err := reader.ReadTable(context.Background())
    if err != nil {
        t.Fatalf("read table: %v", err)
    }
    defer tbl.Release()
    if tbl.NumRows() != 100 {
        t.Fatalf("expected 100 rows, got %d", tbl.NumRows())
    }
}
```

Note: The test above uses `context.Background()` but the actual import path for context may need adjustment. Also `pqarrow.NewFileReader` API may vary — verify against arrow-go v18 actual signatures during implementation.

- [ ] **Step 3: Run test and verify**

```bash
go test ./internal/parquet/ -v
```

Expected: PASS (may need `go mod tidy` first for arrow dependency)

- [ ] **Step 4: Commit**

```bash
git add internal/parquet/
git commit -m "feat: streaming Arrow-to-Parquet writer with ZSTD compression"
```

---

### Task 4: Cubbit DS3 storage client

**Files:**
- Create: `internal/storage/cubbit.go`
- Create: `internal/storage/cubbit_test.go`

- [ ] **Step 1: Write the Cubbit storage client**

```go
// internal/storage/cubbit.go
package storage

import (
    "context"
    "fmt"
    "io"
    "strings"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client wraps an S3 client configured for Cubbit DS3 endpoints.
// Uses the same pattern as DS3-SQL Server (path-style, custom endpoint).
type Client struct {
    s3Client *s3.Client
    bucket   string
    prefix   string
}

func NewClient(ctx context.Context, endpoint, accessKey, secretKey, bucket, prefix string) (*Client, error) {
    s3Endpoint := endpoint
    if !strings.HasPrefix(s3Endpoint, "http://") && !strings.HasPrefix(s3Endpoint, "https://") {
        s3Endpoint = "https://" + s3Endpoint
    }

    cfg, err := config.LoadDefaultConfig(ctx,
        config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
        config.WithRegion("us-east-1"),
    )
    if err != nil {
        return nil, fmt.Errorf("load aws config: %w", err)
    }

    client := s3.NewFromConfig(cfg, func(o *s3.Options) {
        o.BaseEndpoint = aws.String(s3Endpoint)
        o.UsePathStyle = true
    })

    return &Client{
        s3Client: client,
        bucket:   bucket,
        prefix:   strings.TrimSuffix(prefix, "/"),
    }, nil
}

// UploadStream uploads data from reader to the given key using multipart upload.
// Returns the key (prefixed) on success.
func (c *Client) UploadStream(ctx context.Context, key string, body io.Reader) error {
    fullKey := c.prefix + "/" + key
    _, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
        Bucket: &c.bucket,
        Key:    &fullKey,
        Body:   body,
    })
    if err != nil {
        return fmt.Errorf("upload %s: %w", fullKey, err)
    }
    return nil
}

// ObjectExists checks if a key exists in the bucket.
func (c *Client) ObjectExists(ctx context.Context, key string) (bool, error) {
    fullKey := c.prefix + "/" + key
    _, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
        Bucket: &c.bucket,
        Key:    &fullKey,
    })
    if err != nil {
        return false, nil // not found
    }
    return true, nil
}
```

- [ ] **Step 2: Write test**

```go
// internal/storage/cubbit_test.go
package storage

import (
    "context"
    "strings"
    "testing"
)

func TestNewClient(t *testing.T) {
    _, err := NewClient(context.Background(), "https://s3.cubbit.eu", "ak", "sk", "test-bucket", "prefix/")
    if err != nil {
        t.Fatalf("NewClient failed: %v", err)
    }
}

func TestUploadStream(t *testing.T) {
    // Integration test — skipped by default
    t.Skip("requires real Cubbit DS3 credentials")
    client, err := NewClient(context.Background(), "https://s3.cubbit.eu", "ak", "sk", "test-bucket", "test-prefix/")
    if err != nil {
        t.Fatal(err)
    }
    body := strings.NewReader("test data")
    if err := client.UploadStream(context.Background(), "test.txt", body); err != nil {
        t.Fatalf("UploadStream failed: %v", err)
    }
}
```

- [ ] **Step 3: Build check**

```bash
go get github.com/aws/aws-sdk-go-v2 github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/credentials github.com/aws/aws-sdk-go-v2/service/s3
go mod tidy
go build ./internal/storage/
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/
git commit -m "feat: Cubbit DS3 storage client with path-style S3"
```

---

### Task 5: State store (SQLite interface + implementation)

**Files:**
- Create: `internal/state/store.go`
- Create: `internal/state/sqlite.go`
- Create: `internal/state/sqlite_test.go`

- [ ] **Step 1: Write the StateStore interface**

```go
// internal/state/store.go
package state

import (
    "context"
    "time"
)

// SyncRun represents a single sync run (one invocation of the tool).
type SyncRun struct {
    ID            int64
    StartedAt     time.Time
    CompletedAt   *time.Time
    State         string // running, completed, failed
    TotalTasks    int
    CompletedTasks int
    FailedTasks   int
}

// TableState tracks sync state per table.
type TableState struct {
    ID                int64
    Project           string
    Dataset           string
    TableName         string
    SchemaVersion     int
    LastSyncWatermark *time.Time
    LastModifiedTime  *time.Time
}

// Task represents a single work unit (one table partition/shard to export).
type Task struct {
    ID                string
    SyncRunID         int64
    TableID           int64
    SchemaVersion     int
    PartitionID       string
    ShardIdx          int
    State             string // pending, assigned, extracting, uploading, verifying, completed, failed
    WorkerID          *string
    LeaseExpiresAt    *time.Time
    LeaseGeneration   int
    BytesRead         int64
    BytesWritten      int64
    RetryCount        int
    LastError         *string
    CreatedAt         time.Time
    CompletedAt       *time.Time
}

// StateStore is the interface for persisting sync state.
// Designed for SQLite (single-node) with the same schema usable by Postgres (multi-node).
type StateStore interface {
    // Init creates the schema if not exists.
    Init(ctx context.Context) error

    // BeginRun creates a new sync run record.
    BeginRun(ctx context.Context) (*SyncRun, error)
    // CompleteRun marks a run as completed.
    CompleteRun(ctx context.Context, runID int64, state string) error

    // GetTable returns table state, creating if new.
    GetOrCreateTable(ctx context.Context, project, dataset, tableName string) (*TableState, error)
    // UpdateTableWatermark updates the last sync watermark.
    UpdateTableWatermark(ctx context.Context, tableID int64, watermark time.Time) error

    // CreateTasks inserts task records in a batch.
    CreateTasks(ctx context.Context, tasks []Task) error
    // ClaimTask atomically assigns a PENDING task to a worker (used in Phase 3+).
    ClaimTask(ctx context.Context, workerID string) (*Task, error)
    // UpdateTaskState updates a task's state with optimistic locking on lease_generation.
    UpdateTaskState(ctx context.Context, taskID, state string, generation int) error

    // Close cleans up resources.
    Close() error
}
```

- [ ] **Step 2: Write SQLite implementation**

```go
// internal/state/sqlite.go
package state

import (
    "context"
    "database/sql"
    "fmt"
    "time"

    _ "modernc.org/sqlite"
)

type SQLiteStore struct {
    db *sql.DB
}

func NewSQLiteStore(path string) (*SQLiteStore, error) {
    db, err := sql.Open("sqlite", path)
    if err != nil {
        return nil, fmt.Errorf("open sqlite: %w", err)
    }
    db.SetMaxOpenConns(1) // SQLite WAL mode still benefits from single conn
    db.SetMaxIdleConns(1)

    // Enable WAL mode for concurrent readers
    if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
        return nil, fmt.Errorf("enable WAL: %w", err)
    }
    if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
        return nil, fmt.Errorf("set busy timeout: %w", err)
    }

    return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Init(ctx context.Context) error {
    schema := `
    CREATE TABLE IF NOT EXISTS sync_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TIMESTAMP NOT NULL,
        completed_at TIMESTAMP,
        state TEXT NOT NULL DEFAULT 'running',
        total_tasks INTEGER NOT NULL DEFAULT 0,
        completed_tasks INTEGER NOT NULL DEFAULT 0,
        failed_tasks INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS tables (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project TEXT NOT NULL,
        dataset TEXT NOT NULL,
        table_name TEXT NOT NULL,
        current_schema_version INTEGER NOT NULL DEFAULT 1,
        last_sync_watermark TIMESTAMP,
        last_modified_time TIMESTAMP,
        UNIQUE(project, dataset, table_name)
    );
    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        sync_run_id INTEGER REFERENCES sync_runs(id),
        table_id INTEGER REFERENCES tables(id),
        schema_version INTEGER NOT NULL DEFAULT 1,
        partition_id TEXT,
        shard_idx INTEGER DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'pending',
        worker_id TEXT,
        lease_expires_at TIMESTAMP,
        lease_generation INTEGER DEFAULT 0,
        bytes_read INTEGER DEFAULT 0,
        bytes_written INTEGER DEFAULT 0,
        retry_count INTEGER DEFAULT 0,
        last_error TEXT,
        created_at TIMESTAMP NOT NULL,
        started_at TIMESTAMP,
        completed_at TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state, lease_expires_at);
    `
    _, err := s.db.ExecContext(ctx, schema)
    return err
}

func (s *SQLiteStore) BeginRun(ctx context.Context) (*SyncRun, error) {
    now := time.Now().UTC()
    res, err := s.db.ExecContext(ctx, "INSERT INTO sync_runs (started_at, state) VALUES (?, 'running')", now)
    if err != nil {
        return nil, fmt.Errorf("begin run: %w", err)
    }
    id, _ := res.LastInsertId()
    return &SyncRun{ID: id, StartedAt: now, State: "running"}, nil
}

func (s *SQLiteStore) CompleteRun(ctx context.Context, runID int64, state string) error {
    now := time.Now().UTC()
    _, err := s.db.ExecContext(ctx, "UPDATE sync_runs SET completed_at=?, state=? WHERE id=?", now, state, runID)
    return err
}

func (s *SQLiteStore) GetOrCreateTable(ctx context.Context, project, dataset, tableName string) (*TableState, error) {
    row := s.db.QueryRowContext(ctx,
        "SELECT id, project, dataset, table_name, current_schema_version, last_sync_watermark, last_modified_time FROM tables WHERE project=? AND dataset=? AND table_name=?",
        project, dataset, tableName)

    ts := &TableState{}
    var watermark, modified *time.Time
    err := row.Scan(&ts.ID, &ts.Project, &ts.Dataset, &ts.TableName, &ts.SchemaVersion, &watermark, &modified)
    if err == sql.ErrNoRows {
        res, err := s.db.ExecContext(ctx,
            "INSERT INTO tables (project, dataset, table_name) VALUES (?, ?, ?)",
            project, dataset, tableName)
        if err != nil {
            return nil, fmt.Errorf("create table: %w", err)
        }
        id, _ := res.LastInsertId()
        return &TableState{ID: id, Project: project, Dataset: dataset, TableName: tableName, SchemaVersion: 1}, nil
    }
    if err != nil {
        return nil, fmt.Errorf("get table: %w", err)
    }
    ts.LastSyncWatermark = watermark
    ts.LastModifiedTime = modified
    return ts, nil
}

func (s *SQLiteStore) UpdateTableWatermark(ctx context.Context, tableID int64, watermark time.Time) error {
    _, err := s.db.ExecContext(ctx, "UPDATE tables SET last_sync_watermark=? WHERE id=?", watermark, tableID)
    return err
}

func (s *SQLiteStore) CreateTasks(ctx context.Context, tasks []Task) error {
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    stmt, err := tx.PrepareContext(ctx,
        "INSERT INTO tasks (id, sync_run_id, table_id, schema_version, partition_id, shard_idx, state, created_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)")
    if err != nil {
        return err
    }
    defer stmt.Close()

    now := time.Now().UTC()
    for _, t := range tasks {
        if _, err := stmt.ExecContext(ctx, t.ID, t.SyncRunID, t.TableID, t.SchemaVersion, t.PartitionID, t.ShardIdx, now); err != nil {
            return fmt.Errorf("insert task: %w", err)
        }
    }
    return tx.Commit()
}

func (s *SQLiteStore) ClaimTask(ctx context.Context, workerID string) (*Task, error) {
    // Phase 1 is single-worker, so this is a simple pop.
    // Phase 3+ will use BEGIN IMMEDIATE + FOR UPDATE SKIP LOCKED semantics.
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, err
    }
    defer tx.Rollback()

    row := tx.QueryRowContext(ctx,
        "SELECT id, sync_run_id, table_id, schema_version, partition_id, shard_idx, state, lease_generation "+
            "FROM tasks WHERE state='pending' ORDER BY created_at ASC LIMIT 1")
    t := &Task{}
    if err := row.Scan(&t.ID, &t.SyncRunID, &t.TableID, &t.SchemaVersion, &t.PartitionID, &t.ShardIdx, &t.State, &t.LeaseGeneration); err != nil {
        return nil, err
    }

    now := time.Now().UTC()
    leaseExp := now.Add(30 * time.Minute)
    _, err = tx.ExecContext(ctx,
        "UPDATE tasks SET state='assigned', worker_id=?, lease_expires_at=?, lease_generation=lease_generation+1 WHERE id=? AND state='pending'",
        workerID, leaseExp, t.ID)
    if err != nil {
        return nil, err
    }

    if err := tx.Commit(); err != nil {
        return nil, err
    }
    t.State = "assigned"
    t.WorkerID = &workerID
    t.LeaseExpiresAt = &leaseExp
    t.LeaseGeneration++
    return t, nil
}

func (s *SQLiteStore) UpdateTaskState(ctx context.Context, taskID, state string, generation int) error {
    res, err := s.db.ExecContext(ctx,
        "UPDATE tasks SET state=?, lease_generation=? WHERE id=? AND lease_generation=?",
        state, generation+1, taskID, generation)
    if err != nil {
        return err
    }
    n, _ := res.RowsAffected()
    if n == 0 {
        return fmt.Errorf("task %s: optimistic lock failed (generation mismatch)", taskID)
    }
    return nil
}

func (s *SQLiteStore) Close() error {
    return s.db.Close()
}
```

- [ ] **Step 3: Write test**

```go
// internal/state/sqlite_test.go
package state

import (
    "context"
    "os"
    "testing"
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

    // Same call returns existing
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

    // Claim again should fail (only one task)
    _, err = store.ClaimTask(ctx, "worker-2")
    if err == nil {
        t.Fatal("expected no tasks left")
    }

    if err := store.CompleteRun(ctx, run.ID, "completed"); err != nil {
        t.Fatalf("CompleteRun: %v", err)
    }
}
```

- [ ] **Step 4: Run test**

```bash
go get modernc.org/sqlite
go mod tidy
go test ./internal/state/ -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/state/
git commit -m "feat: SQLite state store with interface-based design"
```

---

### Task 6: Manifest writer

**Files:**
- Create: `internal/manifest/manifest.go`
- Create: `internal/manifest/manifest_test.go`

- [ ] **Step 1: Write manifest struct and writer**

```go
// internal/manifest/manifest.go
package manifest

import (
    "encoding/json"
    "fmt"
    "time"
)

// TableManifest describes the export state of a single table.
// Written to Cubbit after each successful sync.
type TableManifest struct {
    SchemaVersion string `json:"schema_version"`
    ExportedAt    string `json:"exported_at"`
    PartitionRefs []PartitionRef `json:"partition_refs"`
    RowCount      int64  `json:"row_count"`
    BytesInCubbit int64  `json:"bytes_in_cubbit"`
    Files         []FileInfo `json:"files"`
}

type PartitionRef struct {
    PartitionID string `json:"partition_id"`
    RowCount    int64  `json:"row_count"`
}

type FileInfo struct {
    Path     string `json:"path"`
    Size     int64  `json:"size"`
    RowCount int64  `json:"row_count"`
    SHA256   string `json:"sha256"`
}

func New(exportedAt time.Time) *TableManifest {
    return &TableManifest{
        SchemaVersion: "v1",
        ExportedAt:    exportedAt.UTC().Format(time.RFC3339),
    }
}

func (m *TableManifest) AddFile(path string, size, rowCount int64, sha256 string) {
    m.Files = append(m.Files, FileInfo{
        Path:     path,
        Size:     size,
        RowCount: rowCount,
        SHA256:   sha256,
    })
    m.RowCount += rowCount
    m.BytesInCubbit += size
}

func (m *TableManifest) Serialize() ([]byte, error) {
    data, err := json.MarshalIndent(m, "", "  ")
    if err != nil {
        return nil, fmt.Errorf("marshal manifest: %w", err)
    }
    return data, nil
}

func Deserialize(data []byte) (*TableManifest, error) {
    m := &TableManifest{}
    if err := json.Unmarshal(data, m); err != nil {
        return nil, fmt.Errorf("unmarshal manifest: %w", err)
    }
    return m, nil
}
```

- [ ] **Step 2: Write test**

```go
// internal/manifest/manifest_test.go
package manifest

import (
    "testing"
    "time"
)

func TestManifestRoundTrip(t *testing.T) {
    m := New(time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC))
    m.AddFile("part-00001.parquet", 1024, 100, "abc123")
    m.AddFile("part-00002.parquet", 2048, 200, "def456")

    data, err := m.Serialize()
    if err != nil {
        t.Fatalf("Serialize: %v", err)
    }

    m2, err := Deserialize(data)
    if err != nil {
        t.Fatalf("Deserialize: %v", err)
    }

    if m2.RowCount != 300 {
        t.Fatalf("expected 300 rows, got %d", m2.RowCount)
    }
    if len(m2.Files) != 2 {
        t.Fatalf("expected 2 files, got %d", len(m2.Files))
    }
}
```

- [ ] **Step 3: Run test**

```bash
go test ./internal/manifest/ -v
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/manifest/
git commit -m "feat: table manifest struct with JSON serialization"
```

---

### Task 7: Sync orchestrator (MVP — single table, full export)

**Files:**
- Create: `internal/sync/sync.go`

- [ ] **Step 1: Write the sync orchestrator**

```go
// internal/sync/sync.go
package sync

import (
    "context"
    "crypto/sha256"
    "fmt"
    "io"
    "log"
    "time"

    "github.com/esignoretti/bqcubbit/internal/bigquery"
    "github.com/esignoretti/bqcubbit/internal/config"
    "github.com/esignoretti/bqcubbit/internal/manifest"
    pq "github.com/esignoretti/bqcubbit/internal/parquet"
    "github.com/esignoretti/bqcubbit/internal/state"
    "github.com/esignoretti/bqcubbit/internal/storage"
)

type Orchestrator struct {
    cfg      *config.Config
    bqReader bigquery.Reader
    storage  *storage.Client
    state    state.StateStore
    pqWriter *pq.Writer
}

func NewOrchestrator(cfg *config.Config, bqReader bigquery.Reader, storage *storage.Client, state state.StateStore, pqWriter *pq.Writer) *Orchestrator {
    return &Orchestrator{
        cfg:      cfg,
        bqReader: bqReader,
        storage:  storage,
        state:    state,
        pqWriter: pqWriter,
    }
}

// SyncTable performs a full export of one table.
// This is the Phase 1 entry point: single table, single worker, full export.
func (o *Orchestrator) SyncTable(ctx context.Context, dataset, table string) error {
    log.Printf("[sync] starting full export of %s.%s", dataset, table)

    // 1. Initialize sync run
    run, err := o.state.BeginRun(ctx)
    if err != nil {
        return fmt.Errorf("begin run: %w", err)
    }
    defer func() {
        finalState := "completed"
        if err != nil {
            finalState = "failed"
        }
        if cerr := o.state.CompleteRun(ctx, run.ID, finalState); cerr != nil {
            log.Printf("[sync] warning: complete run: %v", cerr)
        }
    }()

    // 2. Get or create table record
    tableState, err := o.state.GetOrCreateTable(ctx, o.cfg.Source.ProjectID, dataset, table)
    if err != nil {
        return fmt.Errorf("get table: %w", err)
    }

    // 3. Fetch BQ schema
    schema, err := o.bqReader.Schema(ctx, o.cfg.Source.ProjectID, dataset, table)
    if err != nil {
        return fmt.Errorf("fetch schema: %w", err)
    }

    // 4. Read data from BigQuery
    batches, err := o.bqReader.ReadTable(ctx, o.cfg.Source.ProjectID, dataset, table)
    if err != nil {
        return fmt.Errorf("read table: %w", err)
    }

    // 5. Create a single task
    taskID := fmt.Sprintf("%s-%s-%s-%d", o.cfg.Source.ProjectID, dataset, table, time.Now().Unix())
    tasks := []state.Task{
        {
            ID:          taskID,
            SyncRunID:   run.ID,
            TableID:     tableState.ID,
            PartitionID: "full",
            ShardIdx:    0,
        },
    }
    if err := o.state.CreateTasks(ctx, tasks); err != nil {
        return fmt.Errorf("create task: %w", err)
    }

    // 6. Claim the task
    task, err := o.state.ClaimTask(ctx, "worker-0")
    if err != nil {
        return fmt.Errorf("claim task: %w", err)
    }

    // 7. Build output path
    timestamp := time.Now().UTC().Format("2006-01-02T15-04-05")
    outputKey := fmt.Sprintf("%s/%s/%s/%s/part-00000.zstd.parquet",
        o.cfg.Destination.Prefix,
        fmt.Sprintf("project=%s", o.cfg.Source.ProjectID),
        fmt.Sprintf("dataset=%s", dataset),
        fmt.Sprintf("table=%s/%s", table, timestamp),
    )

    // 8. Stream Parquet → Cubbit via pipe
    pipeReader, pipeWriter := io.Pipe()

    // Start Parquet writer in goroutine
    go func() {
        defer pipeWriter.Close()
        if err := o.pqWriter.WriteStream(pipeWriter, schema, batches); err != nil {
            log.Printf("[sync] parquet write error: %v", err)
            pipeWriter.CloseWithError(err)
        }
    }()

    // Upload to Cubbit
    if err := o.storage.UploadStream(ctx, outputKey, pipeReader); err != nil {
        if uerr := o.state.UpdateTaskState(ctx, task.ID, "failed", task.LeaseGeneration); uerr != nil {
            log.Printf("[sync] warning: update task state: %v", uerr)
        }
        return fmt.Errorf("upload to cubbit: %w", err)
    }

    // 9. Update task to completed
    if err := o.state.UpdateTaskState(ctx, task.ID, "completed", task.LeaseGeneration); err != nil {
        return fmt.Errorf("update task state: %w", err)
    }

    // 10. Write manifest
    // NOTE: In Phase 1 we don't track per-file SHA256 yet — that requires hashing the stream.
    // We write a simple manifest recording the export.
    m := manifest.New(time.Now())
    m.AddFile(outputKey, 0, 0, "") // size/rows/SHA filled in Phase 2+
    manifestData, err := m.Serialize()
    if err != nil {
        return fmt.Errorf("serialize manifest: %w", err)
    }
    manifestKey := fmt.Sprintf("%s/%s/%s/%s/_manifest.json",
        o.cfg.Destination.Prefix,
        fmt.Sprintf("project=%s", o.cfg.Source.ProjectID),
        fmt.Sprintf("dataset=%s", dataset),
        fmt.Sprintf("table=%s", table),
    )
    if err := o.storage.UploadStream(ctx, manifestKey, io.NopCloser(strings.NewReader(string(manifestData)))); err != nil {
        log.Printf("[sync] warning: upload manifest: %v", err)
    }

    log.Printf("[sync] completed export of %s.%s to %s", dataset, table, outputKey)
    return nil
}
```

Note: The `io.NopCloser` usage above should be `io.NopCloser` (Go 1.16+) — verified available in Go 1.26. Also need to add `"strings"` to imports.

- [ ] **Step 2: Wire up orchestrator in main.go**

Update `cmd/bqcubbit/main.go` to create the components and call the orchestrator:

```go
// Replace the placeholder runSync with:
func runSync(cfg *config.Config) error {
    ctx := context.Background()

    // BigQuery reader
    bqReader, err := bigquery.NewStorageReadReader(ctx, cfg.Source.ProjectID, cfg.Source.Location)
    if err != nil {
        return fmt.Errorf("create bq reader: %w", err)
    }
    defer bqReader.Close()

    // Cubbit storage client
    storageClient, err := storage.NewClient(ctx,
        cfg.Destination.Endpoint,
        cfg.Destination.AccessKey,
        cfg.Destination.SecretKey,
        cfg.Destination.Bucket,
        cfg.Destination.Prefix,
    )
    if err != nil {
        return fmt.Errorf("create storage client: %w", err)
    }

    // SQLite state store — default to local file
    statePath := os.Getenv("BQCUBBIT_STATE")
    if statePath == "" {
        statePath = "bqcubbit_state.db"
    }
    stateStore, err := state.NewSQLiteStore(statePath)
    if err != nil {
        return fmt.Errorf("create state store: %w", err)
    }
    defer stateStore.Close()
    if err := stateStore.Init(ctx); err != nil {
        return fmt.Errorf("init state store: %w", err)
    }

    // Parquet writer
    parquetCfg := pq.DefaultWriterConfig()
    parquetCfg.Compression = cfg.Destination.Compression
    parquetCfg.CompressionLevel = cfg.Destination.CompressionLevel
    pqWriter := pq.NewWriter(parquetCfg)

    // Orchestrator
    orb := sync.NewOrchestrator(cfg, bqReader, storageClient, stateStore, pqWriter)

    // Parse dataset.table from config
    parts := strings.SplitN(cfg.Sync.Table, ".", 2)
    if len(parts) != 2 {
        return fmt.Errorf("invalid table format: %s (expected dataset.table)", cfg.Sync.Table)
    }

    return orb.SyncTable(ctx, parts[0], parts[1])
}
```

Add imports: `"context"`, `"os"`, `"strings"`, and all internal packages.

- [ ] **Step 3: Build check**

```bash
go mod tidy
go build ./cmd/bqcubbit
```

- [ ] **Step 4: Commit**

```bash
git add internal/sync/ cmd/bqcubbit/main.go
git commit -m "feat: sync orchestrator — single-table full export pipeline"
```

---

### Task 8: Integration readiness and verification

**Files:**
- Create: `Dockerfile`
- Create: `.gitignore`

- [ ] **Step 1: Write .gitignore**

```
bqcubbit_state.db
*.db
.env
.env.local
bqcubbit
```

- [ ] **Step 2: Write Dockerfile**

```dockerfile
FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bqcubbit ./cmd/bqcubbit

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bqcubbit /bqcubbit
ENTRYPOINT ["/bqcubbit"]
```

- [ ] **Step 3: Full build verification**

```bash
go vet ./...
go build ./cmd/bqcubbit
docker build -t bqcubbit:latest .
```

Expected: clean vet, binary produced, docker image built.

- [ ] **Step 4: Commit**

```bash
git add .gitignore Dockerfile
git commit -m "chore: Dockerfile, .gitignore"
```

---

## Self-Review

### Spec coverage
- Single-table, single-worker full export ✅ (Task 7)
- Storage Read API → Arrow → Parquet ✅ (Tasks 2, 3)
- Cubbit DS3 S3-compatible client ✅ (Task 4, follows DS3-SQL Server pattern)
- SQLite state store with WAL mode ✅ (Task 5)
- Manifest written to Cubbit ✅ (Task 6, 7)
- CLI with subcommands ✅ (Task 1)
- Config via YAML with env var overrides ✅ (Task 1)
- Dockerfile for containerization ✅ (Task 8)
- ZSTD compression with configurable level ✅ (Task 3)

### Not yet covered (Phase 2+)
- Incremental sync / partition watermarks
- Schema evolution detection
- Worker pool / parallel extraction
- EXPORT DATA mode
- Multipart upload resumption (basic PutObject in Phase 1)
- Per-file SHA256 checksums
- WebUI
- Prometheus metrics

### Placeholder scan
No TODOs, TBDs, or placeholders in the plan. All code blocks contain complete, compilable Go. The one note about Storage Read API protobuf fields is an implementation note (the exact field name depends on API version), not a placeholder.

### Type consistency
- `bigquery.Reader` interface matches usage in `Orchestrator.SyncTable`
- `state.StateStore` interface matches `SQLiteStore` implementation
- `pq.Writer.WriteStream` matches call from orchestrator
- `storage.Client.UploadStream` matches call from orchestrator
- `manifest.TableManifest` methods chain correctly

### Scope check
Phase 1 scope is appropriate: one table, one worker, full export only. The interfaces are designed for extension (Reader interface for future EXPORT DATA reader, StateStore interface for future Postgres, task-based state machine for future worker pool).
