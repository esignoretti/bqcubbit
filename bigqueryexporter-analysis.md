# BigQuery → Cubbit DS3 Export Tool: Design Analysis

Brainstorming and architectural analysis for a Go-based tool that exports BigQuery data to Cubbit DS3 (S3-compatible) as Parquet files, with full \+ incremental sync, CLI \+ WebUI, optimized for multi-TB scale and minimal egress cost.

---

## Table of Contents

1. [Initial Architecture Brainstorm](#part-1-initial-architecture-brainstorm)  
2. [Multi-TB Scale Deep Dive](#part-2-multi-tb-scale-deep-dive)  
3. [Three Hardest Subsystems](#part-3-three-hardest-subsystems)  
   - [Parquet Writing with Nested Types](#31-parquet-writing-with-nested-types)  
   - [Schema Evolution as a State Machine](#32-schema-evolution-as-a-state-machine)  
   - [Worker Pool and Coordinator Concurrency](#33-worker-pool-and-coordinator-concurrency)

---

# Part 1: Initial Architecture Brainstorm

## Core Architecture Decision: How to Extract from BigQuery

This is the single most important cost decision. Three main paths:

**Option A: BigQuery Storage Read API (gRPC)** The Storage Read API streams Arrow/Avro directly from BigQuery's columnar storage. It's the cheapest extraction method per GB (currently around $1.10/TiB read), supports parallel streams, and has native Go bindings. Read into Arrow record batches in memory and convert to Parquet on the fly.

**Option B: BigQuery EXPORT DATA / `bq extract`** BigQuery itself writes Parquet directly to a GCS bucket using `EXPORT DATA OPTIONS(format='PARQUET', compression='SNAPPY')`. This is *free* (no Storage API charges) but you pay GCS storage temporarily, and you still need to move data from GCS to Cubbit. Catch: exports are limited to 1 GB per file, so large tables produce many shards.

**Option C: Query results to GCS, then transfer** Run a `SELECT *` and export results. Worst option — you pay query bytes-scanned costs.

**Recommendation:** Hybrid. Use **EXPORT DATA** as the default (free at the BigQuery side), then stream from GCS → Cubbit. Fall back to Storage Read API for incremental syncs of small deltas where spinning up an export job is overhead.

## The Egress Problem (This Is the Big One)

**If your tool runs *outside* GCP, you pay GCP egress fees (\~$0.08–$0.12/GB) on every byte leaving Google's network.** For a multi-TB export, that's the dominant cost — far more than compute or BigQuery itself.

Options to minimize:

1. **Run the tool inside GCP** (GCE VM, Cloud Run job, or GKE) in the same region as your BigQuery datasets. Avoid GCS → external double-hop.  
2. **Use GCS as a staging buffer in the same region as BigQuery.** Intra-region GCS↔BigQuery is free. Then egress once from GCS to Cubbit.  
3. **Compression before egress is non-negotiable.** Parquet with ZSTD (level 3–9) typically gets 5–15× compression on analytical data versus uncompressed, and 2–3× versus Snappy. ZSTD is the sweet spot.  
4. **Check if Cubbit has a GCP-peered ingress point** or partner network.  
5. **Consider Google's network service tier** — Standard tier (vs Premium) is cheaper for bulk egress.

## Incremental Sync Strategy (Day 2+)

Options:

- **Partition-based**: If tables use `_PARTITIONTIME` or ingestion-time partitioning, only re-export latest partition(s). Auto-detect partitioning.  
- **Snapshot diffing**: BigQuery table snapshots are cheap; diff two snapshots. Complex but precise.  
- **CDC via `INFORMATION_SCHEMA.PARTITIONS`**: Query last-modified time per partition, re-export only modified ones. **Probably the right default.**  
- **Append-only assumption**: For event/log tables, track `MAX(timestamp)` and pull newer rows.  
- **Full re-export**: Brute force, fine for small tables (\< 10 GB).

Config should let users pick strategy per-table, with smart defaults based on detected partition columns.

## The Multi-Environment Constraint

One configured BQ project per tool instance, not multi-tenant. Keeps service account scoping simple.

```
source:
  project_id: my-bq-project
  location: EU
  datasets: [analytics, marketing]  # or "*" for all
  exclude_tables: [analytics.temp_*]
destination:
  endpoint: https://s3.cubbit.eu
  bucket: my-bigquery-backup
  prefix: bq-export/
  compression: zstd
  compression_level: 9
schedule:
  cron: "0 2 * * *"
  initial_mode: full
  incremental_strategy: partition
```

## Go-Specific Implementation Notes

- **BigQuery client**: `cloud.google.com/go/bigquery` (official)  
- **Parquet writer**: `github.com/apache/arrow/go/v15/parquet` — production-grade, supports ZSTD, streaming writes. Avoid `xitongsys/parquet-go` for new projects.  
- **S3 client for Cubbit**: AWS SDK v2 (`github.com/aws/aws-sdk-go-v2`) with custom endpoint. Cubbit DS3 is S3-compatible.  
- **WebUI**: Embed with `//go:embed`. Use htmx \+ Go templates (dramatically simpler than React for monitoring). For more interactivity, Templ \+ Alpine.js.  
- **State store**: SQLite (`modernc.org/sqlite` — pure Go, no CGO) for tracking sync history, table watermarks, job status.  
- **Scheduling**: `github.com/robfig/cron/v3` or a simple ticker for daemon mode.

## Resumability and Incomplete Transfers

1. **S3 multipart upload with parts ≥ 16 MB** — Cubbit supports this. Resume by listing in-progress multipart uploads on startup.  
2. **Per-table manifest files** in the destination bucket recording successful uploads with content hashes. Skip re-upload if hash matches.  
3. **Atomic table commits**: Write all Parquet shards to a `_tmp/` prefix, then rename/copy to final location only when complete.  
4. **Idempotency keys** on every upload — derived from `project.dataset.table.partition.shard_hash`.  
5. **Checkpoint frequency**: After each table (not each row), persist progress to SQLite.

## WebUI Scope (Minimal v1)

- Connection status (BQ auth, Cubbit auth, last successful check)  
- Current/recent job status with progress bars  
- Per-table sync history (last sync time, rows, bytes, compression ratio)  
- Manual trigger button for ad-hoc sync of one table  
- Live log tail (WebSocket or SSE)  
- Config viewer (read-only — keep editing in YAML to avoid drift bugs)

Bind to localhost by default. Skip auth/RBAC in v1.

## Deployment Modes

Single binary with subcommands:

- `bqcubbit serve` — daemon mode with scheduler \+ WebUI  
- `bqcubbit sync --table foo.bar` — one-shot CLI  
- `bqcubbit init` — interactive config setup  
- `bqcubbit verify` — checksum verification against destination

For containerization: distroless base image, \~30 MB image.

---

# Part 2: Multi-TB Scale Deep Dive

## Egress Math: Concrete Numbers

Assume 50 TB initial export, then 2% daily change (1 TB/day delta):

**Without compression, tool outside GCP:**

- Initial: 50 TB × \~$0.11/GB egress ≈ $5,500 one-time  
- Daily: 1 TB × $0.11/GB ≈ $110/day \= \~$40,000/year

**With ZSTD-9 Parquet, tool inside GCP same-region as BQ:**

- Compression ratio on analytical data: 6-12× over raw (assume 8× as middle estimate)  
- Initial: 50 TB → \~6.25 TB compressed → \~$690 egress  
- Daily: 1 TB → \~125 GB → \~$14/day \= \~$5,100/year  
- Plus modest GCE/Cloud Run compute (\~$50-200/month)

**The compression decision alone saves roughly $35,000/year at this scale.** Running inside GCP is essentially required.

Benchmark your specific data: wide event tables with high-cardinality string columns compress poorly (3-4×); time-series with repeated dimensions compress brilliantly (15-20×). Run ZSTD-3, ZSTD-9, ZSTD-19 on a representative table sample.

**Cubbit DS3 advantage**: Cubbit charges for storage but their geo-distributed model means no egress fees on the Cubbit side — real advantage if reading frequently from anywhere. Confirm pricing tiers directly with them for multi-TB commitments.

## Architecture for Multi-TB Scale

### Worker Pool Pattern

Coordinator process scans BigQuery for work, decomposes into per-partition tasks, dispatches to worker pool. Each worker handles one partition end-to-end (extract → Parquet → compress → upload).

**Sizing for \~50 TB:**

- 1 coordinator (e2-standard-2)  
- 4-8 workers (e2-standard-8 or n2-standard-8, each handling 1-2 partitions concurrently)  
- Workers stateless — coordination via SQLite (single-node) or Postgres (multi-node)

For initial bulk load, scale workers up temporarily (16-32 workers, finish in hours), then scale back down.

### Extraction Mechanism Revision

At dozens of TB: **use EXPORT DATA for initial bulk load, but Storage Read API for incremental syncs.**

Reasoning: EXPORT DATA is free but the 1 GB file limit means a 5 TB table becomes 5,000+ files. Storage Read API gives control over partitioning/parallelism, and for delta loads of 10-100 GB/day, the $1.10/TiB cost is trivial.

BigQuery now supports ZSTD compression directly in EXPORT DATA:

```sql
EXPORT DATA OPTIONS(
  uri='gs://.../*.parquet',
  format='PARQUET',
  compression='ZSTD',
  overwrite=true
)
```

BigQuery does compression for free. Then GCS-to-Cubbit is just a transfer of already-compressed bytes.

### Partition Discovery

```sql
SELECT 
  table_catalog, table_schema, table_name, 
  partition_id, total_rows, total_logical_bytes,
  last_modified_time, storage_tier
FROM `project.region.INFORMATION_SCHEMA.PARTITIONS`
WHERE last_modified_time > @last_sync_watermark
```

This is your work queue. Each row becomes a task. SQLite states: `pending`, `extracting`, `uploading`, `complete`, `failed`.

For non-partitioned tables:

- Full re-export if under threshold (10 GB)  
- Synthetic partitioning: `MOD(FARM_FINGERPRINT(primary_key), N)` for parallelism

## Schema Evolution Strategy (No BQ Restore Needed)

### Layout: Hive-Partitioned with Schema Versioning

```
s3://bucket/bq-export/
  project=my-project/
    dataset=analytics/
      table=events/
        _schema/
          v1_20250115.json
          v2_20250403.json
          current.json -> v2_20250403.json
        date=2025-05-23/
          schema_version=v2/
            part-00001.zstd.parquet
            part-00002.zstd.parquet
```

The `schema_version=` directory in the path is key. Trino, Spark, DuckDB, Athena-compatible engines can read specific versions or unify across versions if changes are additive.

### Detection and Handling Logic

**Additive changes (new column, new nested field):**

- Increment minor schema version  
- New Parquet files include the new column  
- Old files remain valid (engines handle NULL)  
- No action needed

**Type widening (INT64 → NUMERIC, etc.):**

- Treat as new major schema version  
- Mark older partitions as "compatible with older schema"  
- Most engines handle with explicit casts

**Breaking changes (column dropped, type narrowed, renamed):**

- Major schema version bump  
- New data goes into `schema_version=v2/`  
- Two strategies for old data:  
  - **Preserve** (default): Leave v1 files; downstream uses `UNION ALL`  
  - **Rewrite**: Re-export historical partitions under new schema (expensive)

### Schema Manifest Format

```json
{
  "table": "project.dataset.events",
  "current_schema_version": "v2",
  "schemas": {
    "v1": {
      "valid_from": "2024-01-01",
      "valid_until": "2025-04-03",
      "bigquery_schema": [...],
      "parquet_schema": "...",
      "field_mappings": {...}
    },
    "v2": {
      "valid_from": "2025-04-03",
      "valid_until": null,
      "bigquery_schema": [...],
      "parquet_schema": "...",
      "changes_from_v1": [
        {"type": "add", "field": "user_agent_parsed", "data_type": "STRUCT"},
        {"type": "rename", "old": "ts", "new": "event_timestamp"}
      ]
    }
  },
  "partitioning": {
    "type": "TIME",
    "field": "event_date",
    "granularity": "DAY"
  },
  "clustering": ["user_id", "event_type"],
  "row_count_estimate": 1234567890,
  "last_full_export": "2025-01-15T00:00:00Z",
  "last_incremental_export": "2025-05-23T02:00:00Z"
}
```

## BigQuery → Parquet Type Mapping (The Gotchas)

| BigQuery Type | Parquet Mapping | Notes |
| :---- | :---- | :---- |
| INT64 | INT64 | Clean |
| FLOAT64 | DOUBLE | Clean |
| NUMERIC | DECIMAL(38,9) | Set logical type, not raw bytes |
| BIGNUMERIC | DECIMAL(76,38) | Some engines don't support; consider DOUBLE fallback |
| STRING | BYTE\_ARRAY w/ UTF8 logical type | Always UTF8 logical |
| BYTES | BYTE\_ARRAY | No logical type |
| BOOL | BOOLEAN | Clean |
| TIMESTAMP | INT64 w/ TIMESTAMP(MICROS, UTC) | BQ stores microsecond precision, UTC |
| DATETIME | INT64 w/ TIMESTAMP(MICROS, isAdjustedToUTC=false) | Local time semantics |
| DATE | INT32 w/ DATE logical type | Days since epoch |
| TIME | INT64 w/ TIME(MICROS) | Microseconds since midnight |
| GEOGRAPHY | BYTE\_ARRAY w/ UTF8 | Export as WKT; document in manifest |
| JSON | BYTE\_ARRAY w/ JSON logical type | Some engines parse; some don't |
| STRUCT (RECORD) | Group field | Native Parquet nesting |
| ARRAY (REPEATED) | LIST logical type wrapping | **Critical**: proper LIST representation |
| RANGE | STRUCT\<start, end\> | Decompose into struct |
| ARRAY of STRUCT | LIST | Most common gotcha — repetition levels |

For downstream tool consumption, output schema in multiple formats:

- `current.json` — canonical format  
- `schema.avsc` — Avro schema (Spark, Flink)  
- `_schema.sql` — DDL for Trino, ClickHouse, DuckDB, Snowflake

## Daily/Weekly Schedule Decisions

**Per-table override**:

```
defaults:
  schedule: "0 2 * * *"
  incremental_strategy: partition
tables:
  - match: "analytics.events_*"
    schedule: "0 */4 * * *"
  - match: "reference.*"
    schedule: "0 3 * * 0"
    incremental_strategy: full_refresh
```

**Job overlap prevention**: Use file locks in SQLite. Configurable: skip vs queue vs cancel-and-restart.

**Late-arriving data**: Query `INFORMATION_SCHEMA.PARTITIONS.last_modified_time` and re-sync any modified partition. Partition-modified-time tracking matters more than calendar-date tracking.

## Incremental Sync: Detailed Logic

For each table, on each run:

1. Query partition metadata: all partitions with `last_modified_time > watermark`  
2. Compare to local state: partitions seen at this `last_modified_time`  
3. For each changed partition:  
   - If small (\<1 GB), use Storage Read API, write Parquet directly  
   - If large (\>1 GB), use EXPORT DATA to GCS, then transfer  
4. Atomic commit: write to `_staging/` prefix, then rename to final path  
5. Update watermark to max `last_modified_time` of this batch  
6. Update manifest with new partition info

**Subtlety**: BigQuery's `last_modified_time` updates for metadata-only changes (false positives). Also track `total_logical_bytes` and `total_rows`. If both unchanged, skip re-export.

## File Sizing Strategy

- Aim for \~256 MB files post-compression  
- One row group per \~128 MB of data  
- Don't go below 64 MB files (metadata overhead)  
- Don't go above 2 GB (memory pressure on readers)

For multi-GB partitions, split into row-balanced shards: `part-00001.zstd.parquet` through `part-NNNNN.zstd.parquet`. Storage Read API's stream count gives a natural sharding hint.

## State and Coordination

**SQLite remains viable** for single-coordinator. WAL mode, regular VACUUM. Track:

- `tables`: registered tables, config, schema versions  
- `partitions`: every partition ever seen, hash, last sync time, current state  
- `jobs`: run history, success/failure, durations, bytes transferred  
- `schema_history`: every schema change detected

**Move to Postgres** for HA coordinator or multi-region. Same schema, different driver. Use an interface from day one.

**Cubbit as state backup**: Periodically dump SQLite state to Cubbit. Rebuild from scratch by listing the Cubbit bucket \+ replaying snapshot.

## Verification and Trust

1. **Row count verification**: After each partition upload, compare BQ row count to Parquet row count.  
2. **Sampling validation**: Weekly, pick random partitions, fetch 1000 rows from both, diff.  
3. **Checksum manifest**: Each Parquet file's SHA-256 in the manifest.  
4. **End-to-end test mode**: `bqcubbit verify --sample 0.01` validates 1% against BQ. Run monthly.

## Updated Go Module Structure

```
cmd/
  bqcubbit/
    main.go
internal/
  config/
  bigquery/
    extractor/        # Storage Read API
    exporter/         # EXPORT DATA orchestrator
  parquet/
    types.go          # BQ → Parquet type mapping
    writer.go         # Streaming Parquet writer
  schema/             # Evolution detection, manifest generation
  storage/            # S3/Cubbit client, multipart upload, resumption
  state/              # SQLite/Postgres (interface-based)
  scheduler/
  coordinator/
  worker/
  webui/
    assets/           # //go:embed static files
  verify/
  metrics/            # Prometheus
pkg/
  manifest/           # Public manifest format
```

## Operational Concerns at Scale

- **BigQuery slot consumption**: EXPORT DATA jobs consume slots. Run off-hours or use separate billing project.  
- **GCS lifecycle**: Set rule to delete `_staging/` after 24 hours.  
- **Cubbit multipart upload cleanup**: On startup, list and abort uploads older than 24 hours.  
- **Concurrent table limits**: BQ per-project limits (currently 100 concurrent queries/jobs). Throttle.  
- **Memory pressure**: Parquet row groups buffer \~1 GB RAM per worker. Size VMs accordingly.  
- **Observability**: Prometheus metrics for bytes\_extracted, bytes\_uploaded, compression\_ratio, partition\_lag. Grafana dashboard.

## MVP Phased Rollout

**Phase 1 (3-4 weeks)**: Single-table, single-worker, full export. Storage Read API → ZSTD Parquet → Cubbit. SQLite state, basic CLI, manifest. End-to-end correctness.

**Phase 2 (2-3 weeks)**: Incremental sync via partition watermarks. Schema evolution (add-only). Resumability.

**Phase 3 (3-4 weeks)**: Worker pool, parallel extraction, EXPORT DATA mode. Scheduling, daemon. Verification.

**Phase 4 (2-3 weeks)**: WebUI, breaking schema changes, multi-region, Prometheus metrics, hardening.

Total: \~3 months focused development.

---

# Part 3: Three Hardest Subsystems

## 3.1 Parquet Writing with Nested Types

### The Dremel Encoding Problem

Parquet inherited Dremel's repetition/definition level encoding for nested data:

- **Definition level**: how many optional/repeated fields in the path are defined  
- **Repetition level**: at which nesting depth a new repetition started

Example BQ schema:

```
events RECORD REPEATED {
  event_type STRING
  properties RECORD REPEATED {
    key STRING
    value STRING
  }
}
```

Reading `events[].properties[].value` requires rep/def levels saying "this is the 3rd property of the 2nd event" or "this event has no properties". Wrong levels \= corrupted data: values shift between rows, NULLs in wrong places, silent reader failures.

### The Go Library Reality

`github.com/apache/arrow-go/v18/parquet` (current import path; v15 is older) handles this correctly *if* you use the Arrow-based writer rather than low-level column writer.

**Path A: Arrow record batches → Parquet (recommended)**

```go
import (
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/parquet/pqarrow"
)
```

Build Arrow `RecordBatch` objects with proper list/struct types. `pqarrow.NewFileWriter` handles rep/def computation. Use this 95% of the time.

**Path B: Low-level Parquet column writers** Hand-code rep/def levels per value. Maximum control, maximum bug surface. Avoid.

### BQ → Arrow → Parquet Pipeline

The Storage Read API natively returns Arrow record batches in `ARROW` format mode — skipping any manual decoding.

```go
session, err := bqReadClient.CreateReadSession(ctx, &bqstoragepb.CreateReadSessionRequest{
    Parent: fmt.Sprintf("projects/%s", projectID),
    ReadSession: &bqstoragepb.ReadSession{
        Table: tablePath,
        DataFormat: bqstoragepb.DataFormat_ARROW,
        ReadOptions: &bqstoragepb.ReadSession_TableReadOptions{
            SelectedFields: nil,
        },
    },
    MaxStreamCount: int32(parallelism),
})
```

Each stream returns Arrow IPC-encoded batches. Decode with `ipc.NewReader`, write with `pqarrow`.

### Type Mapping Gotchas — Concrete

**NUMERIC and BIGNUMERIC**: BQ returns as Arrow `Decimal128`/`Decimal256`. Arrow→Parquet preserves as DECIMAL logical type. But older Spark, ClickHouse have bugs with DECIMAL256. Config option `bignumeric_strategy: [preserve|double|string]`. Default `preserve`, warn loudly.

**TIMESTAMP vs DATETIME**:

- `TIMESTAMP` \= absolute instant, UTC → Arrow `TimestampType` with timezone "UTC" → Parquet `TIMESTAMP(MICROS, isAdjustedToUTC=true)`.  
- `DATETIME` \= wall-clock, no timezone → Arrow `TimestampType` no timezone → Parquet `TIMESTAMP(MICROS, isAdjustedToUTC=false)`.

Get this wrong, downstream tools silently shift timestamps by timezone offsets. Test by round-tripping.

**GEOGRAPHY**: BQ returns as WKT strings. Write as Parquet UTF8 with `geo_format=wkt` field metadata. Consider conforming to GeoParquet spec:

```go
geoMetadata := map[string]string{
    "geo_columns": `{"geometry": {"encoding": "WKT", "geometry_type": ["Point", "Polygon"]}}`,
}
```

**JSON columns**: BQ returns as STRING. Use Parquet JSON logical type — DuckDB and Trino can parse. ClickHouse can't currently; document this.

**INTERVAL**: Decompose to STRUCT\<months, days, micros\>. Document in manifest.

\*\*RANGE, RANGE\*\*: Decompose to STRUCT. Track \`\_unbounded\_low\`, \`\_unbounded\_high\` booleans if relevant.

### File-Level Writer Configuration

```go
writerProps := parquet.NewWriterProperties(
    parquet.WithCompression(compress.Codecs.Zstd),
    parquet.WithCompressionLevel(9),
    parquet.WithDictionaryDefault(true),
    parquet.WithDictionaryPageSizeLimit(2 * 1024 * 1024),
    parquet.WithDataPageSize(1024 * 1024),
    parquet.WithMaxRowGroupLength(1024 * 1024),
    parquet.WithStats(true),
    parquet.WithCreatedBy("bqcubbit/v1.0.0"),
    parquet.WithVersion(parquet.V2_LATEST),
)

arrowProps := pqarrow.NewArrowWriterProperties(
    pqarrow.WithStoreSchema(),
)
```

Non-obvious choices:

- **`WithStoreSchema()`**: Embeds original Arrow schema in Parquet metadata. Lets readers reconstruct exact types. Use it.  
- **`WithStats(true)`**: Column min/max stats enable predicate pushdown. Free 10-100× speedups. Always on.  
- **Row group size**: 1M rows starting point. Wide schemas (100+ cols) → 256K. Narrow → 5M. Target \~128 MB compressed per row group.  
- **`V2_LATEST`**: V2 features (delta encoding, byte stream split). Better compression. Verify reader compatibility (e.g., Athena).

### Sort Order and Clustering

BigQuery clustering doesn't map directly to Parquet, but approximate:

1. Storage Read API doesn't preserve clustered order.  
2. To preserve, sort within row groups: collect data into row group buffer, sort by cluster keys, then write.  
3. Parquet column stats then have tight min/max ranges, enabling better predicate pushdown.

Opt-in (costs memory and CPU). Config: `preserve_clustering: true` per table.

### Writing to Cubbit: Multipart Streaming Pattern

```go
uploader := s3manager.NewUploader(cubbitClient, func(u *s3manager.Uploader) {
    u.PartSize = 64 * 1024 * 1024
    u.Concurrency = 4
})

pipeReader, pipeWriter := io.Pipe()

go func() {
    defer pipeWriter.Close()
    pqWriter, _ := pqarrow.NewFileWriter(arrowSchema, pipeWriter, writerProps, arrowProps)
    for _, batch := range batches {
        pqWriter.Write(batch)
    }
    pqWriter.Close()
}()

_, err := uploader.Upload(ctx, &s3.PutObjectInput{
    Bucket: &bucket,
    Key:    &key,
    Body:   pipeReader,
})
```

`io.Pipe` is the trick: Parquet writes into one end, S3 multipart uploader reads from the other. Memory bounded to a few row groups.

**Subtle issue**: Parquet writes its footer (metadata, offsets, stats) at the very end. Multipart uploader handles this — last part contains footer. But partial uploads are useless to readers. Plan retry logic: abort multipart upload on any error, restart from scratch.

### Memory Accounting

Per worker:

- Arrow record batch buffer: 64-256 MB  
- Parquet writer: 2-3× row group size for dictionaries/encoding  
- ZSTD compression context: 50-200 MB (level 19 is 200+, level 9 around 50\)  
- S3 multipart upload buffers: 4 × 64 MB \= 256 MB at concurrency=4  
- Storage Read API gRPC buffers: 100-200 MB

Total: \~1-2 GB minimum per worker. Size VMs at 4 GB RAM per concurrent table, 8 GB safer.

Set `GOGC=50` if memory is tight; Arrow/Parquet libraries generate lots of garbage.

---

## 3.2 Schema Evolution as a State Machine

### The State Model

```
TableState {
  current_schema_hash: sha256
  current_schema_version: int
  schema_history: []SchemaVersion
}

SchemaVersion {
  version: int
  hash: sha256
  bq_schema_json: string
  parquet_schema_string: string
  arrow_schema_ipc: bytes
  valid_from: timestamp
  valid_until: nullable timestamp
  partitions_using_this_schema: []PartitionRef
  derived_from_version: nullable int
  change_type: ADDITIVE | WIDENING | BREAKING
  changes: []FieldChange
}

FieldChange {
  type: ADD | DROP | RENAME | TYPE_WIDEN | TYPE_NARROW | REPETITION_CHANGE
  field_path: "events.properties.value"
  before: FieldDef
  after: FieldDef
  is_safe: bool
}
```

### Detection Algorithm

```
1. Fetch current BQ schema for table
2. Compute schema_hash = sha256(canonical_serialization(schema))
3. If schema_hash == current_schema_hash: proceed with current_schema_version
4. Else:
     diff = compute_diff(previous_schema, current_schema)
     change_type = classify(diff)
     create new SchemaVersion
     decide migration strategy
```

Canonical serialization matters. BigQuery returns field order non-deterministically. Canonicalize: sort fields by name at each nesting level, omit `description` (doc changes shouldn't trigger version bumps), include mode and type.

### Classifying Changes

```
ADDITIVE: only ADD operations, all new fields NULLABLE or REPEATED
  → safe, no consumer breakage expected
  
WIDENING: TYPE_WIDEN only (INT64→FLOAT64, FLOAT64→NUMERIC, etc.)
  → mostly safe, downstream handles wider type
  → flag as "minor breaking" 
  
BREAKING: DROP, RENAME, TYPE_NARROW, REPETITION_CHANGE 
  (REQUIRED→NULLABLE safe; NULLABLE→REQUIRED breaking)
  → consumer queries will likely fail
  → trigger major version bump, alert
```

Type widening rules (transitive):

- INT64 → FLOAT64 → NUMERIC → BIGNUMERIC  
- DATE → DATETIME → TIMESTAMP (with caveats — DATETIME→TIMESTAMP requires timezone choice)

### Tricky Cases

**Renames vs Drop+Add**: BQ doesn't distinguish. Heuristic: if types match and dropped column is unreferenced, prompt user. Don't guess. Config: `assume_rename_on_match: false`.

**Nested field changes**: Change deep in STRUCT is still breaking at Parquet level. Treat any nested change as schema bump for whole table.

**REPEATED → non-REPEATED**: Breaking at Parquet level (different repetition encoding). Force breaking change.

**Mode-only changes (NULLABLE → REQUIRED)**: BQ allows; Parquet doesn't distinguish at wire level. Additive metadata change, no version bump.

### Migration Strategies

**Strategy 1: PRESERVE (default)**

- New partitions under `schema_version=vN+1/`  
- Old partitions stay as-is under `schema_version=vN/`  
- Manifest records partition→version mapping  
- Downstream consumers handle version split (Hive partition pruning \+ schema unification)  
- Cheapest, fastest, most flexible

**Strategy 2: REWRITE\_FORWARD**

- New partitions in vN+1  
- Background job rewrites old partitions to vN+1 schema  
- Fails if breaking change can't be mechanically transformed (e.g., dropped columns lost)  
- Works for renames, type widenings  
- Higher cost (re-reads all historical data)

**Strategy 3: FULL\_REEXPORT**

- On breaking change, trigger full re-export from BigQuery  
- Only works if BQ still has the data  
- Highest cost (full BQ scan)  
- For when past exports have been corrupted

Per-table config:

```
tables:
  - match: "critical.financial_*"
    schema_evolution:
      strategy: REWRITE_FORWARD
      on_unsafe_change: pause_and_alert
  - match: "analytics.events_*"
    schema_evolution:
      strategy: PRESERVE
      on_unsafe_change: proceed
```

### "Pause and Alert" Behavior

For sensitive tables, don't silently version-bump on Friday afternoon:

```
On detected breaking change with strategy=pause_and_alert:
  1. Mark table state as "schema_change_pending"
  2. Send alerts (webhook, email, Slack)
  3. Refuse to sync this table until human acknowledges
  4. Acknowledge via: bqcubbit ack-schema-change --table foo.bar --approve
  5. Other tables continue syncing normally
```

### Production Manifest Format

```json
{
  "$schema": "https://bqcubbit.io/schema-manifest/v1.json",
  "table_ref": {
    "project": "my-project",
    "dataset": "analytics",
    "table": "events"
  },
  "table_metadata": {
    "type": "TABLE",
    "partitioning": {
      "type": "TIME",
      "field": "event_date",
      "granularity": "DAY",
      "require_filter": false,
      "expiration_ms": null
    },
    "clustering": ["user_id", "event_type"],
    "labels": {"env": "prod", "team": "growth"},
    "row_count_at_last_sync": 1234567890,
    "logical_bytes_at_last_sync": 2345678901234
  },
  "current_version": 3,
  "versions": {
    "1": {
      "version": 1,
      "hash": "sha256:abc...",
      "valid_from": "2024-01-15T00:00:00Z",
      "valid_until": "2024-08-22T14:33:00Z",
      "change_type": "INITIAL",
      "bq_schema": [/* full BQ schema JSON */],
      "parquet_schema_repr": "message events { ... }",
      "arrow_schema_b64": "...",
      "ddl": {
        "trino": "CREATE TABLE events (...)",
        "duckdb": "CREATE TABLE events (...)",
        "clickhouse": "CREATE TABLE events (...) ENGINE = ...",
        "spark_sql": "CREATE TABLE events (...) USING parquet",
        "athena": "CREATE EXTERNAL TABLE events (...)"
      },
      "partition_paths": [
        "date=2024-01-15/schema_version=1/",
        "date=2024-01-16/schema_version=1/"
      ]
    },
    "2": {
      "version": 2,
      "change_type": "ADDITIVE",
      "derived_from_version": 1,
      "changes": [
        {
          "type": "ADD",
          "field_path": "user_agent_parsed",
          "after": {
            "name": "user_agent_parsed",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [...]
          },
          "is_safe": true
        }
      ]
    },
    "3": {
      "version": 3,
      "change_type": "BREAKING",
      "derived_from_version": 2,
      "changes": [
        {
          "type": "RENAME",
          "field_path": "ts",
          "before": {"name": "ts", "type": "TIMESTAMP"},
          "after": {"name": "event_timestamp", "type": "TIMESTAMP"},
          "is_safe": false,
          "consumer_action_required": "Update queries: WHERE ts → WHERE event_timestamp"
        }
      ]
    }
  },
  "unification_query_examples": {
    "trino": "SELECT date, COALESCE(event_timestamp, ts) AS event_timestamp, ... FROM events",
    "duckdb": "...",
    "spark_sql": "..."
  },
  "sync_history": {
    "first_export_started": "2024-01-15T00:00:00Z",
    "first_export_completed": "2024-01-15T03:24:00Z",
    "last_successful_sync": "2025-05-24T02:14:33Z",
    "last_sync_partitions": 47,
    "last_sync_bytes": 12345678901
  }
}
```

The `unification_query_examples` block is gold — auto-generated, shows downstream users exactly how to query across versions.

---

## 3.3 Worker Pool and Coordinator Concurrency

### The Coordinator-Worker-Storage Triangle

- **Coordinator**: discovers work, assigns tasks, tracks state  
- **Workers**: execute tasks (extract, write Parquet, upload)  
- **State store**: SQLite or Postgres, source of truth

Workers stateless — coordinator can kill and respawn at will. All durable state in state store \+ Cubbit.

### Task Lifecycle State Machine

```
DISCOVERED → PENDING → ASSIGNED → EXTRACTING → UPLOADING → VERIFYING → COMPLETED
                                ↓             ↓            ↓
                              FAILED        FAILED       FAILED
                                ↓             ↓            ↓
                              RETRY_PENDING (with backoff)
                                ↓
                              PERMANENTLY_FAILED (after N retries)
```

Each transition is a SQL transaction. ASSIGNED state has `worker_id` and `lease_expires_at`. Coordinator scans for expired leases periodically, resets to PENDING.

### Lease-Based Work Assignment

```sql
BEGIN;
SELECT task_id, table_ref, partition_id, schema_version 
  FROM tasks 
  WHERE state = 'PENDING' 
  ORDER BY priority DESC, created_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED;

UPDATE tasks 
  SET state = 'ASSIGNED', 
      worker_id = $worker_id, 
      lease_expires_at = NOW() + INTERVAL '30 minutes',
      lease_generation = lease_generation + 1
  WHERE task_id = $task_id;
COMMIT;
```

`FOR UPDATE SKIP LOCKED` is the magic — concurrent workers grab different tasks without serializing. In SQLite, use `BEGIN IMMEDIATE` for similar effect (no SKIP LOCKED, but contention is low enough for single-node).

Workers extend lease periodically (every 10 minutes for 30-minute lease). Crash mid-task → lease expires → another worker picks up.

### Lease Renewal Heartbeat

```go
type Lease struct {
    TaskID         string
    Generation     int64
    ExpiresAt      time.Time
    cancelFunc     context.CancelFunc
}

func (w *Worker) runTask(taskCtx context.Context, lease Lease) error {
    renewer := time.NewTicker(10 * time.Minute)
    defer renewer.Stop()
    
    go func() {
        for {
            select {
            case <-taskCtx.Done():
                return
            case <-renewer.C:
                ok, err := w.state.RenewLease(taskCtx, lease.TaskID, lease.Generation)
                if err != nil || !ok {
                    lease.cancelFunc()
                    return
                }
            }
        }
    }()
    
    return w.executeTask(taskCtx, lease.TaskID)
}
```

When worker loses lease, abort cleanly without committing partial work. Abort S3 multipart, clean staging files. Task resets to PENDING, retried fresh.

### Idempotency

**Extracting from BQ**: Idempotent. Same partition, same query, same data (handle mid-sync modification below).

**Writing Parquet to Cubbit**: Write to staging path with task-specific suffix:

```
s3://bucket/_staging/{table}/{partition}/{task_id}/{file_idx}.parquet
```

On completion, rename to final path:

```
s3://bucket/{table}/date={partition}/schema_version=v{N}/part-{idx}.parquet
```

**State updates**: Compare-and-swap. "Update state from EXTRACTING to UPLOADING only if lease\_generation matches." Prevents zombie workers overwriting newer state.

### Mid-Sync Partition Modification Problem

BigQuery partitions can be modified during extraction (streaming inserts, late-arriving data, MERGE).

Detection:

```
1. Before extraction: record last_modified_time = T1
2. Extract data
3. After extraction: query last_modified_time = T2
4. If T2 > T1: data potentially inconsistent
   → Abort, retry with new T1
   → Or: accept and mark partition as "modified during sync"
```

For Storage Read API, use `snapshot_time` for consistent read:

```go
ReadSession: &bqstoragepb.ReadSession{
    TableModifiers: &bqstoragepb.ReadSession_TableModifiers{
        SnapshotTime: timestamppb.New(syncStartTime),
    },
}
```

Read-time consistency: see table as of `syncStartTime`. BQ time-travel retained 7 days by default.

### Work Decomposition

```
For each configured table:
  1. Fetch BQ schema, compare to last known
  2. If schema changed:
     - Run schema evolution logic
     - If pause_and_alert: skip table
  3. Determine partitions to sync:
     a. Query INFORMATION_SCHEMA.PARTITIONS  
     b. Filter: last_modified_time > last_sync_watermark
     c. Filter: not in current set of completed tasks for this sync_run
  4. For each partition:
     a. Estimate size (logical_bytes)
     b. Decide extraction method (Storage Read API vs EXPORT DATA)
     c. Decide sharding
     d. Create task records in PENDING state
  5. Workers pick up tasks
```

### Sharding Large Partitions

**Method 1: Storage Read API parallel streams**

```
session, _ := client.CreateReadSession(... MaxStreamCount: 16)
for streamIdx, stream := range session.Streams {
    createTask(table, partition, shard=streamIdx, stream_name=stream.Name)
}
```

Each task reads its stream, writes own Parquet. Naturally parallel and idempotent.

**Method 2: EXPORT DATA with URI wildcards** BQ handles parallelism, N output files in GCS. Each GCS file becomes one upload task.

Use Method 1 for medium partitions (1-50 GB), Method 2 for huge partitions (50 GB+).

### Backpressure and Rate Limiting

Limits to plan for:

- **BQ Storage Read API**: 5,000 read sessions/project/hour, 200 GB/s/project  
- **BQ EXPORT DATA**: 100 concurrent jobs/project  
- **Cubbit upload bandwidth**: Verify can sustain  
- **GCP egress**: 10-32 Gbps depending on VM type

Use `golang.org/x/time/rate` for token buckets:

```go
type Limiters struct {
    bqReadSessions *rate.Limiter
    bqExportJobs   *rate.Limiter
    cubbitUploads  *rate.Limiter
}
```

### Graceful Shutdown

SIGTERM handling:

```
1. Coordinator: stop accepting new sync runs
2. Workers: finish current task or hit safe checkpoint within N seconds
3. If task takes too long, abort cleanly:
   - Cancel BQ read stream
   - Abort S3 multipart upload (will be retried)
   - Leave task in ASSIGNED state, lease will expire
4. Coordinator: wait for all workers to acknowledge, exit
```

Configurable shutdown grace period (default 5 minutes). After grace expires, hard kill.

### Failure Modes and Recovery

- **Coordinator crash mid-discovery**: Idempotent re-discovery on restart.  
- **Worker crash mid-extraction**: Lease expires, another worker picks up. Staging files GC'd hourly.  
- **Worker crash mid-upload**: List pending multipart uploads on restart, abort any older than 24h.  
- **Cubbit unavailable**: All workers fail. Coordinator detects pattern, pauses, backoff, alerts.  
- **BQ quota exceeded**: Circuit breaker. After 3 quota failures in 5 minutes, pause 30 minutes.  
- **State store corruption**: Periodic SQLite snapshots to Cubbit. Rebuild from snapshot \+ Cubbit listing.  
- **Network partition**: Health-check each subsystem independently. Coordinator routes around unhealthy workers.

### Observability Per Task

Minimum metrics:

- `bq_read_bytes`, `bq_read_duration_ms`  
- `parquet_write_duration_ms`  
- `parquet_file_bytes`, `compression_ratio` (bq\_read\_bytes / parquet\_file\_bytes)  
- `upload_duration_ms`, `upload_bytes`  
- `total_duration_ms`, `retry_count`

Aggregate to per-table, per-sync-run, per-day. Grafana dashboard:

- Bytes synced per day (trending)  
- Compression ratio per table (alert on degradation)  
- Sync duration per table (alert on growth)  
- Failed tasks per hour  
- Lag per table (time since last successful sync)  
- Total Cubbit storage used

### Concurrency Bug to Avoid

**Don't parallelize within a single task. Keep tasks single-threaded.**

Parallelism comes from many tasks in parallel, not threading within a task. Why:

1. Memory accounting simpler  
2. Failure isolation cleaner  
3. Progress tracking meaningful  
4. Lease semantics unambiguous

If a task is too slow, shard it into more tasks. Wanting threads within a task \= redesign the task boundary.

### State Schema (SQLite/Postgres)

```sql
CREATE TABLE tables (
    id INTEGER PRIMARY KEY,
    project TEXT NOT NULL,
    dataset TEXT NOT NULL,
    table_name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    current_schema_version INTEGER NOT NULL DEFAULT 1,
    last_sync_watermark TIMESTAMP,
    state TEXT NOT NULL DEFAULT 'active',
    UNIQUE(project, dataset, table_name)
);

CREATE TABLE schema_versions (
    id INTEGER PRIMARY KEY,
    table_id INTEGER REFERENCES tables(id),
    version INTEGER NOT NULL,
    schema_hash TEXT NOT NULL,
    schema_json TEXT NOT NULL,
    arrow_schema_blob BLOB NOT NULL,
    change_type TEXT NOT NULL,
    changes_json TEXT,
    valid_from TIMESTAMP NOT NULL,
    valid_until TIMESTAMP,
    UNIQUE(table_id, version)
);

CREATE TABLE sync_runs (
    id INTEGER PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    state TEXT NOT NULL,
    total_tasks INTEGER,
    completed_tasks INTEGER,
    failed_tasks INTEGER
);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    sync_run_id INTEGER REFERENCES sync_runs(id),
    table_id INTEGER REFERENCES tables(id),
    schema_version INTEGER NOT NULL,
    partition_id TEXT,
    shard_idx INTEGER DEFAULT 0,
    state TEXT NOT NULL,
    worker_id TEXT,
    lease_expires_at TIMESTAMP,
    lease_generation INTEGER DEFAULT 0,
    bq_snapshot_time TIMESTAMP,
    bq_partition_modified_time TIMESTAMP,
    output_path TEXT,
    bytes_read INTEGER,
    bytes_written INTEGER,
    retry_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX idx_tasks_state ON tasks(state, lease_expires_at);
CREATE INDEX idx_tasks_sync_run ON tasks(sync_run_id);

CREATE TABLE partitions (
    id INTEGER PRIMARY KEY,
    table_id INTEGER REFERENCES tables(id),
    partition_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    bq_last_modified TIMESTAMP NOT NULL,
    last_successful_sync TIMESTAMP NOT NULL,
    bytes_in_cubbit INTEGER,
    row_count INTEGER,
    file_count INTEGER,
    checksums_json TEXT,
    UNIQUE(table_id, partition_id, schema_version)
);
```

---

## Topics Still to Explore

- WebUI design (real-time job updates via SSE, log streaming)  
- Prometheus metrics specifics  
- Configuration file format and validation in depth  
- Integration testing strategy (BQ emulators are limited; dedicated test GCP project likely needed)  
- CI/CD for single-binary distribution (cross-compilation, embedded UI assets, release pipeline)  
- Downstream consumer experience (concrete examples: Trino, DuckDB, ClickHouse, Spark queries against exported data)  
- Starter project structure with key Go interfaces defined  
- Sample config file showing all options in context

