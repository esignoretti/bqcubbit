# bqcubbit

Export BigQuery tables to Cubbit DS3 (S3-compatible) as ZSTD-compressed Parquet files. Single binary, CLI + WebUI, designed for multi-TB scale.

**Features:**
- **Two extraction backends:** BigQuery Storage Read API (gRPC, streaming) and EXPORT DATA (free at BigQuery side, via GCS staging)
- **Full + incremental sync:** Partition-level discovery via `INFORMATION_SCHEMA.PARTITIONS`, watermark tracking, staging → atomic rename
- **Schema evolution:** Canonical hash comparison, additive/breaking change classification, versioned partition paths
- **Worker pool:** 2–8 workers with lease heartbeats, claimed tasks, graceful shutdown
- **Cron scheduler:** Configurable schedule with overlap prevention (`skip`, `queue`, `cancel_and_restart`)
- **WebUI:** HTMX + Go templates dashboard with SSE live log streaming, partition status, schema version history
- **Verification:** `bqcubbit verify` samples rows from BigQuery and compares counts against Cubbit manifests
- **Crash recovery:** Aborts stale runs, cleans up staging files (`_staging/` prefix), skips already-exported partitions via `last_exported_path`
- **Metrics:** Prometheus counters/gauges for bytes extracted, bytes uploaded, compression ratio, task duration, partition lag
- **Single binary:** Static binary, no runtime dependencies, container-ready (distroless Docker image)

## Quick Start

### 1. Install

```bash
go install github.com/esignoretti/bqcubbit/cmd/bqcubbit@latest
```

Or build from source:

```bash
git clone https://github.com/esignoretti/bqcubbit.git
cd bqcubbit
CGO_ENABLED=0 go build -o bqcubbit ./cmd/bqcubbit
```

### 2. Configure

Create `config.yaml`:

```yaml
source:
  project_id: my-gcp-project
  location: EU
  datasets:
    - my_dataset

destination:
  endpoint: https://s3.cubbit.eu
  bucket: my-bucket
  prefix: bq-export/
  access_key: YOUR_ACCESS_KEY
  secret_key: YOUR_SECRET_KEY
  compression: zstd
  compression_level: 9

sync:
  datasets:
    - my_dataset
  incremental_strategy: partition
  max_concurrent: 4

worker_pool:
  min_workers: 2
  max_workers: 8

rate_limit:
  bq_read_sessions_per_hour: 100
  bq_export_jobs_per_hour: 50
  cubbit_uploads_per_minute: 60
```

### 3. Sync

```bash
# One-shot sync
export GOOGLE_APPLICATION_CREDENTIALS=key.json
export BQCUBBIT_CONFIG=config.yaml
bqcubbit sync

# Daemon mode with scheduler + WebUI
bqcubbit serve
# Open http://localhost:8080
```

### 4. Verify

```bash
bqcubbit verify
```

## Commands

| Command | Description |
|---|---|
| `bqcubbit sync` | One-shot full/incremental export |
| `bqcubbit serve` | Daemon mode with cron scheduler + WebUI on `:8080` |
| `bqcubbit verify` | Compare row counts BQ ↔ Cubbit |
| `bqcubbit ack-schema-change <dataset.table>` | Acknowledge breaking schema change |

## Architecture

```
BigQuery Storage Read API / EXPORT DATA
    ↓
Arrow record batches (streaming)
    ↓
Parquet ZSTD writer (streaming SHA256 hash)
    ↓
Cubbit DS3 via _staging/ prefix
    ↓
RenameObject → final path (atomic commit)
    ↓
Manifest merge + partition state update
```

State is tracked in SQLite (WAL mode, single-file). The `StateStore` interface is designed for easy replacement with Postgres.

## Incremental Sync Strategy

- Discovers partitions via `INFORMATION_SCHEMA.PARTITIONS`
- Compares `last_modified_time` against per-table watermark
- Only exports partitions modified since last successful sync
- After crash: aborts stale `sync_runs` (>24h), cleans up `_staging/` objects, skips partitions where `last_exported_path` still exists

## Schema Evolution

- Canonical hash of all field names/types → schema fingerprint
- Additive changes (new columns): automatically accepted
- Breaking changes (DROP, RENAME, TYPE_CHANGE): new data goes to `schema_version=vN+1/` prefix
- Human-in-the-loop: `bqcubbit ack-schema-change` acknowledges and promotes version

## License

MIT
