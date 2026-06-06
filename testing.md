# Testing bqcubbit End-to-End

## 1. Create fake data in BigQuery

```sql
-- Create a dataset
CREATE SCHEMA IF NOT EXISTS bqcubbit_test
OPTIONS(location='EU');

-- Create a date-partitioned table with sample data (~500K rows)
CREATE OR REPLACE TABLE bqcubbit_test.events
PARTITION BY DATE(event_timestamp)
AS
SELECT
  GENERATE_UUID() AS event_id,
  TIMESTAMP_ADD('2026-01-01', INTERVAL CAST(RAND() * 365 * 24 * 60 AS INT64) MINUTE) AS event_timestamp,
  CONCAT('user_', CAST(CAST(RAND() * 1000 AS INT64) AS STRING)) AS user_id,
  CASE CAST(RAND() * 5 AS INT64)
    WHEN 0 THEN 'page_view' WHEN 1 THEN 'click' WHEN 2 THEN 'purchase'
    WHEN 3 THEN 'signup' WHEN 4 THEN 'logout' ELSE 'other'
  END AS event_type,
  ROUND(RAND() * 1000, 2) AS amount,
  CONCAT('page_', CAST(CAST(RAND() * 50 AS INT64) AS STRING)) AS page
FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS a,
     UNNEST(GENERATE_ARRAY(1, 50)) AS b;

-- Verify
SELECT COUNT(*) AS total_rows FROM bqcubbit_test.events;
SELECT COUNT(DISTINCT DATE(event_timestamp)) AS partition_count FROM bqcubbit_test.events;
SELECT MIN(event_timestamp), MAX(event_timestamp) FROM bqcubbit_test.events;

-- List partitions via INFORMATION_SCHEMA
SELECT table_name, partition_id, total_rows
FROM `your-project.bqcubbit_test.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'events';
```

## 2. Create a second table for multi-table testing

```sql
CREATE OR REPLACE TABLE bqcubbit_test.users
AS
SELECT
  CONCAT('user_', CAST(n AS STRING)) AS user_id,
  CONCAT('User ', CAST(n AS STRING)) AS full_name,
  CONCAT('user', CAST(n AS STRING), '@example.com') AS email,
  TIMESTAMP_ADD('2025-01-01', INTERVAL CAST(RAND() * 365 AS INT64) DAY) AS created_at,
  CASE CAST(RAND() * 3 AS INT64) WHEN 0 THEN 'free' WHEN 1 THEN 'pro' ELSE 'enterprise' END AS plan
FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS n;
```

## 3. Create a service account for bqcubbit

```bash
# Set your project
export PROJECT_ID=your-gcp-project-id

# Create service account (you need roles/iam.serviceAccountAdmin or Owner)
gcloud iam service-accounts create bqcubbit \
  --display-name="BQCubbit Export Service"

# Grant BigQuery roles for the service account
# If you get "does not have permission", ask a Project Owner to run these:
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:bqcubbit@$PROJECT_ID.iam.serviceaccount.com" \
  --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:bqcubbit@$PROJECT_ID.iam.serviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Create and download key
gcloud iam service-accounts keys create bqcubbit-key.json \
  --iam-account="bqcubbit@$PROJECT_ID.iam.serviceaccount.com"
```

Set the environment variable for local testing:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/bqcubbit-key.json
```

> **Note:** If you get `The caller does not have permission` when running `gcloud projects add-iam-policy-binding`, you don't have IAM role-assignment rights on the GCP project. Ask a Project Owner to run the two `add-iam-policy-binding` commands, or use your own user credentials instead:
>
> ```bash
> gcloud auth application-default login
> unset GOOGLE_APPLICATION_CREDENTIALS
> ```
>
> This authenticates bqcubbit as your user (no service account needed). Works if your GCP user has `bigquery.admin` or equivalent access.

## 4. Create a Cubbit DS3 bucket (or use MinIO)

If you don't have Cubbit DS3 yet, use **MinIO** as a drop-in S3-compatible target:

```bash
docker run -d --name minio \
  -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

# Create bucket
docker exec minio mc mb /data/bq-export
```

Or use your real Cubbit DS3 credentials if available.

## 5. Write config file

Save as `bqcubbit.yaml`:

```yaml
source:
  project_id: your-gcp-project-id
  location: EU
  datasets:
    - bqcubbit_test

destination:
  endpoint: http://localhost:9000          # MinIO, or your Cubbit DS3 endpoint
  bucket: bq-export
  prefix: bq-export/
  access_key: minioadmin
  secret_key: minioadmin
  compression: zstd
  compression_level: 9

sync:
  datasets:
    - bqcubbit_test
  incremental_strategy: full_refresh       # first run: export everything
  max_concurrent: 4
  batch_size_days: 7                       # group daily partitions into weekly batches

worker_pool:
  min_workers: 2
  max_workers: 4

rate_limit:
  bq_read_sessions_per_hour: 100
  bq_export_jobs_per_hour: 50
  cubbit_uploads_per_minute: 60
```

## 6. Run first sync (local)

```bash
# Build the binary
CGO_ENABLED=0 go build -o bqcubbit ./cmd/bqcubbit

# Run sync (single run, one-shot)
GOOGLE_APPLICATION_CREDENTIALS=bqcubbit-key.json \
  BQCUBBIT_CONFIG=bqcubbit.yaml \
  ./bqcubbit sync
```

Expected output:
```
[sync] starting sync run (strategy: full_refresh)
[sync] discovered 269 partitions
[sync] processing table bqcubbit_test.events (269 partitions)
[sync] bqcubbit_test.events: grouped 269 partitions into 38 batch(es)
[sync] exporting batch 20260407..20260413 (7 partitions)
...
[sync] completed batch 20260407..20260413 (7 partitions, sha256: ..., 12345 bytes)
```

## 7. Run as daemon with WebUI

```bash
GOOGLE_APPLICATION_CREDENTIALS=bqcubbit-key.json \
  BQCUBBIT_CONFIG=bqcubbit.yaml \
  ./bqcubbit serve
```

Open `http://localhost:8080` to see the dashboard, partition status, and live logs via SSE.

Set a cron schedule in config for recurring syncs:

```yaml
scheduler:
  cron: "0 2 * * *"
  initial_sync_mode: "full_refresh"
```

## 8. Verify exported data

```bash
./bqcubbit verify
```

This samples 1% of rows from BigQuery and compares row counts with Cubbit manifests.

## 9. Deploy to a GCP VM

### Binary deployment

```bash
gcloud compute instances create bqcubbit-worker \
  --machine-type=n2-standard-4 \
  --scopes=bigquery,storage-rw,cloud-platform \
  --service-account="bqcubbit@$PROJECT_ID.iam.gserviceaccount.com"

# SCP the binary and config
gcloud compute scp bqcubbit bqcubbit.yaml bqcubbit-worker:~/

# SSH and run
gcloud compute ssh bqcubbit-worker -- \
  "BQCUBBIT_CONFIG=bqcubbit.yaml ./bqcubbit serve"
```

The VM's attached service account handles authentication — no key file needed.

### Container deployment (Docker)

```bash
# Build and push to Artifact Registry
docker build -t europe-west1-docker.pkg.dev/$PROJECT_ID/bqcubbit/bqcubbit:latest .
docker push europe-west1-docker.pkg.dev/$PROJECT_ID/bqcubbit/bqcubbit:latest

# Run on GCE
gcloud compute instances create-with-container bqcubbit \
  --container-image=europe-west1-docker.pkg.dev/$PROJECT_ID/bqcubbit/bqcubbit:latest \
  --container-env=BQCUBBIT_CONFIG=/etc/bqcubbit/config.yaml \
  --service-account="bqcubbit@$PROJECT_ID.iam.gserviceaccount.com" \
  --scopes=bigquery,cloud-platform
```
