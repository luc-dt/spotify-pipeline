# 📓 Spotify Music Intelligence Platform — Engineering Diary & Implementation Plan

A cumulative engineering diary and architectural implementation plan tracking daily goals, technical decisions, code designs, lessons learned, and definitions of done.

---

# 🗓️ Day 2: Python Extraction Engine (COMPLETED)

### 🎯 Day 2 Objective:
Build a production-grade, modular Python extraction package in `src/extract/` that extracts Spotify catalog metadata for our target cohort and stores immutable raw JSON in `data/raw/`.

---

### 🧩 The 5 Modular Components Built:
```text
src/extract/
├── __init__.py           ──▶ [0] Package Interface & Exports
├── spotify_client.py     ──▶ [1] Token Manager (OAuth 2.0) + 429 Rate-Limit Retry Engine + Circuit Breaker
├── artist_extractor.py   ──▶ [2] Artist Search & Canonical ID Resolver
├── album_extractor.py    ──▶ [3] Discography Paginator (limit=10) + Deduplication
├── track_extractor.py    ──▶ [4] Simplified Track Paginator (limit=10) + Foreign Key Lineage
└── main.py               ──▶ [5] Master Orchestrator (Writes partitioned JSON to data/raw/)
```

---

### 🧠 Core Architectural Decisions & Lessons Learned (Day 2):
1. **OAuth 2.0 Client Credentials & Token Caching**: Tokens cached with a 60s safety buffer.
2. **Rate Limiting (429) vs. Quotas**: Handled with exponential backoff, random jitter ($0.2\text{s}$–$0.8\text{s}$), and a circuit breaker for `Retry-After > 60s`.
3. **Simplified vs Full Track Objects**: Using `/v1/albums/{id}/tracks` saved ~90% of API calls.
4. **Target Cohort Design**: 8 diverse superstars providing rich catalog variances across eras and genres.
5. **Partial Snapshot State Consistency**: Handled partial extractions gracefully with temporal watermarks (`snapshot_date = YYYY-MM-DD`).

---

### 🏁 Day 2 Definition of Done (Verified Results):
* ✅ Extracted **8 Artists, 741 Albums, and 2,500 Tracks** into `data/raw/`:
  * `data/raw/artists/artists_2026-08-31.json` (2.7 KB)
  * `data/raw/albums/albums_2026-08-31.json` (393.7 KB)
  * `data/raw/tracks/tracks_2026-08-31.json` (1,063.7 KB)
* ✅ Committed and pushed to GitHub: [`https://github.com/luc-dt/spotify-pipeline.git`](https://github.com/luc-dt/spotify-pipeline.git).

---
---

# 🗓️ Day 3: AWS S3 Raw Data Lake (COMPLETED)

### 🎯 Day 3 Objective:
Persist the immutable raw JSON payloads into an **Amazon S3 Raw Data Lake** using `boto3` with Hive-style partitioning, object metadata, SSE-S3 encryption, and idempotent verification.

---

### 🏗️ Master Architecture Implemented:

```text
┌─────────────────────────────────┐
│        LOCAL RAW STORAGE        │
│  data/raw/artists_2026-08-31    │
│  data/raw/albums_2026-08-31     │
│  data/raw/tracks_2026-08-31     │
└────────────────┬────────────────┘
                 │
                 │ Boto3 S3 Ingestion Engine (src/storage/s3_uploader.py)
                 │ • Validates Bucket Access (head_bucket)
                 │ • Hive Partitioning: raw/extracted_at=YYYY-MM-DD/
                 │ • Injects ExtraArgs: ContentType & Metadata
                 │ • Enforces SSE-S3 Encryption (AES256)
                 │ • Integrity & Idempotency Verification
                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          AMAZON S3 RAW DATA LAKE                            │
│                  bucket: s3://spotify-music-intelligence-luc/               │
│                                                                             │
│  raw/extracted_at=2026-08-31/                                               │
│  ├── artists/artists_2026-08-31.json (2.7 KB, AES256, Verified)             │
│  ├── albums/albums_2026-08-31.json   (393.7 KB, AES256, Verified)           │
│  └── tracks/tracks_2026-08-31.json   (1.0 MB, AES256, Verified)             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 🧠 Core Architectural Decisions & Lessons Learned (Day 3):
1. **Separation of Concerns (Infrastructure vs. Application)**: S3 Bucket creation and Block Public Access are infrastructure configs; `s3_uploader.py` is application code.
2. **Hive-Style Partitioning (`raw/extracted_at=YYYY-MM-DD/`)**: `key=value` structure for Athena & Spark partition pruning.
3. **ExtraArgs & Object Metadata**: Attached `ContentType: "application/json"`, `ServerSideEncryption: "AES256"`, and audit metadata.
4. **Idempotency & Strong Consistency**: Overwrote deterministic keys cleanly without duplicate objects.
5. **Unit Testing with Mocks (`tests/test_s3_uploader.py`)**: 8/8 tests passed in 0.79s.

---

### 🏁 Day 3 Definition of Done (Verified Results):
* ✅ S3 bucket `spotify-music-intelligence-luc` created in `ap-southeast-2` with Block Public Access enabled.
* ✅ `scripts/verify_aws_credentials.py` verified STS caller identity.
* ✅ `src/storage/s3_uploader.py` successfully uploaded 3/3 datasets.
* ✅ S3 ContentLength matched local file bytes (2,773 B Artists, 403,157 B Albums, 1,089,235 B Tracks).
* ✅ 8/8 unit tests in `tests/test_s3_uploader.py` passed with 100% green status.
* ✅ Committed and pushed to GitHub: [`https://github.com/luc-dt/spotify-pipeline.git`](https://github.com/luc-dt/spotify-pipeline.git).

---
---

# 🗓️ Day 4 — Bronze Layer with PySpark (COMPLETED)

### 🎯 Day 4 Objective:
Build the **Bronze Layer** processing engine with **PySpark** to ingest the immutable raw JSON payloads stored in S3, enforce explicit `StructType` schemas, perform selective structural normalization, add technical lineage metadata (`source`, `ingestion_timestamp`), and write **Snappy-compressed Parquet** datasets partitioned by `snapshot_date=YYYY-MM-DD/`.

---

### 🧩 The Core Components Built:
* `src/transform/spark_session.py`: Decoupled PySpark session factory with Dynamic Partition Overwrite.
* `src/transform/schemas.py`: Explicit `StructType` contracts for `artists`, `albums`, `tracks`.
* `src/transform/bronze_transformer.py`: Multi-line JSON ingestion, type casting, lineage injection, and partitioned Snappy Parquet writer.
* `tests/test_bronze_transformer.py`: 5/5 unit tests verifying contracts, transformations, null coalescing, and Parquet read-back.

---

### 🏁 Day 4 Definition of Done (Verified Results):
* ✅ Transformed multi-day snapshots (`2026-08-31` & `2026-09-01`) totaling **8 Artists, 1,025 Albums, and 3,964 Tracks** into Bronze Snappy Parquet.
* ✅ Enabled Dynamic Partition Overwrite (`spark.sql.sources.partitionOverwriteMode=dynamic`), preserving multi-day partitions on disk and cloud.
* ✅ Reduced storage footprint by **82.8%** on tracks (1,089 KB $\rightarrow$ 186 KB) and **80.5%** on albums (403 KB $\rightarrow$ 78 KB).
* ✅ Synced all Bronze Parquet tables to Amazon S3 (`s3://spotify-music-intelligence-luc/bronze/`).
* ✅ 5/5 PySpark unit tests passed in 49.98s with 100% green status.

---
---

# 🗓️ Day 5: Silver Layer & Automated Data Quality Gate

### 🎯 Day 5 Objective

Transform the **Bronze Parquet layer** into clean, typed, deduplicated, and relationally consistent **Silver entities** using PySpark.

The Silver layer will:
* Standardize data types and dates
* Deterministically deduplicate records
* Preserve source-level date precision
* Apply conservative data sanitization
* Validate primary keys, nulls, ranges, and foreign keys
* Quarantine invalid/orphan records instead of unnecessarily discarding valid data
* Produce a persistent **PASS / WARN / FAIL Data Quality report**
* Support safe, idempotent re-runs

> **Bronze = structurally normalized source data.**
> **Silver = clean and trusted entities.**
> **Gold = business-facing analytical models and metrics.**

---

# 🧠 Core Philosophy

The pipeline should not assume that source data is perfect.

Instead:

```text
Bronze
  ↓
Clean + standardize
  ↓
Validate
  ↓
┌───────────────┬─────────────────┐
│ Valid records │ Invalid records │
│               │                 │
▼               ▼
Silver       Quarantine
                │
                ▼
           DQ Report
```

The objective is therefore not:
> "Reject everything that isn't perfect."

It is:
> **"Produce trustworthy Silver data while preserving visibility into data-quality problems."**

---

# 🏗️ Master Day 5 Architecture

```text
┌────────────────────────────────────────────────────────────┐
│                     BRONZE PARQUET                         │
│                                                            │
│ data/bronze/{artists,albums,tracks}/                       │
│   └── snapshot_date=YYYY-MM-DD/                            │
└──────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────┐
│                SILVER TRANSFORMATION ENGINE                │
│                                                            │
│  1. Deterministic deduplication                            │
│     • PK-based Window                                      │
│     • Keep latest extracted_at                             │
│                                                            │
│  2. Type & date normalization                              │
│     • release_date → DateType                              │
│     • preserve release_date_precision                      │
│     • release_year / release_month / release_decade        │
│                                                            │
│  3. Data sanitization                                      │
│     • trim identifiers/names                               │
│     • standardize categorical values                        │
│     • validate Spotify URIs                                │
│                                                            │
│  4. Conservative business normalization                    │
│     • duration calculations where useful                   │
│     • Boolean / numbering normalization                     │
│     • no unjustified business assumptions                   │
└──────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────┐
│                  AUTOMATED DQ GATE                         │
│                                                            │
│  Rule 1 → Completeness / row count                         │
│  Rule 2 → Primary-key uniqueness                           │
│  Rule 3 → Critical-column null checks                      │
│  Rule 4 → Referential integrity                            │
│  Rule 5 → Value/range validation                           │
│                                                            │
│                  PASS / WARN / FAIL                        │
└──────────────────────┬─────────────────────────────────────┘
                       │
              ┌────────┴────────┐
              │                 │
          Valid data        Invalid/orphan
              │                 │
              ▼                 ▼
┌──────────────────────┐   ┌──────────────────────┐
│    SILVER LAYER      │   │     QUARANTINE       │
│                      │   │                      │
│ data/silver/         │   │ data/quarantine/     │
│ {artists,albums,     │   │ {entity}/            │
│  tracks}/             │   │ snapshot_date=.../   │
│ snapshot_date=.../   │   │                      │
└──────────────────────┘   └──────────────────────┘
              │
              │
              ▼
┌────────────────────────────────────────────────────────────┐
│                  DQ REPORT / OBSERVABILITY                 │
│                                                            │
│ data/quality/reports/                                      │
│   snapshot_date=YYYY-MM-DD/                                │
│       dq_report.json                                       │
└────────────────────────────────────────────────────────────┘
```

---

# 📐 Partitioning Convention

Use **`snapshot_date` consistently across Bronze and Silver**.

```text
data/
├── bronze/
│   ├── artists/
│   │   └── snapshot_date=2026-08-31/
│   ├── albums/
│   │   └── snapshot_date=2026-08-31/
│   └── tracks/
│       └── snapshot_date=2026-08-31/
│
└── silver/
    ├── artists/
    │   └── snapshot_date=2026-08-31/
    ├── albums/
    │   └── snapshot_date=2026-08-31/
    └── tracks/
        └── snapshot_date=2026-08-31/
```

`extracted_at` remains **event/lineage metadata**.
`snapshot_date` identifies the logical data snapshot and is the partition key.

---

# 1️⃣ `silver_artists`

### Deduplication
Use deterministic Window logic:
```text
PARTITION BY artist_id
ORDER BY extracted_at DESC, ingestion_timestamp DESC
```
Keep:
```text
row_number() = 1
```

### Cleaning
* Trim `artist_id`
* Trim `artist_name`
* Preserve `genres` as an array (no premature `genres[0]` assumptions)
* Validate Spotify URI structure
* Preserve lineage metadata

---

# 2️⃣ `silver_albums`

### Deduplication
```text
PARTITION BY album_id
ORDER BY extracted_at DESC, ingestion_timestamp DESC
```

### Date normalization
Normalize Spotify mixed dates (`2024`, `2024-04`, `2024-04-19`) into:
```text
release_date        → DateType (e.g. 2024-01-01 if only year given)
release_year        → Integer (2024)
release_month       → Integer (1)
release_decade      → String/Integer ("2020s" / 2020)
release_date_precision → original precision ("year")
```

### Album type
Normalize categorical values (`album`, `single`, `compilation`) to lowercase.

---

# 3️⃣ `silver_tracks`

### Deduplication
```text
PARTITION BY track_id
ORDER BY extracted_at DESC, ingestion_timestamp DESC
```

### Duration & Numbering
* Derive `duration_min = round(duration_ms / 60000.0, 2)`
* Derive `duration_sec = round(duration_ms / 1000.0, 1)`
* Validate `disc_number >= 1`, `track_number >= 1`
* Enforce `explicit` as BooleanType

---

# 🛡️ Automated Data Quality Framework

Reusable engine in `src/quality/data_quality.py` with `DataQualityChecker`.

## DQ Rules

| # | Check | Logic | Severity |
| :--- | :--- | :--- | :---: |
| **1** | **Completeness** | Row count > 0 | FAIL |
| **2** | **PK Uniqueness** | No duplicate primary keys | FAIL |
| **3** | **Critical Nulls** | PK and Name fields not null | FAIL |
| **4** | **FK Integrity** | Left-anti join: Detect orphan relationships | WARN / FAIL |
| **5** | **Range Validation** | Valid duration / date / number ranges | FAIL |

### Referential Integrity Threshold
* **0%–5% orphan rate**: **WARN** $\rightarrow$ Quarantine orphan records, write valid records to Silver.
* **>5% orphan rate**: **FAIL** $\rightarrow$ Block Silver write, persist DQ report.

---

# 🚧 Quarantine & DQ Report

* **Quarantine Path**: `data/quarantine/{entity}/snapshot_date=YYYY-MM-DD/` with `dq_reason`, `dq_rule`, `snapshot_date`.
* **DQ Report Path**: `data/quality/reports/snapshot_date=YYYY-MM-DD/dq_report.json`.

---

# 📁 Final Day 5 Project Structure

```text
spotify-pipeline/
│
├── src/
│   ├── transform/
│   │   ├── spark_session.py
│   │   ├── schemas.py
│   │   ├── bronze_transformer.py
│   │   └── silver_transformer.py
│   │
│   └── quality/
│       └── data_quality.py
│
├── scripts/
│   ├── verify_bronze.py
│   └── run_silver.py
│
├── tests/
│   ├── test_bronze_transform.py
│   ├── test_silver_transform.py
│   └── test_data_quality.py
│
└── data/
    ├── raw/
    ├── bronze/
    ├── silver/
    ├── quarantine/
    └── quality/
        └── reports/
```

---

## 🏁 Day 5 Success Criteria

1. ✅ `src/quality/data_quality.py` with 5 validation rules, quarantine routing, and JSON reporting.
2. ✅ `src/transform/silver_transformer.py` with Window deduplication, date normalization, and duration derivation.
3. ✅ Persistent `data/silver/`, `data/quarantine/`, and `data/quality/reports/` for snapshot `2026-08-31`.
4. ✅ Test suites in `tests/test_data_quality.py` and `tests/test_silver_transform.py` passing with 100% green status.

