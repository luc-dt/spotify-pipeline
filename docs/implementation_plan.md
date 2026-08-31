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

# 🗓️ Day 4 — Bronze Layer with PySpark *(Draft for Discussion)*

### 🎯 Proposed Objective

Build the **Bronze Layer** processing engine with **PySpark** to ingest the immutable raw JSON payloads stored in S3, enforce explicit `StructType` schemas, perform **selective structural normalization**, add technical lineage metadata, and write **Snappy-compressed Parquet** datasets partitioned by `extracted_at=YYYY-MM-DD/`.

The goal is not simply to convert JSON into Parquet.

> **The goal is to establish a reliable, structured, query-efficient Bronze layer while preserving the meaning and provenance of the original raw data.**

---

# 🏗️ Proposed Day 4 Architecture

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AMAZON S3 RAW DATA LAKE                            │
│                                                                             │
│  s3://spotify-music-intelligence-luc/raw/                                 │
│                                                                             │
│  extracted_at=YYYY-MM-DD/                                                  │
│  ├── artists/artists.json                                                  │
│  ├── albums/albums.json                                                    │
│  └── tracks/tracks.json                                                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PYSPARK TRANSFORMATION                              │
│                                                                             │
│  src/transform/raw_to_bronze.py                                            │
│                                                                             │
│  • Read raw JSON                                                           │
│  • Enforce explicit StructType schemas                                     │
│  • Type casting & null handling                                            │
│  • Selective structural normalization                                      │
│  • Preserve nested relationships where appropriate                         │
│  • Add technical lineage metadata                                          │
│  • Validate transformed data                                               │
│                                                                             │
│  AWS Glue can later serve as the execution environment                      │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          AMAZON S3 BRONZE DATA LAKE                          │
│                                                                             │
│  s3://spotify-music-intelligence-luc/bronze/                               │
│                                                                             │
│  ├── artists/extracted_at=YYYY-MM-DD/                                      │
│  │      └── part-*.snappy.parquet                                          │
│  │                                                                         │
│  ├── albums/extracted_at=YYYY-MM-DD/                                       │
│  │      └── part-*.snappy.parquet                                          │
│  │                                                                         │
│  └── tracks/extracted_at=YYYY-MM-DD/                                       │
│         └── part-*.snappy.parquet                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

# 📐 Proposed Bronze Schemas

Rather than immediately flattening everything, we define **explicit schemas based on the actual Spotify API payloads**.

### Artist
```text
artist_id
artist_name
spotify_uri
image_url
genres
extracted_at
snapshot_date
source
ingestion_timestamp
```

### Album
```text
album_id
album_name
album_type
release_date
total_tracks
artist_id
spotify_uri
image_url
extracted_at
snapshot_date
source
ingestion_timestamp
```

### Track
```text
track_id
track_name
duration_ms
explicit
track_number
disc_number
album_id
artist_id
spotify_uri
extracted_at
snapshot_date
source
ingestion_timestamp
```

### Important Design Principle
> **Structural normalization in Bronze; business cleaning and modeling in Silver.**

---

# 🪜 Proposed Implementation Roadmap

### Step 1 — PySpark Environment
Set up a reproducible PySpark environment capable of running the Bronze transformation locally (`SparkSession`, S3-compatible input/output).

### Step 2 — Explicit Schema & Data Contract
Define `StructType` schemas for `artist`, `album`, and `track` with strict data types and nullable constraints to prevent Spark from blindly inferring inconsistent schemas.

### Step 3 — Raw JSON → Bronze DataFrame
Read raw JSON, apply explicit schemas, handle nulls, add technical lineage metadata (`source`, `ingestion_timestamp`).

### Step 4 — Write Bronze Parquet
Write resulting DataFrames as Snappy-compressed Parquet partitioned by `extracted_at=YYYY-MM-DD/`.

### Step 5 — Bronze Data Validation
Validate expected columns, data types, required fields, record counts, partition directories, and read-back assertions.

### Step 6 — Idempotent Re-run Test
Run the same Bronze transformation twice for the same raw snapshot to verify that the second run does **not create duplicate logical data** (`mode("overwrite")` on the partition).

### Step 7 — Compression Benchmark
Measure and report actual storage characteristics:
$$\text{Storage Savings} = \frac{\text{Raw JSON Size} - \text{Bronze Parquet Size}}{\text{Raw JSON Size}} \times 100\%$$

### Step 8 — Unit Tests & Documentation
Add PySpark unit tests for schemas, transformations, metadata, partitioning, and idempotency.

---

# 🎯 Day 4 Learning Outcomes

* **Data Architecture**: Understanding Medallion transitions ($\text{Raw} \rightarrow \text{Bronze} \rightarrow \text{Silver} \rightarrow \text{Gold}$).
* **PySpark Mastery**: `SparkSession`, `StructType`, `StructField`, DataFrame transformations, Parquet serialization, Snappy compression.
* **Data Engineering Rigor**: Schema enforcement, data contracts, lineage tracking, partition validation, idempotency, storage benchmarking.

---

# ⚠️ Decisions for Discussion Before Implementation

1. **Should Bronze preserve arrays such as `genres`, or normalize them?**
   * *Consensus*: Keep as `ArrayType(StringType())` in Bronze; explode in Silver.
2. **`extracted_at` vs `snapshot_date` representation?**
   * *Consensus*: `extracted_at` = UTC Timestamp of extraction run; `snapshot_date` = Daily partition key (`YYYY-MM-DD`).
3. **`source` field value?**
   * *Consensus*: `"spotify-web-api"`.
4. **`ingestion_timestamp` in Bronze?**
   * *Consensus*: Yes, attach `current_timestamp()` as audit lineage.
5. **Idempotency Strategy?**
   * *Consensus*: `.mode("overwrite")` on the partition directory.
6. **Compute Execution Model?**
   * *Consensus*: Local PySpark first, cloud Glue-ready.
7. **Dataset Separation?**
   * *Consensus*: 3 separate Bronze tables (`artists`, `albums`, `tracks`) to prepare for Kimball Star Schema.
8. **Schema Drift Definition?**
   * *Consensus*: Breaking drift (missing/corrupt PKs) halts job; non-breaking drift (new optional fields) is ignored gracefully by `StructType`.
9. **Testing Scope?**
   * *Consensus*: Schema contract + partition directory + record count match + Parquet read-back.
