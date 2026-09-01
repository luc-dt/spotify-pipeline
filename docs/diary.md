# 📓 Spotify Music Intelligence Platform — Engineering Diary

A daily engineering log tracking goals, architectural decisions, technical challenges, and verified definitions of done.

---

## 🗓️ Day 1: API Audit & Project Setup (2026-08-29)

### 🎯 Objective:
Audit Spotify Web API 2026 contracts, verify OAuth 2.0 Client Credentials flow, establish dataset scope, and configure project environment.

### 🧩 Work Done:
* Tested OAuth 2.0 Basic Auth token generation against `https://accounts.spotify.com/api/token`.
* Inspected response schemas for `/v1/search`, `/v1/artists/{id}`, `/v1/artists/{id}/albums`, and `/v1/albums/{id}/tracks`.
* Established the 8-artist curated superstar cohort across genres and eras (Taylor Swift, The Weeknd, Drake, Ed Sheeran, Billie Eilish, Ariana Grande, Coldplay, BTS).
* Created `.env.example`, `requirements.txt`, `docs/spotify_api.md`, `docs/business_requirements.md`, and `scripts/test_spotify_auth.py`.

### 🏁 Definition of Done:
* ✅ Verified 2026 Spotify endpoints and authentication flow.
* ✅ Live token generation with $3,600\text{s}$ expiration confirmed.

---

## 🗓️ Day 2: Python Extraction Engine (2026-08-30)

### 🎯 Objective:
Build a production-grade, modular Python extraction package in `src/extract/` that extracts Spotify catalog metadata for our target cohort and stores immutable raw JSON in `data/raw/`.

### 🧩 Work Done:
* Built `src/extract/spotify_client.py` with OAuth 2.0 token caching ($3,600\text{s}$ buffer), HTTP session pooling, 429 `Retry-After` exponential jitter, and circuit breaker.
* Built `artist_extractor.py`, `album_extractor.py` (offset pagination, `limit=10`), `track_extractor.py` (simplified track objects, `limit=10`), and `main.py` orchestrator.
* Injected `snapshot_date` temporal audit watermarks.
* Encountered real-world Spotify Development Mode account quota ceiling; handled partial pipeline state gracefully.

### 🏁 Definition of Done:
* ✅ Extracted **8 Artists, 741 Albums, and 2,500 Tracks** into `data/raw/`.
* ✅ Verified partitioned JSON files in `data/raw/artists/`, `data/raw/albums/`, `data/raw/tracks/`.

---

## 🗓️ Day 3: AWS S3 Raw Data Lake (2026-08-31)

### 🎯 Objective:
Persist the immutable raw JSON payloads into an **Amazon S3 Raw Data Lake** using `boto3` with Hive-style partitioning, object metadata, SSE-S3 encryption, and idempotent verification.

### 🧩 Work Done:
* Audited AWS environment with `scripts/verify_aws_credentials.py` (STS caller verification, resource inventory).
* Cleaned legacy resources and created secure S3 bucket `s3://spotify-music-intelligence-luc/` with **Block Public Access** enabled.
* Built `src/storage/s3_uploader.py` with `validate_bucket()`, `build_s3_key()`, `upload_file()`, and `verify_object()`.
* Uploaded all 3 raw datasets to Hive partitions: `raw/extracted_at=2026-08-31/{artists,albums,tracks}/`.
* Proved **Idempotency**: re-running the upload cleanly overwrote deterministic S3 keys with zero duplicate objects.
* Built unit test suite `tests/test_s3_uploader.py` using `unittest.mock` (8/8 tests passed in 0.79s).

### 🏆 Day 3 Final Scorecard & Definition of Done:

| Requirement | Implementation | Command | Status |
| :--- | :--- | :--- | :---: |
| **AWS IAM Authentication** | Verified STS caller & account inventory | `python scripts/verify_aws_credentials.py` | ✅ **DONE** |
| **S3 Bucket Creation** | `s3://spotify-music-intelligence-luc/` with Block Public Access | `aws s3api get-public-access-block --bucket spotify-music-intelligence-luc` | ✅ **DONE** |
| **Boto3 Ingestion Engine** | `src/storage/s3_uploader.py` | `python -m src.storage.s3_uploader 2026-08-31` | ✅ **DONE** |
| **Hive-Style Partitioning** | `raw/extracted_at=2026-08-31/{artists,albums,tracks}/` | `aws s3 ls s3://spotify-music-intelligence-luc/raw/ --recursive` | ✅ **DONE** |
| **Metadata & Encryption** | `ContentType=application/json`, `AES256` (SSE-S3), custom audit tags | `python -m src.storage.s3_uploader 2026-08-31` | ✅ **DONE** |
| **Data Integrity Verification** | `os.path.getsize()` matched S3 `ContentLength` (2.7 KB, 393.7 KB, 1.0 MB) | `aws s3 ls s3://spotify-music-intelligence-luc/raw/extracted_at=2026-08-31/ --human-readable` | ✅ **DONE** |
| **Idempotency Proof** | Overwrote deterministic keys with 0 duplicate files | `python -m src.storage.s3_uploader 2026-08-31` | ✅ **DONE** |
| **Mock Unit Test Suite** | 8/8 test cases in `tests/test_s3_uploader.py` passed | `pytest tests/test_s3_uploader.py -v` | ✅ **DONE** |

---

## 🗓️ Day 4: Bronze Layer with PySpark (2026-09-01)

### 🎯 Objective:
Build the **Bronze Layer** processing engine with **PySpark** to ingest immutable raw JSON payloads, enforce explicit `StructType` schemas, preserve nested structures, attach technical lineage metadata (`source`, `ingestion_timestamp`), support dynamic partition overwrite, and persist **Snappy-compressed Parquet** datasets partitioned by `snapshot_date=YYYY-MM-DD/`.

### 🧩 Work Done:
* Built decoupled PySpark session factory in `src/transform/spark_session.py` with Windows interpreter alignment (`PYSPARK_PYTHON = sys.executable`), UTC timezone, Snappy compression, and **Dynamic Partition Overwrite (`spark.sql.sources.partitionOverwriteMode = dynamic`)**.
* Defined strict `StructType` data contracts in `src/transform/schemas.py` (`RAW_ARTISTS_SCHEMA`, `RAW_ALBUMS_SCHEMA`, `RAW_TRACKS_SCHEMA`).
* Built `src/transform/bronze_transformer.py` with `read_raw_json(multiLine=True)`, `transform_to_bronze()` (type casting, whitespace trimming, null handling, lineage stamping), and `write_bronze_parquet()`.
* Executed end-to-end transformation on snapshots `2026-08-31` and `2026-09-01` (totaling **8 Artists, 1,025 Albums, and 3,964 Tracks** across both daily partitions).
* Synced all Bronze Parquet partitions to AWS S3: `s3://spotify-music-intelligence-luc/bronze/`.
* Built PySpark unit test suite `tests/test_bronze_transformer.py` (5/5 tests passed in 49.98s).

### 📊 Storage Compression Benchmark:
| Entity | Raw JSON Size | Bronze Parquet (Snappy) | Storage Reduction |
| :--- | :---: | :---: | :---: |
| 🎵 **Tracks** | `1,089 KB` ($1.08\text{ MB}$) | **`186 KB`** | **🔥 82.8% Savings** |
| 💿 **Albums** | `403 KB` | **`78 KB`** | **🔥 80.5% Savings** |
| 🎤 **Artists** | `2.7 KB` | **`1.4 KB`** | **🔥 48.1% Savings** |

### 🏆 Day 4 Final Scorecard & Definition of Done:

| Requirement | Implementation | Command | Status |
| :--- | :--- | :--- | :---: |
| **Decoupled PySpark Session** | `src/transform/spark_session.py` with dynamic partition overwrite | `python -m src.transform.spark_session` | ✅ **DONE** |
| **Explicit StructType Contracts** | `src/transform/schemas.py` (7, 12, 11 fields) | `python -m src.transform.schemas` | ✅ **DONE** |
| **Bronze Ingestion Transformer** | `src/transform/bronze_transformer.py` | `python -m src.transform.bronze_transformer 2026-08-31` | ✅ **DONE** |
| **Multi-Snapshot Execution** | Both `2026-08-31` & `2026-09-01` (3,964 tracks total) | `python -m src.transform.bronze_transformer 2026-09-01` | ✅ **DONE** |
| **Lineage & Audit Metadata** | `source="spotify-web-api"`, `ingestion_timestamp=current_timestamp()` | `pytest tests/test_bronze_transformer.py` | ✅ **DONE** |
| **Dynamic Partition Overwrite** | Preserves historical snapshot partitions without data loss | `python -m src.transform.bronze_transformer 2026-09-01` | ✅ **DONE** |
| **Cloud Lakehouse Ingestion** | Synced Bronze Parquet to AWS S3 bucket | `aws s3 sync data/bronze/ s3://spotify-music-intelligence-luc/bronze/` | ✅ **DONE** |
| **Storage Benchmark** | Proved 82.8% storage reduction (1.08 MB $\rightarrow$ 186 KB) | Verified on disk | ✅ **DONE** |
| **PySpark Unit Test Suite** | 5/5 test cases in `tests/test_bronze_transformer.py` passed | `pytest tests/test_bronze_transformer.py -v` | ✅ **DONE** |


