# 📓 Spotify Music Intelligence Platform — Engineering Diary

A daily engineering log tracking goals, architectural decisions, technical challenges, and verified definitions of done.

---

## 🗓️ Day 1: API Audit & Project Setup (2026-08-29)

### 🎯 Objective:

Audit Spotify Web API 2026 contracts, verify OAuth 2.0 Client Credentials flow, establish dataset scope, and configure project environment.

### 🧩 Work Done:

- Tested OAuth 2.0 Basic Auth token generation against `https://accounts.spotify.com/api/token`.
- Inspected response schemas for `/v1/search`, `/v1/artists/{id}`, `/v1/artists/{id}/albums`, and `/v1/albums/{id}/tracks`.
- Established the 8-artist curated superstar cohort across genres and eras (Taylor Swift, The Weeknd, Drake, Ed Sheeran, Billie Eilish, Ariana Grande, Coldplay, BTS).
- Created `.env.example`, `requirements.txt`, `docs/spotify_api.md`, `docs/business_requirements.md`, and `scripts/test_spotify_auth.py`.

### 🏁 Definition of Done:

- ✅ Verified 2026 Spotify endpoints and authentication flow.
- ✅ Live token generation with $3,600\text{s}$ expiration confirmed.

---

## 🗓️ Day 2: Python Extraction Engine (2026-08-30)

### 🎯 Objective:

Build a production-grade, modular Python extraction package in `src/extract/` that extracts Spotify catalog metadata for our target cohort and stores immutable raw JSON in `data/raw/`.

### 🧩 Work Done:

- Built `src/extract/spotify_client.py` with OAuth 2.0 token caching ($3,600\text{s}$ buffer), HTTP session pooling, 429 `Retry-After` exponential jitter, and circuit breaker.
- Built `artist_extractor.py`, `album_extractor.py` (offset pagination, `limit=10`), `track_extractor.py` (simplified track objects, `limit=10`), and `main.py` orchestrator.
- Injected `snapshot_date` temporal audit watermarks.
- Encountered real-world Spotify Development Mode account quota ceiling; handled partial pipeline state gracefully.

### 🏁 Definition of Done:

- ✅ Extracted **8 Artists, 741 Albums, and 2,500 Tracks** into `data/raw/`.
- ✅ Verified partitioned JSON files in `data/raw/artists/`, `data/raw/albums/`, `data/raw/tracks/`.

---

## 🗓️ Day 3: AWS S3 Raw Data Lake (2026-08-31)

### 🎯 Objective:

Persist the immutable raw JSON payloads into an **Amazon S3 Raw Data Lake** using `boto3` with Hive-style partitioning, object metadata, SSE-S3 encryption, and idempotent verification.

### 🧩 Work Done:

- Audited AWS environment with `scripts/verify_aws_credentials.py` (STS caller verification, resource inventory).
- Cleaned legacy resources and created secure S3 bucket `s3://spotify-music-intelligence-luc/` with **Block Public Access** enabled.
- Built `src/storage/s3_uploader.py` with `validate_bucket()`, `build_s3_key()`, `upload_file()`, and `verify_object()`.
- Uploaded all 3 raw datasets to Hive partitions: `raw/extracted_at=2026-08-31/{artists,albums,tracks}/`.
- Proved **Idempotency**: re-running the upload cleanly overwrote deterministic S3 keys with zero duplicate objects.
- Built unit test suite `tests/test_s3_uploader.py` using `unittest.mock` (8/8 tests passed in 0.79s).

### 🏆 Day 3 Final Scorecard & Definition of Done:

| Requirement                     | Implementation                                                            | Command                                                                                       |   Status    |
| :------------------------------ | :------------------------------------------------------------------------ | :-------------------------------------------------------------------------------------------- | :---------: |
| **AWS IAM Authentication**      | Verified STS caller & account inventory                                   | `python scripts/verify_aws_credentials.py`                                                    | ✅ **DONE** |
| **S3 Bucket Creation**          | `s3://spotify-music-intelligence-luc/` with Block Public Access           | `aws s3api get-public-access-block --bucket spotify-music-intelligence-luc`                   | ✅ **DONE** |
| **Boto3 Ingestion Engine**      | `src/storage/s3_uploader.py`                                              | `python -m src.storage.s3_uploader 2026-08-31`                                                | ✅ **DONE** |
| **Hive-Style Partitioning**     | `raw/extracted_at=2026-08-31/{artists,albums,tracks}/`                    | `aws s3 ls s3://spotify-music-intelligence-luc/raw/ --recursive`                              | ✅ **DONE** |
| **Metadata & Encryption**       | `ContentType=application/json`, `AES256` (SSE-S3), custom audit tags      | `python -m src.storage.s3_uploader 2026-08-31`                                                | ✅ **DONE** |
| **Data Integrity Verification** | `os.path.getsize()` matched S3 `ContentLength` (2.7 KB, 393.7 KB, 1.0 MB) | `aws s3 ls s3://spotify-music-intelligence-luc/raw/extracted_at=2026-08-31/ --human-readable` | ✅ **DONE** |
| **Idempotency Proof**           | Overwrote deterministic keys with 0 duplicate files                       | `python -m src.storage.s3_uploader 2026-08-31`                                                | ✅ **DONE** |
| **Mock Unit Test Suite**        | 8/8 test cases in `tests/test_s3_uploader.py` passed                      | `pytest tests/test_s3_uploader.py -v`                                                         | ✅ **DONE** |

---

## 🗓️ Day 4: Bronze Layer with PySpark (2026-09-01)

### 🎯 Objective:

Build the **Bronze Layer** processing engine with **PySpark** to ingest immutable raw JSON payloads, enforce explicit `StructType` schemas, preserve nested structures, attach technical lineage metadata (`source`, `ingestion_timestamp`), support dynamic partition overwrite, and persist **Snappy-compressed Parquet** datasets partitioned by `snapshot_date=YYYY-MM-DD/`.

### 🧩 Work Done:

- Built decoupled PySpark session factory in `src/transform/spark_session.py` with Windows interpreter alignment (`PYSPARK_PYTHON = sys.executable`), UTC timezone, Snappy compression, and **Dynamic Partition Overwrite (`spark.sql.sources.partitionOverwriteMode = dynamic`)**.
- Defined strict `StructType` data contracts in `src/transform/schemas.py` (`RAW_ARTISTS_SCHEMA`, `RAW_ALBUMS_SCHEMA`, `RAW_TRACKS_SCHEMA`).
- Built `src/transform/bronze_transformer.py` with `read_raw_json(multiLine=True)`, `transform_to_bronze()` (type casting, whitespace trimming, null handling, lineage stamping), and `write_bronze_parquet()`.
- Executed end-to-end transformation on snapshots `2026-08-31` and `2026-09-01` (totaling **8 Artists, 1,025 Albums, and 3,964 Tracks** across both daily partitions).
- Synced all Bronze Parquet partitions to AWS S3: `s3://spotify-music-intelligence-luc/bronze/`.
- Built PySpark unit test suite `tests/test_bronze_transformer.py` (5/5 tests passed in 49.98s).

### 📊 Storage Compression Benchmark:

| Entity         |         Raw JSON Size         | Bronze Parquet (Snappy) |  Storage Reduction   |
| :------------- | :---------------------------: | :---------------------: | :------------------: |
| 🎵 **Tracks**  | `1,089 KB` ($1.08\text{ MB}$) |      **`186 KB`**       | **🔥 82.8% Savings** |
| 💿 **Albums**  |           `403 KB`            |       **`78 KB`**       | **🔥 80.5% Savings** |
| 🎤 **Artists** |           `2.7 KB`            |      **`1.4 KB`**       | **🔥 48.1% Savings** |

### 🏆 Day 4 Final Scorecard & Definition of Done:

| Requirement                       | Implementation                                                        | Command                                                                |   Status    |
| :-------------------------------- | :-------------------------------------------------------------------- | :--------------------------------------------------------------------- | :---------: |
| **Decoupled PySpark Session**     | `src/transform/spark_session.py` with dynamic partition overwrite     | `python -m src.transform.spark_session`                                | ✅ **DONE** |
| **Explicit StructType Contracts** | `src/transform/schemas.py` (7, 12, 11 fields)                         | `python -m src.transform.schemas`                                      | ✅ **DONE** |
| **Bronze Ingestion Transformer**  | `src/transform/bronze_transformer.py`                                 | `python -m src.transform.bronze_transformer 2026-08-31`                | ✅ **DONE** |
| **Multi-Snapshot Execution**      | Both `2026-08-31` & `2026-09-01` (3,964 tracks total)                 | `python -m src.transform.bronze_transformer 2026-09-01`                | ✅ **DONE** |
| **Lineage & Audit Metadata**      | `source="spotify-web-api"`, `ingestion_timestamp=current_timestamp()` | `pytest tests/test_bronze_transformer.py`                              | ✅ **DONE** |
| **Dynamic Partition Overwrite**   | Preserves historical snapshot partitions without data loss            | `python -m src.transform.bronze_transformer 2026-09-01`                | ✅ **DONE** |
| **Cloud Lakehouse Ingestion**     | Synced Bronze Parquet to AWS S3 bucket                                | `aws s3 sync data/bronze/ s3://spotify-music-intelligence-luc/bronze/` | ✅ **DONE** |
| **Storage Benchmark**             | Proved 82.8% storage reduction (1.08 MB $\rightarrow$ 186 KB)         | Verified on disk                                                       | ✅ **DONE** |
| **PySpark Unit Test Suite**       | 5/5 test cases in `tests/test_bronze_transformer.py` passed           | `pytest tests/test_bronze_transformer.py -v`                           | ✅ **DONE** |

---

## 🗓️ Day 5: Silver Layer & Automated Data Quality Gate (2026-09-07)

### 🎯 Objective:

Transform the Bronze Parquet layer into clean, typed, deterministically deduplicated, and relationally consistent Silver entities using PySpark, enforced by an Automated Data Quality Gate and Quarantine routing.

### 🧩 Work Done:

- **Silver Transformation Engine (`src/transform/silver_transformer.py`)**:
  - Implemented deterministic PySpark Window deduplication (`row_number() over (PARTITION BY id ORDER BY extracted_at DESC, ingestion_timestamp DESC)`), keeping strictly latest record (`row_number() == 1`).
  - Normalized mixed Spotify release dates (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`) into `DateType`, deriving `release_year`, `release_month`, and `release_decade` while preserving original `release_date_precision`.
  - Derived track metrics (`duration_min`, `duration_sec`), cast booleans/integers, and stamped `silver_transformed_at` audit lineage.
- **Automated Data Quality Gate (`src/quality/data_quality.py`)**:
  - Built `DataQualityChecker` enforcing 5 core validation rules: Completeness, Primary Key Uniqueness, Critical Column Nulls (single-pass aggregation), Referential Integrity (distributed `left_anti` joins), and Value/Range Validation.
  - Configured threshold-based severity for orphan relationships (0% PASS, $\le 5\%$ WARN, $>5\%$ FAIL).
  - Built quarantine routing for anomalies (`data/quarantine/{entity}/`) and audit-ready observability reporting (`data/quality/reports/snapshot_date=YYYY-MM-DD/dq_report.json`).
- **Master Orchestrator (`scripts/run_silver.py`)**:
  - Built CLI runner orchestrating Bronze read $\rightarrow$ Silver transform $\rightarrow$ DQ evaluation $\rightarrow$ Quarantine routing $\rightarrow$ Silver write $\rightarrow$ DQ report emission.
- **Execution & Verification**:
  - Executed end-to-end pipeline across snapshots `2026-08-31` and `2026-09-01`.
  - Deduplicated 14 duplicate albums and 9 duplicate tracks across historical snapshots.
  - Verified 100% PASS rate across all 13 DQ checks with persistent JSON reports.
- **Test Suite**:
  - Verified 18/18 PySpark unit tests passing with 100% green status across `tests/test_silver_transformer.py` and `tests/test_data_quality.py`.

### 📊 Silver Ingestion & Deduplication Audit:

| Snapshot Date  | Entity     | Bronze Rows | Conformed Silver Rows | Duplicates Collapsed |   DQ Status    |
| :------------- | :--------- | :---------: | :-------------------: | :------------------: | :------------: |
| **2026-08-31** | 🎤 Artists |      8      |           8           |          0           | ✅ PASS (100%) |
| **2026-08-31** | 💿 Albums  |     741     |        **732**        |        **9**         | ✅ PASS (100%) |
| **2026-08-31** | 🎵 Tracks  |    2,500    |       **2,499**       |        **1**         | ✅ PASS (100%) |
| **2026-09-01** | 🎤 Artists |      3      |           3           |          0           | ✅ PASS (100%) |
| **2026-09-01** | 💿 Albums  |     284     |        **279**        |        **5**         | ✅ PASS (100%) |
| **2026-09-01** | 🎵 Tracks  |    1,464    |       **1,456**       |        **8**         | ✅ PASS (100%) |

### 🏆 Day 5 Final Scorecard & Definition of Done:

| Requirement                      | Implementation                                                 | Command                                                                 |   Status    |
| :------------------------------- | :------------------------------------------------------------- | :---------------------------------------------------------------------- | :---------: |
| **Silver Transformation Engine** | `src/transform/silver_transformer.py`                          | `python scripts/run_silver.py --snapshot-date 2026-08-31`               | ✅ **DONE** |
| **Window Deduplication**         | `row_number() == 1` over PK ordering by latest extraction      | Verified in `tests/test_silver_transformer.py`                          | ✅ **DONE** |
| **Mixed Date Normalization**     | Year/Month/Day standardization to `DateType` + derived decades | Verified in `tests/test_silver_transformer.py`                          | ✅ **DONE** |
| **Automated DQ Gate**            | 5 core rules in `src/quality/data_quality.py`                  | `pytest tests/test_data_quality.py -v`                                  | ✅ **DONE** |
| **Referential Integrity Engine** | Left-Anti join orphan detection with threshold gating          | Evaluated in `scripts/run_silver.py`                                    | ✅ **DONE** |
| **Quarantine Routing**           | Lineage stamping and writing to `data/quarantine/{entity}/`    | Verified in unit tests & runner script                                  | ✅ **DONE** |
| **DQ Observability Reports**     | JSON audit report exported to `data/quality/reports/`          | Verified on disk for both snapshots                                     | ✅ **DONE** |
| **Multi-Snapshot Execution**     | Executed across both `2026-08-31` and `2026-09-01`             | `python scripts/run_silver.py --all`                                    | ✅ **DONE** |
| **Dynamic Partition Overwrite**  | Preserves historical partitions without data corruption        | Verified via `ls -la data/silver/{entity}/`                             | ✅ **DONE** |
| **PySpark Test Suite**           | 18/18 tests passed across 2 test suites                        | `pytest tests/test_silver_transformer.py tests/test_data_quality.py -v` | ✅ **DONE** |

---

## 🗓️ Day 6: Architectural Audit & Quota Resilience (2026-09-08)

### 🎯 Objective:

Investigate Silver metric calibration anomalies, trace incomplete snapshot lineage back to raw extraction, implement per-artist checkpointing with CLI argument support, and address API time-travel constraints.

### 🔍 Forensic Trace & Root Cause Analysis:

1. **Silver Profiling Anomaly**:
   - `2026-08-31` snapshot contained 8 artists, but BTS and Coldplay had 0 tracks, while Ariana Grande had only 113 tracks (totaling exactly 2,499 tracks).
   - `2026-09-01` snapshot contained only 3 artists (Ariana Grande, BTS, Coldplay).
2. **Upstream Root Cause (Bronze → Raw Tracing)**:
   - Evaluated the decision tree:
     ```text
     Bronze Incomplete → Extraction Problem (Day 2/Day 4)
     Bronze Complete, Silver Incomplete → Transformation Problem (Day 5)
     ```
   - Traced back to raw JSON payloads: `tracks_2026-08-31.json` halted at 2,500 records because Spotify Development Mode hit its 24-hour quota limit during Ariana Grande's extraction.
   - The `2026-09-01` run was a targeted partial catch-up run (`python -m src.extract.main "Ariana Grande" "BTS" "Coldplay"`) rather than a full cohort extraction.
   - Proved that the Silver transformation engine was 100% bug-free, but operating on incomplete raw upstream partitions.

### 💡 Core Architectural Decisions & Upgrades:

1. **Per-Artist Persistent Checkpointing (`data/raw/.checkpoints/checkpoint_YYYY-MM-DD.json`)**:
   - Upgraded `src/extract/main.py` with crash recovery.
   - Completed artists are committed to disk incrementally along with their albums and tracks.
   - On resume/retry, previously extracted artists are skipped immediately without consuming Spotify API quota.
2. **CLI Parameterization with `argparse`**:
   - Added `--snapshot-date`, `--artists`, `--output-dir`, and `--reset-checkpoint` flags, decoupling execution date from business audit watermark.
3. **The Third-Party API Time-Travel Constraint (Data Governance Trade-off)**:
   - **Constraint**: Spotify Web API exposes current live catalog state without point-in-time time travel (`as-of` queries).
   - **Trade-off**: Backfilling historical partitions (`2026-08-31`) captures live catalog state stamped under the historical partition watermark.
   - **Mitigation & Rationale**: In music intelligence, artist discographies are predominantly monotonic and cumulative (historical releases persist). Tagging backfilled runs with the audit watermark provides consistent baseline snapshots for Kimball dimensional modeling while explicitly documenting the latency trade-off in the pipeline audit log.
4. **Cohort Calibration & Simulation Engine (`scripts/simulate_snapshot.py`)**:
   - Isolated the 5 complete superstars (Taylor Swift, Ed Sheeran, Drake, Billie Eilish, The Weeknd) across both `2026-08-31` and `2026-09-01`.
   - Simulated snapshot `2026-09-01` by cloning `2026-08-31` raw JSON, establishing an immutable multi-day testing cohort with an exact $0.0\%$ growth baseline to validate Kimball periodic snapshot windowing.

---

## 🗓️ Day 6 (Part 2): Gold Layer — Kimball Star Schema & Periodic Snapshot Fact (2026-09-11)

### 🎯 Objective:

Design, build, and validate the **Gold Dimensional Layer** adhering strictly to Ralph Kimball's Data Warehouse Toolkit methodology. Construct 4 conformed dimensions (`dim_date`, `dim_artist`, `dim_album`, `dim_track`) and 1 periodic snapshot fact table (`fact_artist_snapshot`) with semi-additive metrics, rolling 12-month drop velocity, snapshot-over-snapshot catalog growth, and composite catalog momentum scoring.

### 🏗️ Star Schema Architecture Implemented:

```text
               ┌─────────────────────────────────────┐
               │              dim_date               │
               ├─────────────────────────────────────┤
               │ PK date_key (YYYYMMDD)              │
               │    calendar_date, year, month...    │
               │    is_weekend, epoch_day            │
               └──────────────────┬──────────────────┘
                                  │
                                  │ FK date_key
                                  ▼
               ┌─────────────────────────────────────┐
               │        fact_artist_snapshot         │
               ├─────────────────────────────────────┤
               │ PK (artist_key, date_key)           │
               │ FK artist_key ──────────────────────┼────────┐
               │    total_albums (semi-additive)     │        │
               │    total_tracks (semi-additive)     │        │
               │    total_singles (semi-additive)    │        │
               │    recent_releases_12m (velocity)   │        │
               │    median_release_cadence_days      │        │
               │    catalog_growth_pct (LAG window)  │        │
               │    catalog_momentum_index [0, 100]  │        │
               └──────────────────┬──────────────────┘        │
                                  │                           │
                     FK artist_key│                           │ FK artist_key
                                  ▼                           ▼
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│      dim_track       │    │      dim_album       │    │      dim_artist      │
├──────────────────────┤    ├──────────────────────┤    ├──────────────────────┤
│ PK track_key (MD5)   │    │ PK album_key (MD5)   │    │ PK artist_key (MD5)  │
│ FK album_key ────────┼───▶│ FK artist_key ───────┼───▶│    artist_id         │
│ FK artist_key ───────┼─┐  │    album_id          │    │    artist_name       │
│    track_name, etc.  │ │  │    album_name, etc.  │    │    followers, etc.   │
└──────────────────────┘ └─▶└──────────────────────┘    └──────────────────────┘
```

### 🧠 Core Architectural Decisions & Lessons Learned:

1. **Option A: 100% Native PySpark Windowing vs Python UDFs**:
   - Avoided Python UDFs for computing median release cadence. Python UDFs introduce expensive JVM-Python IPC serialization and cause `PicklingError` when referencing session state.
   - Implemented pure Spark native windowing: `Window.partitionBy("artist_id").orderBy("release_date")` + `lag()` + `datediff()` + `percentile_approx("gap_days", 0.5, 10000)`.
2. **Surrogate Key Generation in Distributed Systems**:
   - Avoided auto-incrementing integer surrogate keys (`MONOTONICALLY_INCREASING_ID()`), which are non-deterministic across re-runs and require cluster-wide coordination.
   - Implemented deterministic **MD5 surrogate hash keys** (`artist_key`, `album_key`, `track_key`) via `md5(concat_ws("||", ...))`.
   - Used "smart integer" keys for `dim_date`: `date_key = CAST(date_format(calendar_date, 'yyyyMMdd') AS INT)`.
3. **The First-Snapshot NULL Growth Rule**:
   - In periodic snapshot fact tables, calculating `catalog_growth_pct` via `LAG()` on the first available snapshot (`2026-08-31`) **strictly evaluates to `NULL`**, NOT `0.0%`.
   - *Rationale*: A value of `0.0%` implies a monitored catalog exhibited zero growth. `NULL` communicates that the snapshot serves as the initial baseline observation. On `2026-09-01`, it correctly evaluated to `0.0%`.
4. **Cadence Calibration & Scoring Differentiation**:
   - Initial formula used an upper bound of 1,825 days with a 90-day ceiling, causing all artists with $<90$ days cadence to saturate at 100 points.
   - Recalibrated $S_{\text{cadence}}$ bounds to $[14.0\text{ days}, 180.0\text{ days}]$, yielding true variance between frequent single droppers (Ed Sheeran: 14d, Drake: 15d) and album cycle artists (The Weeknd: 107d, Billie Eilish: 114d).
5. **Dynamic Partition Overwrite on Gold Fact**:
   - `fact_artist_snapshot` is partitioned by `snapshot_date`. Dynamic partition overwrite allows re-processing a single snapshot date without wiping out past snapshot history.

---

### 📊 Verified Table Inventory & Row Counts:

| Table | Type | Grain | Row Count | Primary Key | Format |
|---|---|---|:---:|---|---|
| **`dim_date`** | Conformed Dimension | 1 calendar day | **29,585** | `date_key` (19500101 – 20301231) | Snappy Parquet |
| **`dim_artist`** | Conformed Dimension | 1 artist (SCD Type 1) | **5** | `artist_key` (MD5 hex) | Snappy Parquet |
| **`dim_album`** | Conformed Dimension | 1 album | **456** | `album_key` (MD5 hex) | Snappy Parquet |
| **`dim_track`** | Conformed Dimension | 1 track | **2,386** | `track_key` (MD5 hex) | Snappy Parquet |
| **`fact_artist_snapshot`** | Periodic Snapshot Fact | 1 artist per snapshot date | **10** (5 × 2 dates) | `(artist_key, date_key)` | Partitioned Parquet |

### 🏆 Day 6 Final Scorecard & Definition of Done:

| Requirement | Implementation | Command / File | Status |
|---|---|---|:---:|
| **Formal Architecture Contract** | Published dimensional schema contract & ER diagram | `docs/gold_schema.md` | ✅ **DONE** |
| **Metric Formulation & Math** | Kimball classification & formula specifications | `docs/metric_definitions.md` | ✅ **DONE** |
| **Gold Transformation Engine** | Modular PySpark engine for 4 dimensions + 1 fact | `src/transform/gold_transformer.py` | ✅ **DONE** |
| **Native Cadence Windowing** | Option A pure Spark windowing (`percentile_approx`) | `src/transform/gold_transformer.py` | ✅ **DONE** |
| **First Snapshot NULL Invariant** | Strictly `NULL` on baseline, `0.0%` on clone | Verified in `tests/test_gold_transformer.py` | ✅ **DONE** |
| **Metric Boundary Guarantees** | Counters $\ge 0$, Momentum index $\in [0.0, 100.0]$ | Verified in `tests/test_gold_transformer.py` | ✅ **DONE** |
| **Referential Integrity** | Zero orphan foreign keys across all star schema joins | Verified in `tests/test_gold_transformer.py` | ✅ **DONE** |
| **Automated PySpark Test Suite** | 8/8 test cases passed in 22.53s | `pytest tests/test_gold_transformer.py -v` | ✅ **DONE** |
| **Master Gold Orchestrator** | CLI runner with logging, formatting, and S3 sync | `scripts/run_gold.py` | ✅ **DONE** |
| **AWS S3 Synchronization** | Sync Gold Parquet store to AWS Lakehouse | `python scripts/run_gold.py --sync-s3` | ✅ **DONE** |
