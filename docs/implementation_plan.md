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

- ✅ Extracted **8 Artists, 741 Albums, and 2,500 Tracks** into `data/raw/`:
  - `data/raw/artists/artists_2026-08-31.json` (2.7 KB)
  - `data/raw/albums/albums_2026-08-31.json` (393.7 KB)
  - `data/raw/tracks/tracks_2026-08-31.json` (1,063.7 KB)
- ✅ Committed and pushed to GitHub: [`https://github.com/luc-dt/spotify-pipeline.git`](https://github.com/luc-dt/spotify-pipeline.git).

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

- ✅ S3 bucket `spotify-music-intelligence-luc` created in `ap-southeast-2` with Block Public Access enabled.
- ✅ `scripts/verify_aws_credentials.py` verified STS caller identity.
- ✅ `src/storage/s3_uploader.py` successfully uploaded 3/3 datasets.
- ✅ S3 ContentLength matched local file bytes (2,773 B Artists, 403,157 B Albums, 1,089,235 B Tracks).
- ✅ 8/8 unit tests in `tests/test_s3_uploader.py` passed with 100% green status.
- ✅ Committed and pushed to GitHub: [`https://github.com/luc-dt/spotify-pipeline.git`](https://github.com/luc-dt/spotify-pipeline.git).

---

---

# 🗓️ Day 4 — Bronze Layer with PySpark (COMPLETED)

### 🎯 Day 4 Objective:

Build the **Bronze Layer** processing engine with **PySpark** to ingest the immutable raw JSON payloads stored in S3, enforce explicit `StructType` schemas, perform selective structural normalization, add technical lineage metadata (`source`, `ingestion_timestamp`), and write **Snappy-compressed Parquet** datasets partitioned by `snapshot_date=YYYY-MM-DD/`.

---

### 🧩 The Core Components Built:

- `src/transform/spark_session.py`: Decoupled PySpark session factory with Dynamic Partition Overwrite.
- `src/transform/schemas.py`: Explicit `StructType` contracts for `artists`, `albums`, `tracks`.
- `src/transform/bronze_transformer.py`: Multi-line JSON ingestion, type casting, lineage injection, and partitioned Snappy Parquet writer.
- `tests/test_bronze_transformer.py`: 5/5 unit tests verifying contracts, transformations, null coalescing, and Parquet read-back.

---

### 🏁 Day 4 Definition of Done (Verified Results):

- ✅ Transformed multi-day snapshots (`2026-08-31` & `2026-09-01`) totaling **8 Artists, 1,025 Albums, and 3,964 Tracks** into Bronze Snappy Parquet.
- ✅ Enabled Dynamic Partition Overwrite (`spark.sql.sources.partitionOverwriteMode=dynamic`), preserving multi-day partitions on disk and cloud.
- ✅ Reduced storage footprint by **82.8%** on tracks (1,089 KB $\rightarrow$ 186 KB) and **80.5%** on albums (403 KB $\rightarrow$ 78 KB).
- ✅ Synced all Bronze Parquet tables to Amazon S3 (`s3://spotify-music-intelligence-luc/bronze/`).
- ✅ 5/5 PySpark unit tests passed in 49.98s with 100% green status.

---

---

# 🗓️ Day 5: Silver Layer & Automated Data Quality Gate

### 🎯 Day 5 Objective

Transform the **Bronze Parquet layer** into clean, typed, deduplicated, and relationally consistent **Silver entities** using PySpark.

The Silver layer will:

- Standardize data types and dates
- Deterministically deduplicate records
- Preserve source-level date precision
- Apply conservative data sanitization
- Validate primary keys, nulls, ranges, and foreign keys
- Quarantine invalid/orphan records instead of unnecessarily discarding valid data
- Produce a persistent **PASS / WARN / FAIL Data Quality report**
- Support safe, idempotent re-runs

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
│         PHASE 1: SILVER TRANSFORMATION ENGINE              │
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
│              PHASE 2: AUTOMATED DQ GATE                    │
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
│     SILVER LAYER     │   │     QUARANTINE       │
│                      │   │                      │
│ data/silver/         │   │ data/quarantine/     │
│ {artists,albums,     │   │ {entity}/            │
│  tracks}/             │   │ snapshot_date=.../   │
│ snapshot_date=.../   │   │                      │
└──────────┬───────────┘   └──────────┬───────────┘
           │                          │
           └────────────┬─────────────┘
                        ▼
┌────────────────────────────────────────────────────────────┐
│            PHASE 3: DQ REPORT / OBSERVABILITY              │
│                                                            │
│ data/quality/reports/                                      │
│   snapshot_date=YYYY-MM-DD/                                │
│       dq_report.json                                       │
└──────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────┐
│          PHASE 4: VERIFICATION & DOCUMENTATION             │
│                                                            │
│  • 100% green pytest suites                                │
│    (test_data_quality, test_silver_transform)              │
│  • Update docs/diary.md scorecard                          │
│  • Add Day 5 interview deep-dives to docs/questions.md     │
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

# ⚙️ Phase 1: Silver Entity Transformations

## 1️⃣ `silver_artists`

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

- Trim `artist_id`
- Trim `artist_name`
- Preserve `genres` as an array (no premature `genres[0]` assumptions)
- Validate Spotify URI structure
- Preserve lineage metadata

---

## 2️⃣ `silver_albums`

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

## 3️⃣ `silver_tracks`

### Deduplication

```text
PARTITION BY track_id
ORDER BY extracted_at DESC, ingestion_timestamp DESC
```

### Duration & Numbering

- Derive `duration_min = round(duration_ms / 60000.0, 2)`
- Derive `duration_sec = round(duration_ms / 1000.0, 1)`
- Validate `disc_number >= 1`, `track_number >= 1`
- Enforce `explicit` as BooleanType

---

# 🛡️ Phase 2: Automated Data Quality Gate

Reusable engine in `src/quality/data_quality.py` with `DataQualityChecker`.

## DQ Rules

| #     | Check                | Logic                                       |  Severity   |
| :---- | :------------------- | :------------------------------------------ | :---------: |
| **1** | **Completeness**     | Row count > 0                               |    FAIL     |
| **2** | **PK Uniqueness**    | No duplicate primary keys                   |    FAIL     |
| **3** | **Critical Nulls**   | PK and Name fields not null                 |    FAIL     |
| **4** | **FK Integrity**     | Left-anti join: Detect orphan relationships | WARN / FAIL |
| **5** | **Range Validation** | Valid duration / date / number ranges       |    FAIL     |

### Referential Integrity Threshold

- **0%–5% orphan rate**: **WARN** $\rightarrow$ Quarantine orphan records, write valid records to Silver.
- **>5% orphan rate**: **FAIL** $\rightarrow$ Block Silver write, persist DQ report.

---

# 🚧 Phase 3: Quarantine Routing & DQ Observability Report

- **Quarantine Path**: `data/quarantine/{entity}/snapshot_date=YYYY-MM-DD/` with `dq_reason`, `dq_rule`, `snapshot_date`.
- **DQ Report Path**: `data/quality/reports/snapshot_date=YYYY-MM-DD/dq_report.json`.

---

# 🧪 Phase 4: Verification, Automated Testing & Documentation

A rigorous testing, verification, and documentation gate to guarantee production reliability, evidence capture, and interview readiness before concluding Day 5.

### 1. PySpark Automated Test Suites (100% Green Status)
* **`tests/test_silver_transformer.py`**:
  * **Window Deduplication**: Assert multiple versions of the same entity (different `extracted_at`) keep strictly the latest record (`row_number() = 1`).
  * **Date Normalization**: Test mixed Spotify dates (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`), confirming correct `DateType` casting, `release_year`, `release_month`, `release_decade`, and preservation of `release_date_precision`.
  * **Derived Business Columns**: Validate `duration_min` and `duration_sec` calculations and rounding.
  * **Data Sanitization**: Verify trimming of identifiers and names, boolean casting of `explicit`, and regex validation of `spotify:*` URIs.
* **`tests/test_data_quality.py`**:
  * **Rule 1 (Completeness)**: Verify failure when row count is 0.
  * **Rule 2 (PK Uniqueness)**: Verify detection and quarantine of duplicate primary keys.
  * **Rule 3 (Critical Nulls)**: Verify failure/quarantine when required identifiers or names are null.
  * **Rule 4 (Referential Integrity)**: Test left-anti join behavior:
    * Clean relations ($\le 0\%$ orphans) $\rightarrow$ **PASS**.
    * Minor orphan rate ($0\% < \text{rate} \le 5\%$) $\rightarrow$ **WARN**, routes orphans to `data/quarantine/`, passes valid rows to Silver.
    * Major orphan rate ($> 5\%$) $\rightarrow$ **FAIL**, halts Silver write, persists failure audit in DQ report.
  * **Rule 5 (Range/Value Validation)**: Verify bounds on `duration_ms > 0`, `disc_number >= 1`, `track_number >= 1`.

### 2. Multi-Snapshot End-to-End Execution
* Execute `scripts/run_silver.py` across historical snapshots (`2026-08-31` and `2026-09-01`).
* Verify physical directory outputs:
  * Clean Silver Parquet: `data/silver/{artists,albums,tracks}/snapshot_date=YYYY-MM-DD/`
  * Quarantined Parquet: `data/quarantine/{entity}/snapshot_date=YYYY-MM-DD/`
  * JSON DQ Reports: `data/quality/reports/snapshot_date=YYYY-MM-DD/dq_report.json`
* Verify **Dynamic Partition Overwrite** prevents data corruption across multi-day snapshot runs.

### 3. Engineering Diary & Scorecard Update (`docs/diary.md`)
* Add the comprehensive **Day 5 Final Scorecard & Definition of Done** table.
* Log verified row counts: Clean Silver records vs. Quarantined records.
* Document architectural lessons learned (e.g., handling partial dates, left-anti join efficiency vs. subqueries, fail-fast vs. quarantine trade-offs).

### 4. Technical Interview Question Bank (`docs/questions.md`)
* Add Day 5 interview deep-dives designed for Senior Data Engineer & Hiring Manager interviews:
  * **Q1**: Why use a Spark Window function (`row_number()`) instead of `.dropDuplicates()` for deduplication?
  * **Q2**: Why implement a Quarantine layer instead of discarding bad records or letting the entire pipeline crash?
  * **Q3**: How does the Left-Anti Join pattern efficiently detect orphan records in distributed datasets?
  * **Q4**: How did you handle partial dates (`YYYY` or `YYYY-MM`) without losing temporal precision or corrupting SQL date operations?
  * **Q5**: What is the threshold strategy for Data Quality gates (PASS vs. WARN vs. FAIL)?

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
│   ├── test_bronze_transformer.py
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
3. ✅ `scripts/run_silver.py` orchestrating Bronze $\rightarrow$ Silver/Quarantine with dynamic partition overwrite.
4. ✅ Persistent `data/silver/`, `data/quarantine/`, and `data/quality/reports/` for snapshots `2026-08-31` and `2026-09-01`.
5. ✅ Test suites in `tests/test_data_quality.py` and `tests/test_silver_transform.py` passing with 100% green status.
6. ✅ `docs/diary.md` updated with Day 5 Scorecard, verified row counts, and architectural decisions.
7. ✅ `docs/questions.md` updated with Day 5 technical interview deep-dive questions and answers.

---

---

# 🗓️ Day 6: Gold Layer (Kimball Dimensional Modeling & Periodic Snapshot Fact)

### 🎯 Day 6 in One Sentence

> **Day 6 transforms trusted Silver entities into a Kimball Star Schema consisting of 4 descriptive dimensions (`dim_artist`, `dim_album`, `dim_track`, `dim_date`) and 1 periodic snapshot fact table (`fact_artist_snapshot`) at the grain of `(artist_key, date_key)`, enabling historical catalog growth, release cadence, and momentum analysis.**

---

### 🏛️ The Frozen Star Schema Architecture

```text
┌────────────────────────┐                   ┌────────────────────────┐
│       dim_artist       │                   │       dim_date         │
├────────────────────────┤                   ├────────────────────────┤
│ PK: artist_key         │                   │ PK: date_key (YYYYMMDD)│
│ artist_id (natural)    │                   │ full_date              │
│ artist_name            │                   │ year, quarter, month   │
│ genres (array)         │                   │ month_name, week, day  │
│ spotify_uri, image_url │                   │ day_of_week, is_weekend│
└───────────┬────────────┘                   └───────────┬────────────┘
            │ 1                                          │ 1
            │                                            │
            │          ┌───────────────────────────────┐ │
            └─────────►│     fact_artist_snapshot      │◄┘
                     N ├───────────────────────────────┤ N
                       │ PK: (artist_key, date_key)    │
                       │ FK: artist_key                │
                       │ FK: date_key                  │
                       │ snapshot_date (audit ts)      │
                       │───────────────────────────────│
                       │ total_albums (semi-additive)  │
                       │ total_tracks (semi-additive)  │
                       │ total_singles (semi-additive) │
                       │ recent_releases_12m           │
                       │ median_release_cadence_days   │
                       │ catalog_growth_pct            │
                       │ catalog_momentum_index [0,100]│
                       └───────────────────────────────┘
                                       ▲
    ┌────────────────────────┐         │
    │       dim_album        │         │
    ├────────────────────────┤         │ (Conformed dimensions for
    │ PK: album_key          │         │  downstream drill-down queries)
    │ album_id (natural)     │         │
    │ artist_id              │─────────┤
    │ album_name, album_type │         │
    │ release_date, year,... │         │
    └────────────────────────┘         │
                                       │
    ┌────────────────────────┐         │
    │       dim_track        │         │
    ├────────────────────────┤         │
    │ PK: track_key          │         │
    │ track_id (natural)     │         │
    │ album_id, artist_id    │─────────┘
    │ track_name, duration_ms│
    │ duration_min, explicit │
    └────────────────────────┘
```

---

### 🧠 Core Kimball Principles Enforced

1. **Strict Grain Definition**:
   - Grain: **One row per artist per successfully extracted snapshot date**.
   - Composite Primary Key: `(artist_key, date_key)`.
   - Never allow duplicate `(artist_key, date_key)` pairs.
2. **Semi-Additive vs. Non-Additive Measures**:
   - Semi-additive counters (`total_albums`, `total_tracks`, `total_singles`, `recent_releases_12m`): state observations valid across artists on the same date, invalid across time for the same artist.
   - Non-additive metrics: `median_release_cadence_days` (statistical summary), `catalog_growth_pct` (rate), `catalog_momentum_index` (score).
   - Compilation reconciliation: Compilations contribute to `total_tracks`, but are excluded from `total_albums` and `total_singles`. `recent_releases_12m` filters `album_type IN ('album', 'single')`.
   - Lifetime cadence scope: Cadence tracks lifetime publishing discipline; current activity is captured by `recent_releases_12m`.
3. **Surrogate vs. Natural Keys**:
   - Natural IDs (`artist_id`, `album_id`, `track_id`) originate from Spotify.
   - Surrogate keys (`artist_key`, `album_key`, `track_key`) are warehouse-managed deterministic hash/integer identifiers.
   - Date key is an explicit smart integer key: `date_key = YYYYMMDD` (e.g., `20260831`).
4. **Historical Time Series (Zero Overwrites)**:
   - Gold preserves daily snapshot history: `2026-08-31`, `2026-09-01`, etc.
   - First snapshot rule: `catalog_growth_pct = NULL` (not `0%`, preventing false stagnation signals).
   - Missing snapshot rule: PySpark `LAG()` compares against the most recent available prior snapshot.
5. **Empirical Momentum Normalization**:
   - The formula:
     $$\text{Momentum Index} = 0.40 \times S_{\text{recent}} + 0.35 \times S_{\text{growth}} + 0.25 \times S_{\text{cadence}}$$
   - Normalization thresholds ($0 \rightarrow 100$) calibrated against **real Silver data distributions**, not arbitrary guesses.

---

# 📐 Master Phased Execution Plan for Day 6

---

### 📋 Phase 1: Architecture Freeze & Blueprint (Documentation First)

Following Kimball best practices, data contracts and math definitions are frozen prior to writing PySpark transformation code:

1. **[`docs/gold_schema.md`](file:///d:/data_engineering/Project_DE/spotify-pipeline/docs/gold_schema.md)**:
   - Formal schema specification for all 5 tables (`dim_artist`, `dim_album`, `dim_track`, `dim_date`, `fact_artist_snapshot`).
   - Explicit column names, physical PySpark / SQL types, nullability, PK/FK relationships, and business descriptions.
2. **[`docs/metric_definitions.md`](file:///d:/data_engineering/Project_DE/spotify-pipeline/docs/metric_definitions.md)**:
   - Mathematical formulation for all 7 fact measures (`total_albums`, `total_tracks`, `total_singles`, `recent_releases_12m`, `median_release_cadence_days`, `catalog_growth_pct`, `catalog_momentum_index`).
   - Exact edge case handling: First snapshot nulls, single-release artists (division by zero / undefined cadence), and score bounding $[0, 100]$.
3. **Update [`docs/PLAN.md`](file:///d:/data_engineering/Project_DE/spotify-pipeline/docs/PLAN.md)**:
   - Freeze the single-fact architecture (`fact_artist_snapshot`), formally deprecating redundant entity snapshots (`fact_album_snapshot`, `fact_track_snapshot`) in favor of conformed dimensions.

---

### 📊 Phase 2: Silver Data Profiling & Metric Calibration

Profile the actual distribution of Silver data across `2026-08-31` and `2026-09-01` to establish empirical scaling boundaries:

1. **Catalog Activity Profiling**:
   - Inspect album and single counts per artist.
   - Profile `recent_releases_12m` (min, max, median) relative to snapshot date.
   - Measure inter-release cadence (days between successive album/single drops) to avoid outlier distortions.
2. **Score Normalization Formulation**:
   - Lock in min-max clipping thresholds:
     - $S_{\text{recent}}$: Scaled based on observed 12-month release velocity.
     - $S_{\text{growth}}$: Scaled based on percentage track expansion between snapshots.
     - $S_{\text{cadence}}$: Inverse scale (shorter intervals $\rightarrow$ higher consistency score).

---

### ⚙️ Phase 3: Gold Transformation Engine (`src/transform/gold_transformer.py`)

Build the modular, idempotent PySpark Gold transformation engine:

1. **Date Dimension Generator (`dim_date`)**:
   - Generate Gregorian calendar table covering target time horizons (e.g., 1950 to 2030).
   - Smart integer PK `date_key` (`YYYYMMDD`), `full_date`, `year`, `quarter`, `month`, `month_name`, `week_of_year`, `day_of_month`, `day_of_week`, `day_name`, `is_weekend`.
2. **Conformed Dimensions (`dim_artist`, `dim_album`, `dim_track`)**:
   - Generate deterministic surrogate keys (`artist_key`, `album_key`, `track_key`) via MD5 hash / monotonically increasing IDs.
   - Carry forward clean descriptive attributes, foreign keys, and audit lineage (`created_at`).
3. **Periodic Snapshot Fact (`fact_artist_snapshot`)**:
   - **Grain**: One record per `(artist_key, date_key)`.
   - Aggregate semi-additive counters: `total_albums`, `total_tracks`, `total_singles` as of `snapshot_date`.
   - Window rolling lookback: Calculate `recent_releases_12m` (releases where `release_date >= snapshot_date - 365 days`).
   - Release interval calculation: Compute median days between releases per artist (`median_release_cadence_days`).
   - PySpark Window `LAG()`: Calculate `catalog_growth_pct` against previous available snapshot. Ensure initial snapshot resolves to `NULL`.
   - Calculate composite `catalog_momentum_index` clamped to $[0.0, 100.0]$.
4. **Physical Storage & Partitioning**:
   - Dimensions written as unpartitioned/snapshot Parquet to `data/gold/dim_{artist,album,track,date}/`.
   - Fact written partitioned by `snapshot_date=YYYY-MM-DD` with Dynamic Partition Overwrite to `data/gold/fact_artist_snapshot/`.

---

### 🧪 Phase 4: Automated Gold Quality Gate & Unit Testing

Implement PySpark test suites in `tests/test_gold_transformer.py`:

1. **Grain Uniqueness Test**:
   - Assert `count(*) == count(distinct artist_key, date_key)` on `fact_artist_snapshot`.
2. **Referential Integrity Test**:
   - Assert 100% of `artist_key` in `fact_artist_snapshot` join successfully to `dim_artist`.
   - Assert 100% of `date_key` in `fact_artist_snapshot` join successfully to `dim_date`.
3. **First-Snapshot Null Growth Rule**:
   - Assert that `catalog_growth_pct` is strictly `NULL` on the earliest snapshot (`2026-08-31`) for all artists.
4. **Metric Bounds & Value Invariants**:
   - Assert `total_albums >= 0`, `total_tracks >= 0`, `total_singles >= 0`.
   - Assert `0.0 <= catalog_momentum_index <= 100.0` for all non-null scores.

---

### 🚀 Phase 5: Orchestration, Cloud Sync & Engineering Artifacts

1. **Pipeline Orchestrator (`scripts/run_gold.py`)**:
   - Orchestrate Silver $\rightarrow$ Gold execution across `2026-08-31` and `2026-09-01`.
   - Verify dynamic partition overwrite preserves multi-day snapshot history.
2. **Cloud Lakehouse Synchronization**:
   - Sync Gold Parquet tables to Amazon S3: `s3://spotify-music-intelligence-luc/gold/`.
3. **Engineering Diary & Interview Prep**:
   - Update `docs/diary.md` with Day 6 Scorecard, row counts, and Kimball trade-offs.
   - Update `docs/questions.md` with Day 6 interview deep-dives (Periodic Snapshot vs Accumulating Snapshot vs Transaction Fact, Surrogate Keys in Big Data, Semi-Additive Measures).

---

# 📁 Target Day 6 Project Structure

```text
spotify-pipeline/
│
├── src/
│   ├── transform/
│   │   ├── spark_session.py
│   │   ├── schemas.py
│   │   ├── bronze_transformer.py
│   │   ├── silver_transformer.py
│   │   └── gold_transformer.py          ──▶ [NEW] Kimball Star Schema & Snapshot Engine
│   │
│   └── quality/
│       └── data_quality.py
│
├── scripts/
│   ├── run_silver.py
│   └── run_gold.py                     ──▶ [NEW] Master Gold Pipeline Orchestrator
│
├── tests/
│   ├── test_bronze_transformer.py
│   ├── test_silver_transform.py
│   ├── test_data_quality.py
│   └── test_gold_transformer.py        ──▶ [NEW] Kimball & Snapshot Fact Test Suite
│
├── docs/
│   ├── PLAN.md                         ──▶ [MODIFY] Architecture Freeze
│   ├── gold_schema.md                  ──▶ [NEW] Formal Star Schema Contract
│   ├── metric_definitions.md           ──▶ [NEW] Metric Formulation & Math Formulas
│   ├── diary.md                        ──▶ [MODIFY] Day 6 Scorecard & Verification
│   └── questions.md                    ──▶ [MODIFY] Kimball Data Warehouse Q&A
│
└── data/
    ├── silver/
    └── gold/                           ──▶ [NEW] Star Schema Parquet Store
        ├── dim_artist/
        ├── dim_album/
        ├── dim_track/
        ├── dim_date/
        └── fact_artist_snapshot/
            ├── snapshot_date=2026-08-31/
            └── snapshot_date=2026-09-01/
```

---

## 🏁 Day 6 Success Criteria & Definition of Done

1. ✅ `docs/gold_schema.md` and `docs/metric_definitions.md` published and frozen.
2. ✅ `docs/PLAN.md` updated reflecting the single-fact Kimball Star Schema architecture.
3. ✅ `src/transform/gold_transformer.py` implementing 4 dimensions and `fact_artist_snapshot`.
4. ✅ Surrogate key generation and smart integer `date_key` (`YYYYMMDD`) working reliably.
5. ✅ Historical windowing correctly computing `catalog_growth_pct` (with initial `NULL`) and bounded $[0, 100]$ `catalog_momentum_index`.
6. ✅ `scripts/run_gold.py` successfully populating `data/gold/` across historical snapshots `2026-08-31` and `2026-09-01`.
7. ✅ 100% green test suite in `tests/test_gold_transformer.py` verifying grain uniqueness, referential integrity, and metric bounds.
8. ✅ `data/gold/` synchronized to `s3://spotify-music-intelligence-luc/gold/`.
9. ✅ `docs/diary.md` and `docs/questions.md` updated with Day 6 deliverables, metrics, and interview deep-dives.

