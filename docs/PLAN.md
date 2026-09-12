# 🎵 Spotify Music Intelligence Platform — 10-Day Master Plan

### Cloud Data Engineering + Business Analytics Portfolio Project

---

## 🎯 Executive Summary & Objectives

The **Spotify Music Intelligence Platform** is an enterprise-grade cloud data platform that ingests Spotify catalog metadata, processes semi-structured JSON through a **Bronze / Silver / Gold (Medallion)** architecture, maintains historical snapshot tracking, builds a dimensional analytical star schema, and delivers interactive business intelligence through **Streamlit**.

### 💼 The Core Philosophy: 40% Data Engineering + 60% Business Analytics

- **Junior Data Engineer**: API ingestion, S3 Data Lake, PySpark transformations, Airflow orchestration, Data Quality gates, historical snapshots, and idempotency.
- **Junior Data Analyst**: Business KPI design, catalog momentum metrics, release trend analytics, dimensional SQL queries, and interactive Streamlit UI.
- **2026 API-Aware Design**: Built around verified current Spotify Web API endpoints (individual entity traversal, no reliance on deprecated popularity/followers).

---

## 🏗️ Master Architecture Flow

```text
┌────────────────────────┐
│    SPOTIFY WEB API     │ (OAuth 2.0 Client Credentials Flow)
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│   PYTHON EXTRACTION    │ (Pagination, rate-limiting, error handling)
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│     AWS S3 RAW         │ (Immutable Raw JSON partitioned by extracted_at=YYYY-MM-DD/)
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│     BRONZE LAYER       │ (PySpark: Schema validation, raw Parquet conversion)
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│     SILVER LAYER       │ (PySpark: Cleaned entities: silver_artists, silver_albums, silver_tracks)
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│   DATA QUALITY GATE    │ (PK uniqueness, FK referential integrity, null & date validation)
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│      GOLD LAYER        │ (Star Schema: dim_artist, dim_album, dim_track, fact_artist_snapshot)
└───────────┬────────────┘
            │
    ┌───────┴───────┐
    ▼               ▼
┌──────────────┐ ┌──────────────┐
│ ATHENA / SQL │ │ LOCAL DUCKDB │ (Zero-cost local testing & BI querying)
└───────┬──────┘ └──────┬───────┘
        │               │
        └───────┬───────┘
                ▼
┌────────────────────────┐
│  STREAMLIT ANALYTICS   │ (4-Page Interactive Intelligence App)
└────────────────────────┘
```

---

## 🗓️ 10-Day Execution Roadmap

| Day        | Phase                          | Core Deliverables                                                                       |  Priority  |
| :--------- | :----------------------------- | :-------------------------------------------------------------------------------------- | :--------: |
| **Day 1**  | **API Audit & Project Setup**  | ✅ **DONE** — Verified 2026 Spotify endpoints, live OAuth 2.0 flow, and schema contract | ⭐⭐⭐⭐⭐ |
| **Day 2**  | **Python Extraction Engine**   | ✅ **DONE** — `src/extract/` package (Token caching, 429 backoff, paginators, `data/raw/` JSON) | ⭐⭐⭐⭐⭐ |
| **Day 3**  | **AWS S3 Raw Data Lake**       | ✅ **DONE** — `src/storage/s3_uploader.py`, Hive partitioning, SSE-S3 AES-256, 8 unit tests |  ⭐⭐⭐⭐  |
| **Day 4**  | **Bronze Layer (PySpark)**     | ✅ **DONE** — `src/transform/bronze_transformer.py`, StructType schemas, Snappy Parquet, 82% compression | ⭐⭐⭐⭐⭐ |
| **Day 5**  | **Silver Layer & DQ Gate**     | ✅ **DONE** — `silver_transformer.py`, window deduplication, date normalization, DQ gate (13/13 pass) | ⭐⭐⭐⭐⭐ |
| **Day 6**  | **Gold Layer & Snapshots**     | ✅ **DONE** — Kimball Star Schema (4 dims, `fact_artist_snapshot`), native cadence windowing, 8/8 tests pass | ⭐⭐⭐⭐⭐ |
| **Day 7**  | **Business Analytics (SQL)**   | 10–15 core ANSI SQL queries (Catalog growth, release velocity, momentum)                | ⭐⭐⭐⭐⭐ |
| **Day 8**  | **Airflow Orchestration**      | End-to-end Airflow DAG, retries, sensors, idempotent incremental logic                  | ⭐⭐⭐⭐⭐ |
| **Day 9**  | **Streamlit Intelligence App** | 4-page interactive UI (Executive Overview, Artist 360, Catalog Trends)                  | ⭐⭐⭐⭐⭐ |
| **Day 10** | **CI/CD, Testing & Portfolio** | Docker, unit/integration tests, GitHub Actions, recruiter-ready README                  | ⭐⭐⭐⭐⭐ |

---

## 📋 Detailed Day-by-Day Specifications

### 🗓️ Day 1 — API Research + Project Setup
- **Goal**: Know exactly what data Spotify can give us before writing pipeline code.
- **Tasks**:
  1. Create/verify Spotify Developer account & app in Development Mode.
  2. Test OAuth 2.0 Client Credentials token generation.
  3. Inspect live responses for Artist, Album, and Track endpoints.
  4. Establish dataset scope (e.g. 8-artist curated cohort across genres & eras).
- **Deliverables**: ✅ `.env.example`, `requirements.txt`, `docs/spotify_api.md`, `docs/business_requirements.md`, `scripts/test_spotify_auth.py`.

---

### 🗓️ Day 2 — Python Extraction Engine
- **Goal**: Build a production-grade, modular, and resilient Spotify API extraction package.
- **Tasks**:
  1. Build `src/extract/spotify_client.py` with OAuth 2.0 token caching ($3,600\text{s}$ buffer), HTTP session pooling, 429 `Retry-After` exponential jitter, and circuit breaking.
  2. Implement `artist_extractor.py` (canonical entity & high-res image resolver).
  3. Implement `album_extractor.py` (discography paginator with `limit=10` & deduplication).
  4. Implement `track_extractor.py` (simplified track paginator with `limit=10` & FK lineage).
  5. Build `main.py` master orchestrator injecting `snapshot_date` temporal audit watermarks.
- **Deliverables**: ✅ Working `src/extract/` package, local immutable JSON files in `data/raw/artists/`, `data/raw/albums/`, `data/raw/tracks/`, unit tests in `tests/test_extraction.py`.

---

### 🗓️ Day 3 — S3 Raw Data Lake

- **Goal**: Move extraction into cloud storage.
- **Tasks**:
  1. Configure S3 bucket `s3://spotify-music-intelligence/`.
  2. Partition raw payloads: `raw/extracted_at=YYYY-MM-DD/artists/`, `albums/`, `tracks/`.
  3. Attach metadata: `source`, `endpoint`, `extracted_at`.
- **Deliverable**: Working `Spotify API → Python → AWS S3` ingestion pipeline.

---

### 🗓️ Day 4 — Bronze Layer with PySpark

- **Goal**: Turn raw semi-structured JSON into structured Parquet.
- **Tasks**:
  1. Define and enforce PySpark schemas.
  2. Flatten nested JSON arrays without loss of primary identifiers.
  3. Write Snappy-compressed Parquet files to `bronze/artists/`, `bronze/albums/`, `bronze/tracks/`.

---

### 🗓️ Day 5 — Silver Layer + Automated Data Quality Gate

- **Goal**: Create clean analytical entities with verifiable data quality.
- **Tasks**:
  1. Deduplicate records, normalize date strings (ISO-8601), handle null values.
  2. Build automated DQ validation framework:
     - **Primary Key Uniqueness**: `artist_id`, `album_id`, `track_id`.
     - **Referential Integrity**: Track $\rightarrow$ Album FK, Album $\rightarrow$ Artist FK.
     - **Bounds Validation**: Release dates $\le$ extraction date.
- **Deliverable**: `silver_artists`, `silver_albums`, `silver_tracks` + DQ execution report.

---

### 🗓️ Day 6 — Gold Dimensional Model & Historical Snapshots (Kimball Star Schema)

- **Goal**: Transform trusted Silver entities into an analytical Kimball Star Schema optimized for OLAP query engines (Athena/DuckDB) and dashboard visualization.
- **Frozen Architecture**:
  - **Dimensions (Conformed)**:
    - `dim_artist`: Unique artist profiles, genres, Spotify URIs, surrogate PK `artist_key`.
    - `dim_album`: Discography metadata, release date hierarchies, surrogate PK `album_key`.
    - `dim_track`: Track audio metadata, duration, explicit flags, surrogate PK `track_key`.
    - `dim_date`: Gregorian calendar dimension (1950–2030) with smart integer PK `date_key` (`YYYYMMDD`).
  - **Periodic Snapshot Fact**:
    - `fact_artist_snapshot`: Grain `(artist_key, date_key)` capturing state, cadence, and momentum at uniform extraction points.
- **Key DE Concepts & Rules**:
  - **Semi-Additive Counters**: `total_albums`, `total_tracks`, `total_singles`, `recent_releases_12m`.
  - **Non-Additive Metrics**: `median_release_cadence_days` (statistical summary), `catalog_growth_pct` (rate), `catalog_momentum_index` (score).
  - **First-Snapshot Growth Rule**: `catalog_growth_pct = NULL` on initial baseline (not `0%`).
  - **Bounded Momentum Index**: Calibrated composite score $[0.0, 100.0]$ measuring publishing velocity and pacing.
  - **Dynamic Partition Overwrite**: Safe, idempotent appends to `data/gold/fact_artist_snapshot/snapshot_date=YYYY-MM-DD/`.
- **Deliverables**: `docs/gold_schema.md`, `docs/metric_definitions.md`, `src/transform/gold_transformer.py`, `scripts/run_gold.py`, `tests/test_gold_transformer.py`, `data/gold/`, and S3 sync.

---

### 🗓️ Day 7 — Business Analytics & SQL Marts ✅ (COMPLETED)

- **Goal**: Formulate high-value commercial SQL queries answering executive questions and build curated analytical data marts.
- **Completed Components**:
  1. **In-Process Semantic Layer**: `sql/setup_gold_views.sql` (5 Gold DuckDB views with zero copy overhead).
  2. **8 Commercial Analytics Queries**: `sql/analytics_queries.sql` + `scripts/run_analytics.py`:
     - §1: Fact Sanity & Referential Integrity Audit (0 orphan keys).
     - §2: Activity & Production Mix (Q1: Rolling 12m Volume, Q2: Single-to-Album Strategy).
     - §3: Catalog Growth Dynamics (Q3: Growth Velocity %, Q4: Net Track Additions).
     - §4: Release Seasonality (Q5: Monthly Format Distribution, Q6: Artist Peak Drop Months).
     - §5: Momentum Trajectory (Q7: Momentum Leaderboard & Tiers, Q8: Snapshot-over-Snapshot Delta).
  3. **4 Curated Data Marts**: `sql/marts/*.sql` + `sql/setup_marts.sql` + `scripts/test_marts.py`:
     - `mart_artist_activity` (powers Streamlit Page 1 & Page 4).
     - `mart_catalog_growth` (powers Streamlit Page 1 & Page 2).
     - `mart_release_seasonality` (powers Streamlit Page 4).
     - `mart_artist_momentum` (powers Streamlit Page 1 & Page 2).
  4. **Executive Documentation**: `docs/day7_business_questions.md`.
- **Note on BTS Ingestion**: ✅ **Completed!** BTS backfill for `2026-09-01` completed successfully. All 8 artists are now 100% ingested and verified across Raw, Bronze, Silver, Gold, and DuckDB analytical marts.

---

### 🗓️ Day 8 — Airflow Orchestration & Incremental Loading ✅ COMPLETED

- **Goal**: Orchestrate the entire end-to-end pipeline with Apache Airflow.
- **Tasks**:
  1. Build `dags/spotify_etl_dag.py` with TaskGroups, S3 sensors, and PySpark operators. (✅ Completed & AST Verified)
  2. Implement idempotent watermark manager (`state/watermarks.json` + `snapshot_date`). (✅ Completed & Atomically Verified)
  3. Test incremental execution: Verify zero duplicate records on back-to-back runs. (✅ Completed: 5/5 Invariants Passed)

---

### 🗓️ Day 9 — Streamlit Intelligence Application

- **Goal**: Deliver a portfolio-grade interactive data app.
- **Pages**:
  - **Page 1: Executive Overview**: High-level catalog KPIs, release velocity, top active artists.
  - **Page 2: Artist 360 & Momentum**: Deep-dive artist selector, catalog growth timeline, momentum score.
  - **Page 3: Album & Track Analytics**: Track list breakdowns, duration distributions, explicit flags.
  - **Page 4: Catalog Trends & Patterns**: Album vs single release evolution, yearly cadence.

---

### 🗓️ Day 10 — Production Polish, CI/CD & Documentation

- **Goal**: Package and document the platform for hiring managers.
- **Tasks**:
  1. Unit and integration tests with `pytest`.
  2. Containerize with `Dockerfile` and `docker-compose.yml`.
  3. Set up GitHub Actions CI workflow (linting, test execution).
  4. Deploy Streamlit app to Community Cloud.
  5. Write recruiter-ready `README.md` with architecture diagrams, data models, and business insights.

---

## 🚫 What NOT to Do (Anti-Patterns to Avoid)

- ❌ **No Over-Engineering Infrastructure**: Don't waste days on Kubernetes, complex Terraform, or VPC peering.
- ❌ **No Relying on Deprecated Fields**: Never depend on removed 2026 fields (popularity/followers).
- ❌ **No Giant Monolithic ETL**: Don't perform data transformations inside Streamlit; adhere strictly to `Pipeline → S3/Gold → Streamlit`.
