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
| **Day 2**  | **Python Extraction Engine**   | `spotify_client.py`, artist/album/track extractors, local raw JSON                      | ⭐⭐⭐⭐⭐ |
| **Day 3**  | **AWS S3 Raw Data Lake**       | S3 bucket hierarchy, immutable raw partitioned storage (`extracted_at=...`)             |  ⭐⭐⭐⭐  |
| **Day 4**  | **Bronze Layer (PySpark)**     | Convert Raw JSON $\rightarrow$ structured Bronze Parquet, schema flattening             | ⭐⭐⭐⭐⭐ |
| **Day 5**  | **Silver Layer & DQ Gate**     | Cleaning, deduplication, automated Data Quality validation checks                       | ⭐⭐⭐⭐⭐ |
| **Day 6**  | **Gold Layer & Snapshots**     | Star Schema dimensions & `fact_artist_snapshot` historical tracking                     | ⭐⭐⭐⭐⭐ |
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
  4. Establish dataset scope (e.g. 100–300 artists $\rightarrow$ their albums $\rightarrow$ album tracks).
- **Deliverables**: `.env.example`, `requirements.txt`, `docs/spotify_api.md`, `docs/business_requirements.md`.

---

### 🗓️ Day 2 — Python Extraction Engine

- **Goal**: Build a clean, modular, and reusable Spotify API client.
- **Tasks**:
  1. Create `src/extract/spotify_client.py` with exponential backoff & rate-limit handling.
  2. Implement `extract_artists.py`, `extract_albums.py`, and `extract_tracks.py`.
  3. Handle pagination (`offset` / `limit`), error logging, and extraction timestamps.
- **Output**: Local JSON structure in `data/raw/`.

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

### 🗓️ Day 6 — Gold Dimensional Model & Historical Snapshots

- **Goal**: Build the analytical data warehouse star schema.
- **Architecture**:
  - **Dimensions**: `dim_artist`, `dim_album`, `dim_track`, `dim_date`.
  - **Facts (Snapshots)**: `fact_artist_snapshot`, `fact_album_snapshot`, `fact_track_snapshot`.
- **Key DE Concept**: `snapshot_date` tracking to measure catalog growth, release frequency, and artist trajectory over time.

---

### 🗓️ Day 7 — Business Analytics & SQL Marts

- **Goal**: Formulate high-value commercial SQL queries answering executive questions:
  1. **Top Active Artists**: Who released the most albums/singles over the last 12 months?
  2. **Catalog Growth Velocity**: Which artists expanded their track catalog the fastest?
  3. **Release Seasonality**: Monthly distribution of album vs single drops.
  4. **Artist Catalog Momentum Index**: Composite weighted score (`40% Catalog Growth + 30% Recent Release Activity + 30% Release Frequency`).
- **Deliverable**: `sql/analytics_queries.sql` (10–15 optimized Athena/DuckDB queries).

---

### 🗓️ Day 8 — Airflow Orchestration & Incremental Loading

- **Goal**: Orchestrate the entire end-to-end pipeline with Apache Airflow.
- **Tasks**:
  1. Build `dags/spotify_etl_dag.py` with TaskGroups, S3 sensors, and PySpark operators.
  2. Implement idempotent watermark manager (`state/watermarks.json` + `snapshot_date`).
  3. Test incremental execution: Verify zero duplicate records on back-to-back runs.

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
