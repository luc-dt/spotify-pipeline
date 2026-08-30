# 🎵 Spotify Music Intelligence Platform
### Cloud Data Engineering + Business Analytics (Medallion Lakehouse)

An end-to-end cloud data platform that extracts Spotify catalog metadata via OAuth 2.0, ingests semi-structured JSON into an immutable **AWS S3 Data Lake**, transforms data through **Bronze, Silver, and Gold (Medallion)** layers using **Apache Spark (PySpark)**, maintains historical snapshots for catalog growth and release cadence, enforces automated **Data Quality gates**, orchestrates workflows with **Apache Airflow**, and serves interactive analytics through **Streamlit**.

---

# Introduction & Goals

Music streaming catalogs evolve continuously with daily releases, shifting release formats (albums vs. singles), and varying release cadences. The **Spotify Music Intelligence Platform** transforms raw catalog metadata into a Kimball-style dimensional data warehouse that answers mission-critical business questions for record labels, A&R scouts, and music industry analysts.

### 🎯 Key Business Questions Answered:
1. **Catalog Productivity & Output:** Which artists maintain the most consistent release cadence, and who is slowing down?
2. **Catalog Momentum & Trajectory:** Which artists are experiencing accelerating catalog activity (measured by recent release density vs. career output)?
3. **Format Strategy Shifts:** How has the industry-wide and artist-specific distribution of Singles vs. Studio Albums evolved over time?
4. **Content Profiling:** How have track lengths (`avg_duration_min`) and explicit content ratios shifted across release decades?
5. **Historical Growth Velocity:** How much did an artist's catalog expand month-over-month (`catalog_growth_pct`) across historical snapshot dates?

### 🛠️ Core Engineering Goals:
- **Goal 1: Ingestion & Cloud Lakehouse**: Extract catalog metadata across artists, albums, and tracks using Spotify's OAuth 2.0 Web API into an immutable S3 Bronze data lake partitioned by `extracted_at=YYYY-MM-DD/`.
- **Goal 2: Distributed PySpark Transformations**: Clean, flatten, and conform nested JSON into optimized Snappy Parquet tables across Silver dimensions (`dim_artist`, `dim_album`, `dim_track`).
- **Goal 3: Historical Snapshots & Momentum Analytics**: Implement periodic snapshot tracking (`snapshot_date`) to compute career trajectories, catalog growth velocity, and a project-defined **Catalog Momentum Index**.
- **Goal 4: Automated Quality & Orchestration**: Enforce fail-fast Data Quality gates (PK uniqueness, referential integrity, ANSI date bounds) and orchestrate end-to-end runs with Apache Airflow.
- **Goal 5: Interactive BI Serving**: Deliver an interactive Streamlit intelligence portal with multi-dimensional filtering across artists, formats, and historical timelines.

## Why This Matters (The 2026 Engineering Perspective)

In February 2026, Spotify restructured its Web API: removing deprecated popularity scores, follower counts, and bulk endpoints. Rather than relying on black-box popularity numbers, this platform builds **reproducible, observable business metrics** derived directly from observable catalog behavior: **Catalog Growth % + Recent Releases + Median Release Cadence**.

---

# Contents

- [Introduction & Goals](#introduction--goals)
- [Architecture Flow](#architecture-flow)
- [The Data Set & Scale](#the-data-set--scale)
- [Used Tools & Tech Stack](#used-tools--tech-stack)
- [Constraints & Architectural Design Decisions](#constraints--architectural-design-decisions)
  - [Scale Rationale: Why PySpark at ~15K Tracks?](#scale-rationale-why-pyspark-at-15k-tracks)
  - [Dual-Execution Compute Model (Local vs. Cloud)](#dual-execution-compute-model-local-vs-cloud)
- [Pipelines & Medallion Flow](#pipelines--medallion-flow)
- [Data Flow & Field-Level Lineage](#data-flow--field-level-lineage)
- [Data Quality & Kimball Dimensional Modeling](#data-quality--kimball-dimensional-modeling)
- [Catalog Momentum Index Methodology](#catalog-momentum-index-methodology)
- [Data Resilience, Error Handling & Failure Modes](#data-resilience-error-handling--failure-modes)
- [Demo & Visual Evidence](#demo--visual-evidence)
- [Results & Execution Evidence](#results--execution-evidence)
- [Quickstart & Reproducibility](#quickstart--reproducibility)
- [Known Limitations & Operational Scope](#known-limitations--operational-scope)
- [Conclusion & Roadmap](#conclusion--roadmap)
- [Repository Structure](#repository-structure)
- [Appendix & References](#appendix--references)

---

# Architecture Flow

```text
┌───────────────────────────────────────────────────────────────────────────────────┐
│                                 SPOTIFY WEB API                                   │
│                  https://api.spotify.com/v1 (OAuth 2.0 Token Auth)                │
└──────────────────────────────────────────┬────────────────────────────────────────┘
                                           │ HTTP GET (Automated 429 Retry + Exponential Backoff)
                                           ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│                                 APACHE AIRFLOW                                    │
│                            spotify_etl_dag.py (@daily)                            │
│                                                                                   │
│  TaskGroup: extract_data            TaskGroup: transform_medallion                │
│  ┌──────────────────────┐          ┌──────────────────────────────────────────┐   │
│  │python_extract_task   │ ───────► │check_s3_sensor ──► pyspark_bronze_silver │   │
│  └──────────────────────┘          │                          │               │   │
│                                    │                          ▼               │   │
│                                    │                  pyspark_dq_gate         │   │
│                                    │                          │               │   │
│                                    │                          ▼               │   │
│                                    │                 pyspark_silver_gold      │   │
│                                    └──────────────────────────────────────────┘   │
└──────────────────────────────────────────┬────────────────────────────────────────┘
                                           │
        ┌──────────────────────────────────┴───────────────────────────────────┐
        ▼                                                                      ▼
┌──────────────────────────────┐                        ┌─────────────────────────────────┐
│   PYTHON INGESTION ENGINE    │                        │   DISTRIBUTED COMPUTE ENGINE    │
│   src/extract/               │                        │   Local Spark Container / Glue  │
│                              │                        │                                 │
│  • OAuth 2.0 token manager   │                        │  • Job 1: Bronze -> Silver      │
│  • Paginator (limit=10)      │                        │    (Flatten, Clean, Parquet)    │
│  • Dead-letter quarantine    │                        │  • Job 2: Data Quality Gate     │
│  • Partitioned JSON output   │                        │    (Fail-fast assertions)       │
└──────────────┬───────────────┘                        │  • Job 3: Silver -> Gold        │
               │                                        │    (Snapshots & Star Schema)    │
               │                                        └────────────────┬────────────────┘
               ▼                                                         │
┌────────────────────────────────────────────────────────────────────────┴────────────────┐
│                                   AMAZON S3 LAKEHOUSE                                   │
│                          bucket: spotify-music-intelligence-luc                         │
│                                                                                         │
│   🥉 BRONZE LAYER                     🥈 SILVER LAYER               🥇 GOLD LAYER        │
│   raw/extracted_at=YYYY-MM-DD/       silver/                       gold/                │
│   ├── artists.json                   ├── dim_artist/ (Parquet)     ├── fact_artist_     │
│   ├── albums.json                    ├── dim_album/  (Parquet)     │   snapshot/        │
│   ├── tracks.json                    └── dim_track/  (Parquet)     └── fact_catalog_    │
│   └── quarantine/ (Malformed)                                          trends/          │
└──────────────────────────────────────────────────┬──────────────────────────────────────┘
                                                   │
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                             ANALYTICS & SERVING LAYER                                   │
│                                                                                         │
│   📊 ANALYTICAL QUERY ENGINE             🖥️ STREAMLIT INTELLIGENCE PORTAL               │
│   • Amazon Athena (Cloud Serverless)     • Page 1: Executive Overview                   │
│   • DuckDB SQL (Local Zero-Cost)         • Page 2: Artist 360 & Momentum Index          │
│   • Sub-second analytical scans          • Page 3: Album & Track Analytics              │
│   • Kimball Star Schema Joins            • Page 4: Long-Term Music Release Trends       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# The Data Set & Scale

**Source:** [Spotify Web API (2026 Specification)](https://developer.spotify.com/documentation/web-api)  
**Ingestion Format:** Raw Partitioned JSON (Bronze) $\rightarrow$ Snappy Columnar Parquet (Silver & Gold)  
**Target Entities:** Multi-genre global artist catalog spanning diverse decades, release volumes, and career spans.

### 📐 Target Volume & Ingestion Scope
- **Seed Artists:** ~100 curated global artists across diverse genres (Pop, Hip-Hop, Rock, Electronic, Classical, Jazz) and career eras (legacy acts vs. modern breakout artists).
- **Volume Expectations:** ~1,500+ albums/singles, ~15,000+ individual tracks.
- **Snapshot Cadence:** Daily incremental extractions partitioned by `extracted_at=YYYY-MM-DD/`.
- **Historical Depth:** Configurable multi-snapshot timeline enabling longitudinal comparison across weeks, quarters, and years.

| Entity | Primary Attributes | Business Usage |
| :--- | :--- | :--- |
| **Artist** | `artist_id`, `artist_name`, `spotify_uri`, `image_url` | Master dimension for catalog attribution |
| **Album** | `album_id`, `album_name`, `album_type`, `release_date`, `total_tracks` | Release format analysis & release cadence |
| **Track** | `track_id`, `track_name`, `duration_ms`, `explicit`, `track_number` | Song length duration trends & content profiling |

---

# Used Tools & Tech Stack

### 🛠️ Architecture Stack Summary

| Layer | Technology | Primary Role & Justification |
| :--- | :--- | :--- |
| **Ingestion** | Spotify Web API + OAuth 2.0 | Source of truth with entity-level REST pagination & token management |
| **Data Lake** | Amazon S3 | Immutable Bronze/Silver/Gold tiered lakehouse storage |
| **Distributed Compute**| Apache Spark (PySpark) | Horizontally scalable ETL, array flattening & Parquet encoding |
| **Orchestration** | Apache Airflow | DAG dependency management, S3 state sensors, and automated retries |
| **Analytical Querying**| Amazon Athena + DuckDB | Low-latency columnar SQL queries over partitioned Parquet (Cloud & Local) |
| **Serving & BI** | Streamlit | 4-page interactive business intelligence web portal |

### Layer Details
- **Connect & Ingest:** Python 3.10+ custom client with OAuth 2.0 token caching and automatic `429 Rate Limit` backoff.
- **Buffer & Lakehouse Storage:** Amazon S3 (`spotify-music-intelligence-luc`) separating raw nested payloads from conformed analytics.
- **Transformations & Compute:** PySpark for schema enforcement, timestamp normalization, deduplication, and window-based metric calculation.
- **Orchestration:** Airflow DAG (`spotify_etl_dag.py`) executing extraction, polling S3 readiness via `S3KeySensor`, and running Spark stages with automated retries.
- **Serving & Visualization:** Streamlit application providing interactive filtering, visual distribution charts, and drill-down capabilities.

---

# Constraints & Architectural Design Decisions

### Scale Rationale: Why PySpark at ~15K Tracks?
At ~100 seed artists and ~15,000 tracks, the entire dataset can comfortably fit inside memory using Pandas or DuckDB. However, **PySpark was intentionally chosen as an architectural pattern**:
1. **Production Portability:** The exact same transformation logic (array flattening, windowed lag differences, and partition writes) scales from 15K tracks to Spotify's full 100M+ track catalog without re-architecting code.
2. **Schema Enforcement & Columnar Encoding:** Demonstrates distributed schema projection, predicate pushdown, and Snappy compression that enterprise data lakes require.
3. **Engine Decoupling:** Isolates compute from storage, allowing the pipeline to execute locally in lightweight containers or scale out to managed clusters (AWS Glue / EMR).

### Dual-Execution Compute Model (Local vs. Cloud)
To balance cloud cost efficiency with enterprise cloud-native patterns, the platform supports two interchangeable execution environments:
- **Local Development / Free-Tier Mode:** Runs local PySpark in Docker containers and queries Gold tables via **DuckDB**, enabling 100% free offline development, CI testing, and evaluation.
- **Cloud Production Mode:** The exact same PySpark scripts execute on **AWS Glue Serverless Spark**, storing tables in S3 and querying via **Amazon Athena**.

---

# Pipelines & Medallion Flow

1. **Extraction (Python / Airflow):** Authenticates via OAuth 2.0, queries artists $\rightarrow$ albums $\rightarrow$ tracks with automatic pagination (`limit=10`), and writes partitioned JSON to S3 (`raw/extracted_at=YYYY-MM-DD/`).
2. **Bronze $\rightarrow$ Silver (PySpark):** Reads raw JSON, flattens structures, standardizes ANSI dates (`YYYY-MM-DD`), enforces primary keys, removes duplicates, and saves clean Parquet tables (`dim_artist`, `dim_album`, `dim_track`).
3. **Data Quality Gatekeeper:** Validates primary key uniqueness, referential integrity (Track $\rightarrow$ Album FK, Album $\rightarrow$ Artist FK), and date validity before allowing downstream promotion.
4. **Silver $\rightarrow$ Gold (PySpark):** 
   - Generates `fact_artist_snapshot` with historical tracking (`snapshot_date`), computing catalog size, delta growth %, release cadence, and the **Catalog Momentum Index**.
   - Generates `fact_catalog_trends` aggregated by release period and format.
5. **Serving:** Amazon Athena and DuckDB execute analytical SQL queries to power the Streamlit UI.

---

# Data Flow & Field-Level Lineage

The platform systematically transforms unstructured and nested JSON payloads into high-performance dimensional star schema tables:

| Source Field | Bronze (Raw JSON) | Silver (Conformed Parquet) | Gold (Analytical Marts) | Business Metric / Transformation |
| :--- | :--- | :--- | :--- | :--- |
| `release_date` | `"2023"` or `"2023-05-12"` (String) | `DATE` ANSI normalized (`2023-01-01` or `2023-05-12`) | Windowed lag difference (`release_cadence_days`) | Measures consistency and release intervals |
| `album_type` | `"single"`, `"album"`, `"compilation"` | Cleaned uppercase/lowercase enum | Grouped distribution ratios in `fact_catalog_trends` | Tracks single vs. album strategic pivots |
| `duration_ms` | `214500` (Integer) | `duration_ms` (Int) + `duration_min` (Float) | `avg_track_duration_min` aggregated per artist/year | Analyzes song duration trends across eras |
| `explicit` | `true` / `false` (Boolean) | `is_explicit` (Boolean 0/1) | `explicit_track_ratio` | Content profiling and catalog classification |
| `id` (Artist/Album) | Nested JSON string | Primary Keys (`artist_id`, `album_id`, `track_id`) | Grain key `(artist_id, snapshot_date)` | Enforces referential integrity across all layers |

---

# Data Quality & Kimball Dimensional Modeling

### Kimball Star Schema Design

```text
                  ┌───────────────────────────────┐
                  │       dim_artist (Silver)     │
                  │ PK: artist_id                 │
                  │ artist_name, spotify_uri      │
                  └───────────────┬───────────────┘
                                  │ 1
                                  │
                                  ├──────────────────────────────┐
                                  │ N                            │ N
                  ┌───────────────▼───────────────┐   ┌──────────▼────────────────────┐
                  │       dim_album (Silver)      │   │       dim_track (Silver)      │
                  │ PK: album_id                  │   │ PK: track_id                  │
                  │ FK: artist_id                 │   │ FK: album_id, FK: artist_id   │
                  │ album_name, type, release_date│   │ track_name, duration_ms, expl │
                  └───────────────┬───────────────┘   └──────────┬────────────────────┘
                                  │                              │
                                  │  PySpark Silver-to-Gold      │
                                  ▼                              ▼
  ┌────────────────────────────────────────────────────────┐   ┌──────────────────────────────────────────────┐
  │   fact_artist_snapshot (Gold Periodic Snapshot)       │   │      fact_catalog_trends (Gold Trend Mart)   │
  ├────────────────────────────────────────────────────────┤   ├──────────────────────────────────────────────┤
  │ PK: (artist_id, snapshot_date)                         │   │ PK: (release_year, album_type, snapshot_date)│
  │ FK: artist_id                                          │   │ total_releases, total_tracks                 │
  │ total_albums, total_tracks, total_singles              │   │ avg_track_duration_min, explicit_track_ratio │
  │ recent_releases_12m, median_release_cadence_days       │   │ single_to_album_ratio                        │
  │ catalog_growth_pct, catalog_momentum_index             │   │ snapshot_date                                │
  └────────────────────────────────────────────────────────┘   └──────────────────────────────────────────────┘
```

### Automated Data Quality Gate
- **Uniqueness Check**: Assert zero duplicate primary keys on `artist_id`, `album_id`, `track_id`.
- **Referential Integrity**: Assert 100% of tracks map to a valid `album_id` and `artist_id`.
- **Date Range Validation**: Assert `release_date <= snapshot_date` and valid Gregorian timestamps.
- **Null Completeness**: Assert zero nulls on critical dimensional fields (`artist_name`, `album_name`, `track_name`).

---

# Catalog Momentum Index Methodology

Following Spotify's 2026 API restructuring (which deprecated popularity metrics and follower counts), this platform introduces an empirical, reproducible **Catalog Momentum Index** designed for A&R scouting and catalog valuation.

### 📐 Mathematical Formulation

$$
\text{Momentum Index} = \left( \frac{\text{Releases}_{\text{Last 12M}}}{\max(\text{Total Releases}, 1)} \times 40 \right) + \left( \min(\text{Catalog Growth \%}, 100) \times 0.35 \right) + \left( \frac{365}{\max(\text{Median Cadence Days}, 30)} \times 25 \right)
$$

### 🔍 Component Breakdown & Weighting
1. **Recent Release Velocity (40% Weight):** Proportion of the artist's total catalog published within the trailing 12 months. Identifies active release cycles.
2. **Net Catalog Expansion (35% Weight):** Month-over-month catalog growth percentage captured between historical snapshot runs (`snapshot_date`).
3. **Release Cadence Consistency (25% Weight):** Inverse of median release interval (in days), normalized to an annual cycle. Higher scores reward consistent release schedules over sporadic drops.

---

# Data Resilience, Error Handling & Failure Modes

Production pipelines must fail gracefully and recover predictably. The platform implements specific mitigation strategies across all layers:

| Failure Scenario | Detection Mechanism | Mitigation & Recovery Strategy |
| :--- | :--- | :--- |
| **API Rate Limiting (429)** | HTTP Status `429` & `Retry-After` Header | Extraction client pauses dynamically with exponential backoff and randomized jitter. |
| **Schema Drift / Missing Keys** | Pydantic model & PySpark schema validation | Malformed records are diverted to `raw/quarantine/extracted_at=YYYY-MM-DD/` rather than failing the extraction batch. |
| **Mid-Flight Task Crash** | Airflow task failure & S3 staging checks | Atomic partition overwrites (`mode("overwrite")` on `snapshot_date=...`) prevent dirty partial writes. |
| **Corrupted Data / DQ Breach** | Fail-fast assertions in `pyspark_dq_gate` | Halts downstream Gold table generation, preserving existing clean historical snapshots and raising task alerts. |
| **Network Timeout on S3 Upload**| `boto3` / PySpark retry policy | 3 automatic exponential retries at task level before raising Airflow task failure. |
| **Historical Backfill Re-runs** | Execution date parameterized `{{ ds }}` | Idempotent partition overwrite ensures re-running previous dates produces identical, duplicate-free state. |

---

# Demo & Visual Evidence

*(This section will be populated with interactive Streamlit portal screenshots, UI navigation GIFs, and query execution walkthroughs upon completion of Days 8–10).*

---

# Results & Execution Evidence

*(This section will document execution row counts, PySpark execution timings, Parquet compression metrics, and sample SQL output tables upon Gold layer deployment).*

- **Storage Efficiency:** Estimated ~70–80% storage footprint reduction when converting raw JSON to Snappy Parquet (consistent with columnar compression benchmarks; empirical measurements will be recorded upon full extraction runs).
- **Query Latency:** Columnar partitioning and projection designed for sub-second analytical scans in Athena and DuckDB.

---

# Quickstart & Reproducibility

### 1. Prerequisites
- Python 3.10+
- Spotify Developer Account (Client ID & Secret from [developer.spotify.com](https://developer.spotify.com))
- Docker & Docker Compose (Required for Airflow orchestration in Day 8)
- AWS Credentials with S3/Glue access (Optional: local DuckDB fallback supported)

### 2. Local Setup & Verification (Day 1 Scope)

```bash
# 1. Clone repository
git clone https://github.com/luc-dt/end-to-end-spotify-pipeline.git
cd end-to-end-spotify-pipeline

# 2. Virtual Environment Setup
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

# 3. Install Dependencies
pip install -r requirements.txt

# 4. Environment Configuration
cp .env.example .env
# Edit .env and supply your SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET

# 5. Run Day 1 Authentication & Verification Test
python scripts/test_spotify_auth.py

# 6. Run Ingestion Engine (Extracts sample catalog)
python -m src.extract.main
```

### 3. Full Pipeline Setup (Production Orchestration Scope)

> [!NOTE]
> Full automated orchestration (Airflow DAG + PySpark Medallion Jobs + Streamlit BI Portal) will be incrementally unlocked as roadmap stages 2–10 are completed. See the [Roadmap](#conclusion--roadmap) below.

```bash
# Launch full local stack (Airflow + Postgres + Streamlit)
docker-compose up -d

# Trigger End-to-End Dag
# Access Airflow UI at http://localhost:8080 (airflow/airflow)
# Access Streamlit Intelligence Portal at http://localhost:8501
```

---

# Known Limitations & Operational Scope

- **Spotify 2026 API Scope:** Popularity and follower fields were deprecated by Spotify in February 2026; catalog momentum is calculated from verifiable, observable catalog metadata.
- **Rate Limit Resilience:** Spotify Developer Mode enforces strict rate limits; the custom extraction client includes exponential backoff listening to `429 Retry-After` response headers.
- **Idempotency & Replayability:** Re-running extractions for the same `snapshot_date` performs an idempotent overwrite on S3 partitions to prevent duplicate historical entries.
- **Historical Horizon:** Ingestion starts from historical release dates present in metadata; periodic snapshots accumulate daily/weekly forward history.

---

# Conclusion & Roadmap

### 10-Day Master Roadmap Status:
- [x] **Day 1: API Audit & Project Setup** (OAuth 2.0 verification, schema contract & BRD)
- [ ] **Day 2: Python Extraction Engine** (Modular extractors in `src/extract/`)
- [ ] **Day 3: AWS S3 Raw Data Lake** (Partitioned cloud storage `extracted_at=...`)
- [ ] **Day 4: Bronze Layer (PySpark)** (Raw JSON $\rightarrow$ structured Parquet)
- [ ] **Day 5: Silver Layer & Data Quality** (Deduplication, cleaning & automated assertions)
- [ ] **Day 6: Gold Layer & Snapshots** (Star Schema & `fact_artist_snapshot`)
- [ ] **Day 7: Business Analytics (SQL)** (10–15 analytical business queries)
- [ ] **Day 8: Airflow Orchestration** (End-to-end DAG & incremental loading)
- [ ] **Day 9: Streamlit Intelligence App** (4-page interactive UI)
- [ ] **Day 10: Production Polish & CI/CD** (Docker, pytest suite & GitHub Actions)

---

# Repository Structure

```text
end-to-end-spotify-pipeline/
├── src/
│   ├── extract/                 # API client, artist/album/track extractors
│   │   ├── spotify_client.py
│   │   ├── artist_extractor.py
│   │   ├── album_extractor.py
│   │   ├── track_extractor.py
│   │   └── main.py
│   ├── transform/               # PySpark Medallion ETL (Bronze -> Silver -> Gold)
│   └── utils/                   # Logging, configuration, and helpers
├── dags/                        # Apache Airflow DAG definitions
├── data/raw/                    # Local raw partitioned JSON (Bronze)
├── sql/                         # Analytical business SQL queries
├── streamlit/                   # Streamlit intelligence application
├── tests/                       # Automated pytest & data quality test suite
├── docs/                        # BRD, API specifications, and roadmap
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment configuration template
└── README.md
```

---

# Appendix & References

- [Spotify Web API Documentation](https://developer.spotify.com/documentation/web-api)
- [Spotify February 2026 API Migration Guide](https://developer.spotify.com/documentation/web-api/tutorials/february-2026-migration-guide)
- [Medallion Architecture Standard](https://www.databricks.com/glossary/medallion-architecture)
- [Kimball Dimensional Modeling Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)
