# 💼 Spotify Music Intelligence Platform — Business Requirements Document (BRD)

---

## 1. Problem Statement

The music industry changes rapidly. Record labels, A&R teams, and music analysts need reliable catalog intelligence to monitor **artist productivity, release frequency, catalog growth, and historical release patterns**.

The **Spotify Music Intelligence Platform** will transform Spotify catalog metadata into a structured analytical dataset that enables users to analyze artist discographies, release activity, catalog growth, and long-term music trends.

---

# 2. Business Objectives

## 1. Catalog Productivity Tracking

Measure artist output across:
* Albums
* Singles
* Compilations
* Tracks

Key metrics include:
* Total releases
* Total tracks
* Recent release count
* Release cadence
* Catalog size

---

## 2. Catalog Momentum Index

Construct a **project-defined analytical metric** based on observable catalog activity:

$$
\text{Catalog Momentum Index}
=
f(\text{Catalog Growth},\text{Recent Release Activity},\text{Release Cadence})
$$

The index will be used to identify artists whose catalog activity is increasing over time.

> **Important:** The Catalog Momentum Index is a project-defined analytical metric and is **not a Spotify metric**.

The exact methodology and weighting will be documented and reproducible.

---

## 3. Release Lifecycle Analytics

Track how artist discographies evolve through **historical snapshots** using:

```text
snapshot_date
```

This enables comparison between:

```text
Historical Catalog State
          ↓
Current Catalog State
```

and supports analysis of:
* Catalog growth
* Release frequency
* Recent activity
* Changes in artist output over time

---

## 4. Interactive Analytics

Provide an interactive **Streamlit analytics portal** allowing users to explore:
* Artists
* Albums
* Tracks
* Release types
* Release dates
* Catalog growth
* Historical release trends

The application will consume curated analytical data from the Gold layer rather than performing the core ETL process itself.

---

# 3. Key Business Questions & Metrics

| Business Question | Analytical Metric | Business Impact |
| :--- | :--- | :--- |
| **Who are the most active artists?** | `recent_release_count`, `release_cadence_days` | Identifies high-output artists and release velocity |
| **Which catalogs are expanding fastest?** | `catalog_growth_pct` | Identifies artists with rapidly expanding catalogs |
| **Which artists show the strongest catalog momentum?** | `catalog_momentum_index` | Highlights artists with increasing catalog activity |
| **How does release format vary?** | Album / Single / Compilation ratio | Identifies differences in release strategies |
| **How does catalog activity change over time?** | Monthly / yearly release counts | Identifies long-term release patterns |
| **What is the catalog duration profile?** | `avg_track_duration_min` | Measures track-length characteristics |
| **How does explicit content vary?** | `explicit_track_ratio` | Measures explicit-content distribution |
| **How has track duration changed over time?** | `avg_track_duration_min` by release period | Identifies long-term changes in track length |

---

# 4. Source Data Scope & Limitations

## 4.1 Included Entities

The core analytical model contains:

```text
Artist
   │
   ▼
Album
   │
   ▼
Track
```

Spotify continues to provide catalog metadata for artists, albums, and tracks, including artist-to-album and album-to-track relationships.

---

## 4.2 Core Attributes

### Artist
```text
artist_id
artist_name
spotify_uri
```

### Album
```text
album_id
album_name
album_type
release_date
total_tracks
artist_id
```

### Track
```text
track_id
track_name
album_id
artist_id
duration_ms
explicit
track_number
```

The exact source schema will be validated against the live API during implementation.

---

## 4.3 Excluded / Unavailable Metrics

The project will **not depend on Spotify popularity or follower metrics**.

Spotify's February 2026 Web API changes removed the following fields from the affected responses:
* Artist `popularity`
* Artist `followers`
* Album `popularity`
* Track `popularity`

Therefore, the project will derive its analytics from **observable catalog and release metadata** instead of relying on Spotify popularity scores.

The project will also avoid dependencies on removed API functionality such as:
* Artist top tracks
* New releases browse endpoint
* Available markets endpoint
* Bulk artist/album/track retrieval

Spotify's current API requires individual resource retrieval for artist, album, and track details where applicable.

---

# 5. Analytical Approach

The platform will maintain **historical snapshots** of the catalog.

Example:

```text
artist_id | snapshot_date | album_count | track_count
-------------------------------------------------------
A001      | 2026-08-01    | 15          | 120
A001      | 2026-08-15    | 16          | 128
A001      | 2026-08-29    | 17          | 134
```

Historical snapshots allow the platform to derive:
* `catalog_growth_pct`
* `recent_release_count`
* `release_cadence_days`
* `catalog_momentum_index`

### Catalog Growth
Measures the change in an artist's catalog between two observation periods:

$$
\text{Catalog Growth \%}
=
\frac{\text{Current Catalog Size}-\text{Previous Catalog Size}}
{\text{Previous Catalog Size}}
\times 100
$$

### Release Cadence
Measures the typical time between an artist's consecutive releases.
The project will use **median days between releases** to reduce the effect of unusually long gaps.

### Catalog Momentum Index
Combines:

```text
Catalog Growth
       +
Recent Release Activity
       +
Release Cadence
```

The weighting will be explicitly documented.

---

# 6. Expected Business Outputs

The final Streamlit application will provide four analytical areas:

## 1. Executive Overview
* Total artists
* Total albums
* Total tracks
* Recent releases
* Catalog growth
* Release activity

## 2. Artist Analytics
For a selected artist:
* Catalog size
* Album count
* Track count
* Recent release count
* Release cadence
* Catalog growth
* Catalog Momentum Index
* Historical catalog trajectory

## 3. Album & Track Analytics
* Album type
* Release date
* Track count
* Track duration
* Explicit-content ratio
* Artist relationships

## 4. Trend Analytics
* Releases over time
* Album vs. single activity
* Catalog growth
* Track duration trends
* Explicit-content trends
* Release patterns across time periods

---

# 7. Data Engineering Requirements

The platform will implement a cloud-based data pipeline:

```text
Spotify Web API ──▶ Python Extraction ──▶ AWS S3 Raw ──▶ Bronze (Parquet) ──▶ Silver (Clean) ──▶ DQ Gate ──▶ Gold (Star Schema) ──▶ Athena/DuckDB ──▶ Streamlit
```

The pipeline will support:
* REST API ingestion
* Semi-structured JSON processing
* S3 data lake storage
* Bronze/Silver/Gold architecture
* PySpark transformations
* Historical snapshots
* Incremental processing
* Idempotent loading
* Automated data-quality checks
* Dimensional modeling
* Airflow orchestration

---

# 8. Analytical Data Model

The Gold layer will use a **Kimball-style dimensional model**:

### Dimensions
* `dim_artist`
* `dim_album`
* `dim_track`
* `dim_date`

### Fact Tables
* `fact_artist_snapshot`
* `fact_album_snapshot`
* `fact_track_snapshot`

The model is designed to support both current-state reporting and historical trend analysis.

---

# 9. Success Criteria

| Area | Success Criteria |
| :--- | :--- |
| **Data Engineering** | Reliable Spotify API ingestion → S3 Raw → Bronze/Silver/Gold → automated Data Quality → historical snapshots → Airflow orchestration → idempotent incremental loading |
| **Data Modeling** | Kimball star schema with clear grain, keys, dimensions, and fact tables |
| **Analytics** | Business questions answered through reproducible SQL metrics |
| **Momentum Analysis** | Catalog Momentum Index calculated from documented observable metrics |
| **Application** | Interactive Streamlit portal with filtering across artists, albums, tracks, release types, and time |
| **Portfolio** | Fully documented architecture, data model, Airflow DAG, analytical methodology, Streamlit application, and business insights on GitHub |

---

# 10. Business Value

The platform enables analysts and music-industry stakeholders to move from **raw Spotify catalog metadata** to structured answers about:

```text
Who is producing?
       ↓
Who is growing?
       ↓
How frequently are they releasing?
       ↓
How is their catalog changing?
       ↓
How are music-release patterns evolving over time?
```

The result is a **business-oriented data platform**, rather than a simple Spotify API extraction project.
