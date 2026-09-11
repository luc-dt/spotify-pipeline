# 🏛️ Gold Layer Data Warehouse Schema Specification (Kimball Star Schema)

This document serves as the **frozen architectural contract** for the Gold Layer of the Spotify Music Intelligence Platform.

---

## 🎯 Architecture Overview & Kimball Dimensional Design

The Gold Layer is designed strictly according to **Ralph Kimball's Dimensional Modeling methodology**. It organizes clean, trusted Silver entities into an analytical **Star Schema** optimized for high-performance BI reporting, SQL OLAP engines (Amazon Athena, DuckDB, Trino, Snowflake), and downstream interactive dashboard consumption (Streamlit).

```text
┌──────────────────────────────────────┐                   ┌──────────────────────────────────────┐
│              dim_artist              │                   │               dim_date               │
├──────────────────────────────────────┤                   ├──────────────────────────────────────┤
│ PK: artist_key (MD5 hash)            │                   │ PK: date_key (Integer: YYYYMMDD)     │
│     artist_id (natural)              │                   │     full_date (Date)                 │
│     artist_name                      │                   │     year, quarter, month             │
│     genres (array<string>)           │                   │     month_name, week_of_year         │
│     spotify_uri, image_url           │                   │     day_of_month, day_of_week        │
│     gold_transformed_at              │                   │     day_name, is_weekend             │
└──────────────────┬───────────────────┘                   └──────────────────┬───────────────────┘
                   │ 1                                                        │ 1
                   │                                                          │
                   │          ┌─────────────────────────────────────────────┐ │
                   └─────────►│            fact_artist_snapshot             │◄┘
                            N ├─────────────────────────────────────────────┤ N
                              │ PK: (artist_key, date_key)                  │
                              │ FK: artist_key ─────────────────────────────┼─┐
                              │ FK: date_key ───────────────────────────────┼─┤
                              │     artist_id (natural)                     │ │
                              │     snapshot_date (Date partition)          │ │
                              │     gold_transformed_at                     │ │
                              │─────────────────────────────────────────────│ │
                              │ total_albums (semi-additive)                │ │
                              │ total_tracks (semi-additive)                │ │
                              │ total_singles (semi-additive)               │ │
                              │ recent_releases_12m (semi-additive)         │ │
                              │ median_release_cadence_days (non-additive)  │ │
                              │ catalog_growth_pct (non-additive rate)      │ │
                              │ catalog_momentum_index [0, 100] (score)     │ │
                              └─────────────────────────────────────────────┘ │
                                              ▲                               │
    ┌──────────────────────────────────────┐  │                               │
    │              dim_album               │  │                               │
    ├──────────────────────────────────────┤  │                               │
    │ PK: album_key (MD5 hash)             │  │                               │
    │     album_id (natural)               │  │                               │
    │ FK: artist_key ──────────────────────┼──┤ (Conformed dimensions         │
    │     artist_id (natural)              │  │  for drill-down and           │
    │     album_name, album_type           │  │  cross-sectional slicing)     │
    │     release_date, release_year       │  │                               │
    │     release_month, release_decade    │  │                               │
    │     release_date_precision           │  │                               │
    │     total_tracks, spotify_uri,...    │  │                               │
    └──────────────────────────────────────┘  │                               │
                                              │                               │
    ┌──────────────────────────────────────┐  │                               │
    │              dim_track               │  │                               │
    ├──────────────────────────────────────┤  │                               │
    │ PK: track_key (MD5 hash)             │  │                               │
    │     track_id (natural)               │  │                               │
    │ FK: album_key ───────────────────────┼──┘                               │
    │ FK: artist_key ──────────────────────┼──────────────────────────────────┘
    │     album_id, artist_id (natural)    │
    │     track_name, duration_ms          │
    │     duration_min, duration_sec       │
    │     explicit, track_number,...       │
    └──────────────────────────────────────┘
```

---

## 🧠 Core Kimball Principles Enforced

1. **Explicit Fact Table Grain**:
   - **Grain**: Exactly **one row per artist per extracted snapshot date**.
   - **Composite Primary Key**: `(artist_key, date_key)`.
   - Guaranteed zero duplicate grain records per snapshot.

2. **Semi-Additive vs. Non-Additive Measures**:
   - **Semi-Additive Counters** (`total_albums`, `total_tracks`, `total_singles`, `recent_releases_12m`):
     - Valid to aggregate across artists within a single `date_key`: e.g., $\sum_{\text{cohort}} \text{total\_tracks}$ on August 31.
     - **Strictly invalid** to sum across time for the same artist: $\sum_{\text{dates}} \text{total\_tracks}$ double-counts catalog inventory daily.
   - **Non-Additive Statistical & Trajectory Metrics**:
     - `median_release_cadence_days`: **Strictly non-additive**. A median is a statistical point summary, not an accumulative balance. Summing or averaging medians across artists or dates is statistically meaningless.
     - `catalog_growth_pct`: **Non-additive rate**. Must be recomputed from base aggregations across cohort slices.
     - `catalog_momentum_index`: **Non-additive normalized intensity score** $[0.0, 100.0]$.

3. **Compilation Accounting & Catalog Reconciliation**:
   - `total_albums` counts releases where `album_type = 'album'`.
   - `total_singles` counts releases where `album_type = 'single'`.
   - Compilations (`album_type = 'compilation'`) contribute recordings to `total_tracks`, but are excluded from `total_albums` and `total_singles`.
   - As a result: $\text{total\_albums} + \text{total\_singles} \le \text{total releases}$.
   - `recent_releases_12m` explicitly filters `album_type IN ('album', 'single')` to capture genuine new creative output rather than repackaged compilation box sets.

4. **Cadence Lifetime Scoping Trade-Off**:
   - `median_release_cadence_days` reflects **lifetime publishing consistency** across the entire discography up to `snapshot_date`.
   - Current activity is isolated in `recent_releases_12m` (40% weight). An artist with high lifetime consistency (e.g. 20 releases dropped quarterly between 2005–2010) but on prolonged hiatus receives $S_{\text{recent}} = 0$, yielding a moderate momentum score (~40–42.5), not high. This intentionally rewards proven historical career discipline while penalizing the absence of fresh drops.

5. **Surrogate Keys vs. Natural Keys**:
   - Natural keys (`artist_id`, `album_id`, `track_id`) originate from Spotify and are preserved for traceability.
   - Surrogate keys (`artist_key`, `album_key`, `track_key`) are deterministic 32-character MD5 hashes of trimmed natural IDs. This decouples the warehouse from source format shifts and allows distributed, sequence-free key generation in PySpark.
   - Date Surrogate Key (`date_key`): A smart integer in `YYYYMMDD` format (e.g., `20260831`), enabling partition-aware pruning without expensive date parsing.

6. **Periodic Snapshot Architecture**:
   - Rather than recording events as they occur (Transaction Fact), `fact_artist_snapshot` captures the **cumulative state and trajectory of an artist's discography at uniform temporal intervals** (daily/weekly extractions).
   - **Zero-Overwrite Rule**: Snapshot partitions are immutable. Every extraction run appends a new `snapshot_date=YYYY-MM-DD` partition.

---

## 📋 Entity Data Contracts

### 1. `dim_artist` (Artist Conformed Dimension)

- **Grain**: One row per distinct artist.
- **Physical Path**: `data/gold/dim_artist/` (Unpartitioned Snappy Parquet).
- **Update Strategy**: Type 1 SCD (Overwrite/Upsert with latest profile attributes).

| Column Name | Physical Data Type | Nullable | Key Type | Description |
| :--- | :--- | :--- | :--- | :--- |
| `artist_key` | `StringType` (MD5 32-hex) | No | **PK** | Deterministic surrogate key: `md5(artist_id)`. |
| `artist_id` | `StringType` | No | Natural Key | Spotify base-62 unique artist identifier. |
| `artist_name` | `StringType` | No | Attribute | Cleaned canonical artist name. |
| `genres` | `ArrayType(StringType)` | No | Attribute | Music genres associated with the artist (empty array if none). |
| `spotify_uri` | `StringType` | Yes | Attribute | Official Spotify URI (`spotify:artist:...`). |
| `image_url` | `StringType` | Yes | Attribute | High-resolution artist avatar URL. |
| `gold_transformed_at` | `TimestampType` | No | Audit | UTC timestamp when record was written to Gold. |

---

### 2. `dim_album` (Album Conformed Dimension)

- **Grain**: One row per distinct album.
- **Physical Path**: `data/gold/dim_album/` (Unpartitioned Snappy Parquet).
- **Update Strategy**: Type 1 SCD.

| Column Name | Physical Data Type | Nullable | Key Type | Description |
| :--- | :--- | :--- | :--- | :--- |
| `album_key` | `StringType` (MD5 32-hex) | No | **PK** | Deterministic surrogate key: `md5(album_id)`. |
| `album_id` | `StringType` | No | Natural Key | Spotify base-62 unique album identifier. |
| `artist_key` | `StringType` (MD5 32-hex) | No | **FK** | Foreign key referencing `dim_artist.artist_key`. |
| `artist_id` | `StringType` | No | Natural FK | Spotify natural artist ID for easy querying. |
| `album_name` | `StringType` | No | Attribute | Title of the album or single. |
| `album_type` | `StringType` | No | Attribute | Normalized release type: `'album'`, `'single'`, `'compilation'`. |
| `release_date` | `DateType` | Yes | Attribute | Standardized ISO release date (`YYYY-MM-DD`). |
| `release_year` | `IntegerType` | Yes | Attribute | 4-digit Gregorian calendar release year. |
| `release_month` | `IntegerType` | Yes | Attribute | Month of release ($1$ to $12$). |
| `release_decade` | `StringType` | Yes | Attribute | Decade bucket (e.g., `'1980s'`, `'2010s'`). |
| `release_date_precision` | `StringType` | Yes | Attribute | Original precision provided by Spotify (`'year'`, `'month'`, `'day'`). |
| `total_tracks` | `IntegerType` | Yes | Attribute | Expected total track count declared on album envelope. |
| `spotify_uri` | `StringType` | Yes | Attribute | Official Spotify URI (`spotify:album:...`). |
| `image_url` | `StringType` | Yes | Attribute | Cover artwork image URL. |
| `gold_transformed_at` | `TimestampType` | No | Audit | UTC timestamp when record was written to Gold. |

---

### 3. `dim_track` (Track Conformed Dimension)

- **Grain**: One row per distinct track.
- **Physical Path**: `data/gold/dim_track/` (Unpartitioned Snappy Parquet).
- **Update Strategy**: Type 1 SCD.

| Column Name | Physical Data Type | Nullable | Key Type | Description |
| :--- | :--- | :--- | :--- | :--- |
| `track_key` | `StringType` (MD5 32-hex) | No | **PK** | Deterministic surrogate key: `md5(track_id)`. |
| `track_id` | `StringType` | No | Natural Key | Spotify base-62 unique track identifier. |
| `album_key` | `StringType` (MD5 32-hex) | No | **FK** | Foreign key referencing `dim_album.album_key`. |
| `album_id` | `StringType` | No | Natural FK | Spotify natural album ID. |
| `artist_key` | `StringType` (MD5 32-hex) | No | **FK** | Foreign key referencing `dim_artist.artist_key`. |
| `artist_id` | `StringType` | No | Natural FK | Spotify natural primary artist ID. |
| `track_name` | `StringType` | No | Attribute | Cleaned track name. |
| `duration_ms` | `LongType` | No | Attribute | Track length in milliseconds. |
| `duration_min` | `DoubleType` | No | Attribute | Track length in decimal minutes rounded to 2 decimals. |
| `duration_sec` | `DoubleType` | No | Attribute | Track length in decimal seconds rounded to 1 decimal. |
| `explicit` | `BooleanType` | No | Attribute | Content flag: `true` if explicit lyrics/themes present. |
| `track_number` | `IntegerType` | No | Attribute | Positional track index on disc. |
| `disc_number` | `IntegerType` | No | Attribute | Positional disc index. |
| `spotify_uri` | `StringType` | Yes | Attribute | Official Spotify URI (`spotify:track:...`). |
| `gold_transformed_at` | `TimestampType` | No | Audit | UTC timestamp when record was written to Gold. |

---

### 4. `dim_date` (Gregorian Calendar Dimension)

- **Grain**: One row per calendar day (spanning 1950-01-01 to 2030-12-31).
- **Physical Path**: `data/gold/dim_date/` (Unpartitioned Snappy Parquet).
- **Update Strategy**: Static reference dimension populated once.

| Column Name | Physical Data Type | Nullable | Key Type | Description |
| :--- | :--- | :--- | :--- | :--- |
| `date_key` | `IntegerType` | No | **PK** | Smart integer key in `YYYYMMDD` format (e.g. `20260831`). |
| `full_date` | `DateType` | No | Alternate Key | Calendar date object (`YYYY-MM-DD`). |
| `year` | `IntegerType` | No | Attribute | 4-digit calendar year (e.g., `2026`). |
| `quarter` | `IntegerType` | No | Attribute | Calendar quarter ($1, 2, 3, 4$). |
| `month` | `IntegerType` | No | Attribute | Calendar month ($1$ to $12$). |
| `month_name` | `StringType` | No | Attribute | English month name (e.g., `'August'`). |
| `week_of_year` | `IntegerType` | No | Attribute | ISO calendar week number ($1$ to $53$). |
| `day_of_month` | `IntegerType` | No | Attribute | Day of the month ($1$ to $31$). |
| `day_of_week` | `IntegerType` | No | Attribute | Day of the week ($1 = \text{Monday}, \dots, 7 = \text{Sunday}$). |
| `day_name` | `StringType` | No | Attribute | English weekday name (e.g., `'Monday'`). |
| `is_weekend` | `BooleanType` | No | Attribute | `true` if Saturday or Sunday, `false` otherwise. |

---

### 5. `fact_artist_snapshot` (Periodic Snapshot Fact Table)

- **Grain**: Exactly **one row per artist per snapshot extraction date**.
- **Physical Path**: `data/gold/fact_artist_snapshot/snapshot_date=YYYY-MM-DD/` (Partitioned Snappy Parquet).
- **Update Strategy**: Daily append with Dynamic Partition Overwrite.

| Column Name | Physical Data Type | Nullable | Key Type | Additivity | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `artist_key` | `StringType` (MD5 32-hex) | No | **PK / FK** | Non-additive | Surrogate FK referencing `dim_artist.artist_key`. |
| `date_key` | `IntegerType` | No | **PK / FK** | Non-additive | Smart integer FK referencing `dim_date.date_key` (`YYYYMMDD`). |
| `artist_id` | `StringType` | No | Degenerate | Non-additive | Spotify natural artist ID for denormalized query convenience. |
| `snapshot_date` | `DateType` | No | Partition | Non-additive | Business snapshot date (`YYYY-MM-DD`). |
| `total_albums` | `IntegerType` | No | Measure | **Semi-Additive** | Total distinct albums with `album_type = 'album'` in catalog. |
| `total_tracks` | `IntegerType` | No | Measure | **Semi-Additive** | Total distinct tracks in artist catalog as of snapshot. |
| `total_singles` | `IntegerType` | No | Measure | **Semi-Additive** | Total distinct releases with `album_type = 'single'` in catalog. |
| `recent_releases_12m` | `IntegerType` | No | Measure | **Semi-Additive** | Number of creative drops (`album_type IN ('album', 'single')`) released within rolling 365 days. |
| `median_release_cadence_days` | `DoubleType` | Yes | Measure | **Non-Additive** | Median calendar days between successive releases across lifetime discography. `NULL` if $< 2$ releases. |
| `catalog_growth_pct` | `DoubleType` | Yes | Measure | **Non-Additive** | Track catalog growth percentage versus preceding snapshot date. `NULL` on first run. |
| `catalog_momentum_index` | `DoubleType` | No | Measure | **Non-Additive** | Empirical composite trajectory score bounded between $[0.0, 100.0]$. |
| `gold_transformed_at` | `TimestampType` | No | Audit | Non-additive | UTC timestamp of pipeline execution. |

---

## 🔒 Physical Storage & Partitioning Strategy

```text
data/gold/
├── dim_artist/
│   └── part-*.snappy.parquet           (Unpartitioned, ~8 records)
├── dim_album/
│   └── part-*.snappy.parquet           (Unpartitioned, ~1,000 records)
├── dim_track/
│   └── part-*.snappy.parquet           (Unpartitioned, ~3,500 records)
├── dim_date/
│   └── part-*.snappy.parquet           (Unpartitioned, ~29,585 calendar days)
└── fact_artist_snapshot/
    ├── snapshot_date=2026-08-31/
    │   └── part-*.snappy.parquet       (8 records, one per artist)
    └── snapshot_date=2026-09-01/
        └── part-*.snappy.parquet       (8 records, one per artist)
```

- **Partition Pruning**: Query engines (Athena, Spark, DuckDB) prune full directories when querying `WHERE snapshot_date = '2026-09-01'` or `WHERE date_key = 20260901`.
- **Dynamic Partition Overwrite**: Spark session parameter `spark.sql.sources.partitionOverwriteMode=dynamic` guarantees that reprocessing an existing snapshot replaces only that partition without affecting historical days.
