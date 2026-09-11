# 📐 Gold Layer Metric Definitions & Mathematical Formulations

This document provides the authoritative mathematical specifications, Kimball additivity classifications, edge-case null policies, and implementation guidelines for all metrics generated in `fact_artist_snapshot`.

---

## 📊 Master Measure Summary Table

| Metric Name | Kimball Additivity | SQL / PySpark Return Type | Value Range | Null Policy |
| :--- | :--- | :--- | :--- | :--- |
| `total_albums` | **Semi-Additive** | `IntegerType` | $[0, \infty)$ | Never `NULL` (defaults to `0`) |
| `total_tracks` | **Semi-Additive** | `IntegerType` | $[0, \infty)$ | Never `NULL` (defaults to `0`) |
| `total_singles` | **Semi-Additive** | `IntegerType` | $[0, \infty)$ | Never `NULL` (defaults to `0`) |
| `recent_releases_12m` | **Semi-Additive** | `IntegerType` | $[0, \infty)$ | Never `NULL` (defaults to `0`) |
| `median_release_cadence_days` | **Non-Additive** | `DoubleType` | $[0.0, \infty)$ | `NULL` if $< 2$ distinct release dates |
| `catalog_growth_pct` | **Non-Additive** | `DoubleType` | $[-100.0, \infty)$ | Strictly `NULL` on first snapshot |
| `catalog_momentum_index` | **Non-Additive** | `DoubleType` | $[0.0, 100.0]$ | Never `NULL` (clamped $[0, 100]$) |

---

## 🔬 In-Depth Metric Specifications

---

### 1. `total_albums` (Catalog Studio Album Count)

* **Business Definition**: The total number of unique studio albums associated with the artist present in the Silver catalog as of the given `snapshot_date`.
* **Kimball Classification**: **Semi-Additive**.
  * ✅ Valid to aggregate across artists for a single date: $\sum_{\text{artists}} \text{total\_albums}$ (Total albums across all artists on August 31).
  * ❌ **Strictly Invalid** to aggregate across dates for an artist: $\sum_{\text{dates}} \text{total\_albums}$ (Double-counts the catalog every day).
* **Derivation Logic**:
  ```sql
  COUNT(DISTINCT album_id) 
  WHERE artist_id = target_artist 
    AND album_type = 'album'
    AND release_date <= snapshot_date
  ```
* **Edge Cases & Compilation Accounting**:
  * If an artist only publishes singles (e.g. emerging electronic producers), `total_albums` resolves to `0`, **never** `NULL`.
  * **Compilation Reconciliation**: Compilations (`album_type = 'compilation'`) are excluded from `total_albums` and `total_singles`. However, compilation tracks are preserved in `total_tracks`. Therefore, $\text{total\_albums} + \text{total\_singles} \le \text{total releases}$.

---

### 2. `total_tracks` (Catalog Track Count)

* **Business Definition**: The total number of unique recordings/tracks associated with the artist across all discography items in the Silver catalog as of the given `snapshot_date`.
* **Kimball Classification**: **Semi-Additive**.
* **Derivation Logic**:
  ```sql
  COUNT(DISTINCT track_id) 
  WHERE artist_id = target_artist
  ```
* **Edge Cases**:
  * Minimum value is `0`. Never `NULL`.
  * Counts all playable tracks in the catalog, including studio albums, singles, EPs, and compilations.

---

### 3. `total_singles` (Catalog Single / EP Count)

* **Business Definition**: The total number of unique single or EP releases published by the artist as of the given `snapshot_date`.
* **Kimball Classification**: **Semi-Additive**.
* **Derivation Logic**:
  ```sql
  COUNT(DISTINCT album_id) 
  WHERE artist_id = target_artist 
    AND album_type = 'single'
    AND release_date <= snapshot_date
  ```
* **Edge Cases**:
  * If an artist has no singles, resolves to `0`, never `NULL`.

---

### 4. `recent_releases_12m` (Rolling 12-Month Publishing Velocity)

* **Business Definition**: The total number of new creative catalog drops (albums and singles combined) released by the artist within the 365 days immediately preceding the `snapshot_date`.
* **Kimball Classification**: **Semi-Additive**.
* **Scope & Compilations Policy**:
  * Explicitly filters `album_type IN ('album', 'single')`.
  * Repackaged compilations and greatest hits box sets (`album_type = 'compilation'`) are **excluded** to ensure this measure reflects genuine creative output rather than archival packaging.
* **Mathematical Formula**:
  $$\text{recent\_releases\_12m} = \sum_{r \in \text{Releases}} \mathbb{I}\Big( (\text{snapshot\_date} - 365\text{ days}) \le \text{release\_date}_r \le \text{snapshot\_date} \;\land\; \text{album\_type}_r \in \{\text{'album'}, \text{'single'}\} \Big)$$
* **Derivation Logic**:
  ```sql
  COUNT(DISTINCT album_id)
  WHERE artist_id = target_artist
    AND album_type IN ('album', 'single')
    AND release_date >= DATE_SUB(snapshot_date, 365)
    AND release_date <= snapshot_date
  ```
* **Edge Cases**:
  * Inactive legacy artists (e.g., catalog with no new drops in $> 1$ year) return `0`, never `NULL`.

---

### 5. `median_release_cadence_days` (Pacing Consistency)

* **Business Definition**: The median duration in calendar days between successive releases across the artist's historical discography.
* **Kimball Classification**: **Non-Additive**.
  * A median is a statistical summary, not an accumulative count or balance.
  * Summing medians across artists or across time produces statistically meaningless numbers. Medians also cannot be averaged arithmetically across cohorts without introducing severe distortion.
* **Mathematical Formula**:
  Let the chronological set of unique historical release dates for artist $A$ be:
  $$\mathcal{D} = \{ d_1, d_2, d_3, \dots, d_k \} \quad \text{where } d_1 < d_2 < \dots < d_k \le \text{snapshot\_date}$$
  The set of inter-release intervals (in calendar days) is:
  $$\Delta = \{ d_{i+1} - d_i \mid i \in \{1, 2, \dots, k-1\} \}$$
  $$\text{median\_release\_cadence\_days} = \text{median}(\Delta)$$
* **Edge Cases & Null Policy**:
  * **$k < 2$ (Zero or One release)**: Interval cannot be computed $\rightarrow$ **strictly returns `NULL`**.
  * **Simultaneous drops on same day**: $\Delta_i = 0$ days is valid if an artist drops deluxe/standard versions on the same day.
  * **Median vs Mean**: Median is chosen to make cadence resilient against anomalous multi-year career hiatuses.
* **Design Trade-Off (Lifetime Scoping vs. Recent Activity)**:
  > [!NOTE]
  > **Lifetime Scope Trade-Off & Inactivity Penalization**:
  > Cadence reflects **lifetime publishing consistency**, not current activity. Current activity is captured by `recent_releases_12m` (40% weight). An artist with high historical consistency (e.g., 20 albums dropped every 90 days from 2005–2010) who has released nothing since will receive $S_{\text{recent}} = 0$, leading to a moderate overall momentum score (~40–42.5), not high. This intentionally rewards proven catalog discipline while heavily suppressing the overall score due to lack of fresh output.

---

### 6. `catalog_growth_pct` (Snapshot-over-Snapshot Velocity)

* **Business Definition**: The relative percentage change in total track catalog volume compared to the immediately preceding available snapshot.
* **Kimball Classification**: **Non-Additive**. Must be recalculated when slicing or rolling up.
* **Mathematical Formula**:
  $$\text{catalog\_growth\_pct}_t = \left( \frac{\text{total\_tracks}_t - \text{total\_tracks}_{t-1}}{\text{total\_tracks}_{t-1}} \right) \times 100.0$$
* **PySpark Window Formulation**:
  ```python
  from pyspark.sql.window import Window
  from pyspark.sql.functions import col, lag, round as spark_round, when

  window_spec = Window.partitionBy("artist_id").orderBy(col("snapshot_date").asc())

  prev_tracks = lag("total_tracks", 1).over(window_spec)

  catalog_growth_pct = when(
      prev_tracks.isNull() | (prev_tracks == 0), None
  ).otherwise(
      spark_round(((col("total_tracks") - prev_tracks) / prev_tracks) * 100.0, 2)
  )
  ```
* **Edge Cases & Strict Rules**:
  1. **First Snapshot Rule**:
     * On the very first snapshot recorded for an artist (e.g. `2026-08-31`), `catalog_growth_pct` **MUST BE `NULL`**, NOT `0.0%`.
     * *Rationale*: `0.0%` implies the catalog was monitored and exhibited zero growth. `NULL` accurately communicates that this snapshot serves as the **initial baseline observation**.
  2. **Non-Consecutive Pipeline Runs**:
     * If the pipeline runs on Monday (`2026-08-31`) and skips to Thursday (`2026-09-03`), `LAG(1)` automatically references `2026-08-31`, tracking growth relative to the last verified snapshot.

---

### 7. `catalog_momentum_index` (Composite Trajectory Score)

* **Business Definition**: A standardized, commercially weighted index bounded in $[0.0, 100.0]$ that benchmarks an artist's current publishing momentum by combining recent release output, catalog growth, and historical publishing consistency.
* **Kimball Classification**: **Non-Additive Intensity Score**.
* **Mathematical Formulation**:
  $$\text{Momentum Index} = 0.40 \times S_{\text{recent}} + 0.35 \times S_{\text{growth}} + 0.25 \times S_{\text{cadence}}$$

#### Sub-Component Scoring Functions:

1. **Recent Release Score ($S_{\text{recent}}$)**:
   - Measures publishing activity in the last 12 months.
   - Scaled linearly up to 10 releases:
     $$S_{\text{recent}} = \min\left(100.0, \text{recent\_releases\_12m} \times 10.0\right)$$
   - *Example*: 3 releases in last 12m $\rightarrow S_{\text{recent}} = 30.0$. 10+ releases $\rightarrow 100.0$.

2. **Catalog Growth Score ($S_{\text{growth}}$)**:
   - Evaluates short-term catalog expansion.
   - If `catalog_growth_pct` is `NULL` (first snapshot baseline), default to neutral baseline $S_{\text{growth}} = 50.0$.
   - For positive growth:
     $$S_{\text{growth}} = \min\left(100.0, 50.0 + (\text{catalog\_growth\_pct} \times 5.0)\right)$$
   - For zero growth between snapshots: $S_{\text{growth}} = 50.0$.
   - For negative growth (unreleased/removed tracks): $S_{\text{growth}} = \max(0.0, 50.0 + \text{catalog\_growth\_pct} \times 5.0)$.

3. **Release Cadence Score ($S_{\text{cadence}}$)**:
   - Measures historical publishing frequency (inverse interval).
   - If `median_release_cadence_days` is `NULL` ($< 2$ releases), default to neutral $S_{\text{cadence}} = 50.0$.
   - **Empirical Formula**:
     $$S_{\text{cadence}} = \max\left(10.0, \min\left(100.0, 100.0 - \left(\frac{\text{median\_release\_cadence\_days} - 14.0}{180.0 - 14.0}\right) \times 90.0\right)\right)$$
   - **Calibrated Reference Points (Formula Output)**:
     - Hyper-rapid pace ($\le 14$ days / bi-weekly drops, e.g. Ed Sheeran @ 14d): $S_{\text{cadence}} = \mathbf{100.0}$
     - Rapid single cadence (~15 days, e.g. Drake @ 15d): $S_{\text{cadence}} = 100 - \frac{1}{166} \times 90 \approx \mathbf{99.5}$
     - Monthly pacing (~38 days, e.g. Taylor Swift @ 38d): $S_{\text{cadence}} = 100 - \frac{24}{166} \times 90 \approx \mathbf{87.0}$
     - Quarterly album/EP cadence (~107 days, e.g. The Weeknd @ 107d): $S_{\text{cadence}} = 100 - \frac{93}{166} \times 90 \approx \mathbf{49.6}$
     - Standard album cycle (~114 days, e.g. Billie Eilish @ 114d): $S_{\text{cadence}} = 100 - \frac{100}{166} \times 90 \approx \mathbf{45.8}$
     - Extended hiatus / 6+ months cycle ($\ge 180$ days): $S_{\text{cadence}} = \mathbf{10.0}$ (floor)
   - **Recalibration Rationale**:
     - Modern pop catalogs feature frequent single, remix, and collaboration drops. Under the original $[90, 1825]$ day range, all artists with cadence $< 90$ days saturated at the 100-point ceiling, destroying score differentiation.
     - Recalibrating to $[14.0, 180.0]$ days produces meaningful variance between hyper-prolific single droppers (Ed Sheeran: 14d, Drake: 15d) and album-era artists (The Weeknd: 107d, Billie Eilish: 114d).

4. **Clamping & Precision**:
   - Final index is clamped:
     $$\text{catalog\_momentum\_index} = \text{round}\Big(\max\left(0.0, \min(100.0, \text{Momentum Index})\right), 2\Big)$$

---

## 🚫 Common OLAP Query Anti-Patterns (Kimball Red Flags)

When querying `fact_artist_snapshot` in SQL engines (Athena, DuckDB, Trino):

1. ❌ **Summing semi-additive metrics over time**:
   ```sql
   -- INCORRECT: Summing total_tracks over 30 days multiplies catalog size by 30!
   SELECT artist_id, SUM(total_tracks) FROM fact_artist_snapshot GROUP BY artist_id;
   ```
   ✅ **Correct periodic snapshot pattern**:
   ```sql
   -- CORRECT: Take the value at the target snapshot date
   SELECT artist_id, total_tracks 
   FROM fact_artist_snapshot 
   WHERE snapshot_date = '2026-09-01';
   ```

2. ❌ **Averaging `catalog_growth_pct` directly**:
   ```sql
   -- INCORRECT: Averaging percentages across artists of unequal sizes introduces Simpson's Paradox
   SELECT AVG(catalog_growth_pct) FROM fact_artist_snapshot WHERE snapshot_date = '2026-09-01';
   ```
   ✅ **Correct non-additive rollup pattern**:
   ```sql
   -- CORRECT: Recompute growth using aggregate totals
   WITH cohort_totals AS (
     SELECT 
       snapshot_date,
       SUM(total_tracks) AS cohort_tracks,
       LAG(SUM(total_tracks)) OVER (ORDER BY snapshot_date) AS prev_cohort_tracks
     FROM fact_artist_snapshot
     GROUP BY snapshot_date
   )
   SELECT 
     snapshot_date,
     ROUND((cohort_tracks - prev_cohort_tracks) * 100.0 / prev_cohort_tracks, 2) AS cohort_growth_pct
   FROM cohort_totals;
   ```
