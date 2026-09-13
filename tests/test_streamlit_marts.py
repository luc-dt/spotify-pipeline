"""
tests/test_streamlit_marts.py
-----------------------------
Contract and data verification tests for the Streamlit DuckDB Access Layer.
Validates:
1. Schema integrity across all 4 Curated Data Marts & Gold Views.
2. Non-empty data contracts.
3. Parameterized query execution in streamlit/utils/db.py.
4. Anti-transformation leakage (no business transformations in presentation layer).
"""

import ast
import glob
import os
import sys
import pytest
import duckdb

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
streamlit_dir = os.path.join(project_root, "streamlit")
if streamlit_dir not in sys.path:
    sys.path.insert(0, streamlit_dir)

from utils import db


@pytest.fixture(scope="module")
def duckdb_conn():
    """Provides an initialized DuckDB connection registering Gold views and Marts."""
    # We call get_connection or its underlying logic directly
    conn = duckdb.connect(database=":memory:")
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    gold_views_path = os.path.join(project_root, "sql", "setup_gold_views.sql")
    marts_views_path = os.path.join(project_root, "sql", "setup_marts.sql")

    with open(gold_views_path, "r", encoding="utf-8") as f:
        conn.execute(f.read())
    with open(marts_views_path, "r", encoding="utf-8") as f:
        conn.execute(f.read())

    return conn


# =============================================================================
# 1. Schema & View Contract Tests
# =============================================================================

def test_gold_views_exist(duckdb_conn):
    """Asserts that all 5 Gold semantic views are registered and queryable."""
    expected_views = [
        "dim_artist",
        "dim_album",
        "dim_track",
        "dim_date",
        "fact_artist_snapshot",
    ]
    for view in expected_views:
        df = duckdb_conn.execute(f"SELECT * FROM {view} LIMIT 1;").df()
        assert not df.empty, f"View {view} returned 0 rows on limit 1"


def test_four_curated_marts_exist(duckdb_conn):
    """Asserts that all 4 Curated Data Marts exist and contain required schema contracts."""
    required_marts = [
        "mart_artist_activity",
        "mart_catalog_growth",
        "mart_release_seasonality",
        "mart_artist_momentum",
    ]
    for mart in required_marts:
        count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {mart};").fetchone()[0]
        assert count > 0, f"Mart {mart} must contain > 0 rows"


def test_mart_columns_contract(duckdb_conn):
    """Validates specific schema columns expected by Streamlit pages."""
    # mart_artist_activity
    act_cols = set(duckdb_conn.execute("SELECT * FROM mart_artist_activity LIMIT 0;").df().columns)
    assert {"snapshot_date", "artist_name", "recent_releases_12m", "catalog_strategy", "activity_tier"}.issubset(act_cols)

    # mart_catalog_growth
    growth_cols = set(duckdb_conn.execute("SELECT * FROM mart_catalog_growth LIMIT 0;").df().columns)
    assert {"snapshot_date", "artist_name", "current_tracks", "net_tracks_added", "growth_classification"}.issubset(growth_cols)

    # mart_release_seasonality
    season_cols = set(duckdb_conn.execute("SELECT * FROM mart_release_seasonality LIMIT 0;").df().columns)
    assert {"release_month", "month_name", "total_releases", "single_share_pct", "industry_season_archetype"}.issubset(season_cols)

    # mart_artist_momentum
    momentum_cols = set(duckdb_conn.execute("SELECT * FROM mart_artist_momentum LIMIT 0;").df().columns)
    assert {"snapshot_date", "artist_name", "catalog_momentum_index", "volume_score", "growth_score", "cadence_score", "momentum_tier"}.issubset(momentum_cols)


# =============================================================================
# 2. Query Function Execution Tests
# =============================================================================

def test_dynamic_snapshot_discovery(duckdb_conn):
    """Verifies dynamic snapshot date discovery returns valid dates."""
    dates = db.get_snapshot_dates(duckdb_conn)
    assert len(dates) >= 2, f"Expected at least 2 snapshot dates, found {len(dates)}"
    assert "2026-08-31" in dates or "2026-09-01" in dates


def test_pipeline_health_query(duckdb_conn):
    """Verifies pipeline health metrics."""
    health = db.get_pipeline_health(duckdb_conn)
    assert health["latest_snapshot"] is not None
    assert health["artists_count"] == 8
    assert health["albums_count"] > 500
    assert health["tracks_count"] > 3000
    assert "PASS" in health["dq_status"]


def test_executive_kpis_query(duckdb_conn):
    """Verifies executive KPI aggregation for the latest snapshot."""
    dates = db.get_snapshot_dates(duckdb_conn)
    latest = dates[0]
    kpis = db.get_executive_kpis(duckdb_conn, latest)
    assert kpis["total_artists"] > 0
    assert kpis["total_tracks"] > 1000
    assert kpis["cohort_avg_momentum"] > 0.0


def test_artist_activity_query(duckdb_conn):
    """Verifies artist activity query for the latest snapshot."""
    dates = db.get_snapshot_dates(duckdb_conn)
    latest = dates[0]
    df = db.get_artist_activity(duckdb_conn, latest)
    assert len(df) > 0
    assert "activity_rank" in df.columns
    assert "catalog_strategy" in df.columns


def test_artist_momentum_pillars_reconciliation(duckdb_conn):
    """Verifies that the 3 pillars in mart_artist_momentum exist and are bounded."""
    dates = db.get_snapshot_dates(duckdb_conn)
    latest = dates[0]
    df = db.get_artist_momentum(duckdb_conn, latest)
    assert not df.empty

    for _, row in df.iterrows():
        assert 0.0 <= row["volume_score"] <= 40.0, f"Volume score out of bounds: {row['volume_score']}"
        assert 0.0 <= row["growth_score"] <= 35.0, f"Growth score out of bounds: {row['growth_score']}"
        assert 0.0 <= row["cadence_score"] <= 25.0, f"Cadence score out of bounds: {row['cadence_score']}"
        # Sum of pillars should closely approximate total momentum
        reconstructed = row["volume_score"] + row["growth_score"] + row["cadence_score"]
        assert abs(reconstructed - row["catalog_momentum_index"]) <= 1.0, (
            f"Pillars sum {reconstructed} deviates from total index {row['catalog_momentum_index']} for {row['artist_name']}"
        )


def test_track_analytics_query(duckdb_conn):
    """Verifies parameterized track analytics filtering."""
    df_all = db.get_track_analytics(duckdb_conn, limit=100)
    assert len(df_all) == 100

    df_filtered = db.get_track_analytics(
        duckdb_conn, artist_names=["Taylor Swift"], album_types=["album"], limit=50
    )
    assert len(df_filtered) > 0
    assert (df_filtered["artist_name"] == "Taylor Swift").all()
    assert (df_filtered["album_type"] == "album").all()


def test_yearly_format_evolution_query(duckdb_conn):
    """Verifies annual format evolution query executes and returns valid release years."""
    df = db.get_yearly_format_evolution(duckdb_conn)
    assert not df.empty
    assert "release_year" in df.columns
    assert "studio_albums" in df.columns
    assert "singles" in df.columns
    assert df["release_year"].min() >= 1970


def test_artist_profile_query(duckdb_conn):
    """Verifies single artist profile retrieval with genre array."""
    dates = db.get_snapshot_dates(duckdb_conn)
    latest = dates[0]
    profile = db.get_artist_profile(duckdb_conn, "Taylor Swift", latest)
    assert profile is not None
    assert profile["artist_name"] == "Taylor Swift"
    assert profile["total_tracks"] > 0
    assert "genres" in profile


def test_artist_discography_spotify_url(duckdb_conn):
    """Verifies that artist discography includes valid Spotify URLs."""
    df = db.get_artist_discography(duckdb_conn, "Taylor Swift")
    assert not df.empty
    assert "spotify_url" in df.columns
    assert df["spotify_url"].iloc[0].startswith("https://open.spotify.com/album/")


# =============================================================================
# 3. Anti-Transformation Leakage Test (AST Inspection)
# =============================================================================

def test_no_transformation_leakage_in_pages():
    """
    Scans all python files under streamlit/pages/ and asserts that
    heavy transformation methods (.groupby, .rolling, .rank, .pivot_table)
    are NOT called in presentation layer code.
    """
    forbidden_methods = {"groupby", "rolling", "rank", "pivot_table"}
    pages_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "streamlit", "pages"))
    page_files = glob.glob(os.path.join(pages_dir, "*.py"))

    assert len(page_files) > 0, "No page files found to inspect"

    for fpath in page_files:
        with open(fpath, "r", encoding="utf-8") as f:
            code = f.read()

        tree = ast.parse(code, filename=fpath)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                method_name = node.func.attr
                assert method_name not in forbidden_methods, (
                    f"Architectural Leakage Detected in {os.path.basename(fpath)}: "
                    f"Found analytical method call '{method_name}()'. "
                    f"Transformations must occur upstream in SQL marts or PySpark."
                )
