"""
tests/test_data_quality.py
--------------------------
Unit tests for the Automated Data Quality Gate engine.
Tests core rules, status thresholds, quarantine routing, and JSON reporting.
"""

import os
import sys
import pytest

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
)
from src.transform.spark_session import get_spark_session
from src.quality.data_quality import DataQualityChecker



@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession fixture for testing."""
    session = get_spark_session(app_name="Spotify_DQ_UnitTests")
    yield session
    session.stop()


@pytest.fixture
def checker():
    """Fresh DataQualityChecker instance for each test."""
    return DataQualityChecker(snapshot_date="2026-08-31")


# ===========================================================================
# RULE 1: COMPLETENESS TESTS
# ===========================================================================
def test_check_completeness_pass(spark, checker):
    """Verify that an entity with rows >= min_expected passes."""
    schema = StructType([StructField("artist_id", StringType(), False)])
    df = spark.createDataFrame([("art_001",), ("art_002",)], schema=schema)

    result = checker.check_completeness(df, entity="artists", min_expected=1)

    assert result["status"] == "PASS"
    assert result["rule"] == "completeness"
    assert result["entity"] == "artists"
    assert result["row_count"] == 2
    assert len(checker.results) == 1


def test_check_completeness_fail_on_empty(spark, checker):
    """Verify that an empty DataFrame fails completeness."""
    schema = StructType([StructField("artist_id", StringType(), False)])
    empty_df = spark.createDataFrame([], schema=schema)

    result = checker.check_completeness(empty_df, entity="artists", min_expected=1)

    assert result["status"] == "FAIL"
    assert result["row_count"] == 0
    assert "FAILED completeness" in result["message"]


def test_check_completeness_custom_threshold(spark, checker):
    """Verify failure when row count is below custom min_expected threshold."""
    schema = StructType([StructField("track_id", StringType(), False)])
    df = spark.createDataFrame([("trk_1",), ("trk_2",), ("trk_3",)], schema=schema)

    # 3 rows, but expected at least 10 -> should FAIL
    result = checker.check_completeness(df, entity="tracks", min_expected=10)

    assert result["status"] == "FAIL"
    assert result["row_count"] == 3
    assert result["min_expected"] == 10


# ===========================================================================
# RULE 2: PRIMARY KEY UNIQUENESS TESTS
# ===========================================================================
def test_check_uniqueness_pass(spark, checker):
    """Verify that a DataFrame with unique primary keys passes."""
    schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("name", StringType(), True),
    ])
    data = [("art_1", "Coldplay"), ("art_2", "Taylor Swift")]
    df = spark.createDataFrame(data, schema=schema)

    result = checker.check_uniqueness(df, entity="artists", primary_keys=["artist_id"])

    assert result["status"] == "PASS"
    assert result["total_rows"] == 2
    assert result["distinct_keys"] == 2
    assert result["duplicate_count"] == 0
    assert result["duplicate_rate_pct"] == 0.0


def test_check_uniqueness_fail_with_duplicates(spark, checker):
    """Verify that duplicate primary keys are detected and fail the check."""
    schema = StructType([
        StructField("track_id", StringType(), False),
        StructField("title", StringType(), True),
    ])
    # 3 rows, but track_1 appears twice -> 1 duplicate
    data = [
        ("track_1", "Song A"),
        ("track_2", "Song B"),
        ("track_1", "Song A Duplicate"),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = checker.check_uniqueness(df, entity="tracks", primary_keys=["track_id"])

    assert result["status"] == "FAIL"
    assert result["total_rows"] == 3
    assert result["distinct_keys"] == 2
    assert result["duplicate_count"] == 1
    assert result["duplicate_rate_pct"] == 33.33
    assert "FAILED PK uniqueness" in result["message"]


# ===========================================================================
# RULE 3: CRITICAL NULL CHECKS TESTS
# ===========================================================================
def test_check_critical_nulls_pass(spark, checker):
    """Verify that populated critical columns pass without violations."""
    schema = StructType([
        StructField("album_id", StringType(), True),
        StructField("album_name", StringType(), True),
        StructField("artist_id", StringType(), True),
    ])
    data = [
        ("alb_1", "Thriller", "art_1"),
        ("alb_2", "Bad", "art_1"),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = checker.check_critical_nulls(
        df, entity="albums", critical_columns=["album_id", "album_name", "artist_id"]
    )

    assert result["status"] == "PASS"
    assert result["total_violations"] == 0
    assert result["null_counts"]["album_id"] == 0
    assert result["null_counts"]["album_name"] == 0


def test_check_critical_nulls_fail_on_null_and_empty(spark, checker):
    """Verify that both NULL and blank/empty string values are caught."""
    schema = StructType([
        StructField("track_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("artist_id", StringType(), True),
    ])
    data = [
        ("trk_1", "Song A", "art_1"),     # Valid
        (None, "Song B", "art_1"),        # Null track_id
        ("trk_3", "   ", "art_1"),        # Whitespace track_name
        ("trk_4", "Song D", None),        # Null artist_id
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = checker.check_critical_nulls(
        df, entity="tracks", critical_columns=["track_id", "track_name", "artist_id"]
    )

    assert result["status"] == "FAIL"
    assert result["total_violations"] == 3
    assert result["null_counts"]["track_id"] == 1
    assert result["null_counts"]["track_name"] == 1
    assert result["null_counts"]["artist_id"] == 1
    assert "FAILED critical null check" in result["message"]


# ===========================================================================
# RULE 4: REFERENTIAL INTEGRITY TESTS
# ===========================================================================
def test_referential_integrity_pass(spark, checker):
    """Verify that child records with valid parent keys pass cleanly."""
    track_schema = StructType([
        StructField("track_id", StringType(), False),
        StructField("album_id", StringType(), False),
    ])
    album_schema = StructType([StructField("album_id", StringType(), False)])

    tracks_df = spark.createDataFrame([("t1", "a1"), ("t2", "a2")], schema=track_schema)
    albums_df = spark.createDataFrame([("a1",), ("a2",)], schema=album_schema)

    result = checker.check_referential_integrity(
        child_df=tracks_df,
        parent_df=albums_df,
        foreign_key="album_id",
        child_entity="tracks",
        parent_entity="albums",
    )

    assert result["status"] == "PASS"
    assert result["orphan_count"] == 0
    assert result["orphan_rate_pct"] == 0.0


def test_referential_integrity_warn_below_threshold(spark, checker):
    """Verify that orphan rate <= threshold is flagged as WARN."""
    track_schema = StructType([
        StructField("track_id", StringType(), False),
        StructField("album_id", StringType(), False),
    ])
    album_schema = StructType([StructField("album_id", StringType(), False)])

    # 99 valid tracks + 1 orphan track = 100 rows (1.0% orphan rate)
    data = [(f"t_{i}", "a_valid") for i in range(99)]
    data.append(("t_orphan", "a_missing"))

    tracks_df = spark.createDataFrame(data, schema=track_schema)
    albums_df = spark.createDataFrame([("a_valid",)], schema=album_schema)

    # Threshold is 5.0%, orphan rate is 1.0% -> WARN
    result = checker.check_referential_integrity(
        child_df=tracks_df,
        parent_df=albums_df,
        foreign_key="album_id",
        child_entity="tracks",
        parent_entity="albums",
        max_orphan_threshold_pct=5.0,
    )

    assert result["status"] == "WARN"
    assert result["orphan_count"] == 1
    assert result["orphan_rate_pct"] == 1.0


def test_referential_integrity_fail_above_threshold(spark, checker):
    """Verify that orphan rate > threshold fails the pipeline."""
    track_schema = StructType([
        StructField("track_id", StringType(), False),
        StructField("album_id", StringType(), False),
    ])
    album_schema = StructType([StructField("album_id", StringType(), False)])

    # 90 valid tracks + 10 orphan tracks = 100 rows (10.0% orphan rate)
    data = [(f"t_{i}", "a_valid") for i in range(90)]
    data.extend([(f"t_orphan_{i}", "a_missing") for i in range(10)])

    tracks_df = spark.createDataFrame(data, schema=track_schema)
    albums_df = spark.createDataFrame([("a_valid",)], schema=album_schema)

    # Threshold is 5.0%, orphan rate is 10.0% -> FAIL
    result = checker.check_referential_integrity(
        child_df=tracks_df,
        parent_df=albums_df,
        foreign_key="album_id",
        child_entity="tracks",
        parent_entity="albums",
        max_orphan_threshold_pct=5.0,
    )

    assert result["status"] == "FAIL"
    assert result["orphan_count"] == 10
    assert result["orphan_rate_pct"] == 10.0


# ===========================================================================
# RULE 5: VALUE / RANGE VALIDATION TESTS
# ===========================================================================
def test_check_value_ranges_pass(spark, checker):
    """Verify that records within expected range bounds pass cleanly."""
    schema = StructType([
        StructField("track_id", StringType(), False),
        StructField("duration_ms", LongType(), True),
        StructField("track_number", IntegerType(), True),
    ])
    data = [
        ("t1", 200000, 1),
        ("t2", 180000, 2),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = checker.check_value_ranges(
        df,
        entity="tracks",
        range_conditions={"duration_ms": (1, 7200000), "track_number": (1, None)},
    )

    assert result["status"] == "PASS"
    assert result["total_violations"] == 0
    assert result["violations_by_col"]["duration_ms"] == 0
    assert result["violations_by_col"]["track_number"] == 0


def test_check_value_ranges_fail_out_of_bounds(spark, checker):
    """Verify that negative durations and invalid track numbers fail."""
    schema = StructType([
        StructField("track_id", StringType(), False),
        StructField("duration_ms", LongType(), True),
        StructField("track_number", IntegerType(), True),
    ])
    data = [
        ("t1", 200000, 1),    # Valid
        ("t2", -500, 2),      # Invalid negative duration
        ("t3", 180000, 0),    # Invalid track_number < 1
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = checker.check_value_ranges(
        df,
        entity="tracks",
        range_conditions={"duration_ms": (1, 7200000), "track_number": (1, None)},
    )

    assert result["status"] == "FAIL"
    assert result["total_violations"] == 2
    assert result["violations_by_col"]["duration_ms"] == 1
    assert result["violations_by_col"]["track_number"] == 1
    assert "FAILED range validation" in result["message"]


# ===========================================================================
# STEPS 7 & 8: QUARANTINE & OBSERVABILITY REPORTING TESTS
# ===========================================================================
def test_snapshot_date_traceability(spark, checker):
    """Verify that every DQ check stamps the exact snapshot_date on its result."""
    schema = StructType([StructField("id", StringType(), False)])
    df = spark.createDataFrame([("1",)], schema=schema)

    res1 = checker.check_completeness(df, entity="test")
    res2 = checker.check_uniqueness(df, entity="test", primary_keys=["id"])

    assert res1["snapshot_date"] == "2026-08-31"
    assert res2["snapshot_date"] == "2026-08-31"


def test_quarantine_records(spark, tmp_path):
    """Verify that invalid records are stamped with DQ lineage and written to quarantine."""
    quarantine_dir = str(tmp_path / "quarantine")
    checker = DataQualityChecker(
        snapshot_date="2026-08-31",
        quarantine_base_dir=quarantine_dir,
    )

    schema = StructType([StructField("track_id", StringType(), False)])
    bad_df = spark.createDataFrame([("bad_track_1",)], schema=schema)

    out_path = checker.quarantine_records(
        bad_df,
        entity="tracks",
        rule_name="referential_integrity",
        reason="orphan_album_id",
    )

    assert out_path is not None
    # Read back quarantined parquet to verify stamped lineage
    quarantined = spark.read.parquet(out_path)
    row = quarantined.collect()[0]
    assert row.track_id == "bad_track_1"
    assert row.dq_rule == "referential_integrity"
    assert row.dq_reason == "orphan_album_id"
    assert str(row.snapshot_date) == "2026-08-31"


def test_generate_and_save_report(tmp_path):
    """Verify that report generation aggregates results and writes valid JSON."""
    report_dir = str(tmp_path / "reports")
    checker = DataQualityChecker(
        snapshot_date="2026-08-31",
        report_base_dir=report_dir,
    )

    checker.results = [
        {"rule": "completeness", "entity": "artists", "status": "PASS"},
        {"rule": "referential_integrity", "entity": "tracks", "status": "WARN"},
    ]

    report = checker.generate_report()
    assert report["overall_status"] == "WARN"
    assert report["summary"]["total_checks"] == 2
    assert report["summary"]["passed"] == 1
    assert report["summary"]["warnings"] == 1
    assert report["summary"]["failed"] == 0

    saved_path = checker.save_report(report)
    assert os.path.exists(saved_path)

    with open(saved_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["snapshot_date"] == "2026-08-31"
    assert data["overall_status"] == "WARN"
