"""Tests for jobs.data_quality module."""

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from jobs.data_quality import (
    DataQualityError,
    null_percentage_check,
    range_check,
    row_count_check,
    run_quality_checks,
    uniqueness_check,
)

_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("region", StringType()),
        StructField("rent", DoubleType()),
    ]
)


def _make_df(spark: SparkSession, rows: list) -> DataFrame:
    """Create a small test DataFrame."""
    return spark.createDataFrame(rows, schema=_SCHEMA)


# --- null_percentage_check ---


def test_null_check_passes(spark: SparkSession) -> None:
    """5% nulls with 10% threshold should pass."""
    # 1 null out of 20 rows = 5%
    rows = [(i, "r", float(i)) for i in range(19)] + [(19, "r", None)]
    df = _make_df(spark, rows)
    check = null_percentage_check("rent", threshold=0.10)
    assert check.check_fn(df) is True


def test_null_check_fails(spark: SparkSession) -> None:
    """15% nulls with 10% threshold should fail."""
    # 3 nulls out of 20 rows = 15%
    rows = [(i, "r", float(i)) for i in range(17)] + [
        (17, "r", None),
        (18, "r", None),
        (19, "r", None),
    ]
    df = _make_df(spark, rows)
    check = null_percentage_check("rent", threshold=0.10)
    assert check.check_fn(df) is False


# --- row_count_check ---


def test_row_count_check_passes(spark: SparkSession) -> None:
    """20 rows with min_rows=10 should pass."""
    rows = [(i, "r", float(i)) for i in range(20)]
    df = _make_df(spark, rows)
    check = row_count_check(min_rows=10)
    assert check.check_fn(df) is True


def test_row_count_check_fails(spark: SparkSession) -> None:
    """5 rows with min_rows=10 should fail."""
    rows = [(i, "r", float(i)) for i in range(5)]
    df = _make_df(spark, rows)
    check = row_count_check(min_rows=10)
    assert check.check_fn(df) is False


# --- range_check ---


def test_range_check_passes(spark: SparkSession) -> None:
    """All values in [0, 50000] should pass."""
    rows = [
        (1, "r", 1500.0),
        (2, "r", 3200.0),
        (3, "r", 50000.0),
        (4, "r", 0.0),
    ]
    df = _make_df(spark, rows)
    check = range_check("rent", min_val=0, max_val=50000)
    assert check.check_fn(df) is True


def test_range_check_fails(spark: SparkSession) -> None:
    """A value outside [0, 50000] should fail."""
    rows = [
        (1, "r", 1500.0),
        (2, "r", -100.0),
        (3, "r", 50001.0),
    ]
    df = _make_df(spark, rows)
    check = range_check("rent", min_val=0, max_val=50000)
    assert check.check_fn(df) is False


# --- uniqueness_check ---


def test_uniqueness_check_passes(spark: SparkSession) -> None:
    """No duplicates on [id] should pass."""
    rows = [(1, "a", 100.0), (2, "b", 200.0), (3, "c", 300.0)]
    df = _make_df(spark, rows)
    check = uniqueness_check(["id"])
    assert check.check_fn(df) is True


def test_uniqueness_check_fails(spark: SparkSession) -> None:
    """Duplicate id values should fail."""
    rows = [(1, "a", 100.0), (1, "a", 100.0), (2, "b", 200.0)]
    df = _make_df(spark, rows)
    check = uniqueness_check(["id"])
    assert check.check_fn(df) is False


# --- run_quality_checks ---


def test_run_checks_raises_on_fail_severity(spark: SparkSession) -> None:
    """A failing check with severity='fail' should raise DataQualityError."""
    rows = [(i, "r", float(i)) for i in range(5)]
    df = _make_df(spark, rows)
    checks = [row_count_check(min_rows=100)]  # 5 < 100 → fail

    with pytest.raises(DataQualityError, match="1 DQ check"):
        run_quality_checks(df, checks)


def test_run_checks_warns_but_continues_on_warn_severity(
    spark: SparkSession,
) -> None:
    """A failing check with severity='warn' should not raise, just warn."""
    rows = [(1, "a", 100.0), (1, "a", 100.0)]
    df = _make_df(spark, rows)
    checks = [uniqueness_check(["id"])]  # severity="warn"

    summary = run_quality_checks(df, checks)

    assert summary["total"] == 1
    assert summary["passed"] == 0
    assert summary["warnings"] == 1
    assert summary["failed"] == 0
