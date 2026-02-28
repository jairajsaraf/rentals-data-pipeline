"""Tests for jobs.transform module."""

import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from jobs.transform import (
    add_mom_change,
    add_state_rank,
    clean_nulls,
    deduplicate,
    enforce_schema,
    run_pipeline,
    unpivot_monthly,
)


def _d(year: int, month: int) -> datetime.date:
    """Shorthand to build a first-of-month date."""
    return datetime.date(year, month, 1)


def _make_wide_df(spark: SparkSession) -> DataFrame:
    """Create a small wide-format DataFrame mimicking raw ZORI CSV.

    2 regions × 3 months, with 1 null rent value (SF 2024-03).
    Includes SizeRank and RegionType columns that should be dropped
    by enforce_schema.
    """
    rows = [
        ("102001", "1", "New York", "msa", "NY", "3450.0", "3475.0", "3520.0"),
        ("394913", "2", "San Francisco", "msa", "CA", "3150.0", "3200.0", None),
    ]
    return spark.createDataFrame(
        rows,
        ["RegionID", "SizeRank", "RegionName", "RegionType", "StateName",
         "2024-01", "2024-02", "2024-03"],
    )


_LONG_SCHEMA = StructType(
    [
        StructField("RegionID", IntegerType()),
        StructField("RegionName", StringType()),
        StructField("StateName", StringType()),
        StructField("month", DateType()),
        StructField("median_rent", DoubleType()),
    ]
)


def test_unpivot_correct_shape(spark: SparkSession) -> None:
    """Verify row count and column names after unpivoting wide-format data."""
    wide_df = _make_wide_df(spark)
    result = unpivot_monthly(enforce_schema(wide_df))

    assert result.count() == 6  # 2 regions × 3 months
    assert set(result.columns) == {
        "RegionID", "RegionName", "StateName", "month", "median_rent",
    }


def test_clean_nulls_removes_null_rents(sample_df: DataFrame) -> None:
    """Verify all null median_rent rows are removed."""
    result = clean_nulls(sample_df)

    assert result.count() == 17  # 19 total - 2 nulls
    assert result.filter(F.col("median_rent").isNull()).count() == 0


def test_deduplicate_removes_dupes(sample_df: DataFrame) -> None:
    """Verify duplicate row on (RegionID, month) is removed."""
    result = deduplicate(sample_df)

    assert result.count() == 18  # 19 total - 1 duplicate
    distinct_keys = result.select("RegionID", "month").distinct().count()
    assert distinct_keys == result.count()


def test_mom_change_calculation(spark: SparkSession) -> None:
    """Test MoM % change with hand-calculated expected values.

    Region 1, 3 months:
      2024-01: rent=2000 → MoM=null (no prior)
      2024-02: rent=2100 → MoM=((2100-2000)/2000)*100 = 5.0
      2024-03: rent=2079 → MoM=((2079-2100)/2100)*100 = -1.0
    """
    rows = [
        (1, "TestCity", "TS", _d(2024, 1), 2000.0),
        (1, "TestCity", "TS", _d(2024, 2), 2100.0),
        (1, "TestCity", "TS", _d(2024, 3), 2079.0),
    ]
    df = spark.createDataFrame(rows, schema=_LONG_SCHEMA)
    result = add_mom_change(df).orderBy("month").collect()

    assert result[0]["rent_change_mom"] is None
    assert result[1]["rent_change_mom"] == 5.0
    assert result[2]["rent_change_mom"] == -1.0


def test_state_rank_ordering(spark: SparkSession) -> None:
    """Verify highest rent gets rank 1 within the same state and month."""
    rows = [
        (1, "Buffalo", "NY", _d(2024, 1), 1500.0),
        (2, "New York", "NY", _d(2024, 1), 3500.0),
        (3, "Albany", "NY", _d(2024, 1), 1800.0),
    ]
    df = spark.createDataFrame(rows, schema=_LONG_SCHEMA)
    result = add_state_rank(df)

    ranks = {
        row["RegionName"]: row["state_rent_rank"]
        for row in result.collect()
    }
    assert ranks["New York"] == 1
    assert ranks["Albany"] == 2
    assert ranks["Buffalo"] == 3


def test_run_pipeline_end_to_end(spark: SparkSession) -> None:
    """Run full pipeline on wide-format data and verify output properties."""
    wide_df = _make_wide_df(spark)
    result = run_pipeline(wide_df)

    # Expected columns
    expected_cols = {
        "RegionID", "RegionName", "StateName", "month",
        "median_rent", "rent_change_mom", "state_rent_rank",
    }
    assert set(result.columns) == expected_cols

    # No nulls in median_rent
    assert result.filter(F.col("median_rent").isNull()).count() == 0

    # No duplicates on (RegionID, month)
    total = result.count()
    distinct_keys = result.select("RegionID", "month").distinct().count()
    assert distinct_keys == total

    # First month per region has null rent_change_mom
    w = Window.partitionBy("RegionID").orderBy("month")
    with_rn = result.withColumn("_rn", F.row_number().over(w))
    first_months = with_rn.filter(F.col("_rn") == 1)
    assert first_months.filter(F.col("rent_change_mom").isNotNull()).count() == 0
