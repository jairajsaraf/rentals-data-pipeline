"""Shared pytest fixtures for the rental pipeline test suite."""

import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession for testing.

    Yields:
        A SparkSession configured for local testing.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


_LONG_SCHEMA = StructType(
    [
        StructField("RegionID", IntegerType(), nullable=False),
        StructField("RegionName", StringType(), nullable=False),
        StructField("StateName", StringType(), nullable=False),
        StructField("month", DateType(), nullable=False),
        StructField("median_rent", DoubleType(), nullable=True),
    ]
)


def _d(year: int, month: int) -> datetime.date:
    """Shorthand to build a date from year and month."""
    return datetime.date(year, month, 1)


@pytest.fixture()
def sample_df(spark: SparkSession):
    """Create a small long-format ZORI DataFrame for testing.

    3 regions x 6 months = 18 rows, plus:
    - 2 intentional nulls in median_rent (New York 2024-01, Austin 2024-03)
    - 1 intentional duplicate row (San Francisco 2024-02)
    Total: 19 rows before cleaning.

    Yields:
        A PySpark DataFrame mimicking transformed ZORI data.
    """
    rows = [
        # New York, NY — 6 months, 1 null
        (102001, "New York", "NY", _d(2024, 1), None),
        (102001, "New York", "NY", _d(2024, 2), 3450.0),
        (102001, "New York", "NY", _d(2024, 3), 3475.0),
        (102001, "New York", "NY", _d(2024, 4), 3520.0),
        (102001, "New York", "NY", _d(2024, 5), 3500.0),
        (102001, "New York", "NY", _d(2024, 6), 3510.0),
        # San Francisco, CA — 6 months + 1 duplicate
        (394913, "San Francisco", "CA", _d(2024, 1), 3150.0),
        (394913, "San Francisco", "CA", _d(2024, 2), 3200.0),
        (394913, "San Francisco", "CA", _d(2024, 2), 3200.0),  # duplicate
        (394913, "San Francisco", "CA", _d(2024, 3), 3180.0),
        (394913, "San Francisco", "CA", _d(2024, 4), 3250.0),
        (394913, "San Francisco", "CA", _d(2024, 5), 3275.0),
        (394913, "San Francisco", "CA", _d(2024, 6), 3300.0),
        # Austin, TX — 6 months, 1 null
        (394514, "Austin", "TX", _d(2024, 1), 1750.0),
        (394514, "Austin", "TX", _d(2024, 2), 1725.0),
        (394514, "Austin", "TX", _d(2024, 3), None),
        (394514, "Austin", "TX", _d(2024, 4), 1680.0),
        (394514, "Austin", "TX", _d(2024, 5), 1700.0),
        (394514, "Austin", "TX", _d(2024, 6), 1710.0),
    ]
    return spark.createDataFrame(rows, schema=_LONG_SCHEMA)
