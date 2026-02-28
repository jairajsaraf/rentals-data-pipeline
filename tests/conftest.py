"""Shared pytest fixtures for the rental pipeline test suite."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession for testing.

    Yields:
        A SparkSession configured for local testing.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("zori-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
