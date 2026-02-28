"""I/O utility functions for reading and writing rental pipeline data.

Handles CSV ingestion from S3 and partitioned Parquet output.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession


def read_csv_from_s3(spark: SparkSession, path: str) -> DataFrame:
    """Read a CSV file from S3 into a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        path: S3 path to the CSV file.

    Returns:
        Raw DataFrame from the CSV source.
    """
    raise NotImplementedError


def write_parquet_to_s3(
    df: DataFrame, path: str, partition_keys: list[str]
) -> None:
    """Write a DataFrame as partitioned Parquet to S3.

    Args:
        df: DataFrame to write.
        path: S3 destination path.
        partition_keys: Columns to partition by.
    """
    raise NotImplementedError


def load_config(config_path: str) -> dict[str, Any]:
    """Load and return pipeline configuration from a YAML file.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        Parsed configuration dictionary.
    """
    raise NotImplementedError
