"""I/O utility functions for reading and writing rental pipeline data.

Handles CSV ingestion from S3 or local paths, partitioned Parquet
output, and pipeline configuration loading.
"""

import re
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

_DATE_COL_PATTERN = re.compile(r"^\d{4}-\d{2}(-\d{2})?$")

_FIXED_FIELDS: dict[str, Any] = {
    "RegionID": IntegerType(),
    "SizeRank": IntegerType(),
    "RegionName": StringType(),
    "RegionType": StringType(),
    "StateName": StringType(),
}


def _build_zori_schema(columns: list[str]) -> StructType:
    """Build an explicit StructType for ZORI CSV columns.

    Fixed columns (RegionID, SizeRank, etc.) are mapped to their
    known types. Monthly date columns are typed as DoubleType.
    Any unrecognized columns default to StringType.

    Args:
        columns: List of column names from the CSV header.

    Returns:
        A StructType matching the ZORI CSV layout.
    """
    fields: list[StructField] = []
    for col in columns:
        if col in _FIXED_FIELDS:
            fields.append(StructField(col, _FIXED_FIELDS[col], nullable=True))
        elif _DATE_COL_PATTERN.match(col):
            fields.append(StructField(col, DoubleType(), nullable=True))
        else:
            fields.append(StructField(col, StringType(), nullable=True))
    return StructType(fields)


def load_config(config_path: str) -> dict[str, Any]:
    """Load and return pipeline configuration from a YAML file.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        Parsed configuration dictionary.
    """
    with open(config_path) as f:
        return yaml.safe_load(f)


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a ZORI CSV file into a Spark DataFrame with explicit schema.

    Discovers column names from the CSV header, then builds a typed
    StructType (no inferSchema) and re-reads with it. Supports both
    S3 (s3a://) and local (file://) paths.

    Args:
        spark: Active SparkSession.
        path: Path to the CSV file (s3a:// or file://).

    Returns:
        Raw DataFrame with explicitly typed columns.
    """
    header_df = spark.read.option("header", "true").csv(path)
    schema = _build_zori_schema(header_df.columns)
    return spark.read.option("header", "true").schema(schema).csv(path)


def write_processed(df: DataFrame, path: str) -> None:
    """Write a transformed DataFrame as partitioned Parquet.

    Extracts a 'year' column from the 'month' column, then writes
    with Snappy compression partitioned by (StateName, year) in
    overwrite mode. Supports both S3 and local paths.

    Args:
        df: Transformed ZORI DataFrame (must have 'month' and
            'StateName' columns).
        path: Destination path (s3a:// or file://).
    """
    (
        df.withColumn("year", F.year("month"))
        .coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("StateName", "year")
        .parquet(path)
    )
