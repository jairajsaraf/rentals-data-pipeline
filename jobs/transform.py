"""PySpark transformation job for Zillow Observed Rent Index (ZORI) data.

Reads raw CSV from S3, applies rental market transformations
(unpivot, MoM change, ranking), and writes partitioned Parquet output.
"""

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window

_DATE_COL_PATTERN = re.compile(r"^\d{4}-\d{2}$")


def create_spark_session(app_name: str = "zori-transform") -> SparkSession:
    """Create and return a configured SparkSession.

    Args:
        app_name: Name for the Spark application.

    Returns:
        A configured SparkSession instance.
    """
    raise NotImplementedError


def enforce_schema(df: DataFrame) -> DataFrame:
    """Select needed columns and cast types explicitly.

    Keeps RegionID, RegionName, StateName, and all monthly date columns
    (detected dynamically via regex). Drops SizeRank, RegionType, and
    any other extraneous columns.

    Args:
        df: Raw ZORI DataFrame as read from CSV.

    Returns:
        DataFrame with only the needed columns, explicitly typed.
    """
    date_cols = [c for c in df.columns if _DATE_COL_PATTERN.match(c)]
    return df.select(
        F.col("RegionID").cast(IntegerType()),
        F.col("RegionName").cast(StringType()),
        F.col("StateName").cast(StringType()),
        *[F.col(f"`{c}`").cast(DoubleType()) for c in date_cols],
    )


def unpivot_monthly(df: DataFrame) -> DataFrame:
    """Melt wide-format monthly columns into long-format rows.

    Converts columns like '2015-01', '2015-02', ... into rows with
    'month' (DateType) and 'median_rent' (DoubleType) columns using
    a SQL stack() expression.

    Args:
        df: Schema-enforced ZORI DataFrame with date columns.

    Returns:
        Long-format DataFrame with columns: RegionID, RegionName,
        StateName, month, median_rent.
    """
    date_cols = [c for c in df.columns if _DATE_COL_PATTERN.match(c)]
    stack_entries = ", ".join(
        f"'{c}', `{c}`" for c in date_cols
    )
    stack_expr = f"stack({len(date_cols)}, {stack_entries}) as (month_str, median_rent)"
    return (
        df.selectExpr("RegionID", "RegionName", "StateName", stack_expr)
        .withColumn("month", F.to_date(F.col("month_str"), "yyyy-MM"))
        .drop("month_str")
    )


def clean_nulls(df: DataFrame) -> DataFrame:
    """Drop rows where median_rent is null.

    Many early months have null rent for regions that didn't yet
    have data — these are noise and should be removed before
    downstream window functions.

    Args:
        df: Long-format ZORI DataFrame.

    Returns:
        DataFrame with null median_rent rows removed.
    """
    return df.filter(F.col("median_rent").isNotNull())


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicate rows on (RegionID, month).

    Guards against upstream CSV issues where a region may appear
    more than once. Keeps the first occurrence.

    Args:
        df: Long-format ZORI DataFrame.

    Returns:
        DataFrame with duplicates removed.
    """
    return df.dropDuplicates(["RegionID", "month"])


def add_mom_change(df: DataFrame) -> DataFrame:
    """Add month-over-month percent rent change via lag() window function.

    Window: partitionBy(RegionID) orderBy(month).
    Formula: round(((median_rent - prev_rent) / prev_rent) * 100, 2).
    Result is null for the first month of each region.

    Args:
        df: Long-format ZORI DataFrame.

    Returns:
        DataFrame with an added 'rent_change_mom' column (percentage).
    """
    window = Window.partitionBy("RegionID").orderBy("month")
    prev_rent = F.lag("median_rent", 1).over(window)
    return df.withColumn(
        "rent_change_mom",
        F.round(((F.col("median_rent") - prev_rent) / prev_rent) * 100, 2),
    )


def add_state_rank(df: DataFrame) -> DataFrame:
    """Add rent ranking per state and month via rank() window function.

    Window: partitionBy(StateName, month) orderBy(median_rent DESC).
    Highest rent gets rank 1. Ties receive the same rank with gaps.

    Args:
        df: Long-format ZORI DataFrame.

    Returns:
        DataFrame with an added 'state_rent_rank' column.
    """
    window = Window.partitionBy("StateName", "month").orderBy(
        F.col("median_rent").desc()
    )
    return df.withColumn("state_rent_rank", F.rank().over(window))


def run_pipeline(df: DataFrame) -> DataFrame:
    """Chain all transforms in order and return the final DataFrame.

    Pipeline order:
        enforce_schema → unpivot_monthly → clean_nulls →
        deduplicate → add_mom_change → add_state_rank

    Args:
        df: Raw ZORI DataFrame as read from CSV.

    Returns:
        Fully transformed DataFrame ready for partitioned Parquet output.
    """
    return (
        df.transform(enforce_schema)
        .transform(unpivot_monthly)
        .transform(clean_nulls)
        .transform(deduplicate)
        .transform(add_mom_change)
        .transform(add_state_rank)
    )


if __name__ == "__main__":
    from jobs.io_utils import load_config, read_raw_csv, write_processed

    config = load_config("config/pipeline.yaml")
    spark = create_spark_session()
    raw_df = read_raw_csv(spark, config["s3"]["raw_path"])
    result_df = run_pipeline(raw_df)
    write_processed(result_df, config["s3"]["processed_path"])
