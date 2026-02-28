"""PySpark transformation job for Zillow Observed Rent Index (ZORI) data.

Reads raw CSV from S3, applies rental market transformations
(unpivot, MoM change, ranking), and writes partitioned Parquet output.
"""

from pyspark.sql import DataFrame, SparkSession


def create_spark_session(app_name: str = "zori-transform") -> SparkSession:
    """Create and return a configured SparkSession.

    Args:
        app_name: Name for the Spark application.

    Returns:
        A configured SparkSession instance.
    """
    raise NotImplementedError


def unpivot_date_columns(df: DataFrame) -> DataFrame:
    """Unpivot wide-format date columns into long-format rows.

    Converts columns like '2020-01', '2020-02', ... into rows with
    'date' and 'rent' columns.

    Args:
        df: Raw ZORI DataFrame with date columns.

    Returns:
        Long-format DataFrame with date and rent columns.
    """
    raise NotImplementedError


def add_mom_rent_change(df: DataFrame) -> DataFrame:
    """Add month-over-month rent change using a window lag() function.

    Args:
        df: Long-format ZORI DataFrame.

    Returns:
        DataFrame with an added 'rent_change_mom' column.
    """
    raise NotImplementedError


def add_rent_ranking(df: DataFrame) -> DataFrame:
    """Add rent ranking per state/year using a window rank() function.

    Args:
        df: Long-format ZORI DataFrame.

    Returns:
        DataFrame with an added 'rent_rank' column.
    """
    raise NotImplementedError


def run_transform(config_path: str) -> None:
    """Execute the full transformation pipeline.

    Args:
        config_path: Path to pipeline.yaml configuration file.
    """
    raise NotImplementedError


if __name__ == "__main__":
    run_transform(config_path="config/pipeline.yaml")
