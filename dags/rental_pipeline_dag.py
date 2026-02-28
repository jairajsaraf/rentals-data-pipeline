"""Airflow DAG for the ZORI rental data pipeline.

DAG: rental_market_etl
Schedule: @weekly
Pipeline: download → transform → quality checks → load to DuckDB

Orchestrates the end-to-end ETL for Zillow Observed Rent Index data.
Raw CSVs are downloaded, transformed via PySpark (unpivot, window
functions), validated against configurable DQ thresholds, and loaded
into a DuckDB analytical table from partitioned Parquet output.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

_CONFIG_PATH = "config/pipeline.yaml"

default_args = {
    "owner": "jairajsaraf",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _on_failure_callback(context: dict) -> None:
    """Log task failure details for alerting and debugging.

    Args:
        context: Airflow context dict passed on task failure.
    """
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    logger.error(
        "Task failed: dag=%s task=%s execution_date=%s error=%s",
        dag_id,
        task_id,
        execution_date,
        exception,
    )


@dag(
    dag_id="rental_market_etl",
    schedule="@weekly",
    default_args=default_args,
    catchup=False,
    tags=["rental", "etl", "pyspark"],
    on_failure_callback=_on_failure_callback,
)
def rental_pipeline_dag() -> None:
    """Rental market ETL pipeline DAG."""

    @task()
    def download_data() -> str:
        """Download raw ZORI CSV from Zillow.

        Returns:
            The S3 raw path where data was staged.
        """
        from jobs.io_utils import load_config

        config = load_config(_CONFIG_PATH)
        raw_path = config["s3"]["raw_path"]
        logger.info("Downloading ZORI data from Zillow to %s", raw_path)
        # TODO: implement actual download via requests/boto3
        return raw_path

    @task()
    def run_transforms(raw_path: str) -> str:
        """Run PySpark transform pipeline on raw data.

        Args:
            raw_path: S3 path to raw CSV input.

        Returns:
            The S3 processed path where Parquet was written.
        """
        from jobs.io_utils import load_config, read_raw_csv, write_processed
        from jobs.transform import create_spark_session, run_pipeline

        config = load_config(_CONFIG_PATH)
        spark = create_spark_session()
        try:
            raw_df = read_raw_csv(spark, raw_path)
            result_df = run_pipeline(raw_df)
            processed_path = config["s3"]["processed_path"]
            write_processed(result_df, processed_path)
            logger.info("Transforms complete, output at %s", processed_path)
        finally:
            spark.stop()
        return processed_path

    @task()
    def run_dq_checks(processed_path: str) -> str:
        """Run data quality checks on transformed output.

        Args:
            processed_path: S3 path to processed Parquet.

        Returns:
            The processed path (pass-through for downstream tasks).

        Raises:
            DataQualityError: If any fail-severity check does not pass.
        """
        from jobs.data_quality import (
            null_percentage_check,
            range_check,
            row_count_check,
            run_quality_checks,
            uniqueness_check,
        )
        from jobs.io_utils import load_config
        from jobs.transform import create_spark_session

        config = load_config(_CONFIG_PATH)
        dq = config["dq_thresholds"]
        spark = create_spark_session()
        try:
            df = spark.read.parquet(processed_path)
            checks = [
                null_percentage_check("median_rent", dq["null_pct"]),
                row_count_check(dq["min_rows"]),
                range_check("median_rent", *dq["rent_range"]),
                uniqueness_check(dq["uniqueness_keys"]),
            ]
            summary = run_quality_checks(df, checks)
            logger.info("DQ checks passed: %s", summary)
        finally:
            spark.stop()
        return processed_path

    @task()
    def load_to_duckdb(processed_path: str) -> None:
        """Load processed Parquet data into a DuckDB analytical table.

        Args:
            processed_path: S3 path to processed Parquet.
        """
        import duckdb

        con = duckdb.connect("rental_market.duckdb")
        try:
            con.execute(
                """
                CREATE OR REPLACE TABLE zori_rent AS
                SELECT * FROM read_parquet($1)
                """,
                [f"{processed_path}/**/*.parquet"],
            )
            row_count = con.execute("SELECT count(*) FROM zori_rent").fetchone()[0]
            logger.info("Loaded %d rows into DuckDB zori_rent table", row_count)
        finally:
            con.close()

    raw = download_data()
    processed = run_transforms(raw)
    validated = run_dq_checks(processed)
    load_to_duckdb(validated)


rental_pipeline_dag()
