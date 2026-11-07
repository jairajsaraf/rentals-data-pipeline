"""Lakeflow (Spark Declarative) pipeline for ZORI: bronze -> silver -> gold.

Reuses the pure transforms in ``jobs.transform`` unchanged. The pipeline root
is the bundle root, which Databricks appends to ``sys.path``, so ``jobs`` and
``config/`` resolve without packaging. DQ checks from ``config/pipeline.yaml``
become expectations; see docs/databricks-notes.md for the mapping.

``spark`` is injected by the pipeline runtime.
"""

import sys
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.io_utils import load_config, read_raw_csv
from jobs.transform import (
    add_mom_change,
    add_state_rank,
    deduplicate,
    enforce_schema,
    unpivot_monthly,
)

# __file__ is not defined in pipeline source files (observed), but the pipeline
# root is on sys.path (that is how `jobs` imports), so find config/ there.
_CONFIG_PATH = next(
    p / "config" / "pipeline.yaml"
    for p in map(Path, sys.path)
    if (p / "config" / "pipeline.yaml").is_file()
)
_CONFIG = load_config(str(_CONFIG_PATH))
_DBX = _CONFIG["databricks"]
_DQ = _CONFIG["dq_thresholds"]
_RENT_MIN, _RENT_MAX = _DQ["rent_range"]
_KEYS = _DQ["uniqueness_keys"]
_DEFAULT_CSV_PATH = (
    f"/Volumes/{_DBX['catalog']}/{_DBX['schema']}/{_DBX['volume']}/{_DBX['csv_name']}"
)


@dp.materialized_view()
def bronze_zori() -> DataFrame:
    """Raw wide-format CSV from the UC volume, read with the explicit schema."""
    # The bundle passes the resolved path (dev mode may prefix the schema name).
    csv_path = spark.conf.get("zori.csv_path", _DEFAULT_CSV_PATH)  # noqa: F821
    return read_raw_csv(spark, csv_path)  # noqa: F821


@dp.materialized_view()
@dp.expect_or_drop("median_rent_not_null", "median_rent IS NOT NULL")
def silver_long() -> DataFrame:
    """Long format; null rents dropped (replaces ``clean_nulls``)."""
    return unpivot_monthly(enforce_schema(spark.read.table("bronze_zori")))  # noqa: F821


@dp.materialized_view()
def silver_zori() -> DataFrame:
    """Deduplicated long format."""
    return deduplicate(spark.read.table("silver_long"))  # noqa: F821


@dp.materialized_view()
@dp.expect_or_fail("rent_in_range", f"median_rent BETWEEN {_RENT_MIN} AND {_RENT_MAX}")
def gold_zori() -> DataFrame:
    """Month-over-month change and per-state rent rank."""
    return add_state_rank(add_mom_change(spark.read.table("silver_zori")))  # noqa: F821


@dp.materialized_view()
@dp.expect_or_fail("min_row_count", f"row_count >= {_DQ['min_rows']}")
@dp.expect_or_fail("null_pct_within_threshold", f"null_pct <= {_DQ['null_pct']}")
@dp.expect("no_duplicate_keys", "duplicate_count = 0")
def dq_gold_metrics() -> DataFrame:
    """One-row aggregate over gold so row-level expectations can check table-level rules.

    A failing expectation here does not block ``gold_zori`` (documented behaviour).
    """
    gold = spark.read.table("gold_zori")  # noqa: F821
    return gold.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.coalesce(F.avg(F.col("median_rent").isNull().cast("double")), F.lit(0.0)).alias("null_pct"),
        (F.count(F.lit(1)) - F.countDistinct(*_KEYS)).alias("duplicate_count"),
    )
