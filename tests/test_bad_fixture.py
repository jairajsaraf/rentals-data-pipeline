"""Failure fixture: one null rent, one rent above range, one duplicated key.

Expected (see docs/demo): clean_nulls and deduplicate remove the null and the
duplicate before DQ, so only the range check fails.
"""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from jobs.data_quality import (
    DataQualityError,
    null_percentage_check,
    range_check,
    row_count_check,
    run_quality_checks,
    uniqueness_check,
)
from jobs.io_utils import load_config, read_raw_csv
from jobs.transform import run_pipeline

_ROOT = Path(__file__).resolve().parent.parent
_FIXTURE = str(_ROOT / "tests" / "fixtures" / "bad_zori.csv")


def test_bad_fixture_fails_only_range_check(spark: SparkSession) -> None:
    dq = load_config(str(_ROOT / "config" / "pipeline.yaml"))["dq_thresholds"]
    df = run_pipeline(read_raw_csv(spark, _FIXTURE))

    # 32 ZIPs x 5 months = 160, minus 1 null rent; duplicate ZIP row removed.
    assert df.count() == 159

    checks = [
        null_percentage_check("median_rent", dq["null_pct"]),
        row_count_check(dq["min_rows"]),
        range_check("median_rent", *dq["rent_range"]),
        uniqueness_check(dq["uniqueness_keys"]),
    ]
    with pytest.raises(DataQualityError, match=r"1 DQ check\(s\) failed: range_median_rent"):
        run_quality_checks(df, checks)
