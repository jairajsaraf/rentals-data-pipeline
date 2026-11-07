"""Tests for the order-independent DataFrame fingerprint used for parity checks."""

import pytest
from pyspark.sql import SparkSession

from jobs.parity import diff_fingerprints, fingerprint

_COLS = ["RegionID", "state", "median_rent"]


def _df(spark: SparkSession, rows: list[tuple]):
    return spark.createDataFrame(rows, schema=_COLS)


def test_fingerprint_order_independent(spark: SparkSession) -> None:
    """The same rows in a different order give an identical fingerprint."""
    rows = [(1, "NY", 3500.0), (2, "CA", 3200.0), (3, "TX", None), (4, "TX", 1700.0)]
    assert fingerprint(_df(spark, rows)) == fingerprint(_df(spark, rows[::-1]))


def test_fingerprint_detects_value_change(spark: SparkSession) -> None:
    """Changing one rent by 0.01 changes only that column's checksum."""
    base = [(1, "NY", 3500.0), (2, "CA", 3200.0)]
    changed = [(1, "NY", 3500.01), (2, "CA", 3200.0)]
    a, b = fingerprint(_df(spark, base)), fingerprint(_df(spark, changed))
    assert a["rows"] == b["rows"] == 2
    assert a["columns"]["median_rent"]["checksum"] != b["columns"]["median_rent"]["checksum"]
    assert a["columns"]["RegionID"] == b["columns"]["RegionID"]
    assert a["columns"]["state"] == b["columns"]["state"]


def test_fingerprint_detects_swapped_rows(spark: SparkSession) -> None:
    """Swapping rents between two records keeps column checksums but changes the row checksum."""
    base = [(1, "NY", 3500.0), (2, "CA", 3200.0)]
    swapped = [(1, "NY", 3200.0), (2, "CA", 3500.0)]
    a, b = fingerprint(_df(spark, base)), fingerprint(_df(spark, swapped))
    assert a["columns"] == b["columns"]
    assert a["row_checksum"] != b["row_checksum"]


def test_fingerprint_counts_nulls(spark: SparkSession) -> None:
    """Null counts are reported per column."""
    rows = [(1, "NY", None), (2, "CA", 3200.0), (3, None, None)]
    cols = fingerprint(_df(spark, rows))["columns"]
    assert cols["median_rent"]["nulls"] == 2
    assert cols["state"]["nulls"] == 1
    assert cols["RegionID"]["nulls"] == 0


def _side(sha: str, checksum: int, row_checksum: int | None = 7) -> dict:
    cols = {"RegionID": {"nulls": 0, "checksum": 10}, "median_rent": {"nulls": 0, "checksum": checksum}}
    fp = {"rows": 2, "columns": cols}
    if row_checksum is not None:
        fp["row_checksum"] = row_checksum
    return {"csv_sha256": sha, "fingerprints": {"gold_zori": fp}}


def test_diff_reports_column_delta() -> None:
    """A checksum delta is reported for that column only; equal sides give no diffs."""
    assert diff_fingerprints(_side("a", 5), _side("a", 5)) == []
    assert diff_fingerprints(_side("a", 5), _side("a", 2**70)) == [
        {"layer": "gold_zori", "column": "median_rent", "field": "checksum", "local": 5, "remote": 2**70}
    ]


def test_diff_reports_row_checksum_delta_and_missing() -> None:
    """A row-checksum delta, or a side without one, is reported."""
    assert diff_fingerprints(_side("a", 5, 7), _side("a", 5, 8)) == [
        {"layer": "gold_zori", "column": None, "field": "row_checksum", "local": 7, "remote": 8}
    ]
    assert diff_fingerprints(_side("a", 5, 7), _side("a", 5, None))[0]["remote"] is None


def test_refuses_on_sha_mismatch() -> None:
    """Fingerprints of different input files are not compared."""
    with pytest.raises(ValueError, match="csv_sha256"):
        diff_fingerprints(_side("a", 5), _side("b", 5))
