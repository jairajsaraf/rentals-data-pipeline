"""Data quality checks for the ZORI rental pipeline.

Validates null percentages, row counts, and value ranges
against configurable thresholds from pipeline.yaml.
"""

from typing import Any

from pyspark.sql import DataFrame


def check_null_percentage(df: DataFrame, threshold: float) -> dict[str, Any]:
    """Check that null percentage per column does not exceed threshold.

    Args:
        df: DataFrame to validate.
        threshold: Maximum allowed null fraction (0.0–1.0).

    Returns:
        Dict with column-level null percentages and pass/fail status.
    """
    raise NotImplementedError


def check_row_count(df: DataFrame, min_rows: int) -> dict[str, Any]:
    """Validate that the DataFrame has at least min_rows rows.

    Args:
        df: DataFrame to validate.
        min_rows: Minimum required row count.

    Returns:
        Dict with actual row count and pass/fail status.
    """
    raise NotImplementedError


def check_rent_range(df: DataFrame, rent_col: str, rent_range: list[int]) -> dict[str, Any]:
    """Assert all rent values fall within the specified range.

    Args:
        df: DataFrame to validate.
        rent_col: Name of the rent column.
        rent_range: Two-element list [min, max] for valid rent values.

    Returns:
        Dict with out-of-range count and pass/fail status.
    """
    raise NotImplementedError


def run_all_checks(df: DataFrame, config: dict[str, Any]) -> list[dict[str, Any]]:
    """Run all data quality checks against the given DataFrame.

    Args:
        df: DataFrame to validate.
        config: DQ thresholds from pipeline.yaml.

    Returns:
        List of check result dicts.
    """
    raise NotImplementedError
