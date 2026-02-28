"""Data quality checks for the ZORI rental pipeline.

Provides a configurable DQ framework built around a DataQualityCheck
dataclass. Factory functions create check instances from pipeline.yaml
thresholds, and run_quality_checks executes them with logging and
fail-severity enforcement.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when one or more fail-severity DQ checks do not pass."""


@dataclass
class DataQualityCheck:
    """A single data quality assertion.

    Attributes:
        name: Short identifier for the check.
        check_fn: Callable that takes a DataFrame and returns True if passed.
        severity: "warn" logs a warning; "fail" raises DataQualityError.
        description: Human-readable explanation of what is validated.
    """

    name: str
    check_fn: Callable[[DataFrame], bool]
    severity: Literal["warn", "fail"]
    description: str


def null_percentage_check(column: str, threshold: float) -> DataQualityCheck:
    """Create a check that fails if null percentage exceeds threshold.

    Args:
        column: Column name to inspect.
        threshold: Maximum allowed null fraction (0.0–1.0).

    Returns:
        A DataQualityCheck instance.
    """

    def _check(df: DataFrame) -> bool:
        total = df.count()
        if total == 0:
            return True
        null_count = df.filter(F.col(column).isNull()).count()
        null_pct = null_count / total
        logger.info(
            "null_percentage_check(%s): %.4f (threshold: %.4f)",
            column,
            null_pct,
            threshold,
        )
        return null_pct <= threshold

    return DataQualityCheck(
        name=f"null_pct_{column}",
        check_fn=_check,
        severity="fail",
        description=f"Null % in '{column}' must be <= {threshold:.1%}",
    )


def row_count_check(min_rows: int) -> DataQualityCheck:
    """Create a check that fails if row count is below minimum.

    Args:
        min_rows: Minimum required row count.

    Returns:
        A DataQualityCheck instance.
    """

    def _check(df: DataFrame) -> bool:
        count = df.count()
        logger.info("row_count_check: %d (min: %d)", count, min_rows)
        return count >= min_rows

    return DataQualityCheck(
        name="row_count",
        check_fn=_check,
        severity="fail",
        description=f"Row count must be >= {min_rows}",
    )


def range_check(column: str, min_val: float, max_val: float) -> DataQualityCheck:
    """Create a check that fails if any values fall outside [min_val, max_val].

    Args:
        column: Column name to inspect.
        min_val: Minimum allowed value (inclusive).
        max_val: Maximum allowed value (inclusive).

    Returns:
        A DataQualityCheck instance.
    """

    def _check(df: DataFrame) -> bool:
        out_of_range = df.filter(
            (F.col(column) < min_val) | (F.col(column) > max_val)
        ).count()
        logger.info(
            "range_check(%s): %d out-of-range rows ([%s, %s])",
            column,
            out_of_range,
            min_val,
            max_val,
        )
        return out_of_range == 0

    return DataQualityCheck(
        name=f"range_{column}",
        check_fn=_check,
        severity="fail",
        description=f"All '{column}' values must be in [{min_val}, {max_val}]",
    )


def uniqueness_check(columns: list[str]) -> DataQualityCheck:
    """Create a check that fails if duplicates exist on the given columns.

    Args:
        columns: List of column names forming the composite key.

    Returns:
        A DataQualityCheck instance.
    """
    key_label = ", ".join(columns)

    def _check(df: DataFrame) -> bool:
        total = df.count()
        distinct = df.select(columns).distinct().count()
        duplicate_count = total - distinct
        logger.info(
            "uniqueness_check(%s): %d duplicates out of %d rows",
            key_label,
            duplicate_count,
            total,
        )
        return duplicate_count == 0

    return DataQualityCheck(
        name=f"unique_{'_'.join(columns)}",
        check_fn=_check,
        severity="warn",
        description=f"No duplicate rows on ({key_label})",
    )


def run_quality_checks(
    df: DataFrame, checks: list[DataQualityCheck]
) -> dict[str, Any]:
    """Execute all data quality checks and return a summary.

    Each check is run, logged as PASS/FAIL, and tallied. If any
    check with severity="fail" does not pass, a DataQualityError
    is raised after all checks complete.

    Args:
        df: DataFrame to validate.
        checks: List of DataQualityCheck instances to run.

    Returns:
        Summary dict with keys: total, passed, failed, warnings,
        and a details list of per-check results.

    Raises:
        DataQualityError: If any fail-severity check does not pass.
    """
    results: list[dict[str, Any]] = []
    passed = 0
    failed = 0
    warnings = 0

    for check in checks:
        check_passed = check.check_fn(df)
        status = "PASS" if check_passed else "FAIL"
        log_msg = "[%s] %s — %s (%s)", status, check.name, check.description, check.severity

        if check_passed:
            logger.info(*log_msg)
            passed += 1
        elif check.severity == "warn":
            logger.warning(*log_msg)
            warnings += 1
        else:
            logger.error(*log_msg)
            failed += 1

        results.append(
            {
                "name": check.name,
                "passed": check_passed,
                "severity": check.severity,
                "description": check.description,
            }
        )

    summary: dict[str, Any] = {
        "total": len(checks),
        "passed": passed,
        "failed": failed,
        "warnings": warnings,
        "details": results,
    }

    failed_checks = [r for r in results if not r["passed"] and r["severity"] == "fail"]
    if failed_checks:
        names = ", ".join(r["name"] for r in failed_checks)
        raise DataQualityError(
            f"{len(failed_checks)} DQ check(s) failed: {names}"
        )

    return summary
