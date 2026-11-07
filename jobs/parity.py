"""Order-independent DataFrame fingerprint for cross-runtime parity checks.

The same pure function runs locally (pytest, parity script) and inside
Databricks, so both sides produce comparable numbers.
"""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def fingerprint(df: DataFrame) -> dict[str, Any]:
    """Summarise a DataFrame as row count, row checksum, and per-column null count and checksum.

    Each checksum is the sum of ``xxhash64`` (over a column's values, or over
    the whole row for ``row_checksum``), so it does not depend on row order.
    The row checksum catches values attached to the wrong record, which
    independent column checksums cannot. Hashes are cast to ``decimal(38, 0)``
    before summing so the sum cannot overflow (ANSI mode on Databricks
    serverless would raise on a ``bigint`` overflow).

    Args:
        df: DataFrame to fingerprint.

    Returns:
        ``{"rows": int, "row_checksum": int, "columns": {name: {"nulls": int, "checksum": int}}}``.
    """
    row_hash = F.xxhash64(F.struct(*[F.col(f"`{name}`") for name in df.columns]))
    aggs = [
        F.count(F.lit(1)).alias("__rows"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("__row_checksum"),
    ]
    for i, name in enumerate(df.columns):
        col = F.col(f"`{name}`")
        aggs.append(F.sum(col.isNull().cast("int")).alias(f"n{i}"))
        aggs.append(F.sum(F.xxhash64(col).cast("decimal(38,0)")).alias(f"c{i}"))
    row = df.agg(*aggs).first()
    return {
        "rows": int(row["__rows"]),
        "row_checksum": int(row["__row_checksum"] or 0),
        "columns": {
            name: {
                "nulls": int(row[f"n{i}"] or 0),
                "checksum": int(row[f"c{i}"] or 0),
            }
            for i, name in enumerate(df.columns)
        },
    }


def diff_fingerprints(local: dict[str, Any], remote: dict[str, Any]) -> list[dict[str, Any]]:
    """List every difference between two runtimes' fingerprints.

    Args:
        local: ``{"csv_sha256": str, "fingerprints": {layer: fingerprint}}``.
        remote: Same shape, from the other runtime.

    Returns:
        One ``{"layer", "column", "field", "local", "remote"}`` dict per
        difference; ``field`` is ``rows``, ``row_checksum``, ``nulls``,
        ``checksum`` or ``present``. Empty when the two sides match exactly.

    Raises:
        ValueError: If the sides were computed from different input files.
    """
    if local["csv_sha256"] != remote["csv_sha256"]:
        raise ValueError(
            f"csv_sha256 mismatch: local {local['csv_sha256']} != remote {remote['csv_sha256']}"
        )
    diffs: list[dict[str, Any]] = []

    def add(layer: str, column: str | None, field: str, lv: Any, rv: Any) -> None:
        if lv != rv:
            diffs.append({"layer": layer, "column": column, "field": field, "local": lv, "remote": rv})

    lfp, rfp = local["fingerprints"], remote["fingerprints"]
    for layer in sorted(lfp.keys() | rfp.keys()):
        lf, rf = lfp.get(layer), rfp.get(layer)
        if lf is None or rf is None:
            add(layer, None, "present", lf is not None, rf is not None)
            continue
        add(layer, None, "rows", lf["rows"], rf["rows"])
        add(layer, None, "row_checksum", lf.get("row_checksum"), rf.get("row_checksum"))
        lc, rc = lf["columns"], rf["columns"]
        for col in sorted(lc.keys() | rc.keys()):
            if col not in lc or col not in rc:
                add(layer, col, "present", col in lc, col in rc)
                continue
            add(layer, col, "nulls", lc[col]["nulls"], rc[col]["nulls"])
            add(layer, col, "checksum", lc[col]["checksum"], rc[col]["checksum"])
    return diffs
