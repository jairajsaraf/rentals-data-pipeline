"""Compare local PySpark layers with the Databricks run and write the parity report.

Usage (repo root, Spark-capable environment):
    PYTHONPATH=. python scripts/databricks_parity.py --remote-json data/remote/parity_remote.json

The remote JSON is written by ``databricks/fingerprint_task.py`` and pulled with
``databricks fs cp``. The report holds no workspace host, user or IDs.
"""

import argparse
import datetime
import hashlib
import json
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F

from jobs.data_quality import (
    DataQualityError,
    null_percentage_check,
    range_check,
    row_count_check,
    run_quality_checks,
    uniqueness_check,
)
from jobs.io_utils import load_config, read_raw_csv
from jobs.parity import diff_fingerprints, fingerprint
from jobs.transform import (
    add_mom_change,
    add_state_rank,
    create_spark_session,
    deduplicate,
    enforce_schema,
    unpivot_monthly,
)

_CONFIG_PATH = "config/pipeline.yaml"


def _sha256(path: str) -> str:
    """Hash a file in chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _remote_dq(events: list[dict[str, Any]]) -> tuple[dict[str, Any], int]:
    """Reduce event-log rows to bare table names, expectation counts and drops.

    Args:
        events: ``dq_events`` from the remote JSON. Dataset names carry the
            dev-prefixed schema, so only the last name part is kept.

    Returns:
        ``({table: {expectation: {"passed": n, "failed": n}}}, total dropped records)``.
    """
    expectations: dict[str, Any] = {}
    dropped = 0
    for e in events:
        table = e["dataset"].split(".")[-1]
        dropped += int(e["dropped_records"] or 0)
        for x in json.loads(e["expectations"] or "[]"):
            expectations.setdefault(table, {})[x["name"]] = {
                "passed": x["passed_records"],
                "failed": x["failed_records"],
            }
    return expectations, dropped


def main() -> None:
    """Run the local layers, diff them against the remote JSON and write the report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--remote-json", required=True)
    parser.add_argument("--out", default="results/databricks_parity.json")
    args = parser.parse_args()

    config = load_config(_CONFIG_PATH)
    dq, dbx = config["dq_thresholds"], config["databricks"]
    csv_path = config["local"]["raw_path"]
    remote = json.loads(Path(args.remote_json).read_text())

    spark = create_spark_session(app_name="zori-parity", master=config["spark"]["master"])
    try:
        # Same layer boundaries as databricks/pipeline.py; clean_nulls == the drop expectation.
        bronze = read_raw_csv(spark, csv_path)
        unpivoted = unpivot_monthly(enforce_schema(bronze))
        silver_long = unpivoted.filter(F.col("median_rent").isNotNull())
        silver_zori = deduplicate(silver_long)
        gold = add_state_rank(add_mom_change(silver_zori))
        layers = {
            "bronze_zori": bronze,
            "silver_long": silver_long,
            "silver_zori": silver_zori,
            "gold_zori": gold,
        }
        local = {
            "csv_sha256": _sha256(csv_path),
            "fingerprints": {name: fingerprint(df) for name, df in layers.items()},
        }
        unpivoted_rows = unpivoted.count()

        # The DAG's 4 checks, on the post-clean output the DAG validates.
        checks = [
            null_percentage_check("median_rent", dq["null_pct"]),
            row_count_check(dq["min_rows"]),
            range_check("median_rent", *dq["rent_range"]),
            uniqueness_check(dq["uniqueness_keys"]),
        ]
        try:
            local_dq: dict[str, Any] = run_quality_checks(gold, checks)
        except DataQualityError as exc:
            local_dq = {"error": str(exc)}
        local_spark = spark.version
    finally:
        spark.stop()

    diffs = diff_fingerprints(local, remote)
    remote_expectations, remote_dropped = _remote_dq(remote["dq_events"])
    lfp, rfp = local["fingerprints"], remote["fingerprints"]
    report = {
        "run_date": datetime.date.today().isoformat(),
        "csv_sha256": local["csv_sha256"],
        "source_url": dbx["source_url"],
        "runtimes": {
            "local": {"engine": "PySpark local[*] (Docker, Linux)", "spark_version": local_spark},
            "remote": {"engine": "Databricks Free Edition serverless", "spark_version": remote["spark_version"]},
        },
        "rows": {
            layer: {"local": lfp[layer]["rows"], "remote": rfp[layer]["rows"]} for layer in lfp
        },
        "row_checksum_match": {
            layer: lfp[layer]["row_checksum"] == rfp[layer].get("row_checksum") for layer in lfp if layer in rfp
        },
        "columns_match": {
            layer: {
                col: lfp[layer]["columns"][col] == rfp[layer]["columns"].get(col)
                for col in lfp[layer]["columns"]
            }
            for layer in lfp
            if layer in rfp
        },
        "dropped_records": {
            "local_unpivoted_rows": unpivoted_rows,
            "local": unpivoted_rows - lfp["silver_long"]["rows"],
            "remote": remote_dropped,
        },
        "duplicates_removed": {
            side: fp["silver_long"]["rows"] - fp["silver_zori"]["rows"]
            for side, fp in (("local", lfp), ("remote", rfp))
        },
        "dq": {"local": local_dq, "remote_expectations": remote_expectations},
        "diffs": diffs,
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
