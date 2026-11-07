"""Job task: fingerprint pipeline outputs and collect DQ metrics for the parity check.

Runs after the pipeline refresh succeeds. Writes one JSON file to ``--out``
(a UC volume path) holding no workspace host or IDs.
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

_EVENT_SQL = """
SELECT
  origin.flow_name AS dataset,
  details:flow_progress.data_quality.dropped_records AS dropped_records,
  details:flow_progress.data_quality.expectations AS expectations
FROM event_log('{pipeline_id}')
WHERE event_type = 'flow_progress'
  AND details:flow_progress.status = 'COMPLETED'
  AND origin.update_id = (
    SELECT origin.update_id FROM event_log('{pipeline_id}')
    WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1
  )
"""


def _sha256(path: str) -> str:
    """Hash a file in chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    """Build and write the remote fingerprint JSON."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, help="Bundle files root (for `jobs` imports)")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--csv-path", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.root)  # __file__ is not reliable in job tasks
    from jobs.parity import fingerprint

    spark = SparkSession.builder.getOrCreate()
    cat, sch = args.catalog, args.schema

    events = [r.asDict() for r in spark.sql(_EVENT_SQL.format(pipeline_id=args.pipeline_id)).collect()]
    result = {
        "spark_version": spark.version,
        "csv_sha256": _sha256(args.csv_path),
        "fingerprints": {
            name: fingerprint(spark.read.table(f"{cat}.{sch}.{name}"))
            for name in ("bronze_zori", "silver_long", "silver_zori", "gold_zori")
        },
        "dq_events": events,
    }
    Path(args.out).write_text(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
