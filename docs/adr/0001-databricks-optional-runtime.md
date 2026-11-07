# ADR 0001: Databricks Free Edition as an optional second runtime

- Status: Accepted
- Date: 2026-11-06

## Context

The pipeline runs locally and under Airflow: PySpark transforms write partitioned
Parquet, DuckDB loads it, and dbt builds the marts. We wanted to show the same
transforms and data quality checks running on a managed Spark platform, and to
prove the two runtimes produce the same data.

Databricks Free Edition is the only managed option at no cost. Its terms and
limits (see [`docs/databricks-notes.md`](../databricks-notes.md)) rule it out as
the primary path:

- Non-commercial use only. No SLA and no reliability guarantees. Inactive
  accounts may be deleted.
- Fair-usage quotas: over quota, compute shuts down for the rest of the day (in
  extreme cases, the month).
- Serverless compute only, so the Spark version is whatever serverless ships
  (4.2.0 at the time of the parity run), not the pinned 3.5.4.
- Restricted outbound internet, so the CSV must be uploaded to a volume by hand.
- One active Lakeflow pipeline per type and one workspace per account.

## Decision

Databricks is an optional second runtime, not a replacement. Airflow, DuckDB and
dbt stay primary.

- One Declarative Automation Bundle (`databricks.yml`) deploys a schema, a
  volume, one serverless Lakeflow pipeline (`databricks/pipeline.py`) and a
  weekly job whose schedule is paused in dev mode.
- The pipeline reuses the pure transforms in `jobs/` and reads thresholds from
  `config/pipeline.yaml`, so both runtimes share one config.
- Local DQ checks map to pipeline expectations (mapping table in the notes).
- A fingerprint task writes per-layer row counts, a whole-row checksum and column checksums. The
  parity script (`scripts/databricks_parity.py`) diffs them against the local
  run and writes `results/databricks_parity.json`.

## Consequences

- Nothing in CI, the Makefile's main targets or the Airflow DAG depends on
  Databricks. A quota shutdown or a deleted account breaks only the optional path.
- Parity is checked, not assumed. The committed report shows `diffs: []`.
- Behaviour differs in one known place: a failing aggregate validation dataset
  (`dq_gold_metrics`) does not block `gold_zori`, while the local DAG stops before
  the DuckDB load. No workaround is in scope.
- Raising `dq_thresholds.rent_range` to `[0, 150000]` (found on the Databricks
  run, reproduced locally) changes the main pipeline too, since the config is shared.
- Failed pipeline updates retry within one job run and burn quota. Read the first
  error before rerunning.
