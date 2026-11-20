# ADR 0004: Data quality checks driven by config, with warn and fail severity

- Status: Accepted (recorded after the fact; the code already did this)
- Date: 2026-11-19

## Context

Bad input should stop the pipeline before it reaches DuckDB, but a soft problem
such as duplicate keys should not always do the same.

## Decision

Checks are built by small factory functions in `jobs/data_quality.py` and take
their thresholds from `dq_thresholds` in `config/pipeline.yaml`: null percentage,
minimum row count, value range and uniqueness keys. Each check carries a
severity:

- `fail` (null percentage, row count, range): `run_quality_checks` raises
  `DataQualityError` and the DAG stops before the DuckDB load.
- `warn` (uniqueness): the result is logged and counted in the summary, and the
  run continues.

## Consequences

- Changing a threshold is a config edit, not a code change. The range upper bound
  was raised to 150000 after the Databricks run (see ADR 0001); because the
  config is shared, that changed the local pipeline too.
- The summary dict (`total`, `passed`, `failed`, `warnings`) is what
  `results/databricks_parity.json` records for the local run.
- Severity is fixed per check factory, not per config entry.
