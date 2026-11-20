# ADR 0003: DuckDB plus dbt as the analytical layer

- Status: Accepted (recorded after the fact; the code already did this)
- Date: 2026-11-19

## Context

After the PySpark transforms and data quality checks, the data needs a place
where analytical queries can run without a cluster, with tested, documented
models on top.

## Decision

Load the partitioned Parquet into a local DuckDB file as the table `zori_rent`
(`jobs/load_duckdb.py`, `CREATE OR REPLACE TABLE`), then build models with
`dbt-duckdb` (`dbt/`): one staging view (`stg_zori_rent`) and two mart tables
(`mart_state_rent_summary`, `mart_rent_trends`). Consumers read the `mart_*`
tables, not raw `zori_rent`. dbt runs `not_null` tests and two singular tests
(no future months, unique region-month).

The DuckDB load and the dbt run share one Airflow task. `dbt/profiles.yml` points
at a fixed file path, so both steps must run on the same filesystem (see the
docstring in `dags/rental_pipeline_dag.py`).

## Consequences

- Everything runs on one machine with no warehouse to provision.
- The DuckDB file is rebuilt from Parquet on every load, so it is a cache of the
  Parquet output, not a source of truth.
- Running the DAG on workers that do not share a volume would break the
  colocated task. Splitting it would need a shared or remote DuckDB location.
