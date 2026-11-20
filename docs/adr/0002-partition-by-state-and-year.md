# ADR 0002: Partition processed Parquet by state and year

- Status: Accepted (recorded after the fact; the code already did this)
- Date: 2026-11-19

## Context

The transform output is a long table of ZIP-level monthly rents. Consumers
(DuckDB, dbt) filter and aggregate by state and by time. The write path is
`write_processed` in `jobs/io_utils.py`.

## Decision

Write Parquet with Snappy compression, partitioned by `(StateName, year)`, where
`year` is derived from the `month` column. The partition keys are listed in
`config/pipeline.yaml` under `partitioning.partition_keys`. The write mode is
`overwrite`.

## Consequences

- A query that filters on state or year reads only the matching directories.
- The loader reads the whole tree with one Parquet glob, so the partitioning does
  not change how DuckDB loads it (`jobs/load_duckdb.py`).
- The writer calls `.coalesce(1)` first, so one task writes the data. That is
  fine for the single ZORI CSV used here and would be the first thing to change
  for a much larger input.
- `overwrite` replaces the whole output path on each run. There is no incremental
  append.
