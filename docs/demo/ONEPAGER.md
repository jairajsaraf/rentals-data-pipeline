# Rental market ETL: one page

**Flow:** Raw CSV → PySpark transforms → partitioned Parquet (state, year) → DuckDB → dbt marts, orchestrated by Airflow. Optional second runtime on Databricks Free Edition (ADR 0001).

## Data quality: local check → Databricks expectation
Source: `docs/databricks-notes.md`.

| Local check | Databricks expectation | Action |
|---|---|---|
| null % ≤ 10% | `median_rent IS NOT NULL` (silver) + `null_pct <= 0.1` (aggregate) | drop / fail |
| row count ≥ 100 | `row_count >= 100` | fail |
| rent in [0, 150000] | `median_rent BETWEEN 0 AND 150000` | fail |
| unique (RegionID, month) | `duplicate_count = 0` | warn |

## Parity (`results/databricks_parity.json`, run 2026-11-07)
- Local PySpark 3.5.4 vs Databricks serverless 4.2.0: `diffs` is `[]`.
- Rows: bronze 8,340; silver_long, silver_zori, gold 469,420 each. Whole-row checksum matches.

## Failure drill (`tests/fixtures/bad_zori.csv`)
- Null rent + duplicate key removed before DQ; one rent of 200,000 fails the range check, locally and on Databricks.

## Timings (wall-clock, Free Edition serverless)
Real CSV: 136s and 155s. Failing fixture: 945s (retries).

## Not shown in this repo
Terraform/CloudFormation, Datadog/CloudWatch/Grafana, Scala, Glue, EMR, Redshift, Step Functions. S3 code paths exist but were not run end to end.
