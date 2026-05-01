# CLAUDE.md

## PROJECT OVERVIEW

- PySpark rental market ETL pipeline for portfolio/resume
- Architecture: Raw CSV (S3) → PySpark transforms → Partitioned Parquet (S3) → DuckDB → Airflow DAG
- Target: interview-ready, production-flavored code

## TECH STACK

- Python 3.10+, PySpark 3.5+, Apache Airflow 2.x, AWS S3 (boto3), DuckDB, pytest, ruff, GitHub Actions

## CODE STANDARDS

- PySpark DataFrame API only (never RDD API)
- Type hints on all function signatures
- Google-style docstrings on all public functions
- Config values in config/pipeline.yaml, never hardcoded
- All transforms must be pure functions (DataFrame in → DataFrame out)
- Window functions preferred over groupBy + join where applicable

## PROJECT STRUCTURE

```
pyspark-rental-pipeline/
├── dags/rental_pipeline_dag.py
├── jobs/
│   ├── __init__.py
│   ├── transform.py
│   ├── data_quality.py
│   └── io_utils.py
├── tests/
│   ├── conftest.py
│   ├── test_transform.py
│   └── test_data_quality.py
├── config/pipeline.yaml
├── .github/workflows/ci.yml
├── Makefile
├── requirements.txt
└── README.md
```

## COMMON COMMANDS

- `make test` — run pytest
- `make lint` — run ruff
- `make run-local` — run transform job locally
- `spark-submit jobs/transform.py` — submit PySpark job

## KEY DESIGN DECISIONS

- Partition strategy: by state and year
- Window functions: MoM rent change via lag(), rent ranking via rank()
- Data quality: null % thresholds, row count validation, range assertions
- DQ checks are configurable via pipeline.yaml thresholds

## DBT TRANSFORMATION LAYER

- Project: `dbt/`, profile name `rental_market`, target `dev`
- Backend: dbt-duckdb against `data/rental_market.duckdb` (gitignored)
- Source: `main.zori_rent` — loaded by the Airflow `load_to_duckdb` task from
  partitioned Parquet
- Models:
  - `stg_zori_rent` (view) — typed, snake_cased clean staging
  - `mart_state_rent_summary` (table) — state × month rent aggregates
  - `mart_rent_trends` (table) — 12-month rolling avg per region
- Tests: column-level `not_null` in `dbt/models/**/*.yml`, singular tests in
  `dbt/tests/` (no future months, composite-key uniqueness)
- Run order: download → transform → DQ checks → load_to_duckdb → dbt run →
  dbt test
- **Consumers**: any new analytical query (BI tool, notebook, future API)
  should prefer `mart_*` tables over the raw `zori_rent` source.

## DBT COMMANDS

- `make dbt-run` — build all dbt models
- `make dbt-test` — run all dbt tests (column-level + singular)
- `make dbt-docs` — generate and serve dbt documentation site
- `make pipeline-local` — full local run: PySpark transform → load DuckDB →
  dbt run → dbt test
