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
