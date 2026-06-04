.PHONY: test lint run-local clean dbt-run dbt-test dbt-docs pipeline-local

test:
	python -m pytest tests/ -v --tb=short

lint:
	ruff check jobs/ dags/ tests/

run-local:
	spark-submit jobs/transform.py --local

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

dbt-docs:
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

pipeline-local: run-local
	python jobs/load_duckdb.py --parquet data/processed/zori/ --db data/rental_market.duckdb
	$(MAKE) dbt-run
	$(MAKE) dbt-test

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +
	rm -rf .coverage htmlcov/
	rm -rf dbt/target dbt/dbt_packages dbt/logs
