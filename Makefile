.PHONY: test lint run-local clean

test:
	python -m pytest tests/ -v

lint:
	ruff check jobs/ dags/ tests/

run-local:
	spark-submit jobs/transform.py --config config/pipeline.yaml

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +
	rm -rf .coverage htmlcov/
