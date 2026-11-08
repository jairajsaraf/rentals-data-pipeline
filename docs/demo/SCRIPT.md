# 3-minute offline demo

Everything here runs with Wi-Fi off: local files, Docker (image `rentals-dev:py310` already pulled), pytest.
Run from the repo root in PowerShell. Docker shorthand used below:

```powershell
$D = 'docker run --rm --network none --hostname spark --add-host spark:127.0.0.1 -e SPARK_LOCAL_IP=127.0.0.1 -v "${PWD}:/work" -w /work -e PYTHONPATH=. rentals-dev:py310'
```

`--network none` gives the container no network at all. Without `--hostname`/`--add-host`/`SPARK_LOCAL_IP`, Spark cannot resolve its own host name and the Java gateway exits (observed: 21 of 24 tests errored).

| Time | Step | Command | What you say |
|---|---|---|---|
| 0:00 | Architecture | open `README.md` diagram | Raw CSV → PySpark → Parquet (state, year) → DuckDB → dbt, orchestrated by Airflow. Databricks is an optional second runtime (ADR 0001). |
| 0:20 | Tests green | `Invoke-Expression "$D python -m pytest tests/ -q"` | 24 tests, including window functions, DQ checks and the parity fingerprint. |
| 0:50 | Failure fixture | `Invoke-Expression "$D python -m pytest tests/test_bad_fixture.py -v"` | A CSV with a null rent, a 200,000 rent and a duplicate key. Null and duplicate are removed by `clean_nulls` and `deduplicate` first; only the range check fails. |
| 1:30 | Same thing on Databricks | open `docs/demo/event_log_fixture_result.json` | Same fixture on Databricks: update FAILED, `rent_in_range` failed 1, silver dropped 4,489 null rows. That is the event-log query in `event_log_dq.sql`. |
| 2:00 | Parity | `python -c "import json;d=json.load(open('results/databricks_parity.json'));print(d['diffs'], d['runtimes'])"` | Local Spark 3.5.4 vs Databricks 4.2.0: `diffs` is empty, every layer row count and checksum matches. |
| 2:30 | What it is not | `docs/demo/JD_MAP.md`, gaps section | No Terraform, Datadog, Scala or Glue in this repo. Closest analogues are labelled inferred. |

Fallbacks if a step fails live: screenshots in `docs/demo/` (see `SHOT_LIST.md`), `results/databricks_parity.json`, `docs/databricks-notes.md`.
All steps were run on 2026-11-07 with the container network disabled; results are in `VERIFY.md`.
