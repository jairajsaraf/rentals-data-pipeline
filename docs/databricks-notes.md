# Databricks Free Edition notes

Working notes for the optional Databricks runtime. Each claim is labelled
**C** (confirmed on the named docs page), **I** (inferred), or **U** (unknown).
All pages were read on 2026-11-06.

## Free Edition limits that affect this pipeline

Source: *Databricks Free Edition limitations* (docs.databricks.com/aws/en/getting-started/free-edition-limitations, last updated Oct 18, 2026).

| Limit | Effect on us | Label |
|---|---|---|
| Serverless compute only. No custom compute configurations | The pipeline and job tasks must be serverless. The Spark version is whatever serverless ships, not our pinned 3.5.4 | C |
| Outbound internet restricted to a limited set of trusted domains | We can't count on Databricks fetching the Zillow CSV, so we upload it to a UC volume ourselves | C (whether zillow.com is on the list: U) |
| One active Lakeflow pipeline per pipeline type | Exactly one pipeline. Bronze, silver and gold all live in it | C |
| Max 5 concurrent job tasks per account | The weekly job has 2 sequential tasks, well under the cap | C |
| One SQL warehouse, capped at 2X-Small | Not used for parity. The fingerprint runs as a job task | C |
| Fair usage: over quota, compute shuts down for the rest of the day (extreme cases: the month). Data and settings are kept | A run can die mid-update. The fingerprint task depends on pipeline success, and the event-log query is scoped to the latest update | C |
| No SLA, no reliability guarantees, non-commercial use only, inactive accounts may be deleted | Databricks is an optional second runtime, never the primary path (see ADR 0001) | C |
| One workspace and one metastore. No account-level APIs | The cold-deploy check needs a second free account, not a second workspace in the same account | C |

## Other docs facts relied on

| Fact | Label | Page |
|---|---|---|
| Python API: `from pyspark import pipelines as dp`; `@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_or_fail` | C | *Manage data quality with pipeline expectations* |
| Warn writes invalid rows. Drop removes them and logs the count. Fail rolls back the update and records no metrics | C | same |
| Expectation constraints can't use Python UDFs, external calls, or subqueries | C | same |
| How a NULL constraint result is treated | U | same (not stated) |
| Aggregate validation MVs (row count, PK uniqueness) with `expect_or_fail` are a documented pattern. A validation dataset does **not** block downstream datasets | C | *Expectation recommendations and advanced patterns* |
| Volume file path `/Volumes/<catalog>/<schema>/<volume>/<file>` | C | *What are Unity Catalog volumes?* |
| Pipeline root folder is appended to `sys.path` automatically | C | *Import Python modules from Git folders or workspace files* |
| `event_log(<pipelineId>)` is queryable by the run-as user. DQ counts live in `details:flow_progress.data_quality.expectations`; `dropped_records` is in `details:flow_progress.data_quality` | C | *Pipeline event log* |
| Asset Bundles are now "Declarative Automation Bundles". Bundle pipelines tutorial needs CLI ≥ 0.283.0 | C | *What are Declarative Automation Bundles?*, *Develop pipelines with Declarative Automation Bundles* |
| `mode: development` prefixes resource names `[dev <user>]`, marks pipelines `development: true`, and pauses all schedules | C | *Declarative Automation Bundles deployment modes* |
| Exact bundle YAML keys for serverless pipelines, volumes, schemas and job pipeline tasks | C for schema acceptance: `databricks bundle validate -t dev` (CLI 1.24.0) printed "Validation OK!" for the keys in `databricks.yml`. Validation checks the schema only, not runtime behaviour. One fix was needed: resource keys must be unique across resource types (`zori` was used for both a schema and a pipeline) | *Bundle resources reference* (too large to read in full), then CLI validation |
| Free Edition default catalog is `workspace`; `pyyaml` available on serverless | U | to be confirmed on first deploy |

## Data terms

Read 2026-11-06 in the built-in browser.

| Fact | Label | Source |
|---|---|---|
| The Housing Data page offers ZORI at ZIP Code level and links Metro-level files at `files.zillowstatic.com/research/public_csvs/zori/` | C | *Housing Data \| Zillow Research* (zillow.com/research/data/) |
| That page carries no data-use or attribution notice | C | same |
| The ZIP file `Zip_zori_uc_sfrcondomfr_sm_month.csv` exists at the same path (HTTP 200). The page links only the Metro file, so the ZIP URL follows the naming pattern | C (exists) / I (that it is the intended ZIP-level file) | HEAD request |
| Zillow's general Terms of Use ban "automated queries (including screen and database scraping...)" on the Services and limit listing information to personal, non-commercial use | C | *Zillow Terms of Use* (zillow.com/corporate/terms-of-use/) |
| Whether the research CSVs carry their own licence or attribution rule | **U** | The ZORI methodology page returned 403 in the browser, so it was not read. Not bypassed |

Handling: one manual-equivalent download of the published file, never committed (`data/` is gitignored), used non-commercially for a portfolio repo. The README must credit Zillow Research as the source. Open question for the user: confirm that is acceptable.

Downloaded file (local `data/raw/`):

| Field | Value |
|---|---|
| sha256 | `dbfa71e2fa1e96daf0310bb035d75cfa7968ab489c75ea593eb374eb08e96337` |
| Last-Modified header | Fri, 16 Oct 2026 01:56:21 GMT |
| Size | 10,111,605 bytes, 8,341 lines |

## DQ → expectation mapping

Approved by the user 2026-11-06. Local checks run on processed output, after `clean_nulls` (`dags/rental_pipeline_dag.py`, `jobs/data_quality.py`).

| Local check (config key) | Local severity | Databricks expectation | Where | Action | Maps? |
|---|---|---|---|---|---|
| `null_pct_median_rent` (`null_pct: 0.1`) | fail | (a) `median_rent IS NOT NULL` replaces `clean_nulls`; (b) `null_pct <= {null_pct}` on a 1-row aggregate MV over gold | (a) `silver_long`; (b) `dq_gold_metrics` | (a) drop; (b) fail | Partial. Expectations are per-row, so the threshold needs an aggregate MV. Both local and (b) are vacuous after the drop. (a) adds a dropped-row count the local path lacks |
| `row_count` (`min_rows: 100`) | fail | `row_count >= {min_rows}` | `dq_gold_metrics` | fail | Yes, via aggregate MV (documented pattern) |
| `range_median_rent` (`rent_range: [0, 150000]`) | fail | `median_rent BETWEEN {lo} AND {hi}` | `gold_zori` | fail | Yes, row-level. NULL semantics U, but moot after the drop |
| `unique_RegionID_month` (`uniqueness_keys`) | warn | `duplicate_count = 0` | `dq_gold_metrics` | warn (`dp.expect`) | Yes, via aggregate MV. Event log shows failed_records=1 on violation |

Known gap (C, *Expectation recommendations and advanced patterns*): an aggregate fail MV does not block `gold_zori`. Locally the DAG stops before the DuckDB load, so the two runtimes behave differently. No workaround in scope.

## Failure modes

Observed on real runs.

| # | Run | Symptom | Cause | Fix |
|---|---|---|---|---|
| 1 | First `bundle run zori_weekly -t dev`, 2026-11-06 | Refresh task failed after ~12 min. The pipeline event log shows 6 failed updates, all `ModuleNotFoundError: No module named 'jobs'` at `from jobs.io_utils import ...` | Without `root_path`, the pipeline root is not the bundle root, so `jobs/` is not on `sys.path`. The docs fact (root folder is appended to `sys.path`) holds, but the root was the wrong folder | Set `root_path: .` on the pipeline in `databricks.yml` |
| 2 | Second run after the `root_path` fix | `jobs` import now works. 6 more failed updates, all `NameError: name '__file__' is not defined` at the config lookup | `__file__` is not defined in pipeline source files (this settles the earlier unknown). The pipeline root is on `sys.path` | `pipeline.py` finds `config/pipeline.yaml` by searching `sys.path`. `fingerprint_task.py` guards its `__file__` use |
| 3 | Third run after the `__file__` fix | Pipeline ran through bronze/silver and failed `gold_zori` on expectation `rent_in_range` (6 failed updates). Example violating record: ZIP 11930, NY, 2026-09-30, `median_rent` 103750.0 | Real data, not a code bug. Running the local `run_pipeline` + `range_check` on the same CSV in Docker also fails: 6 rows in 3 NY ZIPs (11930, 11976, 11963) are above the configured `rent_range` upper bound of 50000, max 103750.0. Databricks `expect_or_fail` matches the local fail severity | User chose to raise `dq_thresholds.rent_range` to `[0, 150000]` (shared by both runtimes). The 6 rows look like real high-rent Hamptons ZIPs, inferred not verified |
| 4 | Fourth run after raising `rent_range` | Pipeline refresh succeeded (all expectations passed on the real CSV). The `fingerprint` task failed: `ModuleNotFoundError: No module named 'jobs'` | Job tasks have no bundle root on `sys.path`, and `__file__` was not usable, so the guarded `sys.path.insert` silently did nothing | Pass `${workspace.file_path}` as `--root` and insert it before importing `jobs.parity` |

Side effect: the pipeline retried the failed update several times inside the one job run, which spends Free Edition quota on a failure that cannot succeed. The `fingerprint` task was skipped, as designed.

## Observed gaps

| Gap | Label | Detail |
|---|---|---|
| A failing validation MV does not block `gold_zori` | C (docs, see Known gap above) | `dq_gold_metrics` uses `expect_or_fail`, but `gold_zori` is a separate dataset and is not gated by it. Locally the DAG stops before the DuckDB load. All expectations passed on the real CSV, so the gap was never triggered. No workaround in scope |
| The post-drop null check is vacuous | C (by construction) | Silver drops every null `median_rent`, so `null_pct <= 0.1` on gold is always 0. The local `clean_nulls` + `null_pct_median_rent` check has the same property. The Databricks side adds a dropped-row count the local path lacks: 706,520 of 1,175,940 unpivoted rows |
| Outbound internet is restricted | C (limits table) | Databricks does not fetch the CSV. It is copied to the volume by hand, so the run depends on that upload |
| A failed pipeline update retries within one job run | C (failure modes 1-3) | Each retry spends Free Edition quota. A failing run took about 12-15 minutes |

## Parity result

Source: `results/databricks_parity.json` (the only source of README numbers).

- `diffs` is `[]`: parity was exact, so there are no deltas to explain.
- Rows match on every layer: bronze 8,340 (150 columns), silver_long, silver_zori and gold 469,420 each.
- Every column checksum and the whole-row checksum match (the row checksum catches values attached to the wrong record), including `rent_change_mom`, although local Spark is 3.5.4 (Docker, Linux) and remote is 4.2.0 (Free Edition serverless).
- Dropped null rents: 706,520 on both sides. Duplicates removed: 0 on both sides.
- `run_date` is taken from the container's UTC clock, so it can differ from the local date by one day.

## Cold-deploy checklist

For a second free account. Free Edition allows one workspace per account, so a second workspace in the same account does not count. Keep the workspace host, username and IDs out of committed files.

1. Install the Databricks CLI (>= 0.283.0; 1.24.0 was used) and log in: `databricks auth login --host <workspace-url>`. Use a named profile or `DEFAULT`.
2. From the repo root: `databricks bundle validate -t dev`, then `databricks bundle deploy -t dev`. Dev mode prefixes the schema as `dev_<user>_zori` and pauses the schedule.
3. Download the CSV and check its sha256 against the one in `results/databricks_parity.json` (`dbfa71e2…6337` for the committed run). A different sha256 makes the parity script refuse to diff.
4. Copy it to the volume: `databricks fs cp data/raw/Zip_zori_uc_sfrcondomfr_sm_month.csv dbfs:/Volumes/workspace/dev_<user>_zori/raw/`.
5. Run it: `databricks bundle run zori_weekly -t dev`. On a failure, read the first error and the failure modes table before retrying. Prefer `--only <task>`.
6. Copy the result back: `databricks fs cp dbfs:/Volumes/workspace/dev_<user>_zori/raw/parity_remote.json data/remote/parity_remote.json`.
7. Compare: `PYTHONPATH=. python scripts/databricks_parity.py --remote-json data/remote/parity_remote.json --out <new-path>.json`. Spark must run in Docker on Windows. Then diff the new file against the committed JSON. Expect only `run_date` (and `spark_version` if the runtime changed) to differ.
8. Optional cleanup: `databricks bundle destroy -t dev`.
