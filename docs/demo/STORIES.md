# STAR stories

Every fact below is from a named commit, test or doc in this repo. Items marked **U** are not in the repo; fill them from your memory or cut them.

## 1. Null-key `countDistinct`

- **Situation:** the Databricks `no_duplicate_keys` expectation had to match the local `uniqueness_check` on `(RegionID, month)`.
- **Task:** make the two runtimes agree on what a duplicate is.
- **Action:** found that `countDistinct` over several columns skips any row where one key part is null, so a unique pair with a null `RegionID` was counted as a duplicate. Switched to counting distinct key structs (3149225, one line in `databricks/pipeline.py`).
- **Result:** the expectation now matches the local check on null keys. `tests/test_parity.py` covers the fingerprint's null counting (`test_fingerprint_counts_nulls`), but I did not find a test that exercises the expectation itself. Say that if asked.

## 2. Whole-row checksum in the parity check

- **Situation:** the parity report compared per-column checksums between local Spark 3.5.4 and Databricks serverless Spark 4.2.0.
- **Task:** make parity strong enough to catch values attached to the wrong record.
- **Action:** a73e2eb adds a whole-row `xxhash64` sum to `fingerprint()`, compares it in `diff_fingerprints()`, and records `row_checksum_match` per layer. `tests/test_parity.py` has `test_fingerprint_detects_swapped_rows` and `test_diff_reports_row_checksum_delta_and_missing`.
- **Result:** row checksums match on all four layers, `diffs: []` (`results/databricks_parity.json`, regenerated 2026-11-07 with the same values).

## 3. Free Edition limits and the failure log

- **Situation:** Free Edition is serverless only, has restricted outbound internet, one pipeline, fair-use quotas, and no SLA (`docs/databricks-notes.md`, limits table).
- **Task:** show the same transforms and DQ on a managed platform without making it the primary path.
- **Action:** ADR 0001 makes Databricks an optional second runtime; Airflow, DuckDB and dbt stay primary. The CSV is uploaded to a volume by hand. Four real failures were logged with cause and fix (notes, failure modes 1-4): pipeline root not on `sys.path`, `__file__` undefined in pipeline files, real out-of-range rents (3 NY ZIPs) that led to raising `rent_range` to `[0, 150000]`, and the fingerprint task import.
- **Result:** the demo job ran green in 136s and 155s on the real CSV; a deliberately bad fixture failed in 945s after retries. Cost to mention: a failing update retries inside one job run and spends quota.

## 4. Codex review of the dbt and DAG work (0330ffb)

- **Situation:** a Codex review of the dbt and DAG work.
- **Action (accepted, from the diff):** (a) `mart_rent_trends` rolling average changed from `ROWS BETWEEN 11 PRECEDING` to `RANGE BETWEEN INTERVAL 11 MONTHS PRECEDING`, because `clean_nulls` drops months and a ROWS frame would then span more than 12 calendar months; (b) merged `load_to_duckdb` and `run_dbt` into one Airflow task so dbt reads the DuckDB file on the same worker.
- **Rejected or modified:** none, per you (2026-11-07). Git shows only what was accepted, so this rests on your word. Say "I accepted both findings", not that you pushed back.
- **Result:** both changes shipped in 0330ffb. Whether the original ROWS version produced wrong values on this data was not measured (**I**).
- **Judgment line (use only if true):** you checked the reviewer's reasoning against how `clean_nulls` drops months before accepting the change.
