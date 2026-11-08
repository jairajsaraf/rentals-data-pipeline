# Verify before the interview

Date run: 2026-11-07. Machine: your laptop, PowerShell, Docker Desktop.

## 1. Offline checklist

"Offline" here means the Docker container has no network at all (`--network none`), so a step that passes cannot have used the network. I did not turn off the Wi-Fi adapter itself. Image `rentals-dev:py310` was already local, so no pull happens.

Set the shorthand once (PowerShell, from the repo root):

```powershell
$D = 'docker run --rm --network none --hostname spark --add-host spark:127.0.0.1 -e SPARK_LOCAL_IP=127.0.0.1 -v "${PWD}:/work" -w /work -e PYTHONPATH=. rentals-dev:py310'
```

| # | Command | Expected | Run result (2026-11-07) |
|---|---|---|---|
| 1 | `Invoke-Expression "$D python -m pytest tests/ -q"` | `24 passed` | 24 passed, 20 s wall-clock |
| 2 | `Invoke-Expression "$D python -m pytest tests/test_bad_fixture.py -v"` | `test_bad_fixture_fails_only_range_check PASSED` | PASSED, 12 s |
| 3 | `python -m json.tool docs/demo/event_log_fixture_result.json` | `gold_zori` FAILED `rent_in_range` failed 1; `silver_long` `median_rent_not_null` passed 164, failed 4489 | matches |
| 4 | `python -c "import json;d=json.load(open('results/databricks_parity.json'));print(d['diffs'], d['runtimes'])"` | `[]` and Spark 3.5.4 vs 4.2.0 | matches |
| 5 | open `docs/demo/JD_MAP.md`, section "Gaps to say plainly" | section present | present |
| 6 | open `docs/demo/screenshots/01..03*.png` and `onepager.pdf` | files open; no host, email or IDs visible | checked by viewing the images; the PDF has 1 page |

Without the `--hostname`, `--add-host` and `SPARK_LOCAL_IP` flags, `--network none` fails: Spark cannot resolve its own host name and 21 of 24 tests error. Use the shorthand above, not a plain `docker run --network none`.

Not checked: running the steps with the host's Wi-Fi adapter off, and running them on a different machine.

## 2. Cold deploy on a second free account

Reuse the checklist in `docs/databricks-notes.md` ("Cold-deploy checklist") with `-t demo` instead of `-t dev`. Differences for `demo`:

- Resource names are unprefixed (schema `zori`, volume `raw`, pipeline `zori`), so the volume path is `/Volumes/workspace/zori/raw/` (`dev` uses `dev_<user>_zori`).
- The schedule is paused by `presets.trigger_pause_status: PAUSED`.
- To run the failure fixture: upload `tests/fixtures/bad_zori.csv` to the same volume, then `databricks bundle deploy -t demo --var csv_name=bad_zori.csv` and `databricks bundle run zori_weekly -t demo --var csv_name=bad_zori.csv`. Redeploy without `--var` to go back to the real CSV.
- Observed on 2026-11-07 (one account, one run each): deploy 11 s, real-CSV run 136 s and 155 s, fixture run 945 s (6 failed updates).

Not tried: a second account. Whether it works there is unknown.

## 3. Mock interviewer questions

Pass rubric: the answer names the specific artifact, states one number from this repo, and says what was not verified.

1. **"A data quality check fails in production. What do you do?"**
   Expected: show the fixture. Null and duplicate are removed before DQ; the range check fails with 1 out-of-range row; the job stops before the load. On Databricks the `gold_zori` expectation fails, `dq_gold_metrics` is skipped, and the update is FAILED. Say the failing update retried 6 times and spent quota.
   Pass: names `rent_in_range`, says which checks did not fail and why. Fail: "I would add alerting" with nothing from the repo.

2. **"How do you know the Databricks run gives the same result as your local run?"**
   Expected: `scripts/databricks_parity.py` compares per-column and whole-row checksums per layer; `diffs` is empty; Spark 3.5.4 vs 4.2.0; 469,420 rows in each layer after silver. Both runs used the same CSV, matched by SHA-256.
   Pass: explains why a row checksum was added (per-column checks miss swapped values). Fail: "the row counts matched".

3. **"Tell me about a bug you tracked to its root cause."**
   Expected: 3149225. `countDistinct` over several columns skips rows with a null key part, so a null `RegionID` pair was counted as a duplicate. The fix counts distinct key structs.
   Pass: says what the fix did not test (no test exercises the expectation itself). Fail: presents it as fully covered.

4. **"How do you use AI tools without lowering the review bar?"**
   Expected: the Codex review in 0330ffb. Two findings, both accepted: the RANGE window over 12 calendar months (because `clean_nulls` drops months) and the merged load-and-dbt Airflow task. Say you checked the first against `clean_nulls` only if that is true.
   Pass: concrete diff, honest that nothing was rejected. Fail: claims pushback that did not happen.

5. **"Your resume lists no Terraform, Datadog or Scala. How would you ramp up?"**
   Expected: say they are not in this repo. Nearest real analogues: Asset Bundles declare the job and pipeline as code; the Databricks event log is queried by SQL. Present those as analogues, not equivalents.
   Pass: honest gap plus a specific first step. Fail: claims equivalence.
