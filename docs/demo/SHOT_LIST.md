# Shot list

## Screenshots (built-in browser, your signed-in workspace)
Crop or blur the workspace host, your email/username, run and pipeline IDs, and the account id in the login URL.

| # | Page | Shows |
|---|---|---|
| 1 | Pipeline `zori`, latest (passing) update | Graph bronze → silver → gold; drop count on `silver_long` (706,520 dropped on the real CSV) |
| 2 | Pipeline `zori`, update history, the failed `bad_zori.csv` update (started 22:55Z, failed 23:11Z) | `gold_zori` failed on `rent_in_range` |
| 3 | Jobs → `zori-weekly`, real-CSV run | Timeline: refresh then fingerprint, SUCCESS |
| 4 | SQL editor: `docs/demo/event_log_dq.sql` result | Per-dataset expectation passed/failed |

Status: 1-3 taken and redacted in `screenshots/` (01_pipeline_pass, 02_pipeline_fixture_failed, 03_job_run_timeline). Shot 2 is the last of 6 failed updates from the fixture run; it also shows `dq_gold_metrics` skipped due to upstream failure. Shot 4 is not a screenshot: use `event_log_fixture_result.json`.

## MP4 (you record, about 90 seconds, no audio of IDs)
1. Terminal: `git log --oneline -5`, then the pytest run (SCRIPT.md, 0:20).
2. Terminal: fixture pytest run (SCRIPT.md, 0:50).
3. Screenshot 2, then screenshot 4 (the failure, then the numbers).
4. Screenshot 1 (the passing run).
5. Terminal: parity one-liner (SCRIPT.md, 2:00).
