-- Latest pipeline update: update state, then per-dataset expectation results.
-- Variant of _EVENT_SQL in databricks/fingerprint_task.py (that one keeps only COMPLETED flows).
-- Replace <PIPELINE_ID> (databricks bundle summary -t demo). Run in the SQL editor.
-- Observed 2026-11-07 on the bad_zori.csv update: update_state FAILED; gold_zori flow FAILED with rent_in_range failed_records=1
-- (passed_records NULL); silver_long COMPLETED with median_rent_not_null passed 164, failed 4489, dropped 4489.
-- Result saved in event_log_fixture_result.json.
WITH latest AS (
  SELECT origin.update_id AS update_id
  FROM event_log('<PIPELINE_ID>')
  WHERE event_type = 'create_update'
  ORDER BY timestamp DESC
  LIMIT 1
),
state AS (
  SELECT details:update_progress.state AS update_state, timestamp
  FROM event_log('<PIPELINE_ID>')
  WHERE event_type = 'update_progress'
    AND origin.update_id = (SELECT update_id FROM latest)
  ORDER BY timestamp DESC
  LIMIT 1
),
flows AS (
  SELECT
    origin.flow_name AS dataset,
    details:flow_progress.status AS flow_status,
    details:flow_progress.data_quality.dropped_records AS dropped_records,
    from_json(
      details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
    ) AS expectations
  FROM event_log('<PIPELINE_ID>')
  WHERE event_type = 'flow_progress'
    AND origin.update_id = (SELECT update_id FROM latest)
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
SELECT
  (SELECT update_state FROM state) AS update_state,
  f.dataset,
  f.flow_status,
  e.name AS expectation,
  e.passed_records,
  e.failed_records,
  f.dropped_records
FROM flows f
LATERAL VIEW explode(f.expectations) x AS e
ORDER BY f.dataset, e.name;
