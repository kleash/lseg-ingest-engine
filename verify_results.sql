SELECT target_table, kind,
       COUNT(*) AS files,
       SUM(declared_rows) AS declared,
       SUM(parsed_rows)   AS parsed,
       SUM(inserted_rows) AS inserted,
       SUM(skipped_rows)  AS skipped
FROM lseg_file_audit
WHERE business_date = '2026-04-25'
GROUP BY target_table, kind
ORDER BY target_table, kind;

SELECT file_name, status, error_message
FROM lseg_file_audit
WHERE status IN ('FAILED', 'SKIPPED_SANITY');

SELECT MIN(started_at) AS first_started,
       MAX(finished_at) AS last_finished,
       TIMESTAMPDIFF(SECOND, MIN(started_at), MAX(finished_at)) AS total_seconds
FROM lseg_file_audit;

SELECT 'orgs'   AS t, COUNT(*) FROM lseg_orgs
UNION ALL SELECT 'assets', COUNT(*) FROM lseg_assets
UNION ALL SELECT 'quotes', COUNT(*) FROM lseg_quotes;
