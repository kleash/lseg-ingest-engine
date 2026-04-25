SELECT target_table, kind,
       COUNT(*) AS files,
       SUM(ins_count) AS ins,
       SUM(upd_count) AS upd,
       SUM(del_count) AS del,
       SUM(skipped_rows) AS skipped
FROM lseg_file_audit
WHERE business_date = '2026-04-25'
GROUP BY target_table, kind
ORDER BY target_table, kind;

SELECT file_name, ins_count, upd_count, del_count, skipped_rows
FROM lseg_file_audit
WHERE del_count > 0 OR upd_count > 0
LIMIT 10;
