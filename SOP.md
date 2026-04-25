# LSEG Ingestion - Production Support Standard Operating Procedure (SOP)

This document provides guidelines for day-to-day verification, monitoring, and recovery of the LSEG ingestion process.

## 1. Daily Verification

The primary source of truth for ingestion results is the `lseg_file_audit` table.

### 1.1 Summary Health Check
Run the following SQL to get a summary of the current day's ingestion:
```sql
SELECT 
    target_table, 
    kind,
    status,
    COUNT(*) AS total_files,
    SUM(ins_count) AS total_inserts,
    SUM(upd_count) AS total_updates,
    SUM(del_count) AS total_deletes,
    SUM(skipped_rows) AS total_skipped
FROM lseg_file_audit
WHERE business_date = CURDATE()
GROUP BY target_table, kind, status;
```

### 1.2 Identifying Failures
If any file shows a status other than `SUCCESS`, use this query to find the cause:
```sql
SELECT file_name, status, error_message, started_at, finished_at
FROM lseg_file_audit
WHERE status != 'SUCCESS' 
  AND business_date = CURDATE();
```

## 2. Monitoring Metrics (Micrometer)

The application publishes metrics via Micrometer. In production, these should be viewed in your monitoring dashboard (e.g., Grafana).

- **`ingest.overall.duration`**: Total time for the session.
- **`ingest.file.duration`**: Time per file.
- **`ingest.rows.ops` (tags: op=I|U|D)**: Detailed breakdown of row actions.
- **`ingest.rows.skipped.error`**: Critical row-level failures.

## 3. Recovery Procedures

### 3.1 Idempotency and Re-running
The application is **fully idempotent**. If a job fails or is interrupted:
1. Fix the underlying issue (e.g., DB connectivity, disk space).
2. Simply **restart the application**.
3. It will automatically scan the audit table, identify which files were NOT successfully processed, and resume only from those files.

### 3.2 Troubleshooting Row Failures
If `skipped_rows` is non-zero, it means specific lines in a file failed validation or DB constraints.
1. **Search Logs**: Search the application logs for `ROW FAILURE`.
2. **Analysis**: The log entry will contain the **exact row data** and the **natural key**.
3. **Example Log**: `ROW FAILURE in <file> line=123 key=456. REASON: <error>. DATA: [...]`
4. Since production support may not have file access, these logs are the primary means of diagnosis.

### 3.3 Soft Delete Recovery
All deletions are "soft" (`is_deleted = 1`). If a row was accidentally marked as deleted in an LSEG file:
1. It is not lost; it remains in the DB.
2. To recover, the record simply needs to be re-sent in an INT or DELTA file with an 'I' or 'U' action.
3. The system will automatically reset `is_deleted = 0` on the next upsert.

## 4. Operational API Control

In addition to DB-level monitoring, the following APIs can be used for manual intervention:

### 4.1 Triggering a Job
To manually start the ingestion:
```bash
curl -X POST http://localhost:8080/api/jobs/trigger
```

### 4.2 Stopping Ingestion
To force-stop all active workers (e.g., if a loop is detected or DB load is too high):
```bash
curl -X POST http://localhost:8080/api/jobs/stop
```

### 4.3 Skipping Corrupt Files
If a specific file is blocking progress and should be ignored:
```bash
curl -X POST "http://localhost:8080/api/files/skip?fileName=BAD_FILE.txt.zip&reason=Manual+intervention+due+to+corruption"
```

## 5. Troubleshooting Liquibase
If the application fails at startup during the "Liquibase" phase:
1. Check `DATABASECHANGELOGLOCK`. If a previous run crashed, you may need to manually clear the lock:
   ```sql
   UPDATE DATABASECHANGELOGLOCK SET LOCKED = 0 WHERE ID = 1;
   ```
