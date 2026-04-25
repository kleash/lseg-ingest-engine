# LSEG Ingest — Production Support SOP

This document is the operational runbook for the LSEG ingestion service. It assumes the multi-instance, cluster-locked, heartbeat-reaped design described in `README.md`.

---

## 1. Daily verification

### 1.1 Health summary
```sql
SELECT target_table, kind, status,
       COUNT(*)              AS files,
       SUM(declared_rows)    AS declared,
       SUM(parsed_rows)      AS parsed,
       SUM(inserted_rows)    AS inserted,
       SUM(skipped_rows)     AS skipped,
       SUM(ins_count)        AS ins,
       SUM(upd_count)        AS upd,
       SUM(del_count)        AS del
FROM lseg_file_audit
WHERE business_date = CURDATE()
GROUP BY target_table, kind, status
ORDER BY target_table, kind, status;
```

### 1.2 Failures + sanity skips
```sql
SELECT file_name, status, error_message, started_at, finished_at
FROM lseg_file_audit
WHERE business_date = CURDATE()
  AND status IN ('FAILED','SKIPPED_SANITY','SKIPPED');
```

### 1.3 Job summary (multi-instance)
```sql
SELECT id, status, node_id, business_date, input_dir,
       started_at, finished_at, last_heartbeat_at,
       TIMESTAMPDIFF(SECOND, started_at, COALESCE(finished_at, NOW())) AS seconds,
       LEFT(error_message, 200) AS err
FROM lseg_jobs
WHERE DATE(created_at) = CURDATE()
ORDER BY id DESC;
```

### 1.4 Cluster-singleton invariant (alarm if > 1)
```sql
SELECT COUNT(*) FROM lseg_jobs WHERE status = 'RUNNING';
-- expected: 0 or 1 ALWAYS
```

### 1.5 Heartbeat freshness for the running job
```sql
SELECT id, node_id,
       TIMESTAMPDIFF(SECOND, last_heartbeat_at, NOW()) AS heartbeat_age_s
FROM lseg_jobs
WHERE status = 'RUNNING';
-- expected: heartbeat_age_s < 2 * cluster.heartbeatIntervalSeconds (60s default)
```

### 1.6 RIC caret filter sanity
```sql
SELECT COUNT(*) FROM lseg_quotes WHERE ric LIKE '%^%';
-- All caret rows here originate from DELTA quote files (filter is INT-only by spec).
```

---

## 2. Metrics (Micrometer / actuator)

Surfaced via Spring Boot Actuator. Hook these into Prometheus / Grafana.

| Metric | Tags | Meaning |
|---|---|---|
| `ingest.overall.duration` | — | Job wall time |
| `ingest.file.duration` | `target`, `kind`, `status` | Per-file wall time |
| `ingest.rows.parsed` | `target` | Total rows parsed across all files |
| `ingest.rows.inserted` | `target` | Successful upserts |
| `ingest.rows.skipped.error` | `target` | Row-level skips (bad data, constraint failures) |
| `ingest.rows.skipped.filter` | `target` | Filter skips (RIC `^`) |
| `ingest.rows.ops` | `target`, `op=I\|U\|D` | Action counts |
| `ingest.sanity.failures` | `target` | Pre-ingest sanity rejections |
| `ingest.archive.errors` | — | Archive move failures |
| `ingest.target.errors` | — | Target pipeline-level errors |
| `ingest.orchestrator.errors` | — | Fatal orchestrator errors |

**Recommended alerts:**
* `sum(ingest.rows.skipped.error) by (target) / sum(ingest.rows.parsed) by (target) > 0.005` for 10 min — high row-failure rate.
* `count(lseg_jobs.status='RUNNING') > 1` — cluster lock invariant violation.
* `min_over_time(heartbeat_age_seconds[5m]) > 120` — running job heartbeat stale.

---

## 3. Recovery

### 3.1 Idempotent re-run
The system is fully idempotent at file granularity (skip on `lseg_file_audit.file_name` SUCCESS) and at row granularity (composite UNIQUE + ON DUPLICATE KEY UPDATE). To recover from any non-CRITICAL failure:

```bash
curl -X POST 'http://<host>:8080/api/jobs/trigger?businessDate=20260425&inputDir=/data'
```

Files already in `SUCCESS` status are skipped automatically.

### 3.2 Restarting a failed job
```bash
curl -X POST 'http://<host>:8080/api/jobs/restart?jobId=42'
# requeues that job; next polling node will pick it up
```

### 3.3 Reaper for stuck RUNNING jobs
A node that crashes without releasing its job leaves a row in `RUNNING`. The `JobReaper` runs every 60 s and flips RUNNING jobs whose `last_heartbeat_at` is older than `ingest.reaper.staleJobTimeoutSeconds` (default 3 h) to `FAILED` with `error_message='reaped: heartbeat stale > <N>s'`.

To reap manually before the timeout:
```sql
UPDATE lseg_jobs
SET status='FAILED', finished_at=NOW(), error_message='manual: operator reap'
WHERE id=<jobId> AND status='RUNNING';
```
Then `POST /api/jobs/restart?jobId=<jobId>` to requeue.

### 3.4 Force-stopping a job
```bash
curl -X POST 'http://<host>:8080/api/jobs/stop?jobId=42'
```
Stop semantics: the running node polls `lseg_jobs.status` every `ingest.cancel.checkRows` rows (default 5000) inside the file loop. On detection it throws and rolls back the current file. Subsequent files are not processed. **`STOPPED` is sticky** — the worker will not overwrite it with `COMPLETED` or `FAILED`.

To stop all running/queued jobs cluster-wide:
```bash
curl -X POST 'http://<host>:8080/api/jobs/stop'
```

### 3.5 Manual file-skip
```bash
curl -X POST 'http://<host>:8080/api/files/skip?fileName=BAD_FILE.txt.zip&reason=Manual+intervention'
```
The scanner ignores files already in `SUCCESS` or `SKIPPED` status.

### 3.6 Soft-delete recovery
All deletes are `is_deleted=1`. To re-introduce a row:
1. Wait for the next INT or DELTA file containing the key.
2. The upsert sets `is_deleted=0` automatically.

To resurrect manually:
```sql
UPDATE lseg_quotes SET is_deleted=0 WHERE asset_id=? AND quote_id=?;
```

---

## 4. Operational APIs

| API | Method | Params | Purpose |
|---|---|---|---|
| `/api/jobs/trigger` | POST | `businessDate?`, `inputDir?` | Queue a new job |
| `/api/jobs/stop` | POST | `jobId?` | Stop one or all jobs |
| `/api/jobs/restart` | POST | `jobId?` | Re-queue an existing or new job |
| `/api/jobs/status` | GET | `jobId` | Query status |
| `/api/files/skip` | POST | `fileName`, `reason` | Manually skip a file |
| `/actuator/health` | GET | — | Spring health (basic) |

> Endpoints are not authenticated yet. Place behind ingress with mTLS or restrict at network level until the security phase ships.

---

## 5. Liquibase troubleshooting

Liquibase runs at startup using the `owner` DB account.

### Stuck change-log lock
If a previous startup crashed mid-migration:
```sql
SELECT * FROM DATABASECHANGELOGLOCK;
UPDATE DATABASECHANGELOGLOCK SET LOCKED=0, LOCKEDBY=NULL, LOCKGRANTED=NULL WHERE ID=1;
```

### Validation failure (changeset checksum mismatch)
This means a deployed changeset was edited after-the-fact. Either revert the file to the deployed checksum or, if intentional, run:
```sql
UPDATE DATABASECHANGELOG SET MD5SUM=NULL WHERE ID='<changeset-id>';
-- next startup will re-checksum.
```

---

## 6. Cluster lifecycle quick reference

| Event | What happens |
|---|---|
| Node starts | Liquibase runs (idempotent), JobWorker begins polling every 10 s |
| Job triggered | Row inserted in `lseg_jobs` with `status='QUEUED'` and `business_date`, `input_dir` carried |
| Worker claims | Atomic `UPDATE … WHERE status='QUEUED'`; orchestrator tries `GET_LOCK('lseg-ingest-cluster',0)` |
| Lock unavailable | Job rolled back to `QUEUED`; node retries in 10 s |
| Lock acquired | Heartbeat thread starts; orchestrator runs; lock released on success/fail/exception |
| Node crashes mid-job | DB releases `GET_LOCK` on connection drop; row stays `RUNNING` until reaper flips it |
| Reaper interval elapses | `last_heartbeat_at` stale → row → `FAILED` |
| Operator stop | `lseg_jobs.status='STOPPED'`; worker detects within `cancel.checkRows` rows; rolls back current file |

---

## 7. Diagnostics — log triage

All log lines under the ingest hot path carry MDC `[job=<id> file=<name>]`. To inspect one job:
```bash
docker compose logs ingest | grep 'job=42'
```

Common patterns:
* `Mapped K/N columns for <file>` — file header has fewer schema-known columns than the target supports (extras / missing tolerated).
* `BATCH FAILURE in <file> after N rows. Attempting recovery via row-by-row fallback` — `ResilientBatchExecutor` engaged.
* `ROW FAILURE in <file> line=L key=K. REASON: <e>. DATA: [...]` — single-row skip; the line+key pinpoints the offending row.
* `Transient error on 'ingest:<file>' attempt n/m: <e>. Backing off Xms` — `SqlRetry` engaged for deadlock/lock-wait/connection.
* `ClusterLock 'lseg-ingest-cluster' acquired/released` — singleton lock state transitions.
* `Reaped N stale RUNNING job(s)` — automatic recovery from a crashed node.

---

## 8. Disaster-recovery playbook

| Scenario | Procedure |
|---|---|
| **DB down, all jobs failing** | App keeps polling. Bring DB back; run `POST /api/jobs/restart?jobId=<latest>` for the affected job. Already-processed files skip automatically. |
| **Cluster lock stuck (DB session orphaned)** | `KILL <connection_id>` on the DB session holding `GET_LOCK`. Verify with `SELECT IS_USED_LOCK('lseg-ingest-cluster');`. |
| **Audit table corrupted** | Restore from snapshot. Idempotency means re-running the day produces the same final state in target tables. |
| **Single instance accidentally promoted to multiple** | The cluster lock guarantees only one runs at a time. Excess instances poll harmlessly. |
| **Need to re-ingest a single file** | `DELETE FROM lseg_file_audit WHERE file_name='X';` then `POST /api/jobs/trigger`. Existing target rows persist; upsert overwrites them with the file's content. |
| **Need to reset entire day** | `TRUNCATE lseg_orgs; TRUNCATE lseg_assets; TRUNCATE lseg_quotes; TRUNCATE lseg_file_audit;` then trigger. |
