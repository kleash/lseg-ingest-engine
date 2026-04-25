# LSEG Ingest Engine

A mission-critical, idempotent, multi-instance ingestion engine for LSEG EIS daily reference data. Built on Spring Boot 3.3 + plain JDBC + MariaDB 11 + Liquibase. Designed for hospital-grade reliability: every operation is replayable, every failure is observable, and every running instance is bounded by a cluster-wide singleton lock.

---

## At a Glance

| Property | Value |
|---|---|
| Java | 21 |
| Framework | Spring Boot 3.3.5 (web + actuator + scheduling) |
| DB | MariaDB 11 (`InnoDB`, `utf8mb4`) |
| Migrations | Liquibase (separate `owner` DB account) |
| Hot path | Plain JDBC + HikariCP (`runtime` minimum-privilege DB account) |
| Concurrency model | Cluster-wide singleton (DB `GET_LOCK`) — at most one job RUNS in the cluster at a time |
| Idempotency | File-audit table + composite UNIQUE on natural keys + `ON DUPLICATE KEY UPDATE` |
| Soft delete | `is_deleted` column on every target table |
| Resilience | Per-row fallback, transient-SQL retry with exponential backoff, in-file action ordering, stale-job reaper |
| Observability | MDC `[jobId, file]` log pattern, Micrometer counters/timers, REST status endpoints |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          REST API (port 8080)                        │
│  POST /api/jobs/trigger  POST /api/jobs/stop  GET /api/jobs/status   │
└─────────────────────┬────────────────────────────────────────────────┘
                      │ enqueue (jobId, business_date, input_dir)
                      ▼
            ┌─────────────────┐
            │   lseg_jobs     │  status, node_id, last_heartbeat_at
            └──────┬──────────┘
                   │ polled every 10s by JobWorker
                   ▼
       ┌────────────────────────┐
       │  JobWorker.claimJob()  │  atomic UPDATE … WHERE status='QUEUED'
       └──────┬─────────────────┘
              ▼
   ┌──────────────────────────────────────┐
   │ ClusterLock.tryAcquire()             │  GET_LOCK('lseg-ingest-cluster',0)
   │   — held on a dedicated connection   │  on the ingest pool for the run
   │   — 0 ⇒ leave job QUEUED, retry later│
   └──────┬───────────────────────────────┘
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ IngestOrchestrator.run(jobId)                                       │
│  ① start heartbeat (every 30 s -> last_heartbeat_at)                │
│  ② override props.businessDate / inputDir from lseg_jobs row        │
│  ③ FileScanner: classify into (Target, Kind=INT|DELTA, seq)         │
│  ④ skip files already SUCCESS in lseg_file_audit                    │
│  ⑤ FileSanityCheck: drop bad files as SKIPPED_SANITY                │
│  ⑥ per-target pipelines (parallel across targets):                  │
│       INT phase  (intPerTable threads)        — gates DELTA         │
│       DELTA phase (sequential by seq)                               │
│  ⑦ stop signal polled at submit boundaries AND inside the row loop  │
└─────────────────────────────────────────────────────────────────────┘
              ▼
   ┌─────────────────────────────────────────┐
   │ FileIngestor                            │
   │  - SqlRetry wraps each file (transient  │
   │    SQL errors → exp-backoff retry)      │
   │  - In-file action ordering preserved:   │
   │    flush UPSERT batch when next row is  │
   │    DELETE; flush DELETE batch when next │
   │    row is UPSERT — guarantees that      │
   │    "D <key>" then "I <key>" leaves the  │
   │    row LIVE, "I <key>" then "D <key>"   │
   │    leaves the row SOFT-DELETED.         │
   │  - ResilientBatchExecutor: on batch     │
   │    failure, replay individually; bad    │
   │    rows skipped + logged with line+key. │
   │  - maxSkippedRowsPerFile enforced.      │
   └─────────────────────────────────────────┘
```

### File classification

| Target table | Dataset patterns | Sample filenames |
|---|---|---|
| `lseg_orgs` | `Organization.*`, `EIS_DELTA_GLOABL_ORGN.*` (sic: vendor typo) | `Organization.INT.99999.20260425.1.1.1.txt.zip` |
| `lseg_assets` | `EIS_INT_*_ASSETS.*`, `EIS_DELTA_GLOBAL_ASSETS.*` | `EIS_INT_GLOBAL_EQU_ASSETS.INT.…` |
| `lseg_quotes` | `EIS_INT_*_QUOTE/QUOTES.*`, `EIS_DELTA_*_QUOTE.*` | `EIS_DELTA_ASIA_US_QUOTE.REF.…` |

Skipped by glob: `*.note.txt.zip`, `Reference-INT-EQUI-*`. Skipped by row filter: INT-quote rows where `RIC` contains `^` (DELTA quote rows are kept).

### Schema (composite unique keys)

| Table | UNIQUE | Other indexes |
|---|---|---|
| `lseg_orgs` | `(entity_id)` | `entity_perm_id`, `is_deleted` |
| `lseg_assets` | `(asset_id)` | `issue_perm_id`, `is_deleted` |
| `lseg_quotes` | `(asset_id, quote_id)` | `quote_id`, `asset_id`, `quote_perm_id`, `is_deleted` |
| `lseg_file_audit` | `file_name` | — |
| `lseg_jobs` | — | `status`, `last_heartbeat_at` |

NULL key columns are tolerated: MariaDB UNIQUE treats NULL as distinct, so NULL-keyed rows always insert. This matches the source data (where some perm-ids may be null).

### Multi-instance semantics

The application is designed to be deployed as multiple identical instances. Concurrent instances coordinate strictly through the database:

1. **Cluster lock (`GET_LOCK`)** — at most one node may be running an ingestion at any time. Any other node attempting to start a job will release its claim back to QUEUED so it's retried on the next poll cycle. The lock is released automatically by MariaDB when the holding connection drops, bounding stuck-state duration to `wait_timeout`.
2. **Atomic job claim** — single-statement `UPDATE lseg_jobs SET status='RUNNING' … WHERE id=? AND status='QUEUED'` — only one node sees `rowsAffected=1` for any given job.
3. **`KeyHolder`-based job creation** — avoids the unsafe `LAST_INSERT_ID()` round-trip across pooled connections.
4. **STOPPED-aware status writes** — operator-issued stops are never overwritten by completion writes.
5. **Heartbeat + reaper** — every running job updates `last_heartbeat_at` every 30 s. `JobReaper` (configurable, default 3 h timeout) marks RUNNING jobs FAILED if their heartbeat goes stale, freeing the queue for the next node.

---

## Configuration

All knobs are externalised in `application.yml` and bind to `IngestProperties` (Spring `@ConfigurationProperties("ingest")`).

| Key | Default | Purpose |
|---|---|---|
| `ingest.inputDir` | `/data` | File drop directory |
| `ingest.archiveDir` | `/archive` | Move-after-success destination (failures emit `ingest.archive.errors`) |
| `ingest.businessDate` | `20260425` | yyyymmdd; can be overridden per-job via API |
| `ingest.charset` | `UTF-8` | Character set used to decode pipe-delimited entries inside the zip |
| `ingest.skipPatterns` | `*.note.txt.zip`, `Reference-INT-EQUI-*` | Glob patterns dropped at scan time |
| `ingest.ricCaretFilter` | `true` | Drop INT-quote rows where `RIC` contains `^` |
| `ingest.threads.intPerTable` | `10` | Parallelism within a target's INT phase |
| `ingest.threads.deltaTargetsParallel` | `3` | Number of target pipelines run in parallel |
| `ingest.batch.upsertSize` | `5000` | UPSERT batch size |
| `ingest.batch.deleteSize` | `5000` | DELETE batch size |
| `ingest.resilience.fallbackOnBatchFail` | `true` | On `BatchUpdateException`, replay rows individually |
| `ingest.resilience.maxSkippedRowsPerFile` | `1000` | File is FAILED if exceeded |
| `ingest.cancel.checkRows` | `5000` | Polling cadence for stop signal inside the row loop |
| `ingest.reaper.enabled` | `true` | Enable `JobReaper` |
| `ingest.reaper.staleJobTimeoutSeconds` | `10800` (3 h) | Reap RUNNING jobs whose heartbeat is older than this |
| `ingest.reaper.pollIntervalSeconds` | `60` | Reaper sweep cadence |
| `ingest.cluster.lockName` | `lseg-ingest-cluster` | `GET_LOCK` name; per-environment override possible |
| `ingest.cluster.heartbeatIntervalSeconds` | `30` | Job heartbeat cadence |
| `ingest.retry.maxAttempts` | `3` | Per-file transient-SQL retry attempts |
| `ingest.retry.initialDelayMs` | `250` | First backoff delay |
| `ingest.retry.maxDelayMs` | `5000` | Backoff cap |
| `db.runtime.pool.maxSize` | `40` | Hikari pool max size |
| `db.runtime.pool.minIdle` | `10` | Hikari minIdle |
| `db.runtime.pool.connectionTimeoutMs` | `30000` | Hikari connection timeout |

The Hikari runtime pool also enables `connectionTestQuery=SELECT 1`, `validationTimeout=5s`, `leakDetectionThreshold=10min`, `keepaliveTime=2min`.

---

## REST API

| Method | Path | Params | Effect |
|---|---|---|---|
| POST | `/api/jobs/trigger` | `businessDate?`, `inputDir?` | Queue a job carrying the given (or default) business date + input dir. Returns `{jobId, status, businessDate, inputDir}`. |
| POST | `/api/jobs/stop` | `jobId?` | Stop one specific job (by id) or all RUNNING+QUEUED jobs (no id). STOPPED status is preserved by all later writers. |
| POST | `/api/jobs/restart` | `jobId?` | If `jobId` provided, requeue it; otherwise queue a new job. |
| GET | `/api/jobs/status` | `jobId` | Current status string. |
| POST | `/api/files/skip` | `fileName`, `reason` | Mark a file as `SKIPPED` so the scanner ignores it on future runs. |

> **Note:** these endpoints are not authenticated yet — security is a planned phase. Lock down before production.

---

## Quick start (Docker Compose)

```bash
# 1. wipe any existing state and bring up DB + app
docker compose down -v
docker compose up -d --build

# 2. tail logs
docker compose logs -f ingest

# 3. trigger ingestion (default business date + /data input)
curl -s -X POST http://localhost:8080/api/jobs/trigger

# 4. watch progress
curl -s 'http://localhost:8080/api/jobs/status?jobId=1'

# 5. inspect results
docker compose exec -T mariadb mariadb -uroot -prootpw lseg <<'SQL'
SELECT target_table, status, COUNT(*) files, SUM(declared_rows) declared,
       SUM(parsed_rows) parsed, SUM(inserted_rows) inserted, SUM(skipped_rows) skipped,
       SUM(ins_count) I, SUM(upd_count) U, SUM(del_count) D
FROM lseg_file_audit GROUP BY target_table, status;
SQL
```

Bench result on the production drop (268 files / 28 M declared rows, single instance, MariaDB 11 in Docker on macOS):

* Cold run: **179 s** wall time, 0 failures, 0 sanity skips.
* Warm rerun (idempotency): **0 s** — every file already SUCCESS in audit.

---

## Operations & observability

* **Audit trail:** every file processed has a row in `lseg_file_audit` with status (`STARTED`/`SUCCESS`/`FAILED`/`SKIPPED_SANITY`/`SKIPPED`), `parsed_rows`, `inserted_rows`, `skipped_rows`, per-action counters (`ins_count`/`upd_count`/`del_count`), `started_at`/`finished_at`, `error_message` (truncated to 4 KB).
* **Job trail:** `lseg_jobs` records who claimed (`node_id`), when (`started_at`/`finished_at`/`last_heartbeat_at`), and why if failed (`error_message`).
* **MDC log pattern:** `[job=<id> file=<name>]` is baked into every line emitted under the ingest hot path — grep one job in a multi-job day with `grep 'job=42' app.log`.
* **Micrometer metrics** (exposed via Spring Actuator):
  * `ingest.overall.duration` — full job wall time
  * `ingest.file.duration{target,kind,status}` — per-file timing
  * `ingest.rows.parsed{target}` / `ingest.rows.inserted{target}` / `ingest.rows.skipped.error{target}` / `ingest.rows.skipped.filter{target}`
  * `ingest.rows.ops{target,op=I|U|D}` — per-action counts
  * `ingest.sanity.failures{target}` — pre-ingest sanity rejections
  * `ingest.archive.errors` — archive move failures
  * `ingest.target.errors`, `ingest.orchestrator.errors` — fatal error counters

---

## Testing

| Layer | Location | Run |
|---|---|---|
| Java unit tests | `src/test/java/...` | `mvn test` (18 tests) |
| Python integration tests | `tests/integration/` | `pytest -v` (13 tests, ~3 min) |
| Multi-instance test plan | `CORNER_CASES.md §5` | manual (multi-container compose + monitor) |
| File resilience test plan | `CORNER_CASES.md §6` | scripted matrix (planned) |

Java unit suite covers: file scanner, pipe parser (header detection / extra+missing cols / over-long rows), SQL builder (composite-key composition + ON DUPLICATE clause excludes key cols), RIC caret filter, and the transient-error classifier (`SqlRetry`).

Python integration suite (against running compose stack) covers: happy path with synthetic files, INT→DELTA seq ordering, **D-then-I keeps row live** (C1 fix), **I-then-D leaves row deleted**, **RIC caret filter is INT-only**, sanity failure on wrong business date, corrupt zip is `SKIPPED_SANITY` without aborting the job, extra/missing columns tolerated, empty zip → `SKIPPED_SANITY`, idempotent rerun, **two queued jobs serialise via cluster lock** (`started_at >= prior finished_at`), **stop signal preserved** through completion, **stale RUNNING job auto-FAILED by reaper**.

---

## Design decisions and trade-offs (rationale)

1. **Cluster singleton over per-target locks.** Originally proposed per-target locks for DELTA seq ordering; the actual operational requirement was "one job at a time across the cluster." A single `GET_LOCK` is simpler, cheaper, and DB-released on connection drop.
2. **`KeyHolder` over `LAST_INSERT_ID()`.** With a connection pool, `LAST_INSERT_ID()` issued on a separate JdbcTemplate call can land on a different session and return 0 or another job's id. `KeyHolder` reads the generated key from the same statement.
3. **Composite UNIQUE keys over `*_perm_id` UNIQUE.** Source data has frequent NULL `*_perm_id` values; UNIQUE+NULL allows duplicate inserts for NULL keys, defeating idempotency. `(asset_id)` for assets, `(asset_id, quote_id)` for quotes, `(entity_id)` for orgs — these match the natural identity used downstream and are populated for every row.
4. **In-file action ordering.** The previous design batched all `I/U` then all `D`, which broke `D` then `I` for the same key in a single DELTA file. Now the executor flushes the active batch when the action class flips, preserving file order.
5. **Charset configurable.** UTF-8 default; LSEG drops occasionally use Windows-1252 / Latin-1. `ingest.charset` controls decoding.
6. **Heartbeat + reaper instead of "operator only."** Hospital deployment requires SLA-bounded automatic recovery from crashed nodes. 3 h default is configurable.
7. **STOPPED status is sticky.** If an operator stops a job, the worker's eventual COMPLETED/FAILED write would silently lose that fact in history. The conditional `UPDATE … CASE WHEN status='STOPPED' THEN 'STOPPED' ELSE ? END` keeps the operator action visible.
8. **Per-file retry on transient errors only.** Deadlocks (1213), lock-wait timeouts (1205), connection-class SQLStates (`08*`), and serialization failures (`40001`) are retried with exponential backoff capped at 5 s × 3 attempts. Non-transient errors (constraint violations, syntax) propagate immediately.

---

## Reference documents

* [`SOP.md`](./SOP.md) — production support runbook: monitoring queries, recovery procedures, API control.
* [`CORNER_CASES.md`](./CORNER_CASES.md) — verified resilience scenarios + planned test matrix (multi-instance + advanced file resilience).
* [`GEMINI.md`](./GEMINI.md) — agent-oriented project map (architecture, build/run commands, conventions).

---
*Mission-critical financial reference data ingestion. Built for multi-instance, replayable, observable operation.*
