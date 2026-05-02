# GEMINI.md - LSEG Ingestion Project

Agent-oriented project map. Use this as the entry point for an LLM coding agent picking up work on this repo.

## Project Overview

`lseg-ingest` is a mission-critical Spring Boot 3.3 application that ingests LSEG EIS daily reference data (`.txt.zip` pipe-delimited files) into MariaDB. It is **multi-instance capable**: identical containers can be deployed in parallel, but a database-backed cluster lock guarantees only one is processing at any given time. Hospital-grade reliability is the target.

### Architecture summary
- **Stack:** Java 21, Spring Boot 3.3.5 (web + actuator + scheduling), Plain JDBC (hot path), HikariCP, MariaDB 11, Liquibase (separate `owner` DB account), Micrometer.
- **Coordination:** `lseg_jobs` queue + `JobWorker` poll + atomic UPDATE claim + MariaDB `GET_LOCK` cluster singleton + heartbeat + `JobReaper` for stuck-RUNNING recovery.
- **Idempotency:** composite UNIQUE on natural keys + `INSERT … ON DUPLICATE KEY UPDATE` + `lseg_file_audit` (skip on filename SUCCESS).
- **Priority Deduplication:**
    - `lseg_pricing`: Enforces "Latest Price Only" via `UNIQUE(quote_id)`.
    - `lseg_quotes`: Hybrid approach using a virtual column `asset_id_v` to prevent multiple NULL asset records while allowing multi-asset mappings.
    - **Reconciliation**: `FileIngestor` performs post-ingestion cleanup where NULL asset records are deleted if an anchored (non-NULL asset_id) record exists for the same quote.
- **Soft delete:** `is_deleted` column reset to 0 on every successful upsert.
- **Resilience:** `ResilientBatchExecutor` is generic over a `RowBinder` (used for both UPSERT and DELETE batches); `SqlRetry` with exponential backoff at both batch and per-row level on transient errors (deadlock 1213, lock-wait 1205, connection class `08*`, serialization `40001`); permanent-batch failures fall back to row-by-row replay with per-row exception isolation.
- **In-file ordering:** when the action stream switches between `I/U` and `D`, the active batch flushes before the other side accumulates. Guarantees `D <key>` then `I <key>` leaves row LIVE; `I <key>` then `D <key>` leaves row SOFT-DELETED.
- **Cancellation:** every `ingest.cancel.checkRows` rows the inner loop polls `JobDao.isStopped(jobId)`; on stop, the current transaction rolls back. STOPPED status is sticky (worker writes never overwrite it).
- **Configurability:** every tunable (threads, batch sizes, retry, reaper, cluster lock, charset, archive dir) is bound to `IngestProperties` from `application.yml`.
- **Observability:** MDC `[job=<id> file=<name>]` log pattern; Micrometer counters/timers per target/kind/op; REST status endpoints.

### File classification
- `Organization.*`, `*GLOBAL_ORGN*`, `*GLOABL_ORGN*` (vendor typo tolerated) → `lseg_orgs`
- `EIS_INT_*_ASSETS.*`, `EIS_DELTA_GLOBAL_ASSETS.*` → `lseg_assets`
- `EIS_INT_*_QUOTE/QUOTES.*`, `EIS_DELTA_*_QUOTE.*` → `lseg_quotes`
- Skip: `*.note.txt.zip`, `Reference-INT-EQUI-*`
- Row filter: INT-quote rows with `RIC` containing `^` (DELTA quote rows are kept)

### Schema
- Composite UNIQUE keys: `lseg_orgs(entity_id)`, `lseg_assets(asset_id)`, `lseg_quotes(quote_id, asset_id_v)`. 
- `lseg_quotes.asset_id_v`: A virtual column `IFNULL(asset_id, '')` used to ensure only one NULL entry per `quote_id` can exist.
- `lseg_pricing.quote_id`: Single unique key to enforce "Latest Price Only" logic.
- `lseg_jobs`: `(business_date, input_dir, last_heartbeat_at)` carried per-job; reaper sweeps stale RUNNING after 3 h (configurable).
- `lseg_file_audit`: UNIQUE on `file_name`; `STARTED → SUCCESS|FAILED|SKIPPED_SANITY|SKIPPED`.

## API Endpoints

| Method | Path | Params | Effect |
|---|---|---|---|
| POST | `/api/jobs/trigger` | `businessDate?`, `inputDir?` | Queue a job (per-job business date + input dir override) |
| POST | `/api/jobs/stop` | `jobId?` | Stop one (id) or all running/queued jobs |
| POST | `/api/jobs/restart` | `jobId?` | Re-queue an existing or new job |
| GET | `/api/jobs/status` | `jobId` | Read job status |
| POST | `/api/files/skip` | `fileName`, `reason` | Manual file skip |
| GET | `/actuator/health` | — | Spring health |

> Endpoints are not yet authenticated. Security is a planned phase.

## Building and Running

```bash
# Build
mvn clean package -DskipTests
# (or with tests)
mvn test            # 21 unit tests

# Single-node compose (developer mode)
docker compose down -v
docker compose up -d --build
curl -X POST 'http://localhost:8080/api/jobs/trigger?businessDate=20260425&inputDir=/data'

# Two-instance production cluster (separate date dirs, explicit inputDir per job)
docker compose -f docker-compose.production.yml down -v
docker compose -f docker-compose.production.yml up -d --build
curl -X POST 'http://localhost:8081/api/jobs/trigger?businessDate=20260425&inputDir=/data/20260425'
curl -X POST 'http://localhost:8082/api/jobs/trigger?businessDate=20260426&inputDir=/data/20260426'
# Cluster lock serialises the two jobs; Job 2 waits until Job 1 completes

# Multi-instance test stack (3 ingest containers, health-gated startup)
docker compose -f docker-compose.test.yml up -d --build
```

## Development Conventions

### Coding standards
- Hot ingestion path stays on plain JDBC. Spring is for wiring, configuration, REST, scheduling, MDC.
- Idempotency is non-negotiable. Every operation (file, row, job) must be safely re-executable.
- Defensive parsing: bind columns by header name, not position; tolerate extra/missing columns; warn on duplicates.
- Make every tunable a `@ConfigurationProperties` field — never hard-code thread pools, batch sizes, or timeouts.
- New SQL on the hot path must go through `ResilientBatchExecutor` (or an equivalent retry+fallback wrapper).
- Adding new logging → add MDC tags (`jobId`, `file`).

### Critical bugs fixed (do not regress)
1. `LAST_INSERT_ID()` across pooled connections is unsafe — use `KeyHolder`.
2. In-file `D <key>` then `I <key>` must leave the row live (flush UPSERT batch when a `D` arrives).
3. STOPPED status must not be overwritten by completion writes.
4. A node that claims a job but loses the cluster lock must revert the row to QUEUED (not leave it RUNNING forever).
5. The cluster `GET_LOCK` connection must be held for the duration of the run on a dedicated connection — released automatically on connection drop.
6. `lseg_jobs.RUNNING` must be reaped if heartbeat is stale (default 3 h) — manual recovery alone is insufficient for SLA.

### Testing
- **Unit (`mvn test`)**: classifier, parser, SQL builder, RIC filter, retry classifier, batch executor (Mockito).
- **Integration (`pytest tests/integration/`)**: synthetic-fixture happy path, INT/DELTA seq ordering, in-file action-ordering edges (D-then-I, I-then-D), RIC INT-only filter, sanity rejections (corrupt zip, wrong date, empty zip), idempotent rerun, cluster-lock serialisation, stop-signal stickiness, reaper behaviour.
- **Manual / planned matrix**: `CORNER_CASES.md` — 3-node soak test with continuous DB/log/API monitoring, plus a 30+-case file-resilience matrix (malformed content, mid-ingest rm/mv/lock, DB chaos, etc.).

### Monitoring
- `lseg_file_audit` — per-file outcome.
- `lseg_jobs` — per-run state, heartbeat, node ownership, error message.
- Micrometer metrics: `ingest.overall.duration`, `ingest.file.duration{target,kind,status}`, `ingest.rows.{parsed,inserted,skipped.error,skipped.filter,ops}`, `ingest.sanity.failures{target}`, `ingest.archive.errors`, `ingest.target.errors`, `ingest.orchestrator.errors`.

### Cluster invariants (alarm if violated)
- `SELECT COUNT(*) FROM lseg_jobs WHERE status='RUNNING' <= 1` — always.
- `last_heartbeat_at >= NOW() - INTERVAL 60 SECOND` for any RUNNING job (default 30 s heartbeat × 2).
- `node_id` of the RUNNING job matches the container that most recently logged `ClusterLock 'lseg-ingest-cluster' acquired`.

See `README.md`, `SOP.md`, `CORNER_CASES.md`, and `INGESTION_RUN_REPORT.md` for full detail.
