# GEMINI.md - LSEG Ingestion Project

## Project Overview
The `lseg-ingest` service is a high-throughput, idempotent, and resilient Spring Boot application designed to ingest LSEG EIS daily reference data files into a MariaDB database. It handles both full snapshots (**INT**) and daily incremental updates (**DELTA**).

### Core Architecture
- **Technology Stack:** Java 21, Spring Boot 3.3.5, Plain JDBC (for performance), MariaDB, Liquibase (schema management), Micrometer (metrics).
- **Execution Model:** Multi-instance capable service using a polled job queue.
  1. **Job Queue:** Jobs are enqueued in `lseg_jobs`.
  2. **Job Worker:** `JobWorker` polls for `QUEUED` tasks and claims them for execution.
  3. **Orchestration:** `IngestOrchestrator` handles file scanning, sanity checks, and parallel ingestion.
  4. **Parallelism:** Ingestion is parallelized by target table (`ORGS`, `ASSETS`, `QUOTES`), with **INT** files gated before **DELTA** files for each target.
- **Ingestion Strategy:** Uses `INSERT ... ON DUPLICATE KEY UPDATE` for idempotent upserts and soft deletes (`is_deleted = 1`).
- **Resilience:** `ResilientBatchExecutor` handles batch failures by falling back to row-by-row processing.

## API Endpoints

### Job Management (`/api/jobs`)
- `POST /api/jobs/trigger`: Enqueues a new ingestion job.
- `POST /api/jobs/stop`: Signals all running workers to stop immediately.
- `POST /api/jobs/restart?jobId=N`: Re-enqueues a job for reprocessing.

### File Management (`/api/files`)
- `POST /api/files/skip?fileName=X&reason=Y`: Manually marks a file as `SKIPPED`.

## Building and Running

### Commands
- **Build:** `mvn clean package -DskipTests`
- **Run (Local):**
  ```bash
  DB_HOST=127.0.0.1 DB_PORT=3306 DB_NAME=lseg \
  DB_OWNER_USER=owner DB_OWNER_PASSWORD=ownerpw \
  DB_USER=ingest    DB_PASSWORD=ingestpw \
  INGEST_DIR=/path/to/files INGEST_DATE=20260425 \
  java -jar target/lseg-ingest-1.0.0.jar
  ```
- **Trigger Ingestion:** `curl -X POST http://localhost:8080/api/jobs/trigger`
- **Test:** `mvn test`

## Development Conventions

### Coding Standards
- **Performance:** Use plain JDBC for the hot ingestion loop.
- **Idempotency:** All operations must support safe re-execution.
- **Defensive Parsing:** Bind columns by name to handle feed schema evolution.

### Testing
- **Unit Tests:** Verify business logic (classification, parsing, filtering).
- **Integration Tests:** (Optional but recommended) Verify against a real DB via Testcontainers.

### Monitoring
- **Audit:** All file actions are recorded in `lseg_file_audit`.
- **Metrics:** Published via Micrometer/Actuator.
