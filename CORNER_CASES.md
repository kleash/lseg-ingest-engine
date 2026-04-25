# LSEG Ingestion - Resilience & Corner Case Tracking

This document tracks the verified resilience scenarios and corner cases for the LSEG ingestion system. These cases have been empirically validated using automated end-to-end testing.

## 1. Data Integrity & Sanity Cases

### 1.1 Empty ZIP Files
- **Scenario**: An ingestion file is dropped into the input folder but contains 0 bytes or is an empty ZIP archive.
- **Expected Behavior**: The `FileSanityCheck` identifies the file as invalid.
- **Result**: `SKIPPED_SANITY` status recorded in `lseg_file_audit`. Processing continues for other files.

### 1.2 Missing Metadata/Header
- **Scenario**: A ZIP file contains data but lacks the required header/metadata file (e.g., missing `.note.txt` or header row).
- **Expected Behavior**: Sanity check fails due to missing schema requirements.
- **Result**: `SKIPPED_SANITY` status recorded.

### 1.3 Data Size Violation (Too Big)
- **Scenario**: A column value exceeds the `VARCHAR(255)` limit in the database.
- **Expected Behavior**: Batch execution fails; `ResilientBatchExecutor` falls back to row-by-row execution to isolate the bad row.
- **Result**: Valid rows are ingested; the oversized row is skipped and logged as `ROW FAILURE`. Job status remains `SUCCESS`.

### 1.4 Structural Anomalies (Malformed Rows)
- **Scenario**: Data rows have more or fewer pipe delimiters than the header row.
- **Expected Behavior**: `PipeFileParser` pads short rows with NULLs and returns long rows for index-based mapping.
- **Result**: Short rows are ingested with NULLs (unless NOT NULL constraints apply); long rows are ingested by truncating extra trailing columns.

### 1.5 Corrupt/Partial ZIP
- **Scenario**: A file has a `.zip` extension but is either empty, truncated, or contains non-ZIP binary data.
- **Expected Behavior**: `ZipLineReader` fails to open or iterate the ZIP entries.
- **Result**: `SKIPPED_SANITY` status recorded with a specific I/O or Zip error message.

### 1.6 File Locking / Permission Denial
- **Scenario**: A file is present in the directory but is locked by another process or has restrictive permissions (e.g., `000`).
- **Expected Behavior**: `FileSanityCheck` or `FileIngestor` fails to open the file stream.
- **Result**: `SKIPPED_SANITY` status recorded with `AccessDeniedException` or similar.

## 2. Distributed Coordination Cases

### 2.1 Concurrent API Triggers
- **Scenario**: Multiple instances receive `POST /api/jobs/trigger` simultaneously.
- **Expected Behavior**: MariaDB atomic updates (`UPDATE ... SET status='RUNNING' WHERE status='QUEUED'`) prevent multiple nodes from claiming the same job.
- **Result**: One node claims the job; subsequent triggers remain in `QUEUED` state in the `lseg_jobs` table.

### 2.2 Job Queuing
- **Scenario**: A second job is triggered while a first job is still running.
- **Expected Behavior**: The second job stays `QUEUED`.
- **Result**: The `JobWorker` on an idle node (or the same node after finishing) picks up the next `QUEUED` job sequentially.

## 3. Infrastructure & Runtime Failures

### 3.1 Database Outage (Mid-Ingestion)
- **Scenario**: The MariaDB container or service is hard-killed while a node is in the middle of a batch upsert.
- **Expected Behavior**: HikariCP connection pool throws exceptions; the job remains in a non-completed state or marks itself as `FAILED`.
- **Result**: Upon DB restoration, the job is moved back to `QUEUED` via `/api/jobs/restart` and resumes. Due to `ON DUPLICATE KEY UPDATE` and file auditing, the process is fully idempotent.

### 3.2 Archive Directory Unavailability
- **Scenario**: The `archive/` directory becomes read-only or the disk is full during the move operation.
- **Expected Behavior**: Ingestion to the database completes successfully (primary goal), but a high-priority error is logged for the move failure.
- **Result**: Data integrity in DB is maintained; files remain in `input/` for manual cleanup or retry.

## 4. Operator Intervention

### 4.1 Force Stop (Graceful Abort)
- **Scenario**: An operator sends `POST /api/jobs/stop` to halt a long-running ingestion.
- **Expected Behavior**: Orchestrator checks the `lseg_jobs` status at "checkpoints" (between files/phases) and throws a `RuntimeException` to abort.
- **Result**: Job status becomes `STOPPED`. All unstarted files remain in `input/`.

### 4.2 Manual File Skip
- **Scenario**: A specific file is identified as "poisoned" or invalid and should be bypassed.
- **Expected Behavior**: Operator calls `/api/files/skip?fileName=...`.
- **Result**: The file is pre-emptively marked as `SKIPPED` in `lseg_file_audit`, and the scanner ignores it in future runs.
