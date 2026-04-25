# Resilience & Corner-Case Tracker

Every scenario below is either **verified** (covered by an automated test) or **planned** (documented test plan, not yet executed). Each entry names the spec, expected behaviour, and exact assertion.

---

## Verified — Java unit suite (`mvn test`, 18 tests)

| ID | Scenario | Expected | Verified by |
|---|---|---|---|
| U-1 | File classifier maps known patterns to (target, kind, seq) | Correct enums + seq parsed | `FileScannerTest` |
| U-2 | Skip patterns (`*.note.txt.zip`, `Reference-INT-EQUI-*`) drop matching files | Excluded from scan output | `FileScannerTest` |
| U-3 | Pipe parser locates header by content (`Action\|`) and skips unrelated leading lines | `headerColumns()` populated | `PipeFileParserTest` |
| U-4 | Pipe parser exposes file metadata (kind, business date, declared row count) | Parsed fields | `PipeFileParserTest` |
| U-5 | Pipe parser drops trailing empty token from `\|`-terminated rows | `null` for empty | `PipeFileParserTest` |
| U-6 | RIC caret filter strips rows containing `^` | Boolean correct | `RicCaretFilterTest` |
| U-7 | `SqlBuilder.upsert` builds `INSERT … ON DUPLICATE KEY UPDATE` and excludes composite-key columns from the duplicate clause | Key cols not present, `is_deleted=0` clause appended | `SqlBuilderTest` |
| U-8 | `SqlBuilder.delete` for quotes uses both `(asset_id, quote_id)` in WHERE | Composite WHERE | `SqlBuilderTest` |
| U-9 | `SqlRetry.isTransient` flags 1213 (deadlock), 1205 (lock-wait), `08*` (connection), `40001` (serialization) | True | `SqlRetryTest` |
| U-10 | `SqlRetry.isTransient` does not flag 1062 (unique violation) | False; propagates immediately | `SqlRetryTest` |
| U-11 | Transient error retried up to `maxAttempts` then succeeds | 3 attempts, returns ok | `SqlRetryTest` |

---

## Verified — Python integration suite (`pytest -v`, 13 tests, ~3 min)

Runs against the live `docker compose` stack with synthetic small fixtures and asserts against MariaDB plus REST status.

### 1. Happy path

| ID | Scenario | Expected | Verified by |
|---|---|---|---|
| H-1 | Single INT file with N rows ingested | `lseg_orgs` count == N, audit row SUCCESS | `test_int_only_one_file` |
| H-2 | INT then two DELTA files (seq 1, seq 2) updating the same key | Final state reflects seq 2 (last write per natural-key) | `test_delta_after_int_uses_seq_order` |
| H-3 | Re-run job after success | 0 new rows; identical audit count | `test_idempotent_rerun` |

### 2. File-level corner cases

| ID | Scenario | Expected | Verified by |
|---|---|---|---|
| F-1 | DELTA file containing **`D <key>` then `I <key>`** for the same key | Row is LIVE (`is_deleted=0`) with new values | `test_d_then_i_same_key_keeps_row_live` |
| F-2 | DELTA file containing **`I <key>` then `D <key>`** | Row is SOFT-DELETED (`is_deleted=1`) | `test_i_then_d_same_key_leaves_row_deleted` |
| F-3 | RIC caret filter — INT-quote `^` rows dropped, DELTA-quote `^` rows retained | Only configured INT rows excluded | `test_ric_caret_filter_int_only` |
| F-4 | Wrong `business_date` in metadata vs configured | `SKIPPED_SANITY` with reason | `test_sanity_fail_wrong_business_date` |
| F-5 | File with `.zip` extension but invalid binary content | Single file `SKIPPED_SANITY`; siblings still ingested | `test_corrupt_zip_does_not_kill_job` |
| F-6 | File header has extra columns (FOO) and missing column (Issuer_Name) | Extras ignored; missing column → NULL | `test_extra_and_missing_columns_tolerated` |
| F-7 | Empty (but valid) zip archive | `SKIPPED_SANITY` | `test_empty_zip_marked_sanity_skipped` |

### 3. Multi-instance & lifecycle

| ID | Scenario | Expected | Verified by |
|---|---|---|---|
| M-1 | Two jobs queued back-to-back | Cluster lock serialises them: `job2.started_at >= job1.finished_at` | `test_two_queued_jobs_run_sequentially` |
| M-2 | `/api/jobs/stop` mid-ingest of large file | Final status `STOPPED`, preserved through worker completion write | `test_stop_signal_aborts_running_job` |
| M-3 | RUNNING job with `last_heartbeat_at` 4 hours old | `JobReaper` flips it to `FAILED` with `error_message LIKE 'reaped%'` within 90 s | `test_reaper_marks_stale_running_jobs_failed` |

---

## Planned — Multi-instance soak test (manual)

> Goal: prove the cluster lock serialises three concurrent ingest containers fed from independent input directories. Do not rely solely on row counts — monitor logs, DB, and API responses continuously throughout.

### Setup

Three ingest containers (`ingest-a`, `ingest-b`, `ingest-c`) joined to one MariaDB. Each binds its own host directory:
* `host:./input-a` → `container:/data` for `ingest-a`
* `host:./input-b` → `container:/data` for `ingest-b`
* `host:./input-c` → `container:/data` for `ingest-c`

Each instance exposes a unique host port (`8081`, `8082`, `8083`). All three share the same MariaDB.

### Steps and live monitoring

| Step | Action | What to watch (concurrently) |
|---|---|---|
| 1 | `cp` 100 random files from `/Users/sa/Downloads/LSEG/20260425/` into `input-a/` | `docker compose logs -f ingest-a` shows `JobWorker started` |
| 2 | `curl -X POST http://localhost:8081/api/jobs/trigger` | `lseg_jobs` table shows job 1 transitioning QUEUED → RUNNING; `node_id` matches `ingest-a` |
| 3 | While job 1 is RUNNING (verify via `GET /api/jobs/status?jobId=1`), copy ALL files from `20260425/` into `input-b/` | Disk write completes; `input-b/` contains 615 files |
| 4 | `curl -X POST http://localhost:8082/api/jobs/trigger` | Job 2 row appears as QUEUED; `ingest-b`'s log shows repeated **`Cluster lock unavailable; another node is running. Leaving job 2 QUEUED.`** Status endpoint returns `QUEUED`. |
| 5 | While both 1 and 2 are pending, copy ALL files from `20260425/` into `input-c/` and trigger job 3 on port 8083 | `lseg_jobs` shows three rows: 1=RUNNING, 2=QUEUED, 3=QUEUED. `ingest-b` and `ingest-c` logs both retry every 10 s and bounce off the cluster lock. |
| 6 | When job 1 finishes (status=COMPLETED), one of `ingest-b` / `ingest-c` claims job 2 | Logs of the winner show `ClusterLock 'lseg-ingest-cluster' acquired`; loser keeps polling. |
| 7 | When job 2 finishes, the third node claims job 3 | Same pattern repeats. |
| 8 | Final reconciliation | `lseg_file_audit.file_name` is unique across all three input directories (filenames are unique because each directory contains the same 615 files; the audit table represents the union of files seen across input dirs — files already SUCCESS are skipped on subsequent runs). |

### Continuous assertions

These must hold **at every observation point**, not just at the end:

1. `SELECT COUNT(*) FROM lseg_jobs WHERE status='RUNNING'` is **always ≤ 1**.
2. The `node_id` of any RUNNING job matches the container whose log most recently emitted `ClusterLock 'lseg-ingest-cluster' acquired`.
3. No two ingest logs simultaneously contain `Ingestion started:` for the same `file_name`.
4. `lseg_file_audit` rows for files dropped into multiple input dirs show only ONE successful processing (whichever node ran first).
5. Sliding `last_heartbeat_at` of the RUNNING job is no more than `cluster.heartbeatIntervalSeconds × 2 = 60 s` behind `NOW()`.
6. After all three jobs reach a terminal state, `SELECT status, COUNT(*) FROM lseg_jobs GROUP BY status` shows `COMPLETED=3` (assuming no induced failures).

### Negative variant

Repeat the run but in step 5, `docker kill ingest-b` while it holds the cluster lock during job 2. Expect:
* Job 2 row stays `RUNNING` until reaper window expires (override `staleJobTimeoutSeconds=120` for the test).
* After ≤ 120 s, `JobReaper` flips it to `FAILED` with `error_message LIKE 'reaped%'`.
* Cluster lock is released by MariaDB on `ingest-b`'s connection drop.
* `ingest-c` claims job 3 and runs to completion.
* `POST /api/jobs/restart?jobId=2` requeues job 2; `ingest-a` or `ingest-c` resumes — every file in `input-b/` is either already SUCCESS (skipped) or processed afresh; final state is still consistent.

---

## Planned — Advanced file-resilience matrix

Each scenario below is a planned automated test under `tests/integration/test_file_resilience.py`. Verification requires both DB assertions AND log assertions (e.g., `docker compose logs ingest --tail=500 | grep 'specific message'`).

### A. Malformed content

| ID | Scenario | Expected behaviour | Assertion |
|---|---|---|---|
| A-1 | Header row missing entirely | `FileSanityCheck` fails with "Header row not found within first N lines" | audit `SKIPPED_SANITY`; reason contains `header` |
| A-2 | Metadata row missing (only header + data) | sanity fails: "metadata row missing" | audit `SKIPPED_SANITY`; rest of files succeed |
| A-3 | Header row appears at line 30 with comment lines before it | Parsed successfully (lookahead is 50 by default) | row count matches declared; SUCCESS |
| A-4 | Metadata kind=`INT` but filename says `REF` | sanity fails: "metadata kind=INT expected=REF" | `SKIPPED_SANITY` |
| A-5 | File header contains 5 extra columns not in `TargetSchema` | Extras ignored silently; valid columns map | SUCCESS; log shows `Mapped K/N columns` where K < total schema |
| A-6 | File header missing 3 columns including a non-key column | Missing → NULL on insert | SUCCESS; SQL `INSERT (…)` does not list the missing columns |
| A-7 | File header missing the unique-key column entirely | sanity fails | `SKIPPED_SANITY`; reason `missing key column(s)` |
| A-8 | Duplicate header column name appears twice | First occurrence kept; warning logged | log: `Duplicate header column(s) ignored` |
| A-9 | Data row has 10 tokens but header has 8 | Row truncated to 8; warning logged | log: `Row at line X has 10 tokens > 8 header columns` |
| A-10 | Data row has 4 tokens but header has 8 | Row padded with NULLs | row inserted with 4 trailing NULLs |
| A-11 | Embedded `\|` in a free-text column (eg `Issuer_Name=X\|Y Co`) | Parser splits incorrectly; subsequent column values shifted | row binds with shifted values; SQL may reject (length / type) → row-level fallback skips row + logs |
| A-12 | Action column missing | Defaults to `I` | rows treated as inserts |
| A-13 | Action column contains unknown letter (eg `X`) | Treated as `I` (default branch) | row inserted |
| A-14 | UTF-8 file with multibyte chars (e.g. Cyrillic issuer name) | Round-trips correctly with default `charset=UTF-8` | DB column matches input |
| A-15 | Windows-1252 file containing `é`, `ñ` | With `ingest.charset=windows-1252` set, names round-trip; otherwise mojibake | DB column matches expected |

### B. Data-value anomalies

| ID | Scenario | Expected | Assertion |
|---|---|---|---|
| B-1 | Column value 600 chars long for a `VARCHAR(255)` column | Batch fails → `ResilientBatchExecutor` row-by-row replay → bad row skipped + logged with line + key | other rows succeed; `skipped_rows >= 1`; log: `ROW FAILURE` |
| B-2 | Numeric column receives `"N/A"` / `"-"` | Either accepted (column is VARCHAR) OR same skip path as B-1 | per column type — verify via DB |
| B-3 | NULL key column on `I` row | Row inserts; UNIQUE on NULL is permissive — duplicates accumulate (per spec) | row inserted; subsequent identical NULL-keyed `I` produces second row, not upsert |
| B-4 | NULL key column on `D` row | DELETE WHERE … = NULL never matches → no-op | no row deleted; `del_count` counter still incremented |
| B-5 | Trailing whitespace / control chars in keys | Stored verbatim; whitespace-different keys treated as distinct | DB shows raw value |
| B-6 | Row count declared in metadata mismatches actual data row count | Audit logs declared vs parsed; SUCCESS if no DB errors | `declared_rows != parsed_rows`; ops can monitor drift |
| B-7 | Single file produces `> maxSkippedRowsPerFile` row failures | File aborts, marked `FAILED` with reason | audit `FAILED`, `error_message LIKE 'maxSkippedRowsPerFile exceeded%'` |

### C. Filesystem-state anomalies (the new requested cases)

These probe behaviour when the input directory is mutated during ingestion. The tests use carefully timed `mv`/`rm`/`fuser`/file locks driven by an external test harness (Python + `subprocess`).

| ID | Scenario | Expected | Assertion |
|---|---|---|---|
| C-1 | File deleted from input directory **after** scan but **before** sanity check | Sanity check fails with `NoSuchFileException`; audit `SKIPPED_SANITY` | `SKIPPED_SANITY`; reason contains `NoSuchFileException` |
| C-2 | File deleted **after** sanity but **before** ingest open | `FileIngestor` throws `NoSuchFileException`; file marked `FAILED`; job continues with siblings | per-file `FAILED`; job ends `COMPLETED` |
| C-3 | File deleted **mid-stream** while `ZipInputStream` is reading | `IOException: stream closed`; transaction rolled back; per-file FAILED | `FAILED`; no partial rows in target table; idempotent retry on next run is empty (file gone) |
| C-4 | File **moved** mid-ingest to a different directory | Same as C-3: open file handle survives the rename on Linux/macOS; ingest completes successfully | SUCCESS — the moved file does not affect an open InputStream on POSIX. Verify the row count is correct. |
| C-5 | File **renamed in place** mid-ingest (different basename) | Open handle survives; ingest completes against original content; audit row uses original `file_name`; new name is a separate file scanner sees on next run | SUCCESS; on next run, scanner sees the renamed file and ingests it again as a new file (idempotent due to composite UNIQUE) |
| C-6 | File replaced mid-ingest: original `rm`'d and a new file with same name written | On POSIX: ingest sees the original (open inode); next run sees the new content. On Windows: open file would block deletion. | original SUCCESS; new run brings new file in as a fresh ingest |
| C-7 | File held open by another process (read lock) | Spring app uses `Files.newInputStream` (non-locking on Linux/macOS). Should succeed. | SUCCESS |
| C-8 | File held with exclusive write lock by a writer still appending | Read may see partial / truncated zip → `ZipException` during sanity → `SKIPPED_SANITY` | `SKIPPED_SANITY`; reason contains `ZipException` |
| C-9 | Input directory permission flipped to `000` mid-job | `FileScanner` (already done) is unaffected; subsequent file opens fail → per-file `FAILED` for unprocessed files | the in-flight files complete; later files `FAILED` |
| C-10 | Disk fills up during ingestion (archive move to a separate disk) | DB ingest succeeds; archive `Files.move` throws `IOException`; counter `ingest.archive.errors` increments; file remains in input dir | DB SUCCESS; archive errors visible in metric |
| C-11 | Symlink points outside input dir at a real file | Scanner resolves and ingests | SUCCESS |
| C-12 | Symlink loop (`a -> b -> a`) | `Files.list` does not recurse; the symlink is treated as a regular path; `Files.newInputStream` may fail with `FileSystemException` | SKIPPED_SANITY or per-file FAILED; job continues |
| C-13 | File appears AFTER the orchestrator's initial scan but before the run completes | Not picked up this run; ingested on next trigger | first job's audit unchanged; next job picks up the new file |
| C-14 | Two input dirs supplied via two queued jobs (job 1: dir A, job 2: dir B), with the same file name in both | Cluster lock serialises; job 1 ingests the file from A; job 2 sees it as already-SUCCESS (skip by file_name) and does not ingest the B copy | DB has only one copy; audit shows one row |
| C-15 | Filename longer than 255 chars | INSERT into `lseg_file_audit.file_name VARCHAR(255)` truncates / fails | `FAILED`; error message visible |

### D. Database-side anomalies

| ID | Scenario | Expected | Assertion |
|---|---|---|---|
| D-1 | DB connection dropped mid-batch | `SqlRetry` catches transient class `08*`; backoff + retry up to `maxAttempts`; after exhaustion, file marked FAILED | per-file FAILED if DB never returns; SUCCESS if DB recovers within window |
| D-2 | Deadlock thrown on UPSERT batch | `SqlRetry` retries; eventually succeeds on different ordering | success after retries; logged warnings |
| D-3 | Lock-wait timeout (1205) | Retried | success after retries |
| D-4 | Unique-constraint violation (1062) | NOT retried; row-level fallback skips offending row; rest commit | SUCCESS for file; `skipped_rows >= 1` |
| D-5 | DB read-only / privileges revoked | Per-file FAILED; job ends FAILED | jobs row error message visible |
| D-6 | DB restart while job is running and heartbeat thread is updating `last_heartbeat_at` | Heartbeat write fails (warn-logged), main ingest path retries via `SqlRetry`; if retries exhausted, file FAILED; reaper eventually flips job to FAILED if heartbeat stale | reaper timeout governs recovery |

### E. Multi-instance + chaos combinations

| ID | Scenario | Expected | Assertion |
|---|---|---|---|
| E-1 | Three nodes; node 1 holds cluster lock; nodes 2 + 3 poll concurrently | At every poll, `lseg_jobs.status='RUNNING' COUNT == 1` | continuous SQL probe shows ≤ 1 |
| E-2 | Holding node SIGKILL'd mid-ingest | Job stays RUNNING until reaper window; cluster lock released by MariaDB on connection drop | next node acquires lock; reaper handles stale row |
| E-3 | Holding node loses network to DB but stays running | Hikari pool throws SQLException on every op; per-file retries fail; orchestrator throws; job marked FAILED on the same node when it finally writes | reaper provides safety net |
| E-4 | Job manually `STOPPED` while a slow row loop is in progress | Inside `FileIngestor.doIngest`, every `cancel.checkRows` rows polls `JobDao.isStopped(jobId)`; throws `InterruptedException`; transaction rolls back | `lseg_file_audit` shows partial state FAILED with error containing `Stop signaled mid-file`; job final = STOPPED |
| E-5 | Forced failover: stop node 1, start node 4 with same `cluster.lockName` | Node 4 acquires lock cleanly; jobs flow continues | continuous status probe passes |

---

## Severity & escalation

* **CRITICAL (page on-call):** any pattern in §3 (multi-instance + lifecycle) failing in production. Implies cluster lock or stop-signal logic broken — silent data corruption risk.
* **HIGH:** §B (data anomalies producing >1 % `skipped_rows`), §C-3/C-4 (mid-stream filesystem changes producing partial commits).
* **MEDIUM:** §A row-level anomalies, §D-1/D-4 expected database errors.
* **INFO:** §A header tolerance — visible in INFO logs but not actionable.

Every CRITICAL/HIGH item must produce a row in `lseg_file_audit` (or `lseg_jobs`) AND a Micrometer metric increment AND a structured log line carrying the MDC `[jobId=…, file=…]` tags.
