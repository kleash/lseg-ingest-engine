# Resilience & Correctness Fixes — v1.1.0

This document explains each fix applied in the v1.1.0 release cycle so the same patterns can be applied to similar ingest codebases.

---

## 1. Business Date Age Guard

**Problem**: Jobs with a stale `business_date` (e.g., submitted days late) would silently ingest old data, potentially overwriting current records with outdated values.

**Fix**: In `IngestOrchestrator.run()`, immediately after reading `businessDate` from the job row, compute the age in days and throw `IllegalStateException` if it exceeds the threshold.

```java
LocalDate bd = FileAuditDao.parseBusinessDate(businessDate);
long ageDays = ChronoUnit.DAYS.between(bd, LocalDate.now());
if (ageDays > props.getMaxBusinessDateAgeDays()) {
    throw new IllegalStateException(String.format(
        "Business date %s is %d days old (max allowed: %d). Failing fast.",
        businessDate, ageDays, props.getMaxBusinessDateAgeDays()));
}
```

**Config**:
```yaml
ingest:
  maxBusinessDateAgeDays: 30   # set to 0 to disable
```

**Replication tip**: Apply this pattern to any pipeline that processes date-stamped batches — fail early before expensive I/O.

---

## 2. Bounded Audit Lookback

**Problem**: `FileAuditDao.loadSuccessFileNames()` loaded ALL `SUCCESS` records from `lseg_file_audit` with no date filter. As months of history accumulated, this became a full table scan on every job run, increasing memory pressure and query latency.

**Fix**: Add a `lookbackDays` parameter and a `business_date` range filter:

```java
public Set<String> loadSuccessFileNames(int lookbackDays) {
    return new HashSet<>(jdbc.queryForList(
        "SELECT file_name FROM lseg_file_audit WHERE status = 'SUCCESS' " +
        "AND business_date >= CURDATE() - INTERVAL ? DAY",
        String.class, lookbackDays));
}
```

Called with `props.getAuditLookbackDays()` (default 60). Files older than the window are simply re-ingested — which is safe because ingestion is idempotent (upsert logic).

**Config**:
```yaml
ingest:
  auditLookbackDays: 60
```

**Replication tip**: Any idempotency check table needs a bounded lookback window. Index `business_date` if not already present.

---

## 3. Reconciliation DELETE Wrapped in SqlRetry

**Problem**: `FileIngestor` ran a raw `DELETE ... JOIN lseg_quotes` statement without retry logic. When multiple quote files were ingested in parallel (10-thread INT phase), MariaDB deadlocks on this statement caused the entire file's transaction to roll back and the file to be marked `FAILED`, even though all rows had been successfully staged.

**Fix**: Wrap the reconciliation statement in `SqlRetry.withRetry()`:

```java
private int reconcileNullAssets(Connection conn, IngestFile file) throws Exception {
    try (java.sql.Statement stmt = conn.createStatement()) {
        return SqlRetry.withRetry(props.getRetry(), "reconcile:" + file.fileName(), () ->
            stmt.executeUpdate(
                "DELETE q1 FROM lseg_quotes q1 " +
                "JOIN lseg_quotes q2 ON q1.quote_id = q2.quote_id " +
                "WHERE q1.asset_id IS NULL AND q2.asset_id IS NOT NULL"
            )
        );
    }
}
```

**Replication tip**: Any statement that runs inside a transaction shared with concurrent writes needs transient-error retry. `SqlRetry` handles MariaDB error codes 1205 (lock wait timeout) and 1213 (deadlock).

---

## 4. Final Flush Order Corrected

**Problem**: `FileIngestor.ingestWithParser()` used an inverted condition for the end-of-file batch flush:

```java
// WRONG — flushed upsert first even when last action was DELETE
if (activeSide == 'D') { upserter.flush(); deleter.flush(); }
else                    { deleter.flush(); upserter.flush(); }
```

This was safe only by coincidence: the non-active buffer is always empty (flushed on mid-loop flip), so flushing it first was a no-op. But the code was misleading and a trap for future edits.

**Fix**:
```java
// Correct — flush active side first
if (activeSide == 'D') { deleter.flush();  upserter.flush(); }
else                    { upserter.flush(); deleter.flush();  }
```

**Replication tip**: When maintaining two interleaved buffers with a "flip on side-change" policy, the final drain must flush the most-recently-active buffer first.

---

## 5. Dead `skipped` Variable Removed

**Problem**: `FileIngestor.ingestWithParser()` declared `int skipped = 0` and included it in audit calls, but never incremented it. Real skip counts were tracked by `upserter.skipped()` and `deleter.skipped()`. The catch-block audit call always reported 0 skips.

**Fix**: Remove the `skipped` variable. Use `upserter.skipped() + deleter.skipped()` in both success and failure audit paths.

**Replication tip**: When per-executor counters exist, don't duplicate tracking at the caller level — read from the source of truth.

---

## 6. `perTargetPool` Shutdown Moved to `finally`

**Problem**: In `IngestOrchestrator.run()`, the per-job `ExecutorService perTargetPool` was shut down in the normal code path but not in a `finally` block. If any exception was thrown after pool creation but before `shutdown()`, the daemon threads would linger until the next JVM shutdown — a thread leak in a long-running service.

**Fix**: Wrap the phase-1 wait loop in a `try/finally`:

```java
try {
    for (Future<?> f : phase1Futures) { waitWithHeartbeat(f, ...); }
} finally {
    perTargetPool.shutdown();
    if (!perTargetPool.awaitTermination(2, TimeUnit.MINUTES)) {
        perTargetPool.shutdownNow();
    }
}
```

**Replication tip**: Every `ExecutorService` created inside a method must be shut down in a `finally` block.

---

## 7. `restart()` API Fixed for STOPPED Jobs

**Problem**: `JobController.restart()` called `jobDao.updateStatus(jobId, STATUS_QUEUED, null)`, which internally protects against overwriting a `STOPPED` status. So restarting a STOPPED job silently did nothing, yet the API returned `{status: "QUEUED"}`.

**Fix**: Check the current status and use `forceRequeue()` (an unconditional UPDATE) for STOPPED jobs:

```java
@PostMapping("/restart")
public Map<String, Object> restart(@RequestParam Long jobId) {
    String current = jobDao.getStatus(jobId);
    if (STATUS_STOPPED.equals(current)) {
        jobDao.forceRequeue(jobId);        // bypasses STOPPED guard
    } else {
        jobDao.updateStatus(jobId, STATUS_QUEUED, null);
    }
    return Map.of("jobId", jobId, "status", jobDao.getStatus(jobId));  // actual DB status
}
```

`forceRequeue` also clears `finished_at` and `error_message`:
```java
public void forceRequeue(long jobId) {
    jdbc.update(
        "UPDATE lseg_jobs SET status='QUEUED', finished_at=NULL, error_message=NULL WHERE id=?",
        jobId);
}
```

**Replication tip**: Any "re-queue" operation on a terminal state must bypass the STOPPED guard — use a separate, unconditional method rather than reusing the guarded `updateStatus`.

---

## 8. `FileIngestor` God Method Decomposed

**Problem**: `ingestWithParser()` was ~200 lines and handled column mapping, the row loop, flush ordering, reconciliation, metrics, and audit — all inline. Hard to read, test, or extend.

**Fix**: Extracted into three private methods with clear contracts:

| Method | Responsibility | Returns |
|---|---|---|
| `buildColumnMapping(FileParser, IngestFile)` | Resolve file headers → DB column list, index arrays, filter flags | `ColumnMapping` record (or `null` if no overlap) |
| `processRows(FileParser, ColumnMapping, upserter, deleter, IngestFile, jobId)` | Row loop: filter, action dispatch, flush-on-flip, final flush | `RowStats` record |
| `reconcileNullAssets(Connection, IngestFile)` | Retry-wrapped NULL-asset dedup DELETE | `int` rows deleted |

`ingestWithParser()` is now a ~45-line coordinator.

**Replication tip**: Extract sub-responsibilities into named methods when a method exceeds ~60 lines. Use Java records for lightweight result containers instead of mutable local state.

---

## 9. SQL Status Literals

**Problem**: SQL strings in `JobDao` and `FileAuditDao` embedded status values via Java constant concatenation:
```java
"WHERE status = '" + STATUS_QUEUED + "'"
```
This is not a SQL injection risk (constants are compile-time), but is misleading — it looks like parameterization while actually building strings.

**Fix**: Replace with literal strings:
```java
"WHERE status = 'QUEUED'"
```
Add a class-level comment noting these match `Constants.java`.

**Replication tip**: Use `?` for values that vary at runtime. Use literals for fixed enum-like strings that are part of the schema contract.

---

## 10. `Resilience` Config Block Removed

**Problem**: `IngestProperties.Resilience` contained two properties that were never wired to any production behavior:
- `fallbackOnBatchFail` — `ResilientBatchExecutor` always falls back; this flag had no effect.
- `maxSkippedRowsPerFile` — no threshold check existed anywhere.

**Fix**: Removed the `Resilience` inner class and both properties from `IngestProperties.java` and `application.yml`.

**Replication tip**: Dead configuration is worse than no configuration — it implies behavior that doesn't exist. Remove it promptly.
